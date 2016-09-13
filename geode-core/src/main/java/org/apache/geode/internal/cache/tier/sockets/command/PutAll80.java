/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Author: Gester Zhou
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.ResourceException;
import com.gemstone.gemfire.cache.client.internal.PutAllOp;
import com.gemstone.gemfire.cache.operations.PutAllOperationContext;
import com.gemstone.gemfire.cache.operations.internal.UpdateOnlyMap;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PutAllPartialResultException;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.ha.ThreadIdentifier;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.util.Breadcrumbs;

public class PutAll80 extends BaseCommand {
  
  private final static PutAll80 singleton = new PutAll80();

  public static Command getCommand() {
    return singleton;
  }
  
  protected PutAll80() {
  }
  
  protected String putAllClassName() {
    return "putAll80";
  }
  protected Object getOptionalCallbackArg(Message msg) throws ClassNotFoundException, IOException {
    return null;
  }
  protected int getBasePartCount() {
    return 5;
  }
  
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long startp)
  throws IOException, InterruptedException {
    long start = startp;  // copy this since we need to modify it
    Part regionNamePart = null, numberOfKeysPart = null, keyPart = null, valuePart = null;
    String regionName = null;
    int numberOfKeys = 0;
    Object key = null;
    Part eventPart = null;
    boolean replyWithMetaData = false;
    VersionedObjectList response = null;
    
    StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    
    // requiresResponse = true;
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE); // new in 8.0
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadPutAllRequestTime(start - oldStart);
    }
    
    try {
      // Retrieve the data from the message parts
      // part 0: region name
      regionNamePart = msg.getPart(0);
      regionName = regionNamePart.getString();
      
      if (regionName == null) {
        String putAllMsg = LocalizedStrings.PutAll_THE_INPUT_REGION_NAME_FOR_THE_PUTALL_REQUEST_IS_NULL.toLocalizedString();
        logger.warn("{}: {}", servConn.getName(), putAllMsg);
        errMessage.append(putAllMsg);
        writeChunkedErrorResponse(msg, MessageType.PUT_DATA_ERROR,
            errMessage.toString(), servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }

      LocalRegion region = (LocalRegion)crHelper.getRegion(regionName);
      if (region == null) {
        String reason = " was not found during putAll request";
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }

      final int BASE_PART_COUNT = getBasePartCount();
      
      // part 1: eventID
      eventPart = msg.getPart(1);
      ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart
          .getSerializedForm());
      long threadId = EventID
      .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      long sequenceId = EventID
      .readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      EventID eventId = new EventID(servConn.getEventMemberIDByteArray(),
          threadId, sequenceId);
      
      Breadcrumbs.setEventId(eventId);
      
      // part 2: invoke callbacks (used by import)
      Part callbacksPart = msg.getPart(2);
      boolean skipCallbacks = callbacksPart.getInt() == 1 ? true : false;
      
      // part 3: flags
      int flags = msg.getPart(3).getInt();
      boolean clientIsEmpty = (flags & PutAllOp.FLAG_EMPTY) != 0;
      boolean clientHasCCEnabled = (flags & PutAllOp.FLAG_CONCURRENCY_CHECKS) != 0;
      
      // part 4: number of keys
      numberOfKeysPart = msg.getPart(4);
      numberOfKeys = numberOfKeysPart.getInt();
      
      Object callbackArg = getOptionalCallbackArg(msg);
      
      if (logger.isDebugEnabled()) {
        StringBuilder buffer = new StringBuilder();
        buffer
                .append(servConn.getName())
                .append(": Received ")
                .append(this.putAllClassName())
                .append(" request from ")
                .append(servConn.getSocketString())
                .append(" for region ")
                .append(regionName)
                .append(callbackArg != null ? (" callbackArg " + callbackArg) : "")
                .append(" with ")
                .append(numberOfKeys)
                .append(" entries.");
        logger.debug(buffer.toString());
      }
      // building the map
      Map map = new LinkedHashMap();
      Map<Object, VersionTag> retryVersions = new LinkedHashMap<Object, VersionTag>();
//    Map isObjectMap = new LinkedHashMap();
      for (int i=0; i<numberOfKeys; i++) {
        keyPart = msg.getPart(BASE_PART_COUNT+i*2);
        key = keyPart.getStringOrObject();
        if (key == null) {
          String putAllMsg = LocalizedStrings.PutAll_ONE_OF_THE_INPUT_KEYS_FOR_THE_PUTALL_REQUEST_IS_NULL.toLocalizedString();
          logger.warn("{}: {}", servConn.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeChunkedErrorResponse(msg, MessageType.PUT_DATA_ERROR,
              errMessage.toString(), servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        
        valuePart = msg.getPart(BASE_PART_COUNT+i*2+1);
        if (valuePart.isNull()) {
          String putAllMsg = LocalizedStrings.PutAll_ONE_OF_THE_INPUT_VALUES_FOR_THE_PUTALL_REQUEST_IS_NULL.toLocalizedString();
          logger.warn("{}: {}", servConn.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeChunkedErrorResponse(msg, MessageType.PUT_DATA_ERROR, errMessage
              .toString(), servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        
//      byte[] value = valuePart.getSerializedForm();
        Object value;
        if (valuePart.isObject()) {
          // We're shoe-horning support for invalidated entries into putAll
          // here...however Token.INVALID cannot be wrapped in a DataSerializable.
          // Also, this code is using skipCallbacks as an import flag. If we make 
          // skipCallbacks configurable this code will need to be updated.
          if (skipCallbacks && Token.INVALID.isSerializedValue(valuePart.getSerializedForm())) {
            value = Token.INVALID;
          } else {
            value = CachedDeserializableFactory.create(valuePart.getSerializedForm());
          }
        } else {
          value = valuePart.getSerializedForm();
        }
//      put serializedform for auth. It will be modified with auth callback
        if (msg.isRetry()) {
          //Constuct the thread id/sequence id information for this element in the
          //put all map
          
          //The sequence id is constructed from the base sequence id and the offset
          EventID entryEventId= new EventID(eventId, i);
          
          //For PRs, the thread id assigned as a fake thread id.
          if(region instanceof PartitionedRegion) {
            PartitionedRegion pr = (PartitionedRegion) region;
            int bucketId = pr.getKeyInfo(key).getBucketId();
            long entryThreadId = ThreadIdentifier.createFakeThreadIDForBulkOp(bucketId, entryEventId.getThreadID());
            entryEventId = new EventID(entryEventId.getMembershipID(), entryThreadId, entryEventId.getSequenceID());
          }
            
          VersionTag tag = findVersionTagsForRetriedBulkOp(region, entryEventId);
          if(tag != null) {
            retryVersions.put(key, tag);
          }
          //FIND THE VERSION TAG FOR THIS KEY - but how?  all we have is the
          //    putAll eventId, not individual eventIds for entries, right?
        }
        map.put(key, value);
//      isObjectMap.put(key, new Boolean(isObject));
      } // for
      
      if ( msg.getNumberOfParts() == ( BASE_PART_COUNT + 2*numberOfKeys + 1) ) {//it means optional timeout has been added
        int timeout = msg.getPart(BASE_PART_COUNT + 2*numberOfKeys).getInt();
        servConn.setRequestSpecificTimeout(timeout);
      }

      this.securityService.authorizeRegionWrite(regionName);

      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize(regionName);
        }
        else {
          PutAllOperationContext putAllContext = authzRequest.putAllAuthorize(
              regionName, map, callbackArg);
          map = putAllContext.getMap();
          if (map instanceof UpdateOnlyMap) {
            map = ((UpdateOnlyMap) map).getInternalMap();
          }
          callbackArg = putAllContext.getCallbackArg();
        }
      } else {
        // no auth, so update the map based on isObjectMap here
        /*
         Collection entries = map.entrySet();
         Iterator iterator = entries.iterator();
         Map.Entry mapEntry = null;
         while (iterator.hasNext()) {
         mapEntry = (Map.Entry)iterator.next();
         Object currkey = mapEntry.getKey();
         byte[] serializedValue = (byte[])mapEntry.getValue();
         boolean isObject = ((Boolean)isObjectMap.get(currkey)).booleanValue();
         if (isObject) {
         map.put(currkey, CachedDeserializableFactory.create(serializedValue));
         }
         }
         */
      }
      
      response = region.basicBridgePutAll(map, retryVersions, servConn.getProxyID(), eventId, skipCallbacks, callbackArg);
      if (!region.getConcurrencyChecksEnabled() || clientIsEmpty || !clientHasCCEnabled) {
        // the client only needs this if versioning is being used and the client
        // has storage
        if (logger.isTraceEnabled()) {
          logger.trace("setting response to null. region-cc-enabled={}; clientIsEmpty={}; client-cc-enabled={}", region.getConcurrencyChecksEnabled(), clientIsEmpty, clientHasCCEnabled);
        }
        response = null;
      }
      
      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion)region;
        if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
          writeReplyWithRefreshMetadata(msg, response, servConn, pr, pr.getNetworkHopType());
          pr.clearNetworkHopData();
          replyWithMetaData = true;
        }
      }
    } 
    catch (RegionDestroyedException rde) {
      writeChunkedException(msg, rde, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    catch (ResourceException re) {
      writeChunkedException(msg, re, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    catch (PutAllPartialResultException pre) {
      writeChunkedException(msg, pre, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    catch (Exception ce) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, ce);
      
      // If an exception occurs during the put, preserve the connection
      writeChunkedException(msg, ce, false, servConn);
      servConn.setAsTrue(RESPONDED);
      logger.warn(LocalizedMessage.create(LocalizedStrings.Generic_0_UNEXPECTED_EXCEPTION, servConn.getName()), ce);
      return;
    }
    finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutAllTime(start - oldStart);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} response back to {} for regin {} {}", servConn.getName(), putAllClassName(),
          servConn.getSocketString(), regionName, 
          (logger.isTraceEnabled()? ": " + response : ""));
    }
    
    // Increment statistics and write the reply
    if (!replyWithMetaData) {
      writeReply(msg, response, servConn);
    }
    servConn.setAsTrue(RESPONDED);
    stats.incWritePutAllResponseTime(DistributionStats.getStatTime() - start);
  }
  
  @Override
  protected void writeReply(Message origMsg, ServerConnection servConn)
  throws IOException {
    throw new UnsupportedOperationException();
  }
  
  
  protected void writeReply(Message origMsg, VersionedObjectList response,
      ServerConnection servConn) throws IOException {
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    ChunkedMessage replyMsg = servConn.getChunkedResponseMessage();
    replyMsg.setMessageType(MessageType.RESPONSE);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    int listSize = (response == null) ? 0 : response.size();
    if (response != null) {
      response.setKeys(null);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("sending chunked response header.  version list size={}{}", listSize, (logger.isTraceEnabled()? " list=" + response : ""));
    }
    replyMsg.sendHeader();
    if (listSize > 0) {
      int chunkSize = 2*maximumChunkSize;
      // Chunker will stream over the list in its toData method
      VersionedObjectList.Chunker chunk = new VersionedObjectList.Chunker(response, chunkSize, false, false);
      for (int i=0; i<listSize; i+=chunkSize) {
        boolean lastChunk = (i+chunkSize >= listSize);
        replyMsg.setNumberOfParts(1);
        replyMsg.setMessageType(MessageType.RESPONSE);
        replyMsg.setLastChunk(lastChunk);
        replyMsg.setTransactionId(origMsg.getTransactionId());
        replyMsg.addObjPart(chunk);
        if (logger.isDebugEnabled()) {
          logger.debug("sending chunk at index {} last chunk={} numParts={}", i, lastChunk, replyMsg.getNumberOfParts());
        }
        replyMsg.sendChunk(servConn);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("sending only header");
      }
      replyMsg.addObjPart(null);
      replyMsg.setLastChunk(true);
      replyMsg.sendChunk(servConn);
    }
    servConn.setAsTrue(RESPONDED);
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }

  @Override
  protected void writeReplyWithRefreshMetadata(Message origMsg,
      ServerConnection servConn, PartitionedRegion pr, byte nwHop) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  private void writeReplyWithRefreshMetadata(Message origMsg,
      VersionedObjectList response, ServerConnection servConn,
      PartitionedRegion pr, byte nwHop) throws IOException {
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    ChunkedMessage replyMsg = servConn.getChunkedResponseMessage();
    replyMsg.setMessageType(MessageType.RESPONSE);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.sendHeader();
    int listSize = (response == null) ? 0 : response.size();
    if (logger.isDebugEnabled()) {
      logger.debug("sending chunked response header with metadata refresh status. Version list size = {}{}", listSize, (logger.isTraceEnabled()? "; list=" + response : ""));
    }
    if (response != null) {
      response.setKeys(null);
    }
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[]{pr.getMetadataVersion(), nwHop});
    if (listSize > 0) {
      replyMsg.setLastChunk(false);
      replyMsg.sendChunk(servConn);
      
      int chunkSize = 2*maximumChunkSize; // maximumChunkSize
      // Chunker will stream over the list in its toData method
      VersionedObjectList.Chunker chunk = new VersionedObjectList.Chunker(response, chunkSize, false, false);
      for (int i=0; i<listSize; i+=chunkSize) {
        boolean lastChunk = (i+chunkSize >= listSize);
        replyMsg.setNumberOfParts(1); // resets the message
        replyMsg.setMessageType(MessageType.RESPONSE);
        replyMsg.setLastChunk(lastChunk);
        replyMsg.setTransactionId(origMsg.getTransactionId());
        replyMsg.addObjPart(chunk);
        if (logger.isDebugEnabled()) {
          logger.debug("sending chunk at index {} last chunk={} numParts={}", i, lastChunk, replyMsg.getNumberOfParts());
        }
        replyMsg.sendChunk(servConn);
      }
    } else {
      replyMsg.setLastChunk(true);
      if (logger.isDebugEnabled()) {
        logger.debug("sending first and only part of chunked message");
      }
      replyMsg.sendChunk(servConn);
    }
    pr.getPrStats().incPRMetaDataSentCount();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADAT tx: {}", servConn.getName(), origMsg.getTransactionId());
    }
  }

}
