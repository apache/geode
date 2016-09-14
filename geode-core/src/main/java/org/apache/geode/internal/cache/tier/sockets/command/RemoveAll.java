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
 * Author: dschneider
 * @since GemFire 8.1
 */
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.ResourceException;
import org.apache.geode.cache.client.internal.PutAllOp;
import org.apache.geode.cache.operations.RemoveAllOperationContext;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.util.Breadcrumbs;

public class RemoveAll extends BaseCommand {
  
  private final static RemoveAll singleton = new RemoveAll();
  
  public static Command getCommand() {
    return singleton;
  }
  
  protected RemoveAll() {
  }
  
  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long startp)
  throws IOException, InterruptedException {
    long start = startp;  // copy this since we need to modify it
    Part regionNamePart = null, numberOfKeysPart = null, keyPart = null;
    String regionName = null;
    int numberOfKeys = 0;
    Object key = null;
    Part eventPart = null;
    boolean replyWithMetaData = false;
    VersionedObjectList response = null;
    
    StringBuffer errMessage = new StringBuffer();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadRemoveAllRequestTime(start - oldStart);
    }
    
    try {
      // Retrieve the data from the message parts
      // part 0: region name
      regionNamePart = msg.getPart(0);
      regionName = regionNamePart.getString();
      
      if (regionName == null) {
        String txt = LocalizedStrings.RemoveAll_THE_INPUT_REGION_NAME_FOR_THE_REMOVEALL_REQUEST_IS_NULL.toLocalizedString();
        logger.warn(LocalizedMessage.create(LocalizedStrings.TWO_ARG_COLON, new Object[] {servConn.getName(), txt}));
        errMessage.append(txt);
        writeChunkedErrorResponse(msg, MessageType.PUT_DATA_ERROR,
            errMessage.toString(), servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
      LocalRegion region = (LocalRegion)servConn.getCache().getRegion(regionName);
      if (region == null) {
        String reason = " was not found during removeAll request";
        writeRegionDestroyedEx(msg, regionName, reason, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }

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
      
      // part 2: flags
      int flags = msg.getPart(2).getInt();
      boolean clientIsEmpty = (flags & PutAllOp.FLAG_EMPTY) != 0;
      boolean clientHasCCEnabled = (flags & PutAllOp.FLAG_CONCURRENCY_CHECKS) != 0;
      
      // part 3: callbackArg
      Object callbackArg =  msg.getPart(3).getObject();
      
      // part 4: number of keys
      numberOfKeysPart = msg.getPart(4);
      numberOfKeys = numberOfKeysPart.getInt();
      
      if (logger.isDebugEnabled()) {
        StringBuilder buffer = new StringBuilder();
        buffer
                .append(servConn.getName())
                .append(": Received removeAll request from ")
                .append(servConn.getSocketString())
                .append(" for region ")
                .append(regionName)
                .append(callbackArg != null ? (" callbackArg " + callbackArg) : "")
                .append(" with ")
                .append(numberOfKeys)
                .append(" keys.");
        logger.debug(buffer);
      }
      ArrayList<Object> keys = new ArrayList<Object>(numberOfKeys);
      ArrayList<VersionTag> retryVersions = new ArrayList<VersionTag>(numberOfKeys);
      for (int i=0; i<numberOfKeys; i++) {
        keyPart = msg.getPart(5+i);
        key = keyPart.getStringOrObject();
        if (key == null) {
          String txt = LocalizedStrings.RemoveAll_ONE_OF_THE_INPUT_KEYS_FOR_THE_REMOVEALL_REQUEST_IS_NULL.toLocalizedString();
          logger.warn(LocalizedMessage.create(LocalizedStrings.TWO_ARG_COLON, new Object[] {servConn.getName(), txt}));
          errMessage.append(txt);
          writeChunkedErrorResponse(msg, MessageType.PUT_DATA_ERROR,
              errMessage.toString(), servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
        if (msg.isRetry()) {
          //Constuct the thread id/sequence id information for this element of the bulk op
          
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
          retryVersions.add(tag);
          //FIND THE VERSION TAG FOR THIS KEY - but how?  all we have is the
          //    removeAll eventId, not individual eventIds for entries, right?
        } else {
          retryVersions.add(null);
        }
        keys.add(key);
      } // for
      
      if ( msg.getNumberOfParts() == ( 5 + numberOfKeys + 1) ) {//it means optional timeout has been added
        int timeout = msg.getPart(5 + numberOfKeys).getInt();
        servConn.setRequestSpecificTimeout(timeout);
      }

      this.securityService.authorizeRegionWrite(regionName);

      AuthorizeRequest authzRequest = servConn.getAuthzRequest();
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize(regionName);
        }
        else {
          RemoveAllOperationContext removeAllContext = authzRequest.removeAllAuthorize(regionName, keys, callbackArg);
          callbackArg = removeAllContext.getCallbackArg();
        }
      }
      
      response = region.basicBridgeRemoveAll(keys, retryVersions, servConn.getProxyID(), eventId, callbackArg);
      if (!region.getConcurrencyChecksEnabled() || clientIsEmpty || !clientHasCCEnabled) {
        // the client only needs this if versioning is being used and the client
        // has storage
        if (logger.isTraceEnabled()) {
          logger.trace("setting removeAll response to null. region-cc-enabled={}; clientIsEmpty={}; client-cc-enabled={}",
              region.getConcurrencyChecksEnabled(), clientIsEmpty, clientHasCCEnabled);
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
      
      // If an exception occurs during the op, preserve the connection
      writeChunkedException(msg, ce, false, servConn);
      servConn.setAsTrue(RESPONDED);
      // if (logger.fineEnabled()) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.Generic_0_UNEXPECTED_EXCEPTION, servConn.getName()), ce);
      // }
      return;
    }
    finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessRemoveAllTime(start - oldStart);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending removeAll response back to {} for region {}{}", servConn.getName(),
          servConn.getSocketString(), regionName, (logger.isTraceEnabled()? ": " + response : ""));
    }
    
    // Increment statistics and write the reply
    if (!replyWithMetaData) {
      writeReply(msg, response, servConn);
    }
    servConn.setAsTrue(RESPONDED);
    stats.incWriteRemoveAllResponseTime(DistributionStats.getStatTime() - start);
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
      logger.debug("sending chunked response header.  version list size={}{}", listSize,
          (logger.isTraceEnabled()? " list=" + response : ""));
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
          logger.debug("sending chunk at index {} last chunk={} numParts={}",i , lastChunk, replyMsg.getNumberOfParts());
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
      logger.debug("sending chunked response header with metadata refresh status. Version list size = {}{}",
          listSize, (logger.isTraceEnabled()? "; list=" + response : ""));
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
