/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.ResourceException;
import org.apache.geode.cache.client.internal.PutAllOp;
import org.apache.geode.cache.operations.PutAllOperationContext;
import org.apache.geode.cache.operations.internal.UpdateOnlyMap;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.Token;
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
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class PutAll80 extends BaseCommand {

  private static final PutAll80 singleton = new PutAll80();

  public static Command getCommand() {
    return singleton;
  }

  protected PutAll80() {}

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
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long startp) throws IOException, InterruptedException {
    long start = startp; // copy this since we need to modify it
    Part regionNamePart = null, numberOfKeysPart = null, keyPart = null, valuePart = null;
    String regionName = null;
    int numberOfKeys = 0;
    Object key = null;
    Part eventPart = null;
    boolean replyWithMetaData = false;
    VersionedObjectList response = null;

    StringBuilder errMessage = new StringBuilder();
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    CacheServerStats stats = serverConnection.getCacheServerStats();

    // requiresResponse = true;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE); // new in 8.0
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadPutAllRequestTime(start - oldStart);
    }

    try {
      // Retrieve the data from the message parts
      // part 0: region name
      regionNamePart = clientMessage.getPart(0);
      regionName = regionNamePart.getString();

      if (regionName == null) {
        String putAllMsg =
            "The input region name for the putAll request is null";
        logger.warn("{}: {}", serverConnection.getName(), putAllMsg);
        errMessage.append(putAllMsg);
        writeChunkedErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, errMessage.toString(),
            serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }

      LocalRegion region = (LocalRegion) crHelper.getRegion(regionName);
      if (region == null) {
        String reason = " was not found during putAll request";
        writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }

      final int BASE_PART_COUNT = getBasePartCount();

      // part 1: eventID
      eventPart = clientMessage.getPart(1);
      ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
      long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      EventID eventId =
          new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

      Breadcrumbs.setEventId(eventId);

      // part 2: invoke callbacks (used by import)
      Part callbacksPart = clientMessage.getPart(2);
      boolean skipCallbacks = callbacksPart.getInt() == 1 ? true : false;

      // part 3: flags
      int flags = clientMessage.getPart(3).getInt();
      boolean clientIsEmpty = (flags & PutAllOp.FLAG_EMPTY) != 0;
      boolean clientHasCCEnabled = (flags & PutAllOp.FLAG_CONCURRENCY_CHECKS) != 0;

      // part 4: number of keys
      numberOfKeysPart = clientMessage.getPart(4);
      numberOfKeys = numberOfKeysPart.getInt();

      Object callbackArg = getOptionalCallbackArg(clientMessage);

      if (logger.isDebugEnabled()) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(serverConnection.getName()).append(": Received ")
            .append(this.putAllClassName()).append(" request from ")
            .append(serverConnection.getSocketString()).append(" for region ").append(regionName)
            .append(callbackArg != null ? (" callbackArg " + callbackArg) : "").append(" with ")
            .append(numberOfKeys).append(" entries.");
        logger.debug(buffer.toString());
      }
      // building the map
      Map map = new LinkedHashMap();
      Map<Object, VersionTag> retryVersions = new LinkedHashMap<Object, VersionTag>();
      // Map isObjectMap = new LinkedHashMap();
      for (int i = 0; i < numberOfKeys; i++) {
        keyPart = clientMessage.getPart(BASE_PART_COUNT + i * 2);
        key = keyPart.getStringOrObject();
        if (key == null) {
          String putAllMsg =
              "One of the input keys for the putAll request is null";
          logger.warn("{}: {}", serverConnection.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeChunkedErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR,
              errMessage.toString(), serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }

        valuePart = clientMessage.getPart(BASE_PART_COUNT + i * 2 + 1);
        if (valuePart.isNull()) {
          String putAllMsg =
              "One of the input values for the putAll request is null";
          logger.warn("{}: {}", serverConnection.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeChunkedErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR,
              errMessage.toString(), serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }

        // byte[] value = valuePart.getSerializedForm();
        Object value;
        if (valuePart.isObject()) {
          // We're shoe-horning support for invalidated entries into putAll
          // here...however Token.INVALID cannot be wrapped in a DataSerializable.
          // Also, this code is using skipCallbacks as an import flag. If we make
          // skipCallbacks configurable this code will need to be updated.
          if (skipCallbacks && Token.INVALID.isSerializedValue(valuePart.getSerializedForm())) {
            value = Token.INVALID;
          } else {
            value = CachedDeserializableFactory.create(valuePart.getSerializedForm(),
                region.getCache());
          }
        } else {
          value = valuePart.getSerializedForm();
        }
        // put serializedform for auth. It will be modified with auth callback
        if (clientMessage.isRetry()) {
          // Constuct the thread id/sequence id information for this element in the
          // put all map

          // The sequence id is constructed from the base sequence id and the offset
          EventID entryEventId = new EventID(eventId, i);

          // For PRs, the thread id assigned as a fake thread id.
          if (region instanceof PartitionedRegion) {
            PartitionedRegion pr = (PartitionedRegion) region;
            int bucketId = pr.getKeyInfo(key).getBucketId();
            long entryThreadId =
                ThreadIdentifier.createFakeThreadIDForBulkOp(bucketId, entryEventId.getThreadID());
            entryEventId = new EventID(entryEventId.getMembershipID(), entryThreadId,
                entryEventId.getSequenceID());
          }

          VersionTag tag = findVersionTagsForRetriedBulkOp(region, entryEventId);
          if (tag != null) {
            retryVersions.put(key, tag);
          }
          // FIND THE VERSION TAG FOR THIS KEY - but how? all we have is the
          // putAll eventId, not individual eventIds for entries, right?
        }
        map.put(key, value);
        // isObjectMap.put(key, new Boolean(isObject));
      } // for

      if (clientMessage.getNumberOfParts() == (BASE_PART_COUNT + 2 * numberOfKeys + 1)) {// it means
                                                                                         // optional
        // timeout has been
        // added
        int timeout = clientMessage.getPart(BASE_PART_COUNT + 2 * numberOfKeys).getInt();
        serverConnection.setRequestSpecificTimeout(timeout);
      }

      securityService.authorize(Resource.DATA, Operation.WRITE, regionName);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize(regionName);
        } else {
          PutAllOperationContext putAllContext =
              authzRequest.putAllAuthorize(regionName, map, callbackArg);
          map = putAllContext.getMap();
          if (map instanceof UpdateOnlyMap) {
            map = ((UpdateOnlyMap) map).getInternalMap();
          }
          callbackArg = putAllContext.getCallbackArg();
        }
      } else {
        // no auth, so update the map based on isObjectMap here
        /*
         * Collection entries = map.entrySet(); Iterator iterator = entries.iterator(); Map.Entry
         * mapEntry = null; while (iterator.hasNext()) { mapEntry = (Map.Entry)iterator.next();
         * Object currkey = mapEntry.getKey(); byte[] serializedValue = (byte[])mapEntry.getValue();
         * boolean isObject = ((Boolean)isObjectMap.get(currkey)).booleanValue(); if (isObject) {
         * map.put(currkey, CachedDeserializableFactory.create(serializedValue)); } }
         */
      }

      response = region.basicBridgePutAll(map, retryVersions, serverConnection.getProxyID(),
          eventId, skipCallbacks, callbackArg);
      if (!region.getConcurrencyChecksEnabled() || clientIsEmpty || !clientHasCCEnabled) {
        // the client only needs this if versioning is being used and the client
        // has storage
        if (logger.isTraceEnabled()) {
          logger.trace(
              "setting response to null. region-cc-enabled={}; clientIsEmpty={}; client-cc-enabled={}",
              region.getConcurrencyChecksEnabled(), clientIsEmpty, clientHasCCEnabled);
        }
        response = null;
      }

      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) region;
        if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
          writeReplyWithRefreshMetadata(clientMessage, response, serverConnection, pr,
              pr.getNetworkHopType());
          pr.clearNetworkHopData();
          replyWithMetaData = true;
        }
      }
    } catch (RegionDestroyedException rde) {
      writeChunkedException(clientMessage, rde, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (ResourceException re) {
      writeChunkedException(clientMessage, re, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (PutAllPartialResultException pre) {
      writeChunkedException(clientMessage, pre, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (Exception ce) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, ce);

      // If an exception occurs during the put, preserve the connection
      writeChunkedException(clientMessage, ce, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      logger.warn(String.format("%s: Unexpected Exception",
          serverConnection.getName()), ce);
      return;
    } finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutAllTime(start - oldStart);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending {} response back to {} for regin {} {}", serverConnection.getName(),
          putAllClassName(), serverConnection.getSocketString(), regionName,
          (logger.isTraceEnabled() ? ": " + response : ""));
    }

    // Increment statistics and write the reply
    if (!replyWithMetaData) {
      writeReply(clientMessage, response, serverConnection);
    }
    serverConnection.setAsTrue(RESPONDED);
    stats.incWritePutAllResponseTime(DistributionStats.getStatTime() - start);
  }

  @Override
  protected void writeReply(Message origMsg, ServerConnection serverConnection) throws IOException {
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
          (logger.isTraceEnabled() ? " list=" + response : ""));
    }
    replyMsg.sendHeader();
    if (listSize > 0) {
      int chunkSize = 2 * MAXIMUM_CHUNK_SIZE;
      // Chunker will stream over the list in its toData method
      VersionedObjectList.Chunker chunk =
          new VersionedObjectList.Chunker(response, chunkSize, false, false);
      for (int i = 0; i < listSize; i += chunkSize) {
        boolean lastChunk = (i + chunkSize >= listSize);
        replyMsg.setNumberOfParts(1);
        replyMsg.setMessageType(MessageType.RESPONSE);
        replyMsg.setLastChunk(lastChunk);
        replyMsg.setTransactionId(origMsg.getTransactionId());
        replyMsg.addObjPart(chunk);
        if (logger.isDebugEnabled()) {
          logger.debug("sending chunk at index {} last chunk={} numParts={}", i, lastChunk,
              replyMsg.getNumberOfParts());
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
  protected void writeReplyWithRefreshMetadata(Message origMsg, ServerConnection serverConnection,
      PartitionedRegion pr, byte nwHop) throws IOException {
    throw new UnsupportedOperationException();
  }

  private void writeReplyWithRefreshMetadata(Message origMsg, VersionedObjectList response,
      ServerConnection servConn, PartitionedRegion pr, byte nwHop) throws IOException {
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    ChunkedMessage replyMsg = servConn.getChunkedResponseMessage();
    replyMsg.setMessageType(MessageType.RESPONSE);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.sendHeader();
    int listSize = (response == null) ? 0 : response.size();
    if (logger.isDebugEnabled()) {
      logger.debug(
          "sending chunked response header with metadata refresh status. Version list size = {}{}",
          listSize, (logger.isTraceEnabled() ? "; list=" + response : ""));
    }
    if (response != null) {
      response.setKeys(null);
    }
    replyMsg.setNumberOfParts(1);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHop});
    if (listSize > 0) {
      replyMsg.setLastChunk(false);
      replyMsg.sendChunk(servConn);

      int chunkSize = 2 * MAXIMUM_CHUNK_SIZE; // MAXIMUM_CHUNK_SIZE
      // Chunker will stream over the list in its toData method
      VersionedObjectList.Chunker chunk =
          new VersionedObjectList.Chunker(response, chunkSize, false, false);
      for (int i = 0; i < listSize; i += chunkSize) {
        boolean lastChunk = (i + chunkSize >= listSize);
        replyMsg.setNumberOfParts(1); // resets the message
        replyMsg.setMessageType(MessageType.RESPONSE);
        replyMsg.setLastChunk(lastChunk);
        replyMsg.setTransactionId(origMsg.getTransactionId());
        replyMsg.addObjPart(chunk);
        if (logger.isDebugEnabled()) {
          logger.debug("sending chunk at index {} last chunk={} numParts={}", i, lastChunk,
              replyMsg.getNumberOfParts());
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
      logger.trace("{}: rpl with REFRESH_METADATA tx: {}", servConn.getName(),
          origMsg.getTransactionId());
    }
  }

}
