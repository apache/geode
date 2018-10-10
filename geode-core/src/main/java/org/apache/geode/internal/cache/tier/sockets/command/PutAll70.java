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
import org.apache.geode.cache.operations.PutAllOperationContext;
import org.apache.geode.cache.operations.internal.UpdateOnlyMap;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.Version;
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
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class PutAll70 extends BaseCommand {

  private static final PutAll70 singleton = new PutAll70();

  public static Command getCommand() {
    return singleton;
  }

  private PutAll70() {}

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
        writeErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, errMessage.toString(),
            serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
      LocalRegion region = (LocalRegion) crHelper.getRegion(regionName);
      if (region == null) {
        String reason = " was not found during put request";
        writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }

      // part 1: eventID
      eventPart = clientMessage.getPart(1);
      ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
      long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
      EventID eventId =
          new EventID(serverConnection.getEventMemberIDByteArray(), threadId, sequenceId);

      // part 2: invoke callbacks (used by import)
      Part callbacksPart = clientMessage.getPart(2);
      boolean skipCallbacks = callbacksPart.getInt() == 1 ? true : false;

      // part 3: number of keys
      numberOfKeysPart = clientMessage.getPart(3);
      numberOfKeys = numberOfKeysPart.getInt();

      // building the map
      Map map = new LinkedHashMap();
      Map<Object, VersionTag> retryVersions = new LinkedHashMap<Object, VersionTag>();
      // Map isObjectMap = new LinkedHashMap();
      for (int i = 0; i < numberOfKeys; i++) {
        keyPart = clientMessage.getPart(4 + i * 2);
        key = keyPart.getStringOrObject();
        if (key == null) {
          String putAllMsg =
              "One of the input keys for the putAll request is null";
          logger.warn("{}: {}", serverConnection.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, errMessage.toString(),
              serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }

        valuePart = clientMessage.getPart(4 + i * 2 + 1);
        if (valuePart.isNull()) {
          String putAllMsg =
              "One of the input values for the putAll request is null";
          logger.warn("{}: {}", serverConnection.getName(), putAllMsg);
          errMessage.append(putAllMsg);
          writeErrorResponse(clientMessage, MessageType.PUT_DATA_ERROR, errMessage.toString(),
              serverConnection);
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

      if (clientMessage.getNumberOfParts() == (4 + 2 * numberOfKeys + 1)) {// it means optional
                                                                           // timeout has
        // been added
        int timeout = clientMessage.getPart(4 + 2 * numberOfKeys).getInt();
        serverConnection.setRequestSpecificTimeout(timeout);
      }

      securityService.authorize(Resource.DATA, Operation.WRITE, regionName);

      AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize(regionName);
        } else {
          PutAllOperationContext putAllContext =
              authzRequest.putAllAuthorize(regionName, map, null);
          map = putAllContext.getMap();
          if (map instanceof UpdateOnlyMap) {
            map = ((UpdateOnlyMap) map).getInternalMap();
          }
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

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Received putAll request ({} bytes) from {} for region {}",
            serverConnection.getName(), clientMessage.getPayloadLength(),
            serverConnection.getSocketString(), regionName);
      }

      response = region.basicBridgePutAll(map, retryVersions, serverConnection.getProxyID(),
          eventId, skipCallbacks, null);
      if (!region.getConcurrencyChecksEnabled()) {
        // the client only needs this if versioning is being used
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
      writeException(clientMessage, rde, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (ResourceException re) {
      writeException(clientMessage, re, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (PutAllPartialResultException pre) {
      writeException(clientMessage, pre, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    } catch (Exception ce) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, ce);

      // If an exception occurs during the put, preserve the connection
      writeException(clientMessage, ce, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      // if (logger.fineEnabled()) {
      logger.warn(String.format("%s: Unexpected Exception",
          serverConnection.getName()), ce);
      // }
      return;
    } finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutAllTime(start - oldStart);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending putAll70 response back to {} for region {}: {}",
          serverConnection.getName(), serverConnection.getSocketString(), regionName, response);
    }
    // Starting in 7.0.1 we do not send the keys back
    if (response != null && Version.GFE_70.compareTo(serverConnection.getClientVersion()) < 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("setting putAll keys to null");
      }
      response.setKeys(null);
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
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(2);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(okBytes());
    if (response != null) {
      response.clearObjects();
      replyMsg.addObjPart(response);
    }
    replyMsg.send(servConn);
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
    Message replyMsg = servConn.getReplyMessage();
    servConn.getCache().getCancelCriterion().checkCancelInProgress(null);
    replyMsg.setMessageType(MessageType.REPLY);
    replyMsg.setNumberOfParts(2);
    replyMsg.setTransactionId(origMsg.getTransactionId());
    replyMsg.addBytesPart(new byte[] {pr.getMetadataVersion(), nwHop});
    if (response != null) {
      response.clearObjects();
      replyMsg.addObjPart(response);
    }
    replyMsg.send(servConn);
    pr.getPrStats().incPRMetaDataSentCount();
    if (logger.isTraceEnabled()) {
      logger.trace("{}: rpl with REFRESH_METADATA tx: {}", servConn.getName(),
          origMsg.getTransactionId());
    }
  }

}
