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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.ResourceException;
import org.apache.geode.cache.operations.PutAllOperationContext;
import org.apache.geode.cache.operations.internal.UpdateOnlyMap;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PutAllPartialResultException;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class PutAll extends BaseCommand {

  private static final PutAll singleton = new PutAll();


  public static Command getCommand() {
    return singleton;
  }

  private PutAll() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    Part regionNamePart = null, numberOfKeysPart = null, keyPart = null, valuePart = null;
    String regionName = null;
    int numberOfKeys = 0;
    Object key = null;
    Part eventPart = null;
    StringBuilder errMessage = new StringBuilder();
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    boolean replyWithMetaData = false;

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

      // part 2: number of keys
      numberOfKeysPart = clientMessage.getPart(2);
      numberOfKeys = numberOfKeysPart.getInt();

      // building the map
      Map map = new LinkedHashMap();
      // Map isObjectMap = new LinkedHashMap();
      for (int i = 0; i < numberOfKeys; i++) {
        keyPart = clientMessage.getPart(3 + i * 2);
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

        valuePart = clientMessage.getPart(3 + i * 2 + 1);
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
          value =
              CachedDeserializableFactory.create(valuePart.getSerializedForm(), region.getCache());
        } else {
          value = valuePart.getSerializedForm();
        }
        // put serializedform for auth. It will be modified with auth callback
        map.put(key, value);
        // isObjectMap.put(key, new Boolean(isObject));
      } // for

      if (clientMessage.getNumberOfParts() == (3 + 2 * numberOfKeys + 1)) {// it means optional
                                                                           // timeout has
        // been added
        int timeout = clientMessage.getPart(3 + 2 * numberOfKeys).getInt();
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
      }

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Received putAll request ({} bytes) from {} for region {}",
            serverConnection.getName(), clientMessage.getPayloadLength(),
            serverConnection.getSocketString(), regionName);
      }

      region.basicBridgePutAll(map, Collections.<Object, VersionTag>emptyMap(),
          serverConnection.getProxyID(), eventId, false, null);

      if (region instanceof PartitionedRegion) {
        PartitionedRegion pr = (PartitionedRegion) region;
        if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
          writeReplyWithRefreshMetadata(clientMessage, serverConnection, pr,
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
      logger.warn(String.format("%s: Unexpected Exception",
          serverConnection.getName()), ce);
      return;
    } finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutAllTime(start - oldStart);
    }

    // Increment statistics and write the reply
    if (!replyWithMetaData) {
      writeReply(clientMessage, serverConnection);
    }
    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent putAll response back to {} for region {}", serverConnection.getName(),
          serverConnection.getSocketString(), regionName);
    }
    stats.incWritePutAllResponseTime(DistributionStats.getStatTime() - start);
  }
}
