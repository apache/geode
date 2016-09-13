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
 *
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.ResourceException;
import com.gemstone.gemfire.cache.operations.PutOperationContext;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.EventIDHolder;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * @since GemFire 6.1
 */
public class Put61 extends BaseCommand {

  private final static Put61 singleton = new Put61();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long p_start)
    throws IOException, InterruptedException {
    long start = p_start;
    Part regionNamePart = null, keyPart = null, valuePart = null, callbackArgPart = null;
    String regionName = null;
    Object callbackArg = null, key = null;
    Part eventPart = null;
    StringBuffer errMessage = new StringBuffer();
    boolean isDelta = false;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();

    // requiresResponse = true;
    servConn.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadPutRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
    try {
      isDelta = (Boolean) msg.getPart(2).getObject();
    } catch (Exception e) {
      writeException(msg, MessageType.PUT_DELTA_ERROR, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      // CachePerfStats not available here.
      return;
    }
    valuePart = msg.getPart(3);
    eventPart = msg.getPart(4);
    if (msg.getNumberOfParts() > 5) {
      callbackArgPart = msg.getPart(5);
      try {
        callbackArg = callbackArgPart.getObject();
      } catch (Exception e) {
        writeException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    regionName = regionNamePart.getString();

    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received 6.1{}put request ({} bytes) from {} for region {} key {}", servConn.getName(), (isDelta ? " delta " : " "), msg
        .getPayloadLength(), servConn.getSocketString(), regionName, key);
    }

    // Process the put request
    if (key == null || regionName == null) {
      if (key == null) {
        String putMsg = " The input key for the 6.1 put request is null";
        if (isDebugEnabled) {
          logger.debug("{}:{}", servConn.getName(), putMsg);
        }
        errMessage.append(putMsg);
      }
      if (regionName == null) {
        String putMsg = " The input region name for the 6.1 put request is null";
        if (isDebugEnabled) {
          logger.debug("{}:{}", servConn.getName(), putMsg);
        }
        errMessage.append(putMsg);
      }
      writeErrorResponse(msg, MessageType.PUT_DATA_ERROR, errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) servConn.getCache().getRegion(regionName);
    if (region == null) {
      String reason = " was not found during 6.1 put request";
      writeRegionDestroyedEx(msg, regionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    if (valuePart.isNull() && region.containsKey(key)) {
      // Invalid to 'put' a null value in an existing key
      String putMsg = " Attempted to 6.1 put a null value for existing key " + key;
      if (isDebugEnabled) {
        logger.debug("{}:{}", servConn.getName(), putMsg);
      }
      errMessage.append(putMsg);
      writeErrorResponse(msg, MessageType.PUT_DATA_ERROR, errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    // try {
    // this.eventId = (EventID)eventPart.getObject();
    ByteBuffer eventIdPartsBuffer = ByteBuffer.wrap(eventPart.getSerializedForm());
    long threadId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    long sequenceId = EventID.readEventIdPartsFromOptmizedByteArray(eventIdPartsBuffer);
    EventID eventId = new EventID(servConn.getEventMemberIDByteArray(), threadId, sequenceId);

    try {
      Object value = null;
      if (!isDelta) {
        value = valuePart.getSerializedForm();
      }
      boolean isObject = valuePart.isObject();
      boolean isMetaRegion = region.isUsedForMetaRegion();
      msg.setMetaRegion(isMetaRegion);

      this.securityService.authorizeRegionWrite(regionName, key.toString());

      AuthorizeRequest authzRequest = null;
      if (!isMetaRegion) {
        authzRequest = servConn.getAuthzRequest();
      }
      if (authzRequest != null) {
        if (DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
          authzRequest.createRegionAuthorize((String) key);
        }
        // Allow PUT operations on meta regions (bug #38961)
        else {
          PutOperationContext putContext = authzRequest.putAuthorize(regionName, key, value, isObject, callbackArg);
          value = putContext.getValue();
          isObject = putContext.isObject();
          callbackArg = putContext.getCallbackArg();
        }
      }
      // If the value is 1 byte and the byte represents null,
      // attempt to create the entry. This test needs to be
      // moved to DataSerializer or DataSerializer.NULL needs
      // to be publicly accessible.
      boolean result = false;
      if (value == null && !isDelta) {
        // Create the null entry. Since the value is null, the value of the
        // isObject
        // the true after null doesn't matter and is not used.
        result = region.basicBridgeCreate(key, null, true, callbackArg, servConn.getProxyID(), true, new EventIDHolder(eventId), false);
      } else {
        // Put the entry
        byte[] delta = null;
        if (isDelta) {
          delta = valuePart.getSerializedForm();
        }
        result = region.basicBridgePut(key, value, delta, isObject, callbackArg, servConn.getProxyID(), true, new EventIDHolder(eventId));
      }
      if (result) {
        servConn.setModificationInfo(true, regionName, key);
      } else {
        String message = servConn.getName() + ": Failed to 6.1 put entry for region " + regionName + " key " + key + " value " + valuePart;
        if (isDebugEnabled) {
          logger.debug(message);
        }
        throw new Exception(message);
      }
    } catch (RegionDestroyedException rde) {
      writeException(msg, rde, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    } catch (ResourceException re) {
      writeException(msg, re, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    } catch (InvalidDeltaException ide) {
      logger.info(LocalizedMessage.create(LocalizedStrings.UpdateOperation_ERROR_APPLYING_DELTA_FOR_KEY_0_OF_REGION_1, new Object[] {
        key,
        regionName
      }));
      writeException(msg, MessageType.PUT_DELTA_ERROR, ide, false, servConn);
      servConn.setAsTrue(RESPONDED);
      region.getCachePerfStats().incDeltaFullValuesRequested();
      return;

    } catch (Exception ce) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, ce);

      // If an exception occurs during the put, preserve the connection
      writeException(msg, ce, false, servConn);
      servConn.setAsTrue(RESPONDED);
      if (ce instanceof GemFireSecurityException) {
        // Fine logging for security exceptions since these are already
        // logged by the security logger
        if (isDebugEnabled) {
          logger.debug("{}: Unexpected Security exception", servConn.getName(), ce);
        }
      } else if (isDebugEnabled) {
        logger.debug("{}: Unexpected Exception", servConn.getName(), ce);
      }
      return;
    } finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessPutTime(start - oldStart);
    }

    // Increment statistics and write the reply
    if (region instanceof PartitionedRegion) {
      PartitionedRegion pr = (PartitionedRegion) region;
      if (pr.getNetworkHopType() != PartitionedRegion.NETWORK_HOP_NONE) {
        writeReplyWithRefreshMetadata(msg, servConn, pr, pr.getNetworkHopType());
        pr.clearNetworkHopData();
      } else {
        writeReply(msg, servConn);
      }
    } else {
      writeReply(msg, servConn);
    }
    servConn.setAsTrue(RESPONDED);
    if (isDebugEnabled) {
      logger.debug("{}: Sent 6.1 put response back to {} for region {} key {} value {}", servConn.getName(), servConn.getSocketString(), regionName, key, valuePart);
    }
    stats.incWritePutResponseTime(DistributionStats.getStatTime() - start);
  }

}
