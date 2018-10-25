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

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.GemFireSecurityException;

public class Size extends BaseCommand {

  private static final Size singleton = new Size();

  private Size() {}

  public static Command getCommand() {
    return singleton;
  }

  private static void writeSizeResponse(Integer sizeCount, Message origMsg,
      ServerConnection servConn) throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.addObjPart(sizeCount);
    responseMsg.send(servConn);
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException, InterruptedException {
    StringBuilder errMessage = new StringBuilder();
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    CacheServerStats stats = serverConnection.getCacheServerStats();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    long oldStart = start;
    start = DistributionStats.getStatTime();
    stats.incReadSizeRequestTime(start - oldStart);
    // Retrieve the data from the message parts
    Part regionNamePart = clientMessage.getPart(0);
    String regionName = regionNamePart.getString();

    if (regionName == null) {
      logger.warn("The input region name for the %s request is null", "size");
      errMessage
          .append(String.format("The input region name for the %s request is null",
              "size"));
      writeErrorResponse(clientMessage, MessageType.SIZE_ERROR, errMessage.toString(),
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) crHelper.getRegion(regionName);
    if (region == null) {
      String reason = String.format("%s was not found during %s request",
          regionName, "size");
      writeRegionDestroyedEx(clientMessage, regionName, reason, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    // Size the entry
    try {
      // GEODE-4102
      // securityService.authorize(Resource.DATA, Operation.READ, regionName);

      writeSizeResponse(region.size(), clientMessage, serverConnection);
    } catch (RegionDestroyedException rde) {
      writeException(clientMessage, rde, false, serverConnection);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(serverConnection, e);

      // If an exception occurs during the destroy, preserve the connection
      writeException(clientMessage, e, false, serverConnection);
      if (e instanceof GemFireSecurityException) {
        // Fine logging for security exceptions since these are already
        // logged by the security logger
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Unexpected Security exception", serverConnection.getName(), e);
        }
      } else {
        logger.warn(String.format("%s: Unexpected Exception",
            serverConnection.getName()), e);
      }
    } finally {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sent size response for region {}", serverConnection.getName(),
            regionName);
      }
      serverConnection.setAsTrue(RESPONDED);
      stats.incWriteSizeResponseTime(DistributionStats.getStatTime() - start);
    }
  }
}
