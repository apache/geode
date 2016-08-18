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

import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;
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
import com.gemstone.gemfire.security.GemFireSecurityException;


public class Size extends BaseCommand {

  private final static Size singleton = new Size();

  private Size() {
  }

  public static Command getCommand() {
    return singleton;
  }

  private static void writeSizeResponse(Integer sizeCount, Message origMsg, ServerConnection servConn)
    throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.addObjPart(sizeCount);
    responseMsg.send(servConn);
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException, InterruptedException {
    StringBuilder errMessage = new StringBuilder();
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    CacheServerStats stats = servConn.getCacheServerStats();
    servConn.setAsTrue(REQUIRES_RESPONSE);

    long oldStart = start;
    start = DistributionStats.getStatTime();
    stats.incReadSizeRequestTime(start - oldStart);
    // Retrieve the data from the message parts
    Part regionNamePart = msg.getPart(0);
    String regionName = regionNamePart.getString();

    if (regionName == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.BaseCommand__THE_INPUT_REGION_NAME_FOR_THE_0_REQUEST_IS_NULL, "size"));
      errMessage.append(LocalizedStrings.BaseCommand__THE_INPUT_REGION_NAME_FOR_THE_0_REQUEST_IS_NULL.toLocalizedString("size"));
      writeErrorResponse(msg, MessageType.SIZE_ERROR, errMessage.toString(), servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) crHelper.getRegion(regionName);
    if (region == null) {
      String reason = LocalizedStrings.BaseCommand__0_WAS_NOT_FOUND_DURING_1_REQUEST.toLocalizedString(regionName, "size");
      writeRegionDestroyedEx(msg, regionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    // Size the entry
    try {
      this.securityService.authorizeRegionRead(regionName);
      writeSizeResponse(region.size(), msg, servConn);
    } catch (RegionDestroyedException rde) {
      writeException(msg, rde, false, servConn);
    } catch (Exception e) {
      // If an interrupted exception is thrown , rethrow it
      checkForInterrupt(servConn, e);

      // If an exception occurs during the destroy, preserve the connection
      writeException(msg, e, false, servConn);
      if (e instanceof GemFireSecurityException) {
        // Fine logging for security exceptions since these are already
        // logged by the security logger
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Unexpected Security exception", servConn.getName(), e);
        }
      } else {
        logger.warn(LocalizedMessage.create(LocalizedStrings.BaseCommand_0_UNEXPECTED_EXCEPTION, servConn.getName()), e);
      }
    } finally {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Sent size response for region {}", servConn.getName(), regionName);
      }
      servConn.setAsTrue(RESPONDED);
      stats.incWriteSizeResponseTime(DistributionStats.getStatTime() - start);
    }
  }
}
