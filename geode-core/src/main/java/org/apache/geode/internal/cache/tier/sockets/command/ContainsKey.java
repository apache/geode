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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.security.NotAuthorizedException;

public class ContainsKey extends BaseCommand {

  private final static ContainsKey singleton = new ContainsKey();

  public static Command getCommand() {
    return singleton;
  }

  private static void writeContainsKeyResponse(boolean containsKey, Message origMsg, ServerConnection servConn)
    throws IOException {
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(origMsg.getTransactionId());
    responseMsg.addObjPart(containsKey ? Boolean.TRUE : Boolean.FALSE);
    responseMsg.send(servConn);
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start) throws IOException {
    Part regionNamePart = null;
    Part keyPart = null;
    String regionName = null;
    Object key = null;

    CacheServerStats stats = servConn.getCacheServerStats();

    servConn.setAsTrue(REQUIRES_RESPONSE);
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadContainsKeyRequestTime(start - oldStart);
    }
    // Retrieve the data from the message parts
    regionNamePart = msg.getPart(0);
    keyPart = msg.getPart(1);
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received containsKey request ({} bytes) from {} for region {} key {}", servConn.getName(), msg.getPayloadLength(), servConn
        .getSocketString(), regionName, key);
    }

    // Process the containsKey request
    if (key == null || regionName == null) {
      String errMessage = "";
      if (key == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ContainsKey_0_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL, servConn
          .getName()));
        errMessage = LocalizedStrings.ContainsKey_THE_INPUT_KEY_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL.toLocalizedString();
      }
      if (regionName == null) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.ContainsKey_0_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL, servConn
          .getName()));
        errMessage = LocalizedStrings.ContainsKey_THE_INPUT_REGION_NAME_FOR_THE_CONTAINSKEY_REQUEST_IS_NULL.toLocalizedString();
      }
      writeErrorResponse(msg, MessageType.CONTAINS_KEY_DATA_ERROR, errMessage, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    LocalRegion region = (LocalRegion) servConn.getCache().getRegion(regionName);
    if (region == null) {
      String reason = LocalizedStrings.ContainsKey_WAS_NOT_FOUND_DURING_CONTAINSKEY_REQUEST.toLocalizedString();
      writeRegionDestroyedEx(msg, regionName, reason, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    try {
      this.securityService.authorizeRegionRead(regionName, key.toString());
    } catch (NotAuthorizedException ex) {
      writeException(msg, ex, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    AuthorizeRequest authzRequest = servConn.getAuthzRequest();
    if (authzRequest != null) {
      try {
        authzRequest.containsKeyAuthorize(regionName, key);
      } catch (NotAuthorizedException ex) {
        writeException(msg, ex, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
    }
    // Execute the containsKey
    boolean containsKey = region.containsKey(key);

    // Update the statistics and write the reply
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessContainsKeyTime(start - oldStart);
    }
    writeContainsKeyResponse(containsKey, msg, servConn);
    servConn.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent containsKey response for region {} key {}", servConn.getName(), regionName, key);
    }
    stats.incWriteContainsKeyResponseTime(DistributionStats.getStatTime() - start);
  }

}
