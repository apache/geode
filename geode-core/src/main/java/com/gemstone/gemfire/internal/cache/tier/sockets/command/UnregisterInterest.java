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

import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.operations.UnregisterInterestOperationContext;
import com.gemstone.gemfire.i18n.StringId;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.security.NotAuthorizedException;


public class UnregisterInterest extends BaseCommand {

  private final static UnregisterInterest singleton = new UnregisterInterest();

  public static Command getCommand() {
    return singleton;
  }

  UnregisterInterest() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
    throws ClassNotFoundException, IOException {
    Part regionNamePart = null, keyPart = null;
    String regionName = null;
    Object key = null;
    int interestType = 0;
    StringId errMessage = null;
    servConn.setAsTrue(REQUIRES_RESPONSE);

    regionNamePart = msg.getPart(0);
    interestType = msg.getPart(1).getInt();
    keyPart = msg.getPart(2);
    Part isClosingPart = msg.getPart(3);
    byte[] isClosingPartBytes = (byte[]) isClosingPart.getObject();
    boolean isClosing = isClosingPartBytes[0] == 0x01;
    regionName = regionNamePart.getString();
    try {
      key = keyPart.getStringOrObject();
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    boolean keepalive = false;
    try {
      Part keepalivePart = msg.getPart(4);
      byte[] keepaliveBytes = (byte[]) keepalivePart.getObject();
      keepalive = keepaliveBytes[0] != 0x00;
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received unregister interest request ({} bytes) from {} for region {} key {}", servConn.getName(), msg
        .getPayloadLength(), servConn.getSocketString(), regionName, key);
    }

    // Process the unregister interest request
    if ((key == null) && (regionName == null)) {
      errMessage = LocalizedStrings.UnRegisterInterest_THE_INPUT_REGION_NAME_AND_KEY_FOR_THE_UNREGISTER_INTEREST_REQUEST_ARE_NULL;
    } else if (key == null) {
      errMessage = LocalizedStrings.UnRegisterInterest_THE_INPUT_KEY_FOR_THE_UNREGISTER_INTEREST_REQUEST_IS_NULL;
    } else if (regionName == null) {
      errMessage = LocalizedStrings.UnRegisterInterest_THE_INPUT_REGION_NAME_FOR_THE_UNREGISTER_INTEREST_REQUEST_IS_NULL;
      String s = errMessage.toLocalizedString();
      logger.warn("{}: {}", servConn.getName(), s);
      writeErrorResponse(msg, MessageType.UNREGISTER_INTEREST_DATA_ERROR, s, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    try {
    if (interestType == InterestType.REGULAR_EXPRESSION) {
      this.securityService.authorizeRegionRead(regionName);
    } else {
      this.securityService.authorizeRegionRead(regionName, key.toString());
    }
    } catch (NotAuthorizedException ex) {
      writeException(msg, ex, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    AuthorizeRequest authzRequest = servConn.getAuthzRequest();
    if (authzRequest != null) {
      if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        try {
          UnregisterInterestOperationContext unregisterContext = authzRequest.unregisterInterestAuthorize(regionName, key, interestType);
          key = unregisterContext.getKey();
        } catch (NotAuthorizedException ex) {
          writeException(msg, ex, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
      }
    }
    // Yogesh : bug fix for 36457 :
      /*
       * Region destroy message from server to client results in client calling
       * unregister to server (an unnecessary callback). The unregister
       * encounters an error because the region has been destroyed on the server
       * and hence falsely marks the server dead.
       */
      /*
       * Region region = crHelper.getRegion(regionName); if (region == null) {
       * logger.warning(this.name + ": Region named " + regionName + " was not
       * found during unregister interest request"); writeErrorResponse(msg,
       * MessageType.UNREGISTER_INTEREST_DATA_ERROR); responded = true; } else {
       */
    // Unregister interest irrelevent of whether the region is present it or
    // not
    servConn.getAcceptor()
            .getCacheClientNotifier()
            .unregisterClientInterest(regionName, key, interestType, isClosing, servConn.getProxyID(), keepalive);

    // Update the statistics and write the reply
    // bserverStats.incLong(processDestroyTimeId,
    // DistributionStats.getStatTime() - start);
    // start = DistributionStats.getStatTime();
    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent unregister interest response for region {} key {}", servConn.getName(), regionName, key);
    }
    // bserverStats.incLong(writeDestroyResponseTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyResponsesId, 1);
    // }
  }

}
