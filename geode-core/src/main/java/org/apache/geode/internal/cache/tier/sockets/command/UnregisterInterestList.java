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
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.operations.UnregisterInterestOperationContext;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.AuthorizeRequest;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;


public class UnregisterInterestList extends BaseCommand {

  private static final UnregisterInterestList singleton = new UnregisterInterestList();

  public static Command getCommand() {
    return singleton;
  }

  private UnregisterInterestList() {}

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException {
    Part regionNamePart = null, keyPart = null, numberOfKeysPart = null;
    String regionName = null;
    Object key = null;
    List keys = null;
    int numberOfKeys = 0, partNumber = 0;
    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    // bserverStats.incLong(readDestroyRequestTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyRequestsId, 1);
    // start = DistributionStats.getStatTime();
    // Retrieve the data from the message parts
    regionNamePart = clientMessage.getPart(0);
    regionName = regionNamePart.getString();

    Part isClosingListPart = clientMessage.getPart(1);
    byte[] isClosingListPartBytes = (byte[]) isClosingListPart.getObject();
    boolean isClosingList = isClosingListPartBytes[0] == 0x01;
    boolean keepalive = false;
    try {
      Part keepalivePart = clientMessage.getPart(2);
      byte[] keepalivePartBytes = (byte[]) keepalivePart.getObject();
      keepalive = keepalivePartBytes[0] == 0x01;
    } catch (Exception e) {
      writeChunkedException(clientMessage, e, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }
    numberOfKeysPart = clientMessage.getPart(3);
    numberOfKeys = numberOfKeysPart.getInt();

    partNumber = 4;
    keys = new ArrayList();
    for (int i = 0; i < numberOfKeys; i++) {
      keyPart = clientMessage.getPart(partNumber + i);
      try {
        key = keyPart.getStringOrObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
      keys.add(key);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Received unregister interest request ({} bytes) from {} for the following {} keys in region {}: {}",
          serverConnection.getName(), clientMessage.getPayloadLength(),
          serverConnection.getSocketString(), numberOfKeys, regionName, keys);
    }

    // Process the unregister interest request
    if (keys.isEmpty() || regionName == null) {
      String errMessage = null;
      if (keys.isEmpty() && regionName == null) {
        errMessage =
            "The input list of keys is empty and the input region name is null for the unregister interest request.";
      } else if (keys.isEmpty()) {
        errMessage =
            "The input list of keys for the unregister interest request is empty.";
      } else if (regionName == null) {
        errMessage =
            "The input region name for the unregister interest request is null.";
      }
      String s = errMessage;
      logger.warn("{}: {}", serverConnection.getName(), s);
      writeErrorResponse(clientMessage, MessageType.UNREGISTER_INTEREST_DATA_ERROR, s,
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }

    try {
      securityService.authorize(Resource.DATA, Operation.READ, regionName);
    } catch (NotAuthorizedException ex) {
      writeException(clientMessage, ex, false, serverConnection);
      serverConnection.setAsTrue(RESPONDED);
      return;
    }


    AuthorizeRequest authzRequest = serverConnection.getAuthzRequest();
    if (authzRequest != null) {
      if (!DynamicRegionFactory.regionIsDynamicRegionList(regionName)) {
        try {
          UnregisterInterestOperationContext unregisterContext =
              authzRequest.unregisterInterestListAuthorize(regionName, keys);
          keys = (List) unregisterContext.getKey();
        } catch (NotAuthorizedException ex) {
          writeException(clientMessage, ex, false, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }
      }
    }
    // Yogesh : bug fix for 36457 :
    /*
     * Region destroy message from server to client results in client calling unregister to server
     * (an unnecessary callback). The unregister encounters an error because the region has been
     * destroyed on the server and hence falsely marks the server dead.
     */
    /*
     * Region region = crHelper.getRegion(regionName); if (region == null) {
     * logger.warning(this.name + ": Region named " + regionName + " was not found during register
     * interest list request"); writeErrorResponse(msg, MessageType.UNREGISTER_INTEREST_DATA_ERROR);
     * responded = true; } else {
     */
    // Register interest
    serverConnection.getAcceptor().getCacheClientNotifier().unregisterClientInterest(regionName,
        keys, isClosingList, serverConnection.getProxyID(), keepalive);

    // Update the statistics and write the reply
    // bserverStats.incLong(processDestroyTimeId,
    // DistributionStats.getStatTime() - start);
    // start = DistributionStats.getStatTime(); WHY ARE GETTING START AND NOT
    // USING IT?
    writeReply(clientMessage, serverConnection);
    serverConnection.setAsTrue(RESPONDED);
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Sent unregister interest response for the following {} keys in region {}: {}",
          serverConnection.getName(), numberOfKeys, regionName, keys);
    }
    // bserverStats.incLong(writeDestroyResponseTimeId,
    // DistributionStats.getStatTime() - start);
    // bserverStats.incInt(destroyResponsesId, 1);
    // }

  }

}
