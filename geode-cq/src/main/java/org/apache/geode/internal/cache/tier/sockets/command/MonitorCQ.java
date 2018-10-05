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

import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class MonitorCQ extends BaseCQCommand {

  private static final MonitorCQ singleton = new MonitorCQ();

  public static Command getCommand() {
    return singleton;
  }

  private MonitorCQ() {
    // nothing
  }

  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    int op = clientMessage.getPart(0).getInt();

    if (op < 1) {
      // This should have been taken care at the client - remove?
      String err = String.format("%s: The MonitorCq operation is invalid.",
          serverConnection.getName());
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, clientMessage.getTransactionId(), null,
          serverConnection);
      return;
    }

    String regionName = null;
    if (clientMessage.getNumberOfParts() == 2) {
      // This will be enable/disable on region.
      regionName = clientMessage.getPart(1).getString();
      if (regionName == null) {
        // This should have been taken care at the client - remove?
        String err =
            String.format("%s: A null Region name was passed for MonitorCq operation.",
                serverConnection.getName());
        sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, clientMessage.getTransactionId(),
            null, serverConnection);
        return;
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received MonitorCq request from {} op: {}{}", serverConnection.getName(),
          serverConnection.getSocketString(), op,
          regionName != null ? " RegionName: " + regionName : "");
    }

    securityService.authorize(Resource.CLUSTER, Operation.READ);

    try {
      CqService cqService = crHelper.getCache().getCqService();
      cqService.start();
      // The implementation of enable/disable cq is changed.
      // Instead calling enable/disable client calls execute/stop methods
      // at cache and region level.
      // This method is retained for future purpose, to support admin level apis
      // similar to enable/disable at system/client level.
      // Should never come.
      throw new CqException(
          "Invalid CQ Monitor request received.");
    } catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", clientMessage.getTransactionId(), cqe,
          serverConnection);
    } catch (Exception e) {
      String err =
          String.format("Exception while handling the monitor request, the operation is %s",
              op);
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err, clientMessage.getTransactionId(), e,
          serverConnection);
    }
  }

}
