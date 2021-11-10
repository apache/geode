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
package org.apache.geode.cache.query.cq.internal.command;

import java.io.IOException;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

public class GetCQStats extends BaseCQCommand {

  private static final GetCQStats singleton = new GetCQStats();

  public static Command getCommand() {
    return singleton;
  }

  private GetCQStats() {
    // nothing
  }

  @Override
  public void cmdExecute(final @NotNull Message clientMessage,
      final @NotNull ServerConnection serverConnection,
      final @NotNull SecurityService securityService, long start) throws IOException {
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();

    CacheServerStats stats = serverConnection.getCacheServerStats();

    serverConnection.setAsTrue(REQUIRES_RESPONSE);
    serverConnection.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received close all client CQs request from {}", serverConnection.getName(),
          serverConnection.getSocketString());
    }

    // Retrieve the data from the message parts
    String cqName = clientMessage.getPart(0).getString();

    if (isDebugEnabled) {
      logger.debug("{}: Received close CQ request from {} cqName: {}", serverConnection.getName(),
          serverConnection.getSocketString(), cqName);
    }

    // Process the query request
    if (cqName == null) {
      String err = "The cqName for the cq stats request is null";
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, clientMessage.getTransactionId(), null,
          serverConnection);
      return;
    }

    securityService.authorize(Resource.CLUSTER, Operation.READ);
    // Process the cq request
    try {
      // make sure the cqservice has been created
      // since that is what registers the stats
      CqService cqService = crHelper.getCache().getCqService();
      cqService.start();
    } catch (Exception e) {
      String err = "Exception while Getting the CQ Statistics. ";
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err, clientMessage.getTransactionId(), e,
          serverConnection);
      return;
    }
    // Send OK to client
    sendCqResponse(MessageType.REPLY, "cq stats sent successfully.",
        clientMessage.getTransactionId(), null, serverConnection);
    serverConnection.setAsTrue(RESPONDED);

    long oldStart = start;
    start = DistributionStats.getStatTime();
    stats.incProcessGetCqStatsTime(start - oldStart);
  }

}
