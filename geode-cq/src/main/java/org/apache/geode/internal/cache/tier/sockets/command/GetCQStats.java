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

import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;

public class GetCQStats extends BaseCQCommand {

  private final static GetCQStats singleton = new GetCQStats();

  public static Command getCommand() {
    return singleton;
  }

  private GetCQStats() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();

    CacheServerStats stats = servConn.getCacheServerStats();

    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received close all client CQs request from {}", servConn.getName(), servConn.getSocketString());
    }

    // Retrieve the data from the message parts
    String cqName = msg.getPart(0).getString();

    if (isDebugEnabled) {
      logger.debug("{}: Received close CQ request from {} cqName: {}", servConn.getName(), servConn.getSocketString(), cqName);
    }

    // Process the query request
    if (cqName == null) {
      String err = "The cqName for the cq stats request is null";
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
          .getTransactionId(), null, servConn);
      return;
    }

    this.securityService.authorizeClusterRead();
    // Process the cq request
    try {
      // make sure the cqservice has been created
      // since that is what registers the stats
      CqService cqService = crHelper.getCache().getCqService();
      cqService.start();
    }
    catch (Exception e) {
      String err = "Exception while Getting the CQ Statistics. ";
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err, msg
          .getTransactionId(), e, servConn);
      return;
    }
    // Send OK to client
    sendCqResponse(MessageType.REPLY, "cq stats sent successfully.", msg
        .getTransactionId(), null, servConn);
    servConn.setAsTrue(RESPONDED);

    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessGetCqStatsTime(start - oldStart);
    }
  }

}
