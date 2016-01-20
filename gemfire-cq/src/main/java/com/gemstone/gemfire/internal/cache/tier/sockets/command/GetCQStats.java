/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;


import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;

import java.io.IOException;


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
    else {
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
