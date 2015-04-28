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

import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.distributed.internal.DistributionStats;

import java.io.IOException;


public class ClientReady extends BaseCommand {

  private final static ClientReady singleton = new ClientReady();

  public static Command getCommand() {
    return singleton;
  }

  private ClientReady() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {  
    CacheServerStats stats = servConn.getCacheServerStats();
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadClientReadyRequestTime(start - oldStart);
    }
    try {
      String clientHost = servConn.getSocketHost();
      int clientPort = servConn.getSocketPort();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Received client ready request ({} bytes) from {} on {}:{}", servConn.getName(), msg.getPayloadLength(), servConn.getProxyID(), clientHost, clientPort);
      }

      servConn.getAcceptor().getCacheClientNotifier().readyForEvents(servConn.getProxyID());

      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessClientReadyTime(start - oldStart);

      writeReply(msg, servConn);
      servConn.setAsTrue(RESPONDED);

      if (logger.isDebugEnabled()) {
        logger.debug(servConn.getName() + ": Processed client ready request from " + servConn.getProxyID() + " on " + clientHost + ":" + clientPort);
      }
    }
    finally {
      stats.incWriteClientReadyResponseTime(DistributionStats.getStatTime()
          - start);
    }

  }

}
