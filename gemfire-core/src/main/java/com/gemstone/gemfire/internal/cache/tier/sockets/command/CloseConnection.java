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
 
public class CloseConnection extends BaseCommand {

  private final static CloseConnection singleton = new CloseConnection();

  public static Command getCommand() {
    return singleton;
  }

  private CloseConnection() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    CacheServerStats stats = servConn.getCacheServerStats();
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadCloseConnectionRequestTime(start - oldStart);
    }
    try {
      // clientHost = theSocket.getInetAddress().getCanonicalHostName();
      servConn.setClientDisconnectCleanly();
      String clientHost = servConn.getSocketHost();
      int clientPort = servConn.getSocketPort();
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Received close request ({} bytes) from {}:{}", servConn.getName(), msg.getPayloadLength(), clientHost, clientPort);
      }
      
      Part keepalivePart = msg.getPart(0);
      byte[] keepaliveByte = keepalivePart.getSerializedForm();
      boolean keepalive = (keepaliveByte == null || keepaliveByte[0] == 0) ? false
          : true;

      servConn.getAcceptor().getCacheClientNotifier().setKeepAlive(servConn.getProxyID(), keepalive);

      if (logger.isDebugEnabled()) {
        logger.debug("{}: Processed close request from {}:{}, keepAlive: {}", servConn.getName(), clientHost, clientPort, keepalive);
      }
    }
    finally {
      servConn.setFlagProcessMessagesAsFalse();

      stats.incProcessCloseConnectionTime(DistributionStats.getStatTime()
          - start);
    }

  }

}
