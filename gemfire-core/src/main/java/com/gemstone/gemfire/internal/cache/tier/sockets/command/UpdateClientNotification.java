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


public class UpdateClientNotification extends BaseCommand {

  private final static UpdateClientNotification singleton = new UpdateClientNotification();

  public static Command getCommand() {
    return singleton;
  }

  private UpdateClientNotification() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    CacheServerStats stats = servConn.getCacheServerStats();
    {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incReadUpdateClientNotificationRequestTime(start - oldStart);
    }
    try {
      // this is no longer needed the client membership id is now used
//       // Retrieve the data from the message parts
//       Part clientPortPart = msg.getPart(0);
//       int clientPort = clientPortPart.getInt();
//       if (logger.fineEnabled()) {
//         logger.fine(servConn.getName()
//             + ": Received client notification update request ("
//             + msg.getPayloadLength() + " bytes) for " + clientPort + " from "
//             + servConn.getSocketHost() + ":" + servConn.getSocketPort());
//       }
//       // Update the client socket and remote ports
//       servConn.getAcceptor().getCacheClientNotifier().registerClientPort(
//           servConn.getSocketHost(), clientPort, servConn.getSocketPort(),
//           servConn.getProxyID());

//       if (logger.fineEnabled()) {
//         logger.fine(servConn.getName()
//             + ": Processed client notification update request for "
//             + clientPort + " from " + servConn.getSocketHost() + ":"
//             + servConn.getSocketPort());
//       }
    }
    finally {
      long oldStart = start;
      start = DistributionStats.getStatTime();
      stats.incProcessUpdateClientNotificationTime(start - oldStart);
    }
  }

}
