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
package org.apache.geode.internal.cache.tier.sockets.command;

import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.*;
import org.apache.geode.distributed.internal.DistributionStats;

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
