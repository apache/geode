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
