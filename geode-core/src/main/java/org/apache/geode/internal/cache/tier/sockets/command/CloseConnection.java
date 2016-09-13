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

import java.io.IOException;

import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
 
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
    long oldStart = start;
    boolean respondToClient = servConn.getClientVersion().compareTo(Version.GFE_90) >= 0;
    start = DistributionStats.getStatTime();
    stats.incReadCloseConnectionRequestTime(start - oldStart);

    if (respondToClient) {
      // newer clients will wait for a response or EOFException
      servConn.setAsTrue(REQUIRES_RESPONSE);
    }

    try {
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
      if (respondToClient) {
        writeReply(msg, servConn);
      }
      servConn.setFlagProcessMessagesAsFalse();

      stats.incProcessCloseConnectionTime(DistributionStats.getStatTime()
          - start);
    }

  }

}
