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

import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.*;
import java.io.IOException;

public class PeriodicAck extends BaseCommand {

  private final static PeriodicAck singleton = new PeriodicAck();

  public static Command getCommand() {
    return singleton;
  }

  private PeriodicAck() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received periodic ack request ({} bytes) from {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString());
    }
    try {
      int numEvents = msg.getNumberOfParts();
      boolean success = false;        
      CacheClientNotifier ccn = servConn.getAcceptor().getCacheClientNotifier();
      CacheClientProxy proxy = ccn.getClientProxy(servConn.getProxyID());
      if (proxy != null) {
        proxy.getHARegionQueue().createAckedEventsMap();
        for (int i = 0; i < numEvents; i++) {
          Part eventIdPart = msg.getPart(i);
          eventIdPart.setVersion(servConn.getClientVersion());
          EventID eid = (EventID)eventIdPart.getObject();
          success = ccn.processDispatchedMessage(servConn.getProxyID(), eid);
          if (!success)
            break;
        }
      }
      if (success) {
        proxy.getHARegionQueue().setAckedEvents();
        writeReply(msg, servConn);
        servConn.setAsTrue(RESPONDED);
      }

    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sent periodic ack response for {}", servConn.getName(), servConn.getSocketString());
    }

  }

}
