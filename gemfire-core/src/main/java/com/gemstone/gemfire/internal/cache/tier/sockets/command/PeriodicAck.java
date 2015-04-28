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

import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
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
