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
import java.io.IOException;


public class MakePrimary extends BaseCommand {

  private final static MakePrimary singleton = new MakePrimary();

  public static Command getCommand() {
    return singleton;
  }

  private MakePrimary() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    Part isClientReadyPart = msg.getPart(0);
    byte[] isClientReadyPartBytes = (byte[])isClientReadyPart.getObject();
    boolean isClientReady = isClientReadyPartBytes[0] == 0x01;
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("{}: Received make primary request ({} bytes) isClientReady={}: from {}", servConn.getName(), msg.getPayloadLength(), isClientReady, servConn.getSocketString());
    }
    try {
      servConn.getAcceptor().getCacheClientNotifier()
      .makePrimary(servConn.getProxyID(), isClientReady);
      writeReply(msg, servConn);
      servConn.setAsTrue(RESPONDED);

      if (isDebugEnabled) {
        logger.debug("{}: Sent make primary response for {}", servConn.getName(), servConn.getSocketString());
      }
    }
    catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
    }
  }

}
