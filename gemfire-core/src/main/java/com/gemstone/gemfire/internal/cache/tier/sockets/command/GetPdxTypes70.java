/*=========================================================================
 * Copyright (c) 2012 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.pdx.internal.PdxType;

public class GetPdxTypes70 extends BaseCommand {

  private final static GetPdxTypes70 singleton = new GetPdxTypes70();

  public static Command getCommand() {
    return singleton;
  }

  private GetPdxTypes70() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get pdx types from {}", servConn.getName(), servConn.getSocketString());
    }

    Map<Integer, PdxType> types;
    try {
      GemFireCacheImpl cache = (GemFireCacheImpl) servConn.getCache();
      types = cache.getPdxRegistry().typeMap();
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }
    
    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(msg.getTransactionId());
    responseMsg.addObjPart(types);
    responseMsg.send(servConn, msg.getTransactionId());
    servConn.setAsTrue(RESPONDED);
  }
}
