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

import java.io.IOException;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.pdx.internal.EnumInfo;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;


public class GetPDXIdForEnum extends BaseCommand {

  private final static GetPDXIdForEnum singleton = new GetPDXIdForEnum();

  public static Command getCommand() {
    return singleton;
  }

  private GetPDXIdForEnum() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get pdx id for enum request ({} parts) from {}", servConn.getName(), msg.getNumberOfParts(), servConn.getSocketString());
    }

    EnumInfo enumInfo = (EnumInfo) msg.getPart(0).getObject();
    
    int enumId;
    try {
      GemFireCacheImpl cache = (GemFireCacheImpl) servConn.getCache();
      TypeRegistry registry = cache.getPdxRegistry();
      enumId = registry.defineEnum(enumInfo);
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(msg.getTransactionId());
    responseMsg.addIntPart(enumId);
    responseMsg.send(servConn, msg.getTransactionId());
    servConn.setAsTrue(RESPONDED);
  }
}
