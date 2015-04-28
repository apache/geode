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
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

import java.io.IOException;

public class Invalid extends BaseCommand {

  private final static Invalid singleton = new Invalid();

  public static Command getCommand() {
    return singleton;
  }

  private Invalid() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    logger.error(LocalizedMessage.create(LocalizedStrings.Invalid_0_INVALID_MESSAGE_TYPE_WITH_TX_1_FROM_2, new Object[] {servConn.getName(), Integer.valueOf(msg.getTransactionId()), servConn.getSocketString()}));
    writeErrorResponse(msg, MessageType.INVALID, servConn);

  }

}
