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


public class Default extends BaseCommand {

  private final static Default singleton = new Default();

  public static Command getCommand() {
    return singleton;
  }

  private Default() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    // requiresResponse = true; NOT NEEDED... ALWAYS SEND ERROR RESPONSE
    
    logger.fatal(LocalizedMessage.create(LocalizedStrings.Default_0_UNKNOWN_MESSAGE_TYPE_1_WITH_TX_2_FROM_3, new Object[] {servConn.getName(), MessageType.getString(msg.getMessageType()), Integer.valueOf(msg.getTransactionId()), servConn.getSocketString()}));
    writeErrorResponse(msg, MessageType.UNKNOWN_MESSAGE_TYPE_ERROR, servConn);
    // responded = true; NOT NEEDED... ALWAYS SEND ERROR RESPONSE
  }

}
