/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class GetFunctionAttribute extends BaseCommand {

  private final static GetFunctionAttribute singleton = new GetFunctionAttribute();

  public static Command getCommand() {
    return singleton;
  }

  private GetFunctionAttribute() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    String functionId = msg.getPart(0).getString();
    if (functionId == null) {
      String message = LocalizedStrings.GetFunctionAttribute_THE_INPUT_0_FOR_GET_FUNCTION_ATTRIBUTE_REQUEST_IS_NULL
          .toLocalizedString("functionId");
      logger.warn("{}: {}", servConn.getName(), message);
      sendError(msg, message, servConn);
      return;
    }
    else {
      Function function = FunctionService.getFunction(functionId);
      if (function == null) {
        String message = null;
        message = LocalizedStrings.GetFunctionAttribute_THE_FUNCTION_IS_NOT_REGISTERED_FOR_FUNCTION_ID_0
            .toLocalizedString(functionId);
        logger.warn("{}: {}", servConn.getName(), message);
        sendError(msg, message, servConn);
        return;
      }
      else {
        byte[] functionAttributes = new byte[3];
        functionAttributes[0] = (byte)(function.hasResult() ? 1 : 0);
        functionAttributes[1] = (byte)(function.isHA() ? 1 : 0);
        functionAttributes[2] = (byte)(function.optimizeForWrite() ? 1 : 0);
        writeResponseWithFunctionAttribute(functionAttributes, msg, servConn);
      }
    }
  }

  private void sendError(Message msg, String message, ServerConnection servConn)
      throws IOException {
    synchronized (msg) {
      writeErrorResponse(msg, MessageType.REQUESTDATAERROR, message, servConn);
      servConn.setAsTrue(RESPONDED);
    }
  }

}
