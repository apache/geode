/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli;

import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;

/**
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class CommandResponseBuilder {
  // Command Response Constants
  private static final String NO_TOKEN_ACCESSOR = "__NULL__";
  
  public static CommandResponse prepareCommandResponse(String memberName, CommandResult result) {
    GfJsonObject content = null;
    try {
      content = result.getContent();
    } catch (GfJsonException e) {
      try {
        content = new GfJsonObject(e.getMessage());
      } catch (GfJsonException e1) {
        content = new GfJsonObject();
      }
    }
    return new CommandResponse(memberName,                      // sender
                               getType(result),                 // contentType
                               result.getStatus().getCode(),    // status code
                               "1/1",                           // page --- TODO - Abhishek - define a scrollable ResultData
                               NO_TOKEN_ACCESSOR,  // tokenAccessor for next results
                               getDebugInfo(result),            // debugData
                               result.getHeader(),              // header
                               content,                         // content
                               result.getFooter(),              // footer
                               result.failedToPersist());       // failed to persist        
    }
  
  // De-serializing to CommandResponse
  public static CommandResponse prepareCommandResponseFromJson(String jsonString) {
    GfJsonObject jsonObject = null;
    try {
      jsonObject = new GfJsonObject(jsonString);
    } catch (GfJsonException e) {
      jsonObject = GfJsonObject.getGfJsonErrorObject(CliUtil.stackTraceAsString(e));
    }
    return new CommandResponse(jsonObject);
  }
  
  public static String getCommandResponseJson(CommandResponse commandResponse) {
    return new GfJsonObject(commandResponse).toString();
  }
  
  public static String createCommandResponseJson(String memberName, CommandResult result) {
    return getCommandResponseJson(prepareCommandResponse(memberName, result));
  }
  
  private static String getType(CommandResult result) {
    return result.getType();
  }
  
  private static String getDebugInfo(CommandResult result) {
    String debugInfo = "";
    if (CommandExecutionContext.isSetWrapperThreadLocal()) {
      CommandResponseWriter responseWriter = CommandExecutionContext.getCommandResponseWriter();
      debugInfo = responseWriter.getResponseWritten();
    }
    
    return debugInfo;
  }
}
