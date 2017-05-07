/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import com.gemstone.gemfire.cache.Declarable;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import java.util.ArrayList;
import java.util.Properties;

public class executeFunction_SendException extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext context) {
	DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    LogWriter logger = ds.getLogWriter();
    logger.fine(
        "Executing executeWithSendException in TestFunction on Member : "
            + ds.getDistributedMember()+ "with Context : " + context);
    if (context.getArguments() instanceof Boolean) {
      context.getResultSender().sendException(new Exception("I have been send from executeFunction_SendException for bool"));
    }
    else if (context.getArguments() instanceof String) {
      String arg = (String)context.getArguments();
      if (arg.equals("Multiple")) {
        logger.fine("Sending Exception First time");
        context.getResultSender().sendException(
            new Exception("I have been send from executeFunction_SendException for String"));
        logger.fine("Sending Exception Second time");
        context.getResultSender().sendException(
            new Exception("I have been send from executeFunction_SendException for String"));
      }
    }
    else if (context.getArguments() instanceof ArrayList) {
      ArrayList args = (ArrayList)context.getArguments();
      for(int i = 0 ;i < args.size() ; i++){
        context.getResultSender().sendResult(new Integer(i));
      }
      context.getResultSender().sendException(
          new Exception("I have been thrown from executeFunction_SendException for ArrayList"));
    }
    else {
      logger.fine("Result sent back :"  + Boolean.FALSE);
      context.getResultSender().lastResult(Boolean.FALSE);
    }
	
  }

  public String getId() {
    return "executeFunction_SendException";
  }

  public void init(Properties arg0) {

  }
}
