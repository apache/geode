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
package javaobject;

import org.apache.geode.cache.Declarable;

import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;

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
