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
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import org.springframework.shell.event.ParseResult;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * It represents command being executed and all passed options and option-values.
 * ResourceOpCode returned by CLIOperationContext is retrieved from ResourceOperation
 * annotation on the target command
 *
 * @author tushark
 * @since 9.0
 */
public class CLIOperationContext extends ResourceOperationContext {

	private Map<String,String> commandOptions = null;
	
	private static Map<String,ResourceOperation> commandToCodeMapping = new HashMap<String,ResourceOperation>();
	private static CommandManager commandManager = null;
	private static GfshParser parser = null;	
	
	public CLIOperationContext(String commandString) throws CommandProcessingException, IllegalStateException{
		GfshParseResult parseResult = (GfshParseResult) parseCommand(commandString);
		ResourceOperation op = findResourceCode(parseResult.getCommandName());
		setResourceOperation(op);
		this.commandOptions = parseResult.getParamValueStrings();
  }
	
	private static ParseResult parseCommand(String commentLessLine) throws CommandProcessingException, IllegalStateException {
    if (commentLessLine != null) {
      return parser.parse(commentLessLine);
    }
    throw new IllegalStateException("Command String should not be null.");
  }
	
	public static void registerCommand(CommandManager cmdManager, Method method, CommandTarget commandTarget){
	  if(commandManager==null){
	    commandManager = cmdManager;
	    parser = new GfshParser(cmdManager);
	  }
		Annotation ans[] = method.getDeclaredAnnotations();
		for(Annotation an : ans){
			if(an instanceof ResourceOperation) {
				commandToCodeMapping.put(commandTarget.getCommandName(), (ResourceOperation)an);
			}
		}
	}

	private static void cache(String commandName, ResourceOperation op) {
		commandToCodeMapping.put(commandName, op);
		
	}

	public Map<String, String> getCommandOptions() {
		return commandOptions;
	}

	private static ResourceOperation findResourceCode(String commandName) {
		return commandToCodeMapping.get(commandName);
	}
	
	public String toString(){
	  String str;
	  str = "CLIOperationContext(resourceCode=" + getOperationCode() + ") options=" + commandOptions+")";
	  return str;
	}
}
