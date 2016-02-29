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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import static com.gemstone.gemfire.management.internal.security.ResourceConstants.*;

/**
 * It represents command being executed and all passed options and option-values.
 * ResourceOpCode returned by CLIOperationContext is retrieved from ResourceOperation
 * annotation on the target command
 *
 * @author tushark
 * @since 9.0
 */
public class CLIOperationContext extends ResourceOperationContext {
	
	private OperationCode code = OperationCode.RESOURCE;
	private ResourceOperationCode resourceCode = null;
	private Map<String,String> commandOptions = null;
	
	private static Map<String,ResourceOperationCode> commandToCodeMapping = new HashMap<String,ResourceOperationCode>();
	private static CommandManager commandManager = null;
	private static GfshParser parser = null;	
	
	public CLIOperationContext(String commandString) throws CommandProcessingException, IllegalStateException{
		GfshParseResult parseResult = (GfshParseResult) parseCommand(commandString);		
		this.commandOptions = parseResult.getParamValueStrings();		
    this.resourceCode = findResourceCode(parseResult.getCommandName());
    this.code = findOperationCode(parseResult.getCommandName());
  }

  /**
   * This method returns OperationCode for command. Some commands perform data
   * operations, for such commands OperationCode returned is not RESOURCE but
   * corresponding data operation as defined in OperationCode
   *
   * @param commandName
   * @return OperationCode
   */
  private OperationCode findOperationCode(String commandName) {

    if(CliStrings.GET.equals(commandName) || CliStrings.LOCATE_ENTRY.equals(commandName))
      return OperationCode.GET;

    if(CliStrings.PUT.equals(commandName))
      return OperationCode.PUT;

    if(CliStrings.QUERY.equals(commandName))
      return OperationCode.QUERY;

    if (CliStrings.REMOVE.equals(commandName)) {
      if (commandOptions.containsKey(CliStrings.REMOVE__ALL)
          && "true".equals(commandOptions.get(CliStrings.REMOVE__ALL))) {
        return OperationCode.REMOVEALL;
      } else
        return OperationCode.DESTROY;
    }

    if(CliStrings.CLOSE_DURABLE_CQS.equals(commandName)) {
      return OperationCode.CLOSE_CQ;
    }

    if(CliStrings.CREATE_REGION.equals(commandName)) {
      return OperationCode.REGION_CREATE;
    }

    if(CliStrings.DESTROY_REGION.equals(commandName)) {
      return OperationCode.REGION_DESTROY;
    }

    if(CliStrings.EXECUTE_FUNCTION.equals(commandName)) {
      return OperationCode.EXECUTE_FUNCTION;
    }

    //"stop cq"
    //"removeall",
    //"get durable cqs",
    return OperationCode.RESOURCE;
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
	  
		boolean found=false;
		Annotation ans[] = method.getDeclaredAnnotations();
		for(Annotation an : ans){
			if(an instanceof ResourceOperation) {
				cache(commandTarget.getCommandName(),(ResourceOperation)an);
				found=true;
			}
		}
		if(!found)
			cache(commandTarget.getCommandName(),null);
	}

	private static void cache(String commandName, ResourceOperation op) {
    ResourceOperationCode resourceOpCode = null;
		
		if (op != null) {
			String opString = op.operation();
			if (opString != null)
        resourceOpCode = ResourceOperationCode.parse(opString);
		}
		
    if(resourceOpCode==null){
      if (commandName.startsWith(GETTER_DESCRIBE) || commandName.startsWith(GETTER_LIST)
          || commandName.startsWith(GETTER_STATUS)) {
        resourceOpCode = ResourceOperationCode.LIST_DS;
			} 
		}

		
    if(resourceOpCode!=null) {
      commandToCodeMapping.put(commandName, resourceOpCode);
		} else {			
      throw new GemFireConfigException(
          "Error while configuring authorization for gfsh commands. No opCode defined for command " + commandName);

		}
		
	}

	public Map<String, String> getCommandOptions() {
		return commandOptions;
	}

	private static ResourceOperationCode findResourceCode(String commandName) {		
		return commandToCodeMapping.get(commandName);
	}


	@Override
	public OperationCode getOperationCode() {		
		return code;
	}

	@Override
	public ResourceOperationCode getResourceOperationCode() {
		return resourceCode;
	}
	
	
	public String toString(){
	  String str;
	  str = "CLIOperationContext(resourceCode=" + resourceCode + ") options=" + commandOptions+")";
	  return str;
	}
}
