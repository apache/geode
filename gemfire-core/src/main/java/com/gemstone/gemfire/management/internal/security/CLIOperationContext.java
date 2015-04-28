package com.gemstone.gemfire.management.internal.security;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;


public class CLIOperationContext extends ResourceOperationContext {
	
	private OperationCode code = OperationCode.RESOURCE;
	private ResourceOperationCode resourceCode = null;
	private Map<String,String> commandOptions = null;
	
	private static Map<String,ResourceOperationCode> commandToCodeMapping = new HashMap<String,ResourceOperationCode>();
	private static CommandManager commandManager = null;
	private static GfshParser parser = null;	
	
	public CLIOperationContext(String commandString) throws CommandProcessingException, IllegalStateException{
		code = OperationCode.RESOURCE;
		GfshParseResult parseResult = (GfshParseResult) parseCommand(commandString);		
		this.commandOptions = parseResult.getParamValueStrings();		
		this.resourceCode = findResourceCode(parseResult.getCommandName()); //need to add this to ParseResult 
	}
	
	private static ParseResult parseCommand(String commentLessLine) throws CommandProcessingException, IllegalStateException {
    if (commentLessLine != null) {
      return parser.parse(commentLessLine);
    }
    throw new IllegalStateException("Command String should not be null.");
  }
	
	public static void registerCommand(CommandManager cmdManager, Method method, CommandTarget commandTarget){	  
	  //Save command manager instance and create a local parser for parsing the commands
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
		ResourceOperationCode code = null;
		
		if (op != null) {
			String opString = op.operation();
			if (opString != null)
				code = ResourceOperationCode.parse(opString);
		}
		
		if(code==null){
			if(commandName.startsWith("describe") || commandName.startsWith("list") || commandName.startsWith("status")
					|| commandName.startsWith("show")){
				code = ResourceOperationCode.LIST_DS;
			} 
		}
		
		//TODO : Have map according to each resources
		//TODO : How to save information for retrieving command Option map or region and serverGroup
		
		Resource targetedResource = null;		
		if(op!=null){
			targetedResource = op.resource();
		} else {			
			targetedResource = Resource.DISTRIBUTED_SYSTEM;
			//TODO : Add other resource and mbeans
		}
		
		
		LogService.getLogger().info("#RegisterCommandSecurity : " + commandName + " code " + code + " op="+op);
		
		if(code!=null) {
			commandToCodeMapping.put(commandName, code);
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
