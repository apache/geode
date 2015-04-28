package com.gemstone.gemfire.internal.tools.gfsh.app;

public interface ServerExecutable
{
	Object execute(String command, String regionPath, Object arg) throws Exception;
	
	byte getCode();
	
	String getCodeMessage();

}
