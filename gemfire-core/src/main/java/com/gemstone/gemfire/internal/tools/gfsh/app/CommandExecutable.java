package com.gemstone.gemfire.internal.tools.gfsh.app;

public interface CommandExecutable
{
	void execute(String command) throws Exception;
	
	void help();
}
