package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.List;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.Nextable;

public class next implements CommandExecutable
{
	private Gfsh gfsh;
	private String command;
	private Object userData;
	
	public next(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("next | n [-?]");
		gfsh.println("     Fetch the next set of query results.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("next -?")) {
			help();
		} else {
			next();
		}
	}
	
	public void setCommand(String command, Object userData)
	{
		this.command = command;
		this.userData = userData;
	}
	
	public void setCommand(String command)
	{
		setCommand(command, null);
	}
	
	public String getCommand()
	{
		return command;
	}
	
	public Object getUserData()
	{
		return userData;
	}
	
	public List next() throws Exception
	{
		Nextable nextable = (Nextable)gfsh.getCommand(command);
		return nextable.next(userData);
	}
	
}
