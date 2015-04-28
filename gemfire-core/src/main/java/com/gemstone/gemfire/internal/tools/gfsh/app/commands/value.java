package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class value implements CommandExecutable
{
	private Gfsh gfsh;
	
	public value(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("value [-?] <class name>");
		gfsh.println("     Set the value class to be used for the 'put' command.");
		gfsh.println("     Use the 'key' command to set the key class name.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("value -?")) {
			help();
		} else {
			value(command);
		}
	}
	
	private void value(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("value = " + gfsh.getValueClassName());
			gfsh.println("   Use value <class name> to set the value class");
		} else {
			if (list.size() > 1) {
				gfsh.setValueClass((String) list.get(1));
			}
		}
	}
	
}
