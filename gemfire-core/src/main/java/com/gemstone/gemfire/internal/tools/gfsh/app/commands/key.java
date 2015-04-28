package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;

public class key implements CommandExecutable
{
	private Gfsh gfsh;
	
	public key(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("key [-l] [<class name>] | [-?]");
		gfsh.println("     Set the key class to be used for the 'get', 'put' and 'query'");
		gfsh.println("     commands. Use the 'value' command to set the value class name.");
		gfsh.println("     -l List the last enumerated keys. These keys were obtained");
		gfsh.println("        by executing one of the following commands: ");
		gfsh.println("        " + gfsh.getEnumCommands());
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("key -?")) {
			help();
		} else if (command.startsWith("key -l")) {
			key_l();
		} else {
			key(command);
		}
	}
	
	private void key(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("key = " + gfsh.getQueryKeyClassName());
			gfsh.println("   Use key <class name> to set the key class");
		} else {
			if (list.size() > 1) {
				gfsh.setKeyClass((String) list.get(1));
			}
		}
	}
	
	private void key_l() throws Exception
	{
		List keyList = gfsh.getLsKeyList();
		if (gfsh.getLsKeyList() == null) {
			gfsh.println("Error: Key list undefined. The folowing commands create the key list: ");
			gfsh.println("       " + gfsh.getEnumCommands());
			return;
		}
	
		PrintUtil.printList(keyList);
	}
	
}
