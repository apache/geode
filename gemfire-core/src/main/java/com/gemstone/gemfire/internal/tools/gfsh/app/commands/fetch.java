package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class fetch implements CommandExecutable
{
	private Gfsh gfsh;
	
	public fetch(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("fetch [-?] <size>");
		gfsh.println("     Set the fetch size of the query result set.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("fetch -?")) {
			help();
		} else {
			fetch(command);
		}
	}
	
	private void fetch(String command)
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("fetch = " + gfsh.getFetchSize());
			gfsh.println("   Use fetch <size> to set the fetch size");
		} else {
			try {
				gfsh.setFetchSize(Integer.parseInt(list.get(1).toString()));
			} catch (Exception ex) {
				System.out.println("Error: " + ex.getMessage());
			}
		}
	}
}
