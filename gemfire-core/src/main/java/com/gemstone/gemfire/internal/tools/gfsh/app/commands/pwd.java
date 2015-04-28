package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class pwd implements CommandExecutable
{
	private Gfsh gfsh;
	
	public pwd(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("pwd [-?]");
		gfsh.println("     Display the current region path.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("pwd -?")) {
			help();
		} else {
			pwd();
		}
	}
	
	@SuppressFBWarnings(value="NM_METHOD_CONSTRUCTOR_CONFUSION",justification="This is method and not constructor")
	private void pwd()
	{
		gfsh.println(gfsh.getCurrentPath());
	}
}
