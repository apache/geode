package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class help implements CommandExecutable
{
	private Gfsh gfsh;
	
	public help(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	
	@SuppressFBWarnings(value="NM_METHOD_CONSTRUCTOR_CONFUSION",justification="This is method and not constructor")
	public void help()
	{
		gfsh.println("help or ?");
		gfsh.println("     List command descriptions");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
    
		if (command.startsWith("help -?")) {
			help();
		} else {
	    String[] splitted = command.split(" ");
	    if (splitted.length > 1) {
	      gfsh.showHelp(splitted[1]);
      } else {
        gfsh.showHelp();
      }
		}
	}
}
