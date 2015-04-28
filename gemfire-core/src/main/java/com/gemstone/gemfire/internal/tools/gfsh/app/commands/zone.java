package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class zone implements CommandExecutable
{
	private Gfsh gfsh;
	
	public zone(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("zone [-?] <hours>");
		gfsh.println("     Set the zone difference. This value is added to all time-related data.");
		gfsh.println("     For example, set this value to -3 if the data in the cache is");
		gfsh.println("     timestamped in EST and you are running this program in PST.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("zone -?")) {
			help();
		} else {
			zone(command);
		}
	}
	
	// zone hours
	// zone -3
	private void zone(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("zone = " + gfsh.getZoneDifference() / (60 * 60 * 1000));
			gfsh.println("   Use zone <hours> to change the zone hour difference");
		} else {
			int hours = Integer.parseInt((String) list.get(1));
			gfsh.setZoneDifference(hours * 60 * 60 * 1000L);//FindBugs - integer multiplication cast to long
		}
	}
}
