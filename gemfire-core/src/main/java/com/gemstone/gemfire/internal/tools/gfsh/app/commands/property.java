package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.ArrayList;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class property implements CommandExecutable
{
	private Gfsh gfsh;
	
	public property(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("property [<key>[=<value>]] | [-u <key>] | [?] ");
		gfsh.println("   Sets the property that can be used using ${key},");
		gfsh.println("   which gfsh expands with the matching value.");
		gfsh.println();
		gfsh.println("   -u <key> This option unsets (removes) the property.");
		gfsh.println("            'property <key>=' (with no value) also removes the");
		gfsh.println("             property (key).");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("property -?")) {
			help();
		} else if (command.startsWith("property -u")) {
			property_u(command);
		} else {
			property(command);
		}
	}
	
	private void property_u(String command)
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			return;
		} 
		
		String key = list.get(2);
		gfsh.setProperty(key, null);
	}
	
	private void property(String command)
	{
		ArrayList<String> list = new ArrayList();
		gfsh.parseCommand(command, list);
		if (list.size() == 1) {
			// list all properties
			gfsh.printProperties();
			gfsh.println();
			return;
		} 
		
		String prop = "";
		for (int i = 1; i < list.size(); i++) {
			prop += list.get(i) + " ";
		}
		prop = prop.trim();
		int index = prop.indexOf("=");
		
		if (index == -1) {
			// show the property value
			String key = list.get(1);
			String value = gfsh.getProperty(key);
			gfsh.println(key + "=" + value);
		} else {
			
			String key = prop.substring(0, index);
			String value = prop.substring(index+1);
			
			gfsh.setProperty(key, value);
		}
		
		gfsh.println();
	}
}
