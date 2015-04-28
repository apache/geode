package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.ObjectUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;

public class get implements CommandExecutable
{
	private Gfsh gfsh;
	
	public get(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("get [<query predicate>] | [-k <number list>] | [-?]");
		gfsh.println("     Get value from the current region.");
		gfsh.println("        <query predicate>: field=val1 and field2='val1' \\");
		gfsh.println("                           and field3=to_date('<date>', '<format>')");
		gfsh.println("     Data formats: primitives, String, and java.util.Date");
		gfsh.println("         <decimal>b|B - Byte      (e.g., 1b)");
		gfsh.println("         <decimal>c|C - Character (e.g., 1c)");
		gfsh.println("         <decimal>s|S - Short     (e.g., 12s)");
		gfsh.println("         <decimal>i|I - Integer   (e.g., 15 or 15i)");
		gfsh.println("         <decimal>l|L - Long      (e.g., 20l)");
		gfsh.println("         <decimal>f|F - Float     (e.g., 15.5 or 15.5f)");
		gfsh.println("         <decimal>d|D - Double    (e.g., 20.0d)");
		gfsh.println("         '<string with \\ delimiter>' (e.g., '\\'Wow!\\'!' Hello, world')");
		gfsh.println("         to_date('<date string>', '<simple date format>')");
		gfsh.println("                       (e.g., to_date('04/10/2009', 'MM/dd/yyyy')");
		gfsh.println("     -k <number list>   Get values from the current region using the");
		gfsh.println("                        enumerated keys. Use 'ls -k' to get the list");
		gfsh.println("                        of enumerated keys.");
		gfsh.println("     <number list> format: num1 num2 num3-num5 ... e.g., 'get -k 1 2 4 10-20'");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("get -?")) {
			help();
		} else if (command.startsWith("get -k")) {
			get_k(command);
		} else if (command.startsWith("get")) {
			get(command);
		}
	}
	
	private void get(String command) throws Exception
	{
		if (gfsh.getCurrentRegion() == null) {
			gfsh.println("Error: Region undefined. Use 'cd' to change region first before executing this command.");
			return;
		}

		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("Error: get requires a query predicate");
		} else {
			String input = (String) list.get(1);
			Object key = null;
			Object value;
			if (input.startsWith("'")) {
				int lastIndex = -1;
				if (input.endsWith("'") == false) {
					lastIndex = input.length();
				} else {
					lastIndex = input.lastIndexOf("'");
				}
				if (lastIndex <= 1) {
					gfsh.println("Error: Invalid key. Empty string not allowed.");
					return;
				}
				key = input.subSequence(1, lastIndex); // lastIndex exclusive
			} else {
				key = ObjectUtil.getPrimitive(gfsh, input, false);
				if (key == null) {
					key = gfsh.getQueryKey(list, 1);
				}
			}
			if (key == null) {
				return;
			}
			long startTime = System.currentTimeMillis();
			value = gfsh.getCurrentRegion().get(key);
			long stopTime = System.currentTimeMillis();
			
			if (value == null) {
				gfsh.println("Key not found.");
				return;
			}
			
			HashMap keyMap = new HashMap();
			keyMap.put(1, key);
			PrintUtil.printEntries(gfsh.getCurrentRegion(), keyMap, null);
			if (gfsh.isShowTime()) {
				gfsh.println("elapsed (msec): " + (stopTime - startTime));
			}
		}
	}
	
	private void get_k(String command) throws Exception
	{
		if (gfsh.getCurrentRegion() == null) {
			gfsh.println("Error: Region undefined. Use 'cd' to change region first before executing this command.");
			return;
		}
		
		// get -k #
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 3) {
			gfsh.println("Error: get -k requires number(s)");
			return;
		}
		
		if (gfsh.getLsKeyList() == null) {
			gfsh.println("Error: No keys obtained. Execute 'ls -k' first to obtain the keys");
			return;
		}
		
		Map keyMap = gfsh.getKeyMap(list, 2);
		long startTime = System.currentTimeMillis();
		gfsh.getCurrentRegion().getAll(keyMap.values());
		long stopTime = System.currentTimeMillis();
		if (gfsh.isShowResults()) {
			PrintUtil.printEntries(gfsh.getCurrentRegion(), keyMap, null);
		} else {
			gfsh.println("Fetched: " + keyMap.size());
		}
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}
}
