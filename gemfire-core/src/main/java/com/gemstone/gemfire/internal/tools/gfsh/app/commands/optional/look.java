package com.gemstone.gemfire.internal.tools.gfsh.app.commands.optional;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.Nextable;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.IndexInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.LookupService;
import com.gemstone.gemfire.internal.tools.gfsh.app.commands.next;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;

public class look implements CommandExecutable, Nextable
{
	private static LookupService lookupService;
	
	private Gfsh gfsh;
	
	public look(Gfsh gfsh)
	{
		this.gfsh = gfsh;
		gfsh.addEnumCommand("look");
	}
	
	public void help()
	{
		gfsh.println("look [-i] | [-?] | [<query predicate>]");
		gfsh.println("     Execute the compound key lookup service. This command requires");
		gfsh.println("     the server to configure the gfcommand addon component,");
		gfsh.println("     com.gemstone.gemfire.internal.tools.gfsh.cache.index.IndexBuilder");
		gfsh.println("        <query predicate>: field=val1 and field2='val1'");
		gfsh.println("                          and field3=to_date('<date>', '<format>'");
		gfsh.println("           Primitives: no quotes");
		gfsh.println("           String: 'string value' (single quotes)");
		gfsh.println("           java.util.Date: to_date('<date>', '<format'>, i.e.,");
		gfsh.println("                           to_date('10/18/2008', 'MM/dd/yyyy'");
		gfsh.println("     -k Retrieve keys only. Values are not returned.");
		gfsh.println("     -i Print compound key index information.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("look -?")) {
			help();
		} else if (command.startsWith("look -i")) {
			look_i(command);
		} else if (command.startsWith("look")) {
			look(command);
		}
	}
	
	private LookupService getLookupService()
	{
		if (lookupService == null) {
			lookupService = new LookupService(gfsh.getCommandClient());
		}
		return lookupService;
	}
	
	private void look(String command) throws Exception
	{
		if (gfsh.getCurrentRegion() == null) {
			gfsh.println("Error: Region undefined. Use 'cd' to change region first before executing this command.");
			return;
		}

		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		
		boolean keysOnly = false;
		String input = null;
		for (int i = 1; i < list.size(); i++) {
			String token = (String)list.get(i);
			if (token.equals("-k")) {
				keysOnly = true;
			} else {
				input = token;
			}
		}
		
		if (input == null) {
			gfsh.println("Error: look requires query predicate");
		} else {
			Object key = null;
			if (keysOnly) {
				key = gfsh.getQueryKey(list, 2);
			} else {
				key = gfsh.getQueryKey(list, 1);
			}
			
			long startTime = 0;
			long stopTime = 0;
			ArrayList keyList = new ArrayList();
			LookupService lookup = getLookupService();
			if (keysOnly) {
				startTime = System.currentTimeMillis();
				Set set = lookup.keySet(gfsh.getCurrentPath(), key);
				stopTime = System.currentTimeMillis();
				if (gfsh.isShowResults()) {
					PrintUtil.printSet(set, gfsh.getFetchSize(), keyList);
				} else {
					gfsh.println("Fetch size: " + gfsh.getFetchSize());
					gfsh.println("   Results: " + set.size() + 
							", Returned: " + set.size() + "/" + set.size());
				}
			} else {
				startTime = System.currentTimeMillis();
				Map map = lookup.entryMap(gfsh.getCurrentPath(), key);
				stopTime = System.currentTimeMillis();

				if (gfsh.isShowResults()) {
					PrintUtil.printEntries(map, gfsh.getFetchSize(), keyList);
				} else {
					gfsh.println("Fetch size: " + gfsh.getFetchSize());
					gfsh.println("   Results: " + map.size() + 
							", Returned: " + map.size() + "/" + map.size());
				}
			}
			gfsh.setLsKeyList(keyList);
			if (gfsh.isShowTime()) {
				gfsh.println("elapsed (msec): " + (stopTime - startTime));
			}
			next n = (next)gfsh.getCommand("next");
			n.setCommand(getClass().getSimpleName());
		}
	}
	
	private void look_i(String command) throws Exception
	{
		if (gfsh.getCurrentRegion() == null) {
			gfsh.println("Error: Region undefined. Use 'cd' to change region first before executing this command.");
			return;
		}
		
		// look -i
		LookupService lookup = getLookupService();
		IndexInfo[] indexInfoArray = lookup.getIndexInfoArray(gfsh.getCurrentPath());
		if (indexInfoArray == null) {
			System.out.println("No index info available for " + gfsh.getCurrentPath());
		} else {
			for (int i = 0; i < indexInfoArray.length; i++) {
				IndexInfo indexInfo = indexInfoArray[i];
				System.out.println((i + 1) + ". IndexInfo:");
				System.out.println("   indexListSize = " + indexInfo.indexListSize);
				System.out.println("   indexMapSize = " + indexInfo.indexMapSize);
				System.out.println("   minSetSize = " + indexInfo.minSetSize);
				System.out.println("   maxSetSize = " + indexInfo.maxSetSize);
				System.out.println("   minSetQueryKey = " + indexInfo.minSetQueryKey);
				System.out.println("   maxSetQueryKey = " + indexInfo.maxSetQueryKey);
				System.out.println();
			}
		}
	}
	
	// Next not supported
	public List next(Object userData) throws Exception
	{
		gfsh.println("The command next is not supported for look.");
		return null;
	}
}
