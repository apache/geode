package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.ObjectUtil;

public class rm implements CommandExecutable
{
	private Gfsh gfsh;
	
	public rm(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("rm [-a|-g] [-k] <key>|<enum list>");
		gfsh.println("     Remove keys locally and/or remotely. If no options are specified,");
		gfsh.println("     it removes <key> from the local region only.");
		gfsh.println("     -a Remove keys from both the local region and the server");
		gfsh.println("        region. This command will be distributed to other caches if");
        gfsh.println("        scope is not Scope.LOCAL.");
		gfsh.println("     -g Remove keys globally. Remote from the local region and all");
		gfsh.println("        server regions regardless of scope. This option also removes");
		gfsh.println("        keys from server regions with Scope.LOCAL.");
		gfsh.println("     -k Remove enumerated keys. If this option is not specified, then");
		gfsh.println("        <key> is expected.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("rm -?")) {
			help();
		} else {
			rm(command);
		}
	}
	
	private void rm(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 1) {
			regionPath = (String)list.get(1);
		} else {
			gfsh.println("Error: must specify a region path to remove");
			return;
		}
		
		boolean enumerated = false;
		boolean removeAll = false;
		boolean removeServer = false;
		
		String val;
		int keyIndex = 0;
		for (int i = 1; i < list.size(); i++) {
			val = list.get(i);
			if (val.equals("-a")) {
				removeServer = true;
			} else if (val.equals("-g")) {
				removeAll = true;
			} else if (val.equals("-k")) {
				enumerated = true;
			} else {
				keyIndex = i;
				break;
			}
		}
		
		Region region = gfsh.getCurrentRegion();
		String numbers;
		Object key;
		if (removeServer) {
			
			if (enumerated) {
				Map keyMap = gfsh.getKeyMap(list, keyIndex);
				Object keys[] = keyMap.values().toArray();
				for (Object k: keyMap.values()) {
					region.remove(k);
					gfsh.println("removed server: " + ObjectUtil.getPrintableObject(k));
				}
				
			} else {
				key = gfsh.getQueryKey(list, keyIndex);
				region.remove(key);
				gfsh.println("removed server: " + ObjectUtil.getPrintableObject(key));
			}
			
		} else if (removeAll) {
			
			if (enumerated) {
				Map keyMap = gfsh.getKeyMap(list, keyIndex);
				Object keys[] = keyMap.values().toArray();

				boolean serverError = false;
				List<AggregateResults> results = (List<AggregateResults>)gfsh.getAggregator().aggregate(new GfshFunction(command, gfsh.getCurrentPath(), keys), gfsh.getAggregateRegionPath());
				for (Object k: keyMap.values()) {
					try {
						region.localDestroy(k);
					} catch (Exception ex) {
						// ignore
					}
					gfsh.println("removed all: " + ObjectUtil.getPrintableObject(k));
				}
				for (AggregateResults aggregateResults : results) {
					if (aggregateResults.getCode() == AggregateResults.CODE_ERROR) {
						gfsh.println("Error from server: " + aggregateResults.getCodeMessage());
					}
				}
				if (serverError) {
					gfsh.println("Error: One or more keys may have not been removed from the server(s)");
				}
			} else {
				key = gfsh.getQueryKey(list, keyIndex);
				
				List<AggregateResults> results = (List<AggregateResults>)gfsh.getAggregator().aggregate(new GfshFunction(command, gfsh.getCurrentPath(), new Object[] { key }), gfsh.getAggregateRegionPath());
				try {
					region.localDestroy(key);
				} catch (Exception ex) {
					// ignore
				}
				boolean serverError = false;
				for (AggregateResults aggregateResults : results) {
					if (aggregateResults.getCode() == AggregateResults.CODE_ERROR) {
						gfsh.println("Error from server: " + aggregateResults.getCodeMessage());
						serverError = true;
					}
				}
				if (serverError) {
					gfsh.println("Error: One or more keys may have not been removed from the server(s)");
				} else {
					gfsh.println("removed: " + ObjectUtil.getPrintableObject(key));
				}
			}

		} else {
			// remove local
			if (enumerated) {
				Map keyMap = gfsh.getKeyMap(list, keyIndex);
				for (Object k: keyMap.values()) {
					// remove local
					try {
						region.localDestroy(k);
						gfsh.println("removed local: " + ObjectUtil.getPrintableObject(k));
					} catch (EntryNotFoundException ex) {
						gfsh.println("local (not found): " + ObjectUtil.getPrintableObject(k));
					}
					
				}
			} else {
				key = gfsh.getQueryKey(list, keyIndex);
				// remove local
				try {
					region.localDestroy(key);
					gfsh.println("removed local: " + ObjectUtil.getPrintableObject(key));
				} catch (EntryNotFoundException ex) {
					gfsh.println("local (not found): " + ObjectUtil.getPrintableObject(key));
				}
			}
		}
	}
	
	
}
