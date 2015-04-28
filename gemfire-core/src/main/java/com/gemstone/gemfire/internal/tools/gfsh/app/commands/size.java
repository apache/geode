package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.functions.util.LocalRegionInfoFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionSizeTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.StringUtil;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

public class size implements CommandExecutable
{
	private Gfsh gfsh;
	
	public size(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("size [-m|-s] | [-?] <region path>");
		gfsh.println("     Display the local and server region sizes. If no option");
		gfsh.println("     is provided then it displays the local region size");
		gfsh.println("     -m List all server member region sizes.");
		gfsh.println("     -s Display the region size of the connected server.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("size -?")) {
			help();
		} else if (command.startsWith("size -m")) {
			size_m(command);
		} else if (command.startsWith("size -s")) {
			size_s(command);
		} else {
			size(command);
		}
	}
	
	// local region size
	private void size(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath;
		if (list.size() == 1) {
			regionPath = gfsh.getCurrentPath();
		} else if (list.size() == 2) {
			regionPath = (String)list.get(1);
		} else {
			regionPath = (String)list.get(2);
		}
	
		regionPath = regionPath.trim();
		if (regionPath.equals("/")) {
			gfsh.println("Error: Invalid path. Root path not allowed.");
			return;
		}
		
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
		
		gfsh.println("            Region: " + regionPath);
		
		// Local region
		Cache cache = gfsh.getCache();
		Region region = cache.getRegion(regionPath);
		if (region == null) {
			gfsh.println("Error: region undefine - " + regionPath);
		} else {
			gfsh.println(" Local region size: " + region.size());
		}
	}
	
	private void size_m(String command) throws Exception
	{
		if (gfsh.getAggregateRegionPath() == null) {
			gfsh.println("Error: The aggregate region path is not specified. Use the command ");
			gfsh.println("'connect -a <region path>' to specify any existing partitioned region path in the server.");
			return;
		}
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath;
		if (list.size() == 2) {
			regionPath = gfsh.getCurrentPath();
		} else {
			regionPath = (String) list.get(2);
		}
		
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
		
		if (regionPath.equals("/")) {
			gfsh.println("Error: invalid region \"/\". Change to a valid region or specify the region path, i.e. size -a /foo");
			return;
		}

		long startTime = System.currentTimeMillis();
		List<MapMessage> resultList = (List<MapMessage>)gfsh.getAggregator().aggregate(new LocalRegionInfoFunction(regionPath), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();
		
		String memberIdHeader = "Member Id";
		String memberNameHeader = "Member Name";
		String regionSizeHeader = "Region Size";
		
		
		// Find the max string sizes
		int memberIdMax = memberIdHeader.length();
		int memberNameMax = memberNameHeader.length();
		int regionSizeMax = regionSizeHeader.length();
		boolean isPR = false;
		boolean isPeerClient = false;
		String returnedRegionPath = null;
		HashMap<String, Mappable> infoMap = new HashMap<String, Mappable>();
		for (int i = 0; i < resultList.size(); i++) {
			Mappable info = resultList.get(i);
			try {
				if (info.getByte("Code") == AggregateResults.CODE_ERROR) {
					gfsh.println("Error: " + info.getString("CodeMessage"));
					return;
				}
			} catch (Exception ex) {
				// ignore
			}

			isPR = info.getBoolean("IsPR");
			if (isPR) {
				try {
					isPeerClient = info.getBoolean("IsPeerClient");
				} catch (Exception ex) {
					continue;
				}
				if (isPeerClient) {
					continue;
				}
			}
			
			returnedRegionPath = info.getString("RegionPath");
			String memberId = info.getString("MemberId");
			if (memberIdMax < memberId.length()) {
				memberIdMax = memberId.length();
			}
			String memberName = info.getString("MemberName");
			if (memberName != null && memberNameMax < memberName.length()) {
				memberNameMax = memberName.length();
			}
			String val = Integer.toString(info.getInt("RegionSize"));
			if (regionSizeMax < val.length()) {
				regionSizeMax = val.length();
			}
			infoMap.put(info.getString("MemberId"), info);
		}
		
		ArrayList keyList = new ArrayList(infoMap.keySet());
		Collections.sort(keyList);
		
		
		// display
		gfsh.println("     Region: " + returnedRegionPath);
		if (isPR) {
			gfsh.println("Region Type: Partitioned");
		} else {
			gfsh.println("Region Type: Replicated");
		}
		gfsh.print(StringUtil.getRightPaddedString(memberIdHeader, memberIdMax, ' '));
		gfsh.print("  ");
		gfsh.print(StringUtil.getRightPaddedString(memberNameHeader, memberNameMax, ' '));
		gfsh.print("  ");
		gfsh.println(regionSizeHeader);
		gfsh.print(StringUtil.getRightPaddedString("---------", memberIdMax, ' '));
		gfsh.print("  ");
		gfsh.print(StringUtil.getRightPaddedString("-----------", memberNameMax, ' '));
		gfsh.print("  ");
		gfsh.println(StringUtil.getRightPaddedString("-----------", regionSizeMax, ' '));
		
		int totalRegionSize = 0;
		for (int i = 0; i < keyList.size(); i++) {
			Mappable info = infoMap.get(keyList.get(i));
			try {
				if (info.getByte("Code") == AggregateResults.CODE_ERROR) {
					gfsh.println("Error: " + info.getString("CodeMessage"));
					return;
				}
			} catch (Exception ex) {
				// ignore
			}
			isPR = info.getBoolean("IsPR");
			gfsh.print(StringUtil.getRightPaddedString(info.getString("MemberId"), memberIdMax, ' '));
			gfsh.print("  ");
			gfsh.print(StringUtil.getRightPaddedString(info.getString("MemberName"), memberNameMax, ' '));
			gfsh.print("  ");
			gfsh.println(StringUtil.getLeftPaddedString(Integer.toString(info.getInt("RegionSize")), regionSizeMax, ' '));
			totalRegionSize += info.getInt("RegionSize");
		}
		
		gfsh.println();
		if (isPR) {
			gfsh.print(StringUtil.getLeftPaddedString("Total: ", 
					memberIdMax + memberNameMax + 2*2, ' '));
			gfsh.println(StringUtil.getLeftPaddedString(Integer.toString(totalRegionSize), regionSizeMax, ' '));
		}
		gfsh.println();
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}
	
	private void size_s(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath;
		if (list.size() == 2) {
			regionPath = gfsh.getCurrentPath();
		} else {
			regionPath = (String) list.get(2);
		}
		regionPath = regionPath.trim();
		if (regionPath.equals("/")) {
			gfsh.println("Error: Invalid path. Root path not allowed.");
			return;
		}
		
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
		
		gfsh.println("            Region: " + regionPath);
		
		// Local region
		Cache cache = gfsh.getCache();
		Region region = cache.getRegion(regionPath);
		if (region == null) {
			gfsh.println("Error: region undefine - " + regionPath);
		} else {
			gfsh.println(" Local region size: " + region.size());
		}
		
		// Server region
		CommandResults results = gfsh.getCommandClient().execute(new RegionSizeTask(regionPath));
		Object obj = results.getDataObject();
		if (obj == null) {
			gfsh.println("Error: Unable to get size from the server - " + results.getCodeMessage());
		} else {
			ArrayList sizeList = new ArrayList(2);
			sizeList.add(results.getDataObject());
			PrintUtil.printMappableList(sizeList);
		}
	}
	
	
//	private void size(String command) throws Exception
//	{
//		LinkedList list = new LinkedList();
//		gfsh.parseCommand(command, list);
//
//		if (list.size() < 2) {
//			gfsh.println("Error: size requires <query predicate>");
//		} else {
//			Object queryKey = gfsh.getQueryKey(list);
//			if (queryKey == null) {
//				return;
//			}
//			int size = gfsh.getLookupService().size(gfsh.getQueryRegionPath(), queryKey);
//			gfsh.println("Size: " + size);
//		}
//
//	}

}
