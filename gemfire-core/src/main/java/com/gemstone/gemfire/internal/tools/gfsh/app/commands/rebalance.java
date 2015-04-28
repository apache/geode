package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.Aggregator;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;

public class rebalance implements CommandExecutable
{
	private Gfsh gfsh;

	public rebalance(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}

	public void help()
	{
//		gfsh.println("rebalance -k <member number> | -m <member id> [-s|-r] | [-?]");
		gfsh.println("rebalance -m <member id> [-s|-r] [-t <timeout in msec>] | [-?]");
		gfsh.println("     Rebalance partition regions held by the specified member.");
		gfsh.println("     By default, gfsh immediately returns after the rebalance command");
		gfsh.println("     execution. To determine the completion of rebalancing, excute");
		gfsh.println("     'size -m' or 'pr -b'. To wait for the rebalancing to complete,");
		gfsh.println("     supply the '-t' option with the timeout interval in msec.");
//		gfsh.println("     -k <member number>  Rebalance the specified member identified");
//		gfsh.println("           by the member number. The member numbers are the row numbers shown");
//		gfsh.println("           in the member list displayed by executing 'size -m' or 'ls -m'.");
		gfsh.println("     -m <member id>  Execute the rebalance command on the specified member.");
		gfsh.println("           The member Ids can be obtained by executing 'size -m' or 'ls -m'."); 
		gfsh.println("     -s Simulate rebalancing. Actual rebalancing is NOT performed.");
		gfsh.println("     -r Rebalance. Actual rebalancing is performed.");
		gfsh.println("     -t <timeout in msec> Timeout rebalbancing after the specified");
		gfsh.println("          time interval. Rebalancing will continue but gfsh will");
		gfsh.println("          timeout upon reaching the specified time interval. This option");
		gfsh.println("          must be used with the '-r' option. It has no effect with other");
		gfsh.println("          options.");
	}

	public void execute(String command) throws Exception
	{
		if (command.startsWith("rebalance -?")) {
			help();
		} else {
			rebalance(command);
		}
	}

	private Object getKeyFromInput(List list, int index) throws Exception
	{
		String input = (String) list.get(index);
		Object key = null;
		if (input.startsWith("'")) {
			int lastIndex = -1;
			if (input.endsWith("'") == false) {
				lastIndex = input.length();
			} else {
				lastIndex = input.lastIndexOf("'");
			}
			if (lastIndex <= 1) {
				gfsh.println("Error: Invalid key. Empty string not allowed.");
				return null;
			}
			key = input.substring(1, lastIndex); // lastIndex exclusive
		} else {
			key = gfsh.getQueryKey(list, index);
		}
		return key;
	}

	private void rebalance(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("Error: 'rebalance' requries a key number or member id");
			return;
		}

		String regionPath = gfsh.getCurrentPath();
		boolean simulate = true;
		Object key = null;
		String memberId = null;
		long timeout = 0;
		for (int i = 1; i < list.size(); i++) {
			String token = list.get(i);
			if (token.equals("-k")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-k' requires key number");
					return;
				}
				int keyNum = Integer.parseInt((String) list.get(++i));
				key = gfsh.getKeyFromKeyList(keyNum);

			} else if (token.equals("-m")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-m' requires member Id");
					return;
				}
				memberId = (String) list.get(++i);
			} else if (token.equals("-s")) {
				simulate = true;
			} else if (token.equals("-r")) {
				simulate = false;
			} else if (token.equals("-t")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-t' requires a timeout value");
					return;
				}
				timeout = Integer.parseInt(list.get(++i));
			}
		}

		if (key == null && memberId == null) {
			gfsh.println("Error: member Id not defined.");
			return;
		}
		
		
		// Execute rebalance
		executeRebalance(regionPath, memberId, simulate, timeout);
	}

	private void executeRebalance(String regionPath, String memberId, boolean simulate, long timeout) throws Exception
	{
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(regionPath, currentPath);

		Aggregator aggregator = gfsh.getAggregator();
		long startTime = System.currentTimeMillis();
		List<AggregateResults> results = (List<AggregateResults>) gfsh.getAggregator().aggregate(
				new GfshFunction("rebalance", regionPath, new Object[] { memberId, simulate, timeout }), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();

		int i = 0;
		for (AggregateResults aggregateResults : results) {
			GfshData data = (GfshData) aggregateResults.getDataObject();
//			if (aggregateResults.getCode() == AggregateResults.CODE_ERROR) {
//				gfsh.println("Error: " + aggregateResults.getCodeMessage());
//				if (gfsh.isDebug() && aggregateResults.getException() != null) {
//					aggregateResults.getException().printStackTrace();
//				}
//				break;
//			}
			MemberInfo memberInfo = data.getMemberInfo();
			Object value = data.getDataObject();
			if (value != null) {
				String columnName;
				if (simulate) {
					columnName = "Simulated Stats";
				} else {
					columnName = "Rebalanced Stats";
				}
				
				Map map = (Map) value;
				Set<Map.Entry> entrySet = map.entrySet();
				if (map != null && map.size() > 0) {
					i++;
					gfsh.println(i + ". " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
					PrintUtil.printEntries(map, map.size(), null, columnName, "Value", false, gfsh.isShowResults());
					gfsh.println();
				}
			} else if (memberId.equals(memberInfo.getMemberId())) {
				if (simulate == false) {
					gfsh.println("Reblancing has been completed or is being performed by " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
					gfsh.println("Use 'size -m' or 'pr -b' to view rebalance completion.");
					gfsh.println();
				}
			}
		}
		gfsh.println();
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}

}
