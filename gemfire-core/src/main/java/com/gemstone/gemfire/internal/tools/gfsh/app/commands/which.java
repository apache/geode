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

public class which implements CommandExecutable
{
	private Gfsh gfsh;

	public which(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}

	public void help()
	{
		gfsh.println("which [-p <region path>] [-r] <query predicate> | -k <number> | [-?]");
		gfsh.println("     Show the members and regions that have the specified key.");
		gfsh.println("     -p <region path> The region path in which to find the specified key.");
		gfsh.println("     -r Search recursively. It search all of the subregions including");
		gfsh.println("        the specified region or the current region if not specified.");
		gfsh.println("     -k <number>   Use an enumerated key. Use 'ls -k' to get the list");
		gfsh.println("            of enumerated keys. Only one key number is supported.");
		gfsh.println("     <query predicate>: field=val1 and field2='val1' \\");
		gfsh.println("                        and field3=to_date('<date>', '<format>'");
		gfsh.println("     Data formats: primitives, String, and java.util.Date");
		gfsh.println("         <decimal>b|B - Byte      (e.g., 1b)");
		gfsh.println("         <decimal>c|C - Character (e.g., 1c)");
		gfsh.println("         <decimal>s|S - Short     (e.g., 12s)");
		gfsh.println("         <decimal>i|I - Integer   (e.g., 15 or 15i)");
		gfsh.println("         <decimal>l|L - Long      (e.g., 20l");
		gfsh.println("         <decimal>f|F - Float     (e.g., 15.5 or 15.5f)");
		gfsh.println("         <decimal>d|D - Double    (e.g., 20.0d)");
		gfsh.println("         '<string with \\ delimiter>' (e.g., '\\'Wow!\\'!' Hello, world')");
		gfsh.println("         to_date('<date string>', '<simple date format>'");
		gfsh.println("                       (e.g., to_date('04/10/2009', 'MM/dd/yyyy')");
		gfsh.println("Examples: ");
		gfsh.println("      which 'string'  -- string key");
		gfsh.println("      which 10f  -- float key");
		gfsh.println("      which -k 1");
		gfsh.println("      which -p /foo/yong -r x=10.0 and y=1 and date=to_date('04/10/2009', 'MM/dd/yyyy')");
		gfsh.println();
	}

	public void execute(String command) throws Exception
	{
		if (command.startsWith("which -?")) {
			help();
		} else {
			which(command);
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

	private void which(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("Error: 'which' requires a query predicate or key number");
			return;
		}

		String regionPath = gfsh.getCurrentPath();
		boolean recursive = false;
		Object key = null;
		for (int i = 1; i < list.size(); i++) {
			String token = list.get(i);
			if (token.equals("-p")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-p' requires region path");
					return;
				}
				regionPath = list.get(++i);
			} else if (token.equals("-r")) {
				recursive = true;
			} else if (token.equals("-k")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-k' requires key number");
					return;
				}
				int keyNum = Integer.parseInt((String) list.get(++i));
				key = gfsh.getKeyFromKeyList(keyNum);
				break;
			} else {
				int inputIndex = i;
				key = getKeyFromInput(list, inputIndex);
				break;
			}
		}

		if (key == null) {
			gfsh.println("Error: Key is not defined.");
			return;
		}
		executeWhich(regionPath, key, recursive);
	}

	private void executeWhich(String regionPath, Object key, boolean recursive) throws Exception
	{
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(regionPath, currentPath);

		Aggregator aggregator = gfsh.getAggregator();
		long startTime = System.currentTimeMillis();
		List<AggregateResults> results = (List<AggregateResults>) gfsh.getAggregator().aggregate(
				new GfshFunction("which", fullPath, new Object[] { key, recursive }), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();

		int i = 0;
		for (AggregateResults aggregateResults : results) {
			GfshData data = (GfshData) aggregateResults.getDataObject();
			if (aggregateResults.getCode() == AggregateResults.CODE_ERROR) {
				gfsh.println("Error: " + aggregateResults.getCodeMessage());
				if (gfsh.isDebug() && aggregateResults.getException() != null) {
					aggregateResults.getException().printStackTrace();
				}
				break;
			}
			Object value = data.getDataObject();
			if (value != null) {
				MemberInfo memberInfo = data.getMemberInfo();
				Map map = (Map) value;
				Set<Map.Entry> entrySet = map.entrySet();
				if (map != null && map.size() > 0) {
					i++;
					gfsh.print(i + ". " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
					Object obj = data.getUserData();
					if (obj != null) {
						if (obj instanceof Map) {
							Map infoMap = (Map)obj;
							boolean isPrimary = (Boolean)infoMap.get("IsPrimary");
							int bucketId = (Integer)infoMap.get("BucketId");
							if (isPrimary) {
								gfsh.println(" -- BucketId=" + bucketId + " *Primary PR*");
							} else {
								gfsh.println(" -- BucketId=" + bucketId);
							}
						}
					} else {
						gfsh.println();
					}
					PrintUtil.printEntries(map, map.size(), null, "Region", "Value", false, gfsh.isShowResults());
					gfsh.println();
				}
			}
		}
		if (i == 0) {
			gfsh.println("Key is not found.");
		}
		gfsh.println();
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}

}
