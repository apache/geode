package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.Aggregator;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

public class pr implements CommandExecutable
{
	private Gfsh gfsh;

	public pr(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("pr -b | -?");
//		gfsh.println("pr [-k <member number> | -m <member id>] select <tuples where ...> | -?");
//		gfsh.println("     Execute the specified query in the optionally specified member.");
//		gfsh.println("           The query is executed on the local dataset of the member");
//		gfsh.println("           if the '-m' or '-k' option is specified.");
//		gfsh.println("     -k <member number>  Execute the query on the specified member identified");
//		gfsh.println("           by the member number. The member numbers are the row numbers shown");
//		gfsh.println("           in the member list displayed by executing 'size -m' or 'ls -m'.");
//		gfsh.println("           Note that the query is executed on the local");
//		gfsh.println("           dataset of the member if this options is specified.");
//		gfsh.println("     -m <member id>  Execute the query on the specified member identified");
//		gfsh.println("           the member id. The member Ids can be obtained by executing"); 
//		gfsh.println("           'size -m' or 'ls -m'. Note that the query is executed on the local");
//		gfsh.println("           data set of the member if this options is specified.");
		gfsh.println("     -b    Display partitioned region bucket information");
		
		gfsh.println();
	}
	
	private void usage()
	{
//		gfsh.println("pr [-k <member number> | -m <member id>] select <tuples where ...> | -?");
		gfsh.println("pr -b | -?");
	}
	
	public void execute(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			usage();
			return;
		}
		if (list.contains("-?")) {
			help();
		} else if (command.contains("-b")) {
			pr_b(command);
		} else {
			String queryString = command;
			
			pr(command, true);
		}
	}
	
	private void pr_b(String command) throws Exception
	{
		String regionPath = gfsh.getCurrentPath();

		Aggregator aggregator = gfsh.getAggregator();
		long startTime = System.currentTimeMillis();
		List<AggregateResults> results = (List<AggregateResults>) gfsh.getAggregator().aggregate(
				new GfshFunction("pr", regionPath, new Object[] { "-b" }), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();

		int primaryBucketCount = 0;
		int redundantBucketCount = 0;
		int totalNumBuckets = 0;
		int i = 0;
		for (AggregateResults aggregateResults : results) {
			GfshData data = (GfshData) aggregateResults.getDataObject();
			totalNumBuckets = (Integer)data.getUserData();
//			if (aggregateResults.getCode() == AggregateResults.CODE_ERROR) {
//				gfsh.println("Error: " + aggregateResults.getCodeMessage());
//				if (gfsh.isDebug() && aggregateResults.getException() != null) {
//					aggregateResults.getException().printStackTrace();
//				}
//				break;
//			}
			Object value = data.getDataObject();
			if (value != null) {
				
//				if (simulate) {
//					columnName = "Simulated Stats";
//				} else {
//					columnName = "Rebalanced Stats";
//				}
				MemberInfo memberInfo = data.getMemberInfo();
				Map map = (Map) value;
//				Map primaryMap = (Map)map.get("Primary");
				List<Mappable> primaryList = (List<Mappable>)map.get("Primary");
				i++;
//				gfsh.println(i + ". " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
				gfsh.println(i + ". Primary Buckets - " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
				PrintUtil.printMappableList(primaryList, "BucketId");
				gfsh.println();
//				Map redundantMap = (Map)map.get("Redundant");
				List<Mappable> redundantList = (List<Mappable>)map.get("Redundant");
				gfsh.println(i + ". Redundant Buckets - " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
				PrintUtil.printMappableList(redundantList, "BucketId");
				gfsh.println();
				
				primaryBucketCount += primaryList.size();
				redundantBucketCount += redundantList.size();
			}
		}
		gfsh.println();
		gfsh.println("   Primary Bucket Count: " + primaryBucketCount);
		gfsh.println(" Redundant Bucket Count: " + redundantBucketCount);
		gfsh.println("total-num-buckets (max): " + totalNumBuckets);
		
		gfsh.println();
		
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}
	
	private void printMappableList(MemberInfo memberInfo, List<Mappable> list, int row) throws Exception
	{
		String columnName = "Bucket";
		if (list != null) {
			gfsh.println(row + ". " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
			PrintUtil.printMappableList(list);
			gfsh.println();
		}
	}

//	private void printMap(MemberInfo memberInfo, Map map, int row) throws Exception
//	{
//		String columnName = "Bucket";
//		Set<Map.Entry> entrySet = map.entrySet();
//		if (map != null && map.size() > 0) {
//			gfsh.println(row + ". " + memberInfo.getMemberName() + " (" + memberInfo.getMemberId() + ")");
//			PrintUtil.printEntries(map, map.size(), null, columnName, "Value", false, gfsh.isShowResults());
//			gfsh.println();
//		}
//	}
	
	public List getRemoteKeys(String regionPath) throws Exception
	{
		List list = pr("select e.key from " + regionPath + ".entries e", true);	
		return list;
	}
	
	public List pr(String queryString, boolean nextEnabled) throws Exception
	{
		long startTime = System.currentTimeMillis();
		CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(queryString, gfsh.getFetchSize(), nextEnabled, true));
		long stopTime = System.currentTimeMillis();
		if (cr.getCode() == QueryTask.ERROR_QUERY) {
			gfsh.println(cr.getCodeMessage());
			return null;
		}
		QueryResults results = (QueryResults) cr.getDataObject();
		if (results == null) {
			gfsh.println("No results");
			return null;
		}

		List list = null;
		Object obj = results.getResults();
		if (obj instanceof SelectResults) {
			SelectResults sr = (SelectResults) results.getResults();
			list = sr.asList();
			int startRowNum = results.getReturnedSize() - sr.size() + 1;
			if (gfsh.isShowResults()) {
				int rowsPrinted = PrintUtil.printSelectResults(sr, 0, startRowNum, sr.size());
				gfsh.println("Fetch size: " + gfsh.getFetchSize());
				gfsh.println("   Results: " + sr.size()
						+ ", Returned: " + results.getReturnedSize() + "/" + results.getActualSize());
				next n = (next)gfsh.getCommand("next");
				
				// route the next command to select, which has the display routine
				n.setCommand("select");
			} else {
				gfsh.println("Fetch size: " + gfsh.getFetchSize());
				gfsh.println("   Results: " + sr.size() + 
						", Returned: " + results.getReturnedSize() + "/" + results.getActualSize());
			}
		} else {
			gfsh.println("Results: " + obj);
		}
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
		return list;
	}
}
