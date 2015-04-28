package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.Aggregator;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;

public class index implements CommandExecutable
{
	private Gfsh gfsh;
	
	private enum TASK {
		LIST,
		CREATE,
		DELETE,
		STATS
	}
	
	private static List<String>mappableKeyList;
	private static List<String>mappableStatsList;
	
	static {
		mappableKeyList = new ArrayList();
		Collections.addAll(mappableKeyList, "Name", "Type", "Expression", "From");
		mappableStatsList = new ArrayList();
		Collections.addAll(mappableStatsList, "Name", "Type", "Expression", "From", "Keys", "Values", "Updates", "TotalUpdateTime", "TotalUses");	
	}

	public index(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}

	public void help()
	{
          gfsh.println("index [-m <member id>|-g] -n <index name> -e <expression> -from <from clause>");  
          gfsh.println("      [-i <imports>]");
          gfsh.println("      [-primary|-functional] ");
          gfsh.println("      [-r <region path>]");
          gfsh.println("      [-?]");
          gfsh.println("      Creates the specified index in the current or specified region.");
          gfsh.println("      -m <member id> Execute the index command on the specified member only");
          gfsh.println("      -g Execute the index command on all members");
          gfsh.println("      -n <index name> Unique index name.");
          gfsh.println("      -e Index expression.");
          gfsh.println("      -from <from clause>  From clause.");
          gfsh.println("      [-i <imports>] Import statments separated by ; in double quotes,");
          gfsh.println("                    e.g., -i \"import com.foo.ClassA;import com.fool.ClassB\"");
          gfsh.println("      [-primary|-functional] Create a primary index or a functional index.");
          gfsh.println("                    Default: -functional");
          gfsh.println("index [-m <member id>|-g] [-all] [-r <region path>]");
          gfsh.println("      List indexes created in the current or specified region. Default: -g");
          gfsh.println("      -stats Display index statistics along with index info.");
          gfsh.println("      -m <member id> Execute the index command on the specified member only");
          gfsh.println("      -g Execute the index command on all members");
          gfsh.println("      -all List indexes in all regions.");
          gfsh.println("      [-r <region path>] Region path. If not specified, the current region");
          gfsh.println("                         is used.");
          gfsh.println("index -stats [-m <member id>|-g] [-all] [-r <region path>]");
          gfsh.println("      Display index statistics in the current or specified region. Default: -g");
          gfsh.println("      -m <member id> Execute the index command on the specified member only");
          gfsh.println("      -g Execute the index command on all members");
          gfsh.println("      -all Display statistics for all indexes in all regions.");
          gfsh.println("index -d [-m <member id>|-g] -n <index name>|-region|-all [-r <region path>]");  
          gfsh.println("      Delete the specified index in the current or specified region. Default: -g");
          gfsh.println("      -m <member id> Execute the index command on the specified member only");
          gfsh.println("      -g Execute the index command on all members");
          gfsh.println("      -all Delete indexes in all regions.");
          gfsh.println("      -n <index name> Delete the specified index in the current or specified region.");
          gfsh.println("      -region Delete all indexes in the current or specified region.");
	}

	public void execute(String command) throws Exception
	{
		if (command.startsWith("index -?")) {
			help();
		} else {
			index(command);
		}
	}

	private void index(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);

		String regionPath = null;
		boolean all = false;
		boolean delete = false;
		boolean isRegion = false;
		String indexName = null;
		String expression = null;
		String fromClause = null;
		String imports = null;
		boolean global = false;
		String memberId = null;
		boolean isFunctionalIndex = true;
		boolean stats = false;
		for (int i = 1; i < list.size(); i++) {
			String token = list.get(i);
			if (token.equals("-all")) {
				all = true;
			} else if (token.equals("-d")) {
				delete = true;
			} else if (token.equals("-region")) {
				isRegion = true;
			} else if (token.equals("-r")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-r' requires region path");
					return;
				}
				regionPath = (String) list.get(++i);
			} else if (token.equals("-n")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-n' requires index name");
					return;
				}
				indexName = (String) list.get(++i);
			} else if (token.equals("-e")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-e' requires expression");
					return;
				}
				expression = (String) list.get(++i);
			} else if (token.equals("-from")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-from' requires from-clause");
					return;
				}
				fromClause = (String) list.get(++i);
			} else if (token.equals("-i")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-i' requires imports");
					return;
				}
				imports = (String) list.get(++i);
			} else if (token.equals("-m")) {
				if (i + 1 >= list.size()) {
					gfsh.println("Error: '-m' requires member Id");
					return;
				}
				memberId = (String) list.get(++i);
			} else if (token.equals("-g")) {
				global = true;
			} else if (token.equals("-primary")) {
				isFunctionalIndex = false;
			} else if (token.equals("-functional")) {
				isFunctionalIndex = true;
			} else if (token.equals("-stats")) {
				stats = true;
			} else {
				gfsh.println("Error: invalid directive '" + token + "'");
				return;
			}
		}
		
		if (global && memberId != null) {
			gfsh.println("Error: only one option is allowed: '-g' or '-m'");
			return;
		}
		if (delete && stats) {
			gfsh.println("Error: only one option is allowed: '-d' or '-stats'");
			return;
		}

		TASK task = TASK.LIST;
		if (delete) {
			task = TASK.DELETE;
		} else if (indexName == null) {
			task = TASK.LIST;
		} else if (stats) {
			task = TASK.STATS;
		} else {
			task = TASK.CREATE;
		}
	
		
		switch (task) {
		case LIST:
			listIndexes(regionPath, memberId, all, stats);
			break;
		case CREATE:
			createIndexes(regionPath, memberId, indexName, isFunctionalIndex, expression, fromClause, imports);
			break;
		case DELETE:
			if (indexName != null && (all || isRegion)) {
				gfsh.println("Error: '-n' not allowed with '-region' or '-all'");
				return;
			} 
			if (indexName == null && all == false && isRegion == false) {
				gfsh.println("Error: '-d' requires '-n', '-region' or '-all'");
				return;
			}
			com.gemstone.gemfire.internal.tools.gfsh.app.function.command.index.DeleteType deleteType;
			if (all) {
				deleteType = com.gemstone.gemfire.internal.tools.gfsh.app.function.command.index.DeleteType.DELETE_ALL_INDEXES;
			} else if (isRegion) {
				deleteType = com.gemstone.gemfire.internal.tools.gfsh.app.function.command.index.DeleteType.DELETE_REGION_INDEXES;
			} else {
				deleteType = com.gemstone.gemfire.internal.tools.gfsh.app.function.command.index.DeleteType.DELETE_INDEX;
			}
			deleteIndexes(deleteType, regionPath, memberId, indexName);
			break;
		}
		
	}
	
	private void listIndexes(String regionPath, String memberId, boolean isAll, boolean isStats) throws Exception
	{
		// Collect indexes from all members and display common and different sets
		String currentPath = gfsh.getCurrentPath();
		if (regionPath == null) {
			regionPath = currentPath;
		}
		String fullPath = gfsh.getFullPath(regionPath, currentPath);
	
		Aggregator aggregator = gfsh.getAggregator();
		long startTime = System.currentTimeMillis();
		List<AggregateResults> results = (List<AggregateResults>) gfsh.getAggregator().aggregate(
				new GfshFunction("index", fullPath, new Object[] { "-list", memberId, isAll, isStats }), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();
		int i = 0;
		for (AggregateResults aggregateResults : results) {
			GfshData data = (GfshData)aggregateResults.getDataObject();
			if (data != null) {
				Object obj = data.getDataObject();
				if (obj instanceof List) {
					List<Mappable> mappableList = (List<Mappable>)data.getDataObject();
					if (mappableList != null) {
						gfsh.println(++i + ". " + data.getMemberInfo().getMemberId() + " (" + data.getMemberInfo().getMemberName() + "): ");
						if (mappableList.size() > 0) {
							Mappable mappable = mappableList.get(0);
							if (mappable.size() < mappableStatsList.size()) {
								isStats = false;
							}
							if (isStats) {
								PrintUtil.printMappableList(mappableList, "Name", mappableStatsList);
							} else {
								PrintUtil.printMappableList(mappableList, "Name", mappableKeyList);
							}
						}
						gfsh.println();
					}
				} else if (obj != null) {
					gfsh.println(++i + ". " + data.getMemberInfo().getMemberId() + " (" + data.getMemberInfo().getMemberName() + "): " + obj);
					gfsh.println();
				}
			}
		}
		if (i == 0) {
			gfsh.println("Indexes not found");
			gfsh.println();
		}
	}

	/**
	 * 
	 * @param regionPath
	 * @param memberId  If null, creates index on all members.
	 * @param indexName
	 * @param isFunctionalIndex
	 * @param expression
	 * @param fromClause
	 * @throws Exception
	 */
	private void createIndexes(String regionPath, 
			String memberId, 
			String indexName, 
			boolean isFunctionalIndex, 
			String expression, 
			String fromClause,
			String imports) throws Exception
	{
		
		if (indexName == null) {
			gfsh.println("Error: '-n' (index name) is not specified.");
			return;
		}
		if (expression == null) {
			gfsh.println("Error: '-e' (index expression) is not specified.");
			return;
		}
		if (fromClause == null) {
			gfsh.println("Error: '-from' (from clause) is not specified.");
			return;
		}
		
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(regionPath, currentPath);

		Aggregator aggregator = gfsh.getAggregator();
		long startTime = System.currentTimeMillis();
		List<AggregateResults> results = (List<AggregateResults>) gfsh.getAggregator().aggregate(
				new GfshFunction("index", fullPath, new Object[] { "-create", memberId, indexName, isFunctionalIndex, expression, fromClause, imports }), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();

		int i = 0;
		for (AggregateResults aggregateResults : results) {
			
			GfshData data = (GfshData) aggregateResults.getDataObject();
			if (data == null) {
				i++;
				gfsh.println(i + ". " + aggregateResults.getCodeMessage());
			} else {
				MemberInfo memberInfo = data.getMemberInfo();
				Object value = data.getDataObject();
				if (value != null) {
					i++;
					gfsh.println(i + ". " + memberInfo.getMemberId() + " (" + memberInfo.getMemberName() + "): " + value);	
				} else if (aggregateResults.getCodeMessage() != null) {
					i++;
					gfsh.print(i + ". " + memberInfo.getMemberId() + " (" + memberInfo.getMemberName() + "): ");
					gfsh.println(aggregateResults.getCodeMessage());
				}
			}
		}
		gfsh.println();
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}
	
	private void deleteIndexes(com.gemstone.gemfire.internal.tools.gfsh.app.function.command.index.DeleteType deleteType, String regionPath, String memberId, String indexName) throws Exception
	{
		String message = "";
		switch (deleteType) {
		case DELETE_INDEX:
		case DELETE_REGION_INDEXES:
			if (regionPath == null) {
				regionPath = gfsh.getCurrentPath();
			}
			break;
		}
		
		// Collect indexes from all members and display common and different sets
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(regionPath, currentPath);

		switch (deleteType) {
		case DELETE_INDEX:
			message = "This command will remove the " + indexName + " index from the " + fullPath + "region. \nDo you want to proceed? (yes|no): ";
			break;
		case DELETE_REGION_INDEXES:
			message = "This command will remove all of the indexes in the " + fullPath + " region. \nDo you want to proceed? (yes|no): ";
			break;
		case DELETE_ALL_INDEXES:
			message = "This command will remove all of the indexes from all of the members. \nDo you want to proceed? (yes|no): ";
			break;
		}
		String confirmation = gfsh.getLine(message);
		if (confirmation.equalsIgnoreCase("yes") == false) {
			gfsh.println("Command aborted.");
			return;
		}
		
		Aggregator aggregator = gfsh.getAggregator();
		long startTime = System.currentTimeMillis();
		List<AggregateResults> results = (List<AggregateResults>) gfsh.getAggregator().aggregate(
				new GfshFunction("index", fullPath, new Object[] { "-delete", deleteType, memberId, indexName }), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();
		
		int i = 0;
		for (AggregateResults aggregateResults : results) {
			
			GfshData data = (GfshData) aggregateResults.getDataObject();
			if (data == null) {
				i++;
				gfsh.println(i + ". " + aggregateResults.getCodeMessage());
			} else {
				MemberInfo memberInfo = data.getMemberInfo();
				Object value = data.getDataObject();
				if (value != null) {
					i++;
					gfsh.print(i + ". " + memberInfo.getMemberId() + " (" + memberInfo.getMemberName() + "): ");
					gfsh.println(value);
				}
			}
		}
		gfsh.println();
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}

}
