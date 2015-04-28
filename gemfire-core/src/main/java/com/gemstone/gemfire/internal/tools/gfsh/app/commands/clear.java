package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.Aggregator;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.CommandClient;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionClearTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

public class clear implements CommandExecutable
{
	private Gfsh gfsh;
	
	public clear(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("clear [-a|-g|-s] [<region path>] | [-?]");
		gfsh.println("     Clear the local region. If <region path> is not specified");
		gfsh.println("     then it clears the current region. The region path");
		gfsh.println("     can be absolute or relative.");
		gfsh.println("     -a Clear both the local region and the server region.");
		gfsh.println("        The region clear will be distributed to other caches if the");
		gfsh.println("        scope is not Scope.LOCAL.");
		gfsh.println("     -g Clear globally. Clear the local region and all server");
		gfsh.println("        regions regardless of scope. This option also clears server");
		gfsh.println("        regions with Scope.LOCAL. Use this option to clear partioned");
		gfsh.println("        regions. GFE 5.7 partitioned region is not supported.");
		gfsh.println("     -s Clear only the server region. The local region is not cleared.");
		gfsh.println("        The region clear will be distributed to other caches if the");
		gfsh.println("        scope is not Scope.LOCAL.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("clear -?")) {
			help();
		} else if (command.startsWith("clear -a")) {
			clear_a(command);
		} else if (command.startsWith("clear -g")) {
			clear_g(command);
		} else if (command.startsWith("clear -s")) {
			clear_s(command);
		} else {
			clear_local(command);
		}
	}
	
	private void clear_local(String command)
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 1) {
			regionPath = (String)list.get(1);
		} else {
			regionPath = gfsh.getCurrentPath();
		}
		clearLocalRegion(regionPath);
	}
	
	private void clearLocalRegion(String regionPath)
	{	
		Region region = gfsh.getCache().getRegion(regionPath);
		if (region == null) {
			gfsh.println("Error: Undefined region path " + regionPath);
		} else {
			region.localClear();
			gfsh.println("Local region cleared: " + region.getFullPath());
		}
	}
	
	private void clear_a(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 2) {
			regionPath = (String)list.get(2);
		} else {
			regionPath = gfsh.getCurrentPath();
		}
		
		clear_server(regionPath, false);
		clearLocalRegion(regionPath);
	}
	
	private void clear_g(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 2) {
			regionPath = (String)list.get(2);
		} else {
			regionPath = gfsh.getCurrentPath();
		}
		
		clear_server(regionPath, true);
		clearLocalRegion(regionPath);
	}
	
	private void clear_s(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 2) {
			regionPath = (String)list.get(2);
		} else {
			regionPath = gfsh.getCurrentPath();
		}
		
		clear_server(regionPath, false);
	}
	
	private void clear_server(String regionPath, boolean global) throws Exception
	{
		if (regionPath == null) {
			return;
		}
		
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(regionPath, currentPath);
		if (fullPath == null) {
			gfsh.println("Error: invalid region path");
		} else if (fullPath.equals("/")) {
			gfsh.println("Error: cannot clear top level");
		} else {
			
			String confirmation = gfsh.getLine("This command will clear the region " + fullPath + " from the server(s). \nDo you want to proceed? (yes|no): ");
			if (confirmation.equalsIgnoreCase("yes") == false) {
				gfsh.println("Command aborted.");
				return;
			}
			
			if (global) {
				
				Aggregator aggregator = gfsh.getAggregator();
				List<AggregateResults> results = (List<AggregateResults>)gfsh.getAggregator().aggregate(new GfshFunction("clear", fullPath, null), gfsh.getAggregateRegionPath());
				
				int i = 1;
				for (AggregateResults aggregateResults : results) {
					GfshData data = (GfshData)aggregateResults.getDataObject();
					if (data.getDataObject() != null) {
						MapMessage message = (MapMessage)data.getDataObject();
						if (message.getBoolean("IsPeerClient")) {
							continue;
						}
					}
					MemberInfo memberInfo = data.getMemberInfo();
					gfsh.print(i + ". " + memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
					if (aggregateResults.getCode() == AggregateResults.CODE_ERROR) {
						gfsh.println("error - " + aggregateResults.getCodeMessage());
						if (gfsh.isDebug() && aggregateResults.getException() != null) {
							aggregateResults.getException().printStackTrace();
						}
					} else {
						Region region = RegionUtil.getRegion(fullPath, Scope.LOCAL, DataPolicy.NORMAL, null);
						gfsh.println("region cleared: " + region.getFullPath());
					}
					i++;
				}
			} else {
				CommandClient commandClient = gfsh.getCommandClient();
				CommandResults commandResults = commandClient.execute(new RegionClearTask(fullPath));
				MemberInfo memberInfo = (MemberInfo)commandResults.getDataObject();
				gfsh.print(memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
				if (commandResults.getCode() == RegionClearTask.ERROR_REGION_CLEAR) {
					gfsh.println("error - " + commandResults.getCodeMessage());
					if (gfsh.isDebug() && commandResults.getException() != null) {
						commandResults.getException().printStackTrace();
					}
				} else {
					Region region = RegionUtil.getRegion(regionPath, Scope.LOCAL, DataPolicy.NORMAL, null);
					gfsh.println("region cleared: " + region.getFullPath());
				}
			}
		}
	}
}
