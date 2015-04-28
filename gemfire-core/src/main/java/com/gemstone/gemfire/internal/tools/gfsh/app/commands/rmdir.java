package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.Aggregator;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.functions.util.RegionDestroyFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.CommandClient;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionDestroyTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

public class rmdir implements CommandExecutable
{
	private Gfsh gfsh;
	
	public rmdir(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("rmdir [-a|-g|-s] [-?] <region path>");
		gfsh.println("     Remove the local region if no options specified. The region path");
		gfsh.println("     can be absolute or relative.");
		gfsh.println("     -a Remove both the local region and the server region.");
		gfsh.println("        The region destroy will be distributed to other caches if the");
		gfsh.println("        scope is not Scope.LOCAL.");
		gfsh.println("     -g Remove globally. Remove the local region and all server");
		gfsh.println("        regions regardless of scope. This option also removes server");
		gfsh.println("        regions with Scope.LOCAL.");
		gfsh.println("     -s Remove only the server region. The local region is not destroyed.");
		gfsh.println("        The region destroy will be distributed to other caches if the");
		gfsh.println("        scope is not Scope.LOCAL."); 
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("rmdir -?")) {
			help();
		} else if (command.startsWith("rmdir -a")) {
			rmdir_a(command);
		} else if (command.startsWith("rmdir -g")) {
			rmdir_g(command);
		} else if (command.startsWith("rmdir -s")) {
			rmdir_s(command);
		} else {
			rmdir_local(command);
		}
	}
	
	private void rmdir(String command) throws Exception
	{
		int index = command.indexOf(" ");
		if (index == -1) {
			gfsh.println("Error: rmdir requires a region path to remove");
		} else {
			Cache cache = gfsh.getCache();
			Region region;
			String newPath = command.substring(index).trim();
			String fullPath = gfsh.getFullPath(newPath, gfsh.getCurrentPath());
			if (fullPath == null) {
				gfsh.println("Error: region path must be provided. mkdir <regionPath>");
			} else {
				// absolute path
				region = cache.getRegion(fullPath);
				if (region == null) {
					gfsh.println("Error: region does not exist - " + fullPath);
					return;
				}
				region.close();
				gfsh.println("Region removed: " + fullPath);
			}
		}
	}
	
	private void rmdir_local(String command) 
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 1) {
			regionPath = (String)list.get(1);
		} else {
			gfsh.println("Error: must specify a region path to remove");
			return;
		}
		
		remove_local(regionPath);
	}
	
	private void rmdir_a(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 2) {
			regionPath = (String)list.get(2);
		} else {
			gfsh.println("Error: must specify a region path to remove");
			return;
		}
		
		remove_server(regionPath, false);
		remove_local(regionPath);
	}
	
	private void rmdir_g(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 2) {
			regionPath = (String)list.get(2);
		} else {
			gfsh.println("Error: must specify a region path to remove");
			return;
		}
		
		remove_server(regionPath, true);
		remove_local(regionPath);
	}
	
	private void rmdir_s(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = null;
		if (list.size() > 2) {
			regionPath = (String)list.get(2);
		} else {
			gfsh.println("Error: must specify a region path to remove");
			return;
		}
		
		remove_server(regionPath, false);
	}
	
	private void remove_local(String regionPath)
	{
		if (regionPath == null) {
			return;
		}
		
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(regionPath, currentPath);
		if (fullPath == null) {
			gfsh.println("Error: invalid region path");
		} else if (fullPath.equals("/")) {
			gfsh.println("Error: cannot remove top level");
		} else {
			Region region = gfsh.getCache().getRegion(fullPath);
			if (region == null) {
				gfsh.println("Error: undefined region path " + fullPath);
				return;
			} 
			region.close();
			
			// correct the current path if the removed region path
			// lies in the current path
			String currentSplit[] = currentPath.split("/");
			Cache cache = gfsh.getCache();
			Region currentRegion = null;
			if (currentSplit.length > 1) {
				currentRegion = region = cache.getRegion(currentSplit[1]);
				if (region != null) {
					for (int i = 2; i < currentSplit.length; i++) {
						region = region.getSubregion(currentSplit[i]);
						if (region == null) {
							break;
						}
						currentRegion = region;
					}
				}
			}
			if (currentRegion == null) {
				gfsh.setCurrentPath("/");
			} else {
				gfsh.setCurrentPath(currentRegion.getFullPath());
			}
			gfsh.setCurrentRegion(currentRegion);
			
			gfsh.println("Region removed from local VM: " + regionPath);
		}
	}
	
	private void remove_server(String regionPath, boolean global) throws Exception
	{
		if (regionPath == null) {
			return;
		}
		
		String currentPath = gfsh.getCurrentPath();
		String fullPath = gfsh.getFullPath(regionPath, currentPath);
		if (fullPath == null) {
			gfsh.println("Error: invalid region path");
		} else if (fullPath.equals("/")) {
			gfsh.println("Error: cannot remove top level");
		} else {
			
			String confirmation = gfsh.getLine("This command will remove the region " + fullPath + " from the server(s). \nDo you want to proceed? (yes|no): ");
			if (confirmation.equalsIgnoreCase("yes") == false) {
				gfsh.println("Command aborted.");
				return;
			}
			
			if (global) {
				
				Aggregator aggregator = gfsh.getAggregator();
				List<CommandResults> aggregateList = (List<CommandResults>)aggregator.aggregate(new RegionDestroyFunction(fullPath), gfsh.getAggregateRegionPath());
				
				int i = 1;
				for (CommandResults commandResults : aggregateList) {
					
					MemberInfo memberInfo = (MemberInfo)commandResults.getDataObject();
					gfsh.print(i + ". " + memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
					if (commandResults.getCode() == RegionDestroyTask.ERROR_REGION_DESTROY) {
						gfsh.println("error - " + commandResults.getCodeMessage());
					} else {
						Region region = RegionUtil.getRegion(fullPath, Scope.LOCAL, DataPolicy.NORMAL, null);
						gfsh.println("region removed: " + region.getFullPath());
					}
					i++;
				}
			} else {
				CommandClient commandClient = gfsh.getCommandClient();
				CommandResults commandResults = commandClient.execute(new RegionDestroyTask(fullPath));
				MemberInfo memberInfo = (MemberInfo)commandResults.getDataObject();
				gfsh.print(memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
				if (commandResults.getCode() == RegionDestroyTask.ERROR_REGION_DESTROY) {
					gfsh.println("error - " + commandResults.getCodeMessage());
				} else {
					Region region = RegionUtil.getRegion(regionPath, Scope.LOCAL, DataPolicy.NORMAL, null);
					gfsh.println("region removed: " + region.getFullPath());
				}
			}
		}
	}
	
}
