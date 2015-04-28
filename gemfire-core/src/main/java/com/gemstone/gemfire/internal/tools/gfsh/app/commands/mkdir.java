package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.functions.util.RegionCreateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionCreateTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.RegionAttributeInfo;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

public class mkdir implements CommandExecutable
{
	private Gfsh gfsh;
	
	public mkdir(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("mkdir [-g|-s] | [-?] <region path> [<attributes>]");
		gfsh.println("      [data-policy=");
		gfsh.println("     Create a region remotely and/or locally (local only by default). The region path can be");
		gfsh.println("     absolute or relative.");
		gfsh.println("     -g Create a region for all peers.");
		gfsh.println("     -s Create a region for the connected server only.");
		gfsh.println("     Region attributes:");
		gfsh.println("        " + RegionAttributeInfo.CONCURRENCY_LEVEL + "=<integer [16]>");
		gfsh.println("        " + RegionAttributeInfo.DATA_POLICY + "=" + getDataPolicyValues() + " [" + DataPolicy.NORMAL.toString().toLowerCase().replace('_', '-') + "]");
		gfsh.println("        " + RegionAttributeInfo.EARLY_ACK + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.ENABLE_ASYNC_CONFLATION + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.ENABLE_GATEWAY + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.ENABLE_SUBSCRIPTION_CONFLATION + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.HUB_ID + "=<string>");
		gfsh.println("        " + RegionAttributeInfo.IGNORE_JTA + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.INDEX_UPDATE_TYPE + "=" + getIndexUpdateTypeValues() + " [asynchronous]");
		gfsh.println("        " + RegionAttributeInfo.INITIAL_CAPACITY + "=<integer> [16]");
		gfsh.println("        " + RegionAttributeInfo.IS_LOCK_GRANTOR + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.LOAD_FACTOR + "=<float> [0.75]");
		gfsh.println("        " + RegionAttributeInfo.MULTICAST_ENABLED + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.PUBLISHER + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.STATISTICS_ENABLED + "=" + getTrueFalseValues() + " [false]");
		gfsh.println("        " + RegionAttributeInfo.SCOPE + "=" + getScopeValues() + " [" + Scope.DISTRIBUTED_NO_ACK.toString().toLowerCase().replace('_', '-') + "]");
		gfsh.println("     Partition attributes:");
		gfsh.println("        " + RegionAttributeInfo.LOCAL_MAX_MEMORY + "=<MB [90% of local heap]>");
		gfsh.println("        " + RegionAttributeInfo.REDUNDANT_COPIES + "=<integer [0]>");
		gfsh.println("        " + RegionAttributeInfo.TOTAL_MAX_MEMORY + "=<MB>");
		gfsh.println("        " + RegionAttributeInfo.TOTAL_NUM_BUCKETS + "=<integer [113]>");
		gfsh.println("     Region attribute elements:");
		gfsh.println("        " + RegionAttributeInfo.ENTRY_IDLE_TIME_ACTION + "=" + getExpirationValues() + " [" + ExpirationAttributes.DEFAULT.getAction().toString().toLowerCase().replace('_', '-') + "]>");
		gfsh.println("        " + RegionAttributeInfo.ENTRY_IDLE_TIME_TIMEOUT + "=<integer [" + ExpirationAttributes.DEFAULT.getTimeout() + "]>");
		gfsh.println("        " + RegionAttributeInfo.ENTRY_TIME_TO_LIVE_ACTION + "=" + getExpirationValues() + " [" + ExpirationAttributes.DEFAULT.getAction().toString().toLowerCase().replace('_', '-') + "]>");
		gfsh.println("        " + RegionAttributeInfo.ENTRY_TIME_TO_LIVE_TIMEOUT + "=<integer [" + ExpirationAttributes.DEFAULT.getTimeout() + "]>");
		gfsh.println("        " + RegionAttributeInfo.REGION_IDLE_TIME_ACTION + "=" + getExpirationValues() + " [" + ExpirationAttributes.DEFAULT.getAction().toString().toLowerCase().replace('_', '-') + "]>");
		gfsh.println("        " + RegionAttributeInfo.REGION_IDLE_TIME_TIMEOUT + "=<integer [" + ExpirationAttributes.DEFAULT.getTimeout() + "]>");
		gfsh.println("        " + RegionAttributeInfo.REGION_TIME_TO_LIVE_ACTION + "=" + getExpirationValues() + " [" + ExpirationAttributes.DEFAULT.getAction().toString().toLowerCase().replace('_', '-') + "]>");
		gfsh.println("        " + RegionAttributeInfo.REGION_TIME_TO_LIVE_TIMEOUT + "=<integer [" + ExpirationAttributes.DEFAULT.getTimeout() + "]>");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("mkdir -?")) {
			help();
		} else if (command.startsWith("mkdir -g")) {
			mkdir_g(command);
		} else if (command.startsWith("mkdir -s")) {
			mkdir_s(command);
		} else {
			mkdir_local(command);
		}
	}
	
	private RegionAttributeInfo parseAttributes(String attributes) throws Exception
	{
		if (attributes == null) {
			return null;
		}
		attributes = attributes.trim();
		if (attributes.length() == 0) {
			return null;
		}
		RegionAttributeInfo attributeInfo = new RegionAttributeInfo();
		String split[] = attributes.split(" ");
		for (int i = 0; i < split.length; i++) {
			String pair[] = split[i].split("=");
			attributeInfo.setAttribute(pair[0], pair[1]);
		}
		return attributeInfo;
	}
	
	private void mkdir_g(String command) throws Exception
	{	
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String fullPath;
		String attributes;
		RegionAttributeInfo attributeInfo = null;
		if (list.size() == 2) {
			fullPath = gfsh.getCurrentPath();
		} else {
			String regionPath = (String) list.get(2);
			String currentPath = gfsh.getCurrentPath();
			fullPath = gfsh.getFullPath(regionPath, currentPath);
			attributes = "";
			for (int i = 3; i < list.size(); i++) {
				attributes += list.get(i) + " ";
			}
			attributeInfo = parseAttributes(attributes);
		}
		
		if (fullPath.equals(gfsh.getCurrentPath())) {
			gfsh.println("Error: must define region path: mkdir [-g] <regionPath>");
		} else {
			// create for the entire peers
			List<CommandResults> resultList = (List)gfsh.getAggregator().aggregate(new RegionCreateFunction(new RegionCreateTask(fullPath, attributeInfo)), gfsh.getAggregateRegionPath());
			int i = 1;
			for (CommandResults commandResults : resultList) {
				MemberInfo memberInfo = (MemberInfo)commandResults.getDataObject();
				gfsh.print(i + ". " + memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
				if (commandResults.getCode() == RegionCreateTask.SUCCESS_CREATED) {
					Region region;
					if (gfsh.isLocator()) {
						region = RegionUtil.getRegion(fullPath, Scope.LOCAL, DataPolicy.NORMAL, gfsh.getPool(), false);
					} else {
						region = RegionUtil.getRegion(fullPath, Scope.LOCAL, DataPolicy.NORMAL, gfsh.getEndpoints());
					}
					gfsh.println("region created: " + region.getFullPath());
				} else {
					gfsh.println("error - " + commandResults.getCodeMessage());
				}
				i++;
			}
		}
	}
	
	private void mkdir_s(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String regionPath = "";
		String attributes;
		RegionAttributeInfo attributeInfo = null;
		if (list.size() == 2) {
			regionPath = gfsh.getCurrentPath();
		} else {
      if (!"/".equals(gfsh.getCurrentPath())) {
        regionPath = gfsh.getCurrentPath();
      }
			regionPath = regionPath + "/" + (String) list.get(2);
			attributes = "";
			for (int i = 3; i < list.size(); i++) {
				attributes += list.get(i) + " ";
			}
			attributeInfo = parseAttributes(attributes);
		}
		
		if (regionPath.equals(gfsh.getCurrentPath())) {
			gfsh.println("Error: must define region path: mkdir [-s] <regionPath>");
		} else {
			// create for the server only
			CommandResults commandResults = gfsh.getCommandClient().execute(new RegionCreateTask(regionPath, attributeInfo));
			MemberInfo memberInfo = (MemberInfo)commandResults.getDataObject();
			gfsh.print(memberInfo.getMemberName() + "(" + memberInfo.getMemberId() + ")" + ": ");
			if (commandResults.getCode() == RegionCreateTask.SUCCESS_CREATED) {
				Region region = RegionUtil.getRegion(regionPath, Scope.LOCAL, DataPolicy.NORMAL, null);
				gfsh.println("region created: " + region.getFullPath());
			} else {
				gfsh.println("error - " + commandResults.getCodeMessage());
			}
		}
	}
	
	private void mkdir_local(String command) throws Exception
	{
		int index = command.indexOf(" ");
		if (index == -1) {
			gfsh.println("Current region: " + gfsh.getCurrentPath());
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
				if (region != null) {
					gfsh.println("Region already exists: " + region.getFullPath());
					return;
				}
				if (gfsh.isLocator()) {
					region = RegionUtil.getRegion(fullPath, Scope.LOCAL, DataPolicy.NORMAL, gfsh.getPool(), false);
				} else {
					region = RegionUtil.getRegion(fullPath, Scope.LOCAL, DataPolicy.NORMAL, gfsh.getEndpoints());
				}
				gfsh.println("Region created: " + region.getFullPath());
			}
		}
	}
	
	
	private static String getDataPolicyValues()
	{
		String all = DataPolicy.EMPTY + "|" +
					DataPolicy.NORMAL + "|" +
					DataPolicy.PARTITION + "|" +
					DataPolicy.PERSISTENT_REPLICATE + "|" +
					DataPolicy.PRELOADED + "|" +
					DataPolicy.REPLICATE;
		return all.toLowerCase().replace('_', '-');
	}
	
	private static String getScopeValues()
	{
		String all = Scope.DISTRIBUTED_NO_ACK + "|" +
					Scope.DISTRIBUTED_ACK + "|" +
					Scope.GLOBAL + "|" +
					Scope.LOCAL;
		return all.toLowerCase().replace('_', '-');
	}
	
	private static String getTrueFalseValues()
	{
		return "true|false";
	}
	
	private static String getIndexUpdateTypeValues()
	{
		return "asynchronous|synchronous";
	}
	
	private static String getExpirationValues()
	{
		String all = ExpirationAction.DESTROY + "|" +
					ExpirationAction.INVALIDATE + "|" +
					ExpirationAction.LOCAL_DESTROY + "|" +
					ExpirationAction.LOCAL_INVALIDATE;
		return all.toLowerCase().replace('_', '-');
	}
}
