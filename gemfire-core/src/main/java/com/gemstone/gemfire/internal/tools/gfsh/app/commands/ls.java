package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.cache.CacheStatistics;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.Nextable;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.functions.util.LocalRegionInfoFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.ListMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.QueryTask;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

public class ls implements CommandExecutable, Nextable
{
	private static final String HIDDEN_REGION_NAME_PREFIX = "_"; // 1 underscore
	
	private static final int TYPE_LOCAL_REGION = 0;
	private static final int TYPE_REMOTE_REGION = 1;
	public final static int TYPE_REMOTE_KEYS = 2;
	
	private Gfsh gfsh;
	private Region localRegion;
	private Iterator localRegionIterator;
	private List localKeyList;
	private int lastRowPrinted = 0;
	
	public ls(Gfsh gfsh)
	{
		this.gfsh = gfsh;
		gfsh.addEnumCommand("ls -e");
		gfsh.addEnumCommand("ls -k");
		gfsh.addEnumCommand("ls -s");
	}
	
	public void help()
	{
		gfsh.println("ls [-a|-c|-e|-k|-m|-p|-r|-s] [region path] | [-?]");
		gfsh.println("     List subregions or region entries in the current path or in the");
		gfsh.println("     specified path. If no option specified, then it lists all region");
		gfsh.println("     names except the hidden region names. A hidden region name begins");
		gfsh.println("     with the prefix " + HIDDEN_REGION_NAME_PREFIX + " (1 underscore).");
		gfsh.println("     -a  List all regions. This option lists all regions including the region");
		gfsh.println("         names that begin with the prefix " + HIDDEN_REGION_NAME_PREFIX);
		gfsh.println("         (1 underscore).");
		gfsh.println("     -c  List cache server information.");
		gfsh.println("     -e  List local entries up to the fetch size.");
		gfsh.println("     -k  List server keys up to the fetch size. The keys are enumerated. Use");
		gfsh.println("         the key numbers to get values using the 'get -k' command.");
		gfsh.println("         If partitioned region, then it displays the entries in only the");
		gfsh.println("         connected server's local dataset due to the potentially large size");
		gfsh.println("         of the partitioned region.");
		gfsh.println("     -m  List region info of all peer members.");
		gfsh.println("     -p  List the local data set of the partitioned region entries in the");
		gfsh.println("         server up to the fetch size. If the region is not a partitioned");
		gfsh.println("         region then print the region entries (same as 'ls -s' in that case.)");
		gfsh.println("     -r  Recursively list all sub-region paths.");
		gfsh.println("     -s  List server entries up to the fetch size. If partitioned region,");
		gfsh.println("         then it displays the entries in only the connected server's local");
		gfsh.println("         dataset due to the potentially large size of the partitioned region.");
		
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("ls -?")) {
			help();
			return;
		} 
		
		// reset 
		localKeyList = null;
		
		if (command.startsWith("ls -a")) {
			ls_a(command);
		} else if (command.startsWith("ls -c")) {
			ls_c(command);
		} else if (command.startsWith("ls -e")) {
			ls_e(command);
		} else if (command.startsWith("ls -k")) {
			ls_k(command);
		} else if (command.startsWith("ls -m")) {
			ls_m(command);
		} else if (command.startsWith("ls -r")) {
			ls_r(command);
		} else if (command.startsWith("ls -s")) {
			ls_s(command);
		} else if (command.startsWith("ls -p")) {
			ls_p(command);
		} else if (command.startsWith("ls")) {
			ls(command);
		}
	}
	
	private void ls_a(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList<String>();
		Gfsh.parseCommand(command, list);
		String regionPath;
		if (list.size() == 2) {
			regionPath = gfsh.getCurrentPath();
		} else {
			regionPath = (String) list.get(2);
			if(!isRegionArgValid(regionPath)){
			  return;
			}
		}
		listRegions(regionPath, true);
	}
	
	private void ls_c(String command) throws Exception
	{
		
	  String regionPath = retrievePath(command);
		
		if (regionPath.equals("/")) {
			gfsh.println("Error: invalid region \"/\". Change to a valid region or specify the region path, i.e. ls -c /foo");
			return;
		}
		
		if(!isRegionArgValid(regionPath)){
		  return;
		}
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());

		long startTime = System.currentTimeMillis();
		List<AggregateResults> results = (List<AggregateResults>)gfsh.getAggregator().aggregate(new GfshFunction(command, regionPath, null), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();
		
		for (AggregateResults aggregateResults : results) {
			GfshData data = (GfshData)aggregateResults.getDataObject();
			ListMessage message = (ListMessage)data.getDataObject();
			gfsh.println("--------------------------------------");
			gfsh.println("MemberId = " + data.getMemberInfo().getMemberId());
			gfsh.println("MemberName = " + data.getMemberInfo().getMemberName());
			gfsh.println("Host = " + data.getMemberInfo().getHost());
			gfsh.println("Pid = " + data.getMemberInfo().getPid());
			gfsh.println();
			Mappable mappables[] = message.getAllMappables();
			for (int i = 0; i < mappables.length; i++) {
				Set<String> keySet = mappables[i].getKeys();
				List<String> keyList = new ArrayList<String>(keySet);
				java.util.Collections.sort(keyList);
				for (String key : keyList) {
					Object value = mappables[i].getValue(key);
					gfsh.println("   " + key + " = " + value);
				}
				gfsh.println();
			}
			gfsh.println("--------------------------------------");
			gfsh.println();
		}
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}
	
	private void ls_m(String command) throws Exception
	{
	  String regionPath = retrievePath(command);
		
		if (regionPath.equals("/")) {
			gfsh.println("Error: invalid region \"/\". Change to a valid region or specify the region path, i.e. ls -a /foo");
			return;
		}
		
		if(!isRegionArgValid(regionPath)){
      return;
    }
		
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());

		long startTime = System.currentTimeMillis();
		List<Mappable> resultList = (List<Mappable>)gfsh.getAggregator().aggregate(new LocalRegionInfoFunction(regionPath), gfsh.getAggregateRegionPath());
		long stopTime = System.currentTimeMillis();
		
		// First, set the member list in gfsh. This call sorts the list by member id.
		// The member list is kept by gfsh for commands like 'pr' that need to 
		// lookup member ids.
		resultList = gfsh.setMemberList(resultList);
		
		boolean isPR = false;
		int totalRegionSize = 0;
		for (int i = 0; i < resultList.size(); i++) {
			MapMessage info = (MapMessage)resultList.get(i);
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
				totalRegionSize += info.getInt("RegionSize");
			}
		}
		
		PrintUtil.printMappableList(resultList);
		if (isPR) {
			gfsh.println("Total Region Size: " + totalRegionSize);
		}
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}
	}
	
	private void ls_k(String command) throws Exception
	{
	  String regionPath = retrievePath(command);
		
		if (regionPath.equals("/")) {
			gfsh.println("Error: invalid region \"/\". Change to a valid region or specify the region path, i.e. ls -k /foo");
			return;
		}
		
		if(!isRegionArgValid(regionPath)){
      return;
    }
		
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
		
		// ---------- Get keys using select ----------------
//		// get keys from the server
//		// ls -k
//		select s = (select)gfsh.getCommand("select");
//		// get the keys but limit it to 1000
//		localKeyList = s.getRemoteKeys(regionPath, 1000);
		// -------------------------------------------------
		
		// ---------- Get keys using function (QueryTask) -------------
		localKeyList = listRegionKeys(regionPath, true, true);
		// ------------------------------------------------------------
		
		
		gfsh.setLsKeyList(localKeyList);
		next n = (next)gfsh.getCommand("next");
		n.setCommand(getClass().getSimpleName(), TYPE_REMOTE_KEYS);
		
	}
	
	private void ls_r(String command) throws Exception
	{
	  String regionPath = retrievePath(command);

		String regionPaths[];
		if (regionPath.equals("/")) {
			regionPaths = RegionUtil.getAllRegionPaths(gfsh.getCache(), true);
		} else {
		  if(!isRegionArgValid(regionPath)){
        return;
      }
		  regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
			Region<?, ?> region = RegionUtil.getLocalRegion(regionPath);
			regionPaths = RegionUtil.getAllRegionPaths(region, true);
		}

		for (int i = 0; i < regionPaths.length; i++) {
			gfsh.println(regionPaths[i]);
		}
	}
	
  private void ls_s(String command) throws Exception
	{
    String regionPath = retrievePath(command);
		
		if (regionPath.equals("/")) {
			gfsh.println("Error: invalid region \"/\". Change to a valid region or specify the region path, i.e. ls -k /foo");
			return;
		}
		
		if(!isRegionArgValid(regionPath)){
      return;
    }
		// Show only the local dataset entries if it's a partitioned regions
		listRegionEntries(regionPath, true, true);
	}
	
	private void ls_p(String command) throws Exception
	{
	  String regionPath = retrievePath(command);
		
		if (regionPath.equals("/")) {
			gfsh.println("Error: invalid region \"/\". Change to a valid region or specify the region path, i.e. ls -k /foo");
			return;
		}
		if(!isRegionArgValid(regionPath)){
      return;
    }
		listRegionEntries(regionPath, true, true);
	}
	
	private void ls(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		String path;
		if (list.size() == 1) {
			path = gfsh.getCurrentPath();
		} else {
			path = (String) list.get(1);
		}
		
		listRegions(path, false);
	}
	
	private void listRegions(String path, boolean listAllRegions) throws Exception
	{
		Region region = null;
		Set regionSet;
		if (path.equals("/")) {
			regionSet = gfsh.getCache().rootRegions();
		} else {
			path = gfsh.getFullPath(path, gfsh.getCurrentPath());
			region = gfsh.getCache().getRegion(path);
			if (region == null) {
				gfsh.println("Error: Region undefined. Invalid path: " + path + ". Use absolute path.");
				return;
			}
			regionSet = region.subregions(false);
		}
		
		if (regionSet.size() == 0) {
			gfsh.println("Subregions: none");
		} else {
			gfsh.println("Subregions:");
		}
		List regionList = new ArrayList();
		for (Iterator itr = regionSet.iterator(); itr.hasNext();) {
			Region rgn = (Region) itr.next();
			String name = rgn.getName();
			if (listAllRegions == false && name.startsWith(HIDDEN_REGION_NAME_PREFIX)) {
				continue;
			}
			regionList.add(name);
		}
		Collections.sort(regionList);
		for (Iterator<String> itr = regionList.iterator(); itr.hasNext();) {
			String name = itr.next();
			for (Iterator itr2 = regionSet.iterator(); itr2.hasNext();) {
				Region rgn = (Region) itr2.next();
				String regionName = rgn.getName();
				if (name.equals(regionName)) {
					gfsh.println("   " + name);
					if (rgn.getAttributes().getStatisticsEnabled()) {
						CacheStatistics stats = rgn.getStatistics();
						gfsh.println("      " + stats);
					}
					break;
				}
			}
		}
		gfsh.println();
	}
	
	public List listRegionKeys(String regionPath, boolean nextEnabled, boolean isPRLocalData) throws Exception
	{
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
		
		// get keys from the server
		// ls -k
		boolean keysOnly = true;
		long startTime = System.currentTimeMillis();
		CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(regionPath, gfsh.getFetchSize(), nextEnabled, isPRLocalData, keysOnly));
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

		if (regionPath == null) {
			localKeyList = null;
			lastRowPrinted = 0;
		}
		List keyList = localKeyList;
		if (keyList == null) {
			localKeyList = keyList = new ArrayList();
		}
		List list = (List)results.getResults();
		if (gfsh.isShowResults()) {
			lastRowPrinted = PrintUtil.printList(list, 0, 1, list.size(), results.getActualSize(), keyList);
		} else {
			gfsh.println(" Fetch size: " + gfsh.getFetchSize());
			gfsh.println("   Returned: " + list.size() + "/" + results.getActualSize());
		}
		if (results.isPR()) {
			gfsh.println("Partitioned region local dataset retrieval. The actual size maybe larger.");
		}
	
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}

		gfsh.setLsKeyList(keyList);
		next n = (next)gfsh.getCommand("next");
		n.setCommand(getClass().getSimpleName(), TYPE_REMOTE_REGION);
		return keyList;
	}
	
	public List listRegionEntries(String regionPath, boolean nextEnabled, boolean isPRLocalData) throws Exception
	{
		regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
		
		// get entries from the server
		// ls -s
		long startTime = System.currentTimeMillis();
		CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(regionPath, gfsh.getFetchSize(), nextEnabled, isPRLocalData));
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

		if (regionPath == null) {
			localKeyList = null;
			lastRowPrinted = 0;
		}
		List keyList = localKeyList;
		if (keyList == null) {
			localKeyList = keyList = new ArrayList();
		}
		Map map = (Map)results.getResults();
		if (gfsh.isShowResults()) {
			lastRowPrinted = PrintUtil.printEntries(map, 0, 1, map.size(), results.getActualSize(), keyList);
		} else {
			gfsh.println(" Fetch size: " + gfsh.getFetchSize());
			gfsh.println("   Returned: " + map.size() + "/" + results.getActualSize());
		}
		if (results.isPR()) {
			gfsh.println("Partitioned region local dataset retrieval. The actual size maybe larger.");
		}
	
		if (gfsh.isShowTime()) {
			gfsh.println("elapsed (msec): " + (stopTime - startTime));
		}

		gfsh.setLsKeyList(keyList);
		next n = (next)gfsh.getCommand("next");
		n.setCommand(getClass().getSimpleName(), TYPE_REMOTE_REGION);
		return keyList;
	}
	
	public List next(Object userData) throws Exception
	{
		int nexType = (Integer)userData;
		if (nexType == TYPE_LOCAL_REGION) {
			if (localRegion == null) {
				return null;
			}
			int rowsPrinted = PrintUtil.printEntries(localRegion, localRegionIterator, lastRowPrinted, lastRowPrinted+1, gfsh.getFetchSize(), localKeyList);
			lastRowPrinted = lastRowPrinted + rowsPrinted;
		} else if (nexType == TYPE_REMOTE_REGION) {
			CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(null, gfsh.getFetchSize(), true));
			QueryResults results = (QueryResults)cr.getDataObject();
			Map map = (Map)results.getResults();
			if (map != null) {
				int rowsPrinted;
				rowsPrinted = PrintUtil.printEntries(map, lastRowPrinted, lastRowPrinted+1, map.size(), results.getActualSize(), localKeyList);
				if (results.isPR()) {
					gfsh.println("Partitioned region local dataset retrieval. The actual size maybe larger.");
				}
				lastRowPrinted = lastRowPrinted + rowsPrinted;
			}
			
		} else if (nexType == TYPE_REMOTE_KEYS) {
			
			// ---------- Get keys using select ----------------
//			select s = (select)gfsh.getCommand("select");
//			List list = s.select(null, true);
			// -------------------------------------------------
			
			// ---------- Get keys using function (QueryTask) -------------
			CommandResults cr = gfsh.getCommandClient().execute(new QueryTask(null, gfsh.getFetchSize(), true));
			QueryResults results = (QueryResults)cr.getDataObject();
			List list = (List)results.getResults();
			if (list != null) {
				int rowsPrinted;
				rowsPrinted = PrintUtil.printList(list, lastRowPrinted, lastRowPrinted+1, list.size(), results.getActualSize(), localKeyList);
				if (results.isPR()) {
					gfsh.println("Partitioned region local dataset retrieval. The actual size maybe larger.");
				}
				lastRowPrinted = lastRowPrinted + rowsPrinted;
			}
			// ------------------------------------------------------------
		
			if (localKeyList == null) {
				localKeyList = list;
			} else if (list != null) {
				localKeyList.addAll(list);
			}
		}
		next n = (next)gfsh.getCommand("next");
		n.setCommand(getClass().getSimpleName(), nexType);
		
		return null;
	}
	
	private void ls_e(String command) throws Exception
	{
	  String regionPath = retrievePath(command);

		localRegion = null;
    if (!regionPath.equals("/")) {
      if(!isRegionArgValid(regionPath)){
        return;
      }
      regionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
      localRegion = gfsh.getCache().getRegion(regionPath);
			localKeyList = new ArrayList();
			localRegionIterator = localRegion.entrySet().iterator();
			lastRowPrinted = PrintUtil.printEntries(localRegion, localRegionIterator, 0, 1, gfsh.getFetchSize(), localKeyList);
			gfsh.setLsKeyList(localKeyList);
			
			next n = (next)gfsh.getCommand("next");
			n.setCommand(getClass().getSimpleName(), TYPE_LOCAL_REGION);
			gfsh.println();
		}
	}
	
	private boolean isOption(Object object) {
    Pattern pattern = Pattern.compile("^-[acmkrspe]");
    Matcher matcher = pattern.matcher(object.toString());
    if(matcher.matches()){
      return true;
    } else {
      return false;
    }
  }
	
	private boolean isRegionArgValid(String regionPath){
	  String fullRegionPath = gfsh.getFullPath(regionPath, gfsh.getCurrentPath());
    Region<?, ?> region = RegionUtil.getLocalRegion(fullRegionPath);
    if (region == null) {
      if(isOption(regionPath)){
        gfsh.println("Error: ls does not support mulitple options");
      }else{
        gfsh.println("Error: region does not exist - " + regionPath);
      }
      return false;
    }
    return true;
	}
	
	private String retrievePath(String command){
	  LinkedList<String> list = new LinkedList<String>();
    Gfsh.parseCommand(command, list);
    String regionPath;
    if (list.size() == 2) {
      regionPath = gfsh.getCurrentPath();
    } else {
      regionPath = (String) list.get(2);
    }
    return regionPath;
	}
}
