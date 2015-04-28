package com.gemstone.gemfire.internal.tools.gfsh.app.commands.optional;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;

public class perf implements CommandExecutable
{
	private static final String HIDDEN_REGION_NAME_PREFIX = "_"; // 1 underscore
	
	private Gfsh gfsh;
//	Findbugs - unused fields
//	private Region localRegion;
//	private Iterator localRegionIterator;
//	private List localKeyList;
//	private int lastRowPrinted = 0;
	
	public perf(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("perf [-threads <count>]");
		gfsh.println("     [-payload <size>]");
		gfsh.println("     [-type put|get|delete|putall|getall|deleteall <batch size>]");
		gfsh.println("     [-input random|sequence");
		gfsh.println("     [-size <total entry size or count>");
		gfsh.println("     [-key int|long|string|<class name>");
		gfsh.println("     [-loop <count>]");
		gfsh.println("     [-interval <count>]");
		gfsh.println("     [<region path>]");
		gfsh.println("     [-?]");
		gfsh.println("     Measure throughput rates and \"put\" latency.");
		gfsh.println("     -threads <count> The number of threads to concurrently put data into");
		gfsh.println("         the fabric. Default: 1");
		gfsh.println("     -payload <size> The payliod size in bytes. Perf puts byte arrays of the");
		gfsh.println("         specified size into the fabric. Default: 100 bytes.");
		gfsh.println("     -type put|get|delete|putall|getall|deleteall <batch size>");
		gfsh.println("         The operation type. <batch size> is for '*all' only. Default: put");
		gfsh.println("     -input  The input type. 'random' selects keys randomly from the range of");
		gfsh.println("        <total entry size>. 'sequnce' sequntial keys from 1 to <total entry size");
		gfsh.println("        and repeats until the loop count is exhausted. Default: random");
		gfsh.println("     -size   The total size of the cache. This option is only for '-type put'");
		gfsh.println("         and '-type putall'.");
		gfsh.println("     -key int|long|string|<class name>  The key type. The keys of the type");
		gfsh.println("         int or long are numerical values incremented in the loop. The keys");
		gfsh.println("         of type string are String values formed by the prefix and the numerical");
		gfsh.println("         values that are incremented in the loop. The default prefix is \"key\".");
		gfsh.println("         The keys of type <class name> are supplied by the class that implements");
		gfsh.println("         the com.gemstone.gemfire.addons.gfsh.data.PerfKey interface. The class");
		gfsh.println("         implements getKey(int keyNum) which returns a Serializable or preferrably");
		gfsh.println("         DataSerializable object.");
		gfsh.println("     -loop <count>  The number of iterations per thread. Each thread invokes");
		gfsh.println("         put() or putAll() per iteration. Default: 10000");
		gfsh.println("     -inteval <count> The display interval. Each thread prints the average");
		gfsh.println("         throughput and latency after each interval interation count. Default: 1000");
		gfsh.println("     <region path>  The region to put data into. Default: current region.");
		gfsh.println("     Default: perf -threads 1 -payload 100 -type put -input random -size 10000 -loop 10000 -interval 1000 ./");
		
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("perf -?")) {
			help();
			return;
		} 
		
		perf(command);
	}
	
	private void perf(String command) throws Exception
	{
		LinkedList<String> list = new LinkedList();
		gfsh.parseCommand(command, list);
		
		int threadCount = 1;
		int payloadSize = 100;
		int putAllSize = 1;
		int loopCount = 10000;
		int interval = 1000;
		String regionPath = null;
		
		int listSize = list.size();
		for (int i = 1; i < listSize; i++) {
			String arg = list.get(i);
			if (arg.equals("-threads")) {
				i++;
				if (i >= listSize) {
					gfsh.println("Error: '-threads' requires <count>");
					return;
				}
				threadCount = Integer.parseInt(list.get(i));
			} else if (arg.equals("-payload")) {
				i++;
				if (i >= listSize) {
					gfsh.println("Error: '-payload' requires <size>");
					return;
				}
				payloadSize = Integer.parseInt(list.get(i));
			} else if (arg.equals("-putall")) {
				i++;
				if (i >= listSize) {
					gfsh.println("Error: '-putall' requires <batch size>");
					return;
				}
				putAllSize = Integer.parseInt(list.get(i));
			} else if (arg.equals("-loop")) {
				i++;
				if (i >= listSize) {
					gfsh.println("Error: '-loop' requires <count>");
					return;
				}
				loopCount = Integer.parseInt(list.get(i));
			} else if (arg.equals("-interval")) {
				i++;
				if (i >= listSize) {
					gfsh.println("Error: '-interval' requires <count>");
					return;
				}
				interval = Integer.parseInt(list.get(i));
			} else {
				regionPath = list.get(i);
			}
		}
		if (regionPath == null) {
			regionPath = gfsh.getCurrentPath();
		}
		
		benchmark(threadCount, payloadSize, putAllSize, loopCount, interval, regionPath);
	}
	
	private void benchmark(int threadCount, 
			int payloadSize, 
			int putAllSize,
			int loopCount,
			int interval,
			String regionPath)
	{
		
	}
	
}
