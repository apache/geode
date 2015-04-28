package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;

public class rebalance implements ServerExecutable
{
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;

	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		GfshData data = new GfshData(null);
		Cache cache = CacheFactory.getAnyInstance();
		
		// args[0] = memberId
		// args[1] = simulate optional (default true)
		// args[2] = timeout optional (default 20000 mesec)
		
		Object args[] = (Object[])arg;
		boolean simulate = true;
		String memberId = null;
		if (args != null && args.length > 0) {
			memberId = args[0].toString();
		} else {
			return data;
		}
		if (args.length > 1) {
			if (args[1] instanceof Boolean) {
				simulate = (Boolean)args[1];
			}
		}
		long timeout = 60000; // 60 sec default
		if (args.length > 2) {
			timeout = (Long)args[2];
		}

		String thisMemberId = cache.getDistributedSystem().getDistributedMember().getId();
		if (memberId.equals(thisMemberId) == false) {
			return data;
		}
		
		try {
			Map map = null;
			if (simulate) {
				map = simulate();
			} else {
				map = rebalance(timeout);
			}
			data.setDataObject(map);
		} catch (Exception ex) {
			code = AggregateResults.CODE_ERROR;
			codeMessage = ex.getMessage();
		}
		return data;
	}

	public byte getCode()
	{
		return code;
	}

	public String getCodeMessage()
	{
		return codeMessage;
	}

	private Map rebalance(long timeout) throws CancellationException, InterruptedException
	{
		Cache cache = CacheFactory.getAnyInstance();
		ResourceManager manager = cache.getResourceManager();
		RebalanceOperation op = manager.createRebalanceFactory().start();

		// Timeout if it's taking too long. Rebalancing will still complete.
		try {
			RebalanceResults results = op.getResults(timeout, TimeUnit.MILLISECONDS);	
			return convertToMap(results);
		} catch (Exception ex) {
			return null;
		}
	}

	private Map simulate() throws CancellationException, InterruptedException
	{
		Cache cache = CacheFactory.getAnyInstance();
		ResourceManager manager = cache.getResourceManager();
		RebalanceOperation op = manager.createRebalanceFactory().simulate();
		RebalanceResults results = op.getResults();
		Set<PartitionRebalanceInfo> set = results.getPartitionRebalanceDetails();
		return convertToMap(results);
	}
	
	private TreeMap convertToMap(RebalanceResults results)
	{
		TreeMap map = new TreeMap();
//		if (results.getPartitionRebalanceDetails() != null) {
//			map.put("RebalanceDetails", results.getPartitionRebalanceDetails());
//		}
//		Set<PartitionRebalanceInfo> set = results.getPartitionRebalanceDetails();
//		if (set != null) {
//			for (PartitionRebalanceInfo info : set) {
//				info.
//			}
//		}
		map.put("TotalBucketCreateBytes", results.getTotalBucketCreateBytes());
		map.put("TotalBucketCreatesCompleted", results.getTotalBucketCreatesCompleted());
		map.put("TotalBucketCreateTime", results.getTotalBucketCreateTime());
		map.put("TotalBucketTransferBytes", results.getTotalBucketTransferBytes());
		map.put("TotalBucketTransfersCompleted", results.getTotalBucketTransfersCompleted());
		map.put("TotalBucketTransferTime", results.getTotalBucketTransferTime());
		map.put("TotalPrimaryTransfersCompleted", results.getTotalPrimaryTransfersCompleted());
		map.put("TotalPrimaryTransferTime", results.getTotalPrimaryTransferTime());
		map.put("TotalTime", results.getTotalTime());
		return map;
	}
}
