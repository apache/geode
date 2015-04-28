package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.ProxyBucketRegion;
import com.gemstone.gemfire.internal.cache.partitioned.Bucket;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;

public class clear implements ServerExecutable
{
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;
	
	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		Cache cache = CacheFactory.getAnyInstance();
		Region region = cache.getRegion(regionPath);
		
		if (region == null) {
			code = AggregateResults.CODE_ERROR;
			codeMessage = "Undefined region: " + regionPath;
			return null;
		}
		
		MapMessage message = new MapMessage();
		
		if (region instanceof PartitionedRegion) {
			PartitionedRegion pr = (PartitionedRegion)region;
			if (pr.getDataStore() == null) {
				code = AggregateResults.CODE_NORMAL;
				codeMessage = "No data store: " + regionPath;
				message.put("IsPeerClient", true);
				return new GfshData(message);
			}
		}

		message.put("IsPeerClient", false);
		try {
			synchronized (region) {
				if (region instanceof PartitionedRegion) {
					// PR clear is not supported. Must clear the local data set
					// individually.
					clearPartitionedRegion((PartitionedRegion)region);
				} else {
					region.clear();
				}
				codeMessage = "Cleared";
			}
		} catch (Exception ex) {
			code = AggregateResults.CODE_ERROR;
			codeMessage = ex.getMessage();
		}
		return new GfshData(message);
	}

	public byte getCode()
	{
		return code;
	}
	
	public String getCodeMessage()
	{
		return codeMessage;
	}
	
	private void clearPartitionedRegion(PartitionedRegion partitionedRegion)
	{
		LocalDataSet lds = (LocalDataSet)PartitionRegionHelper.getLocalPrimaryData(partitionedRegion);
		Set<Integer>set = lds.getBucketSet(); // this returns bucket ids in the function context 
		for (Integer bucketId : set) {
			Bucket bucket = partitionedRegion.getRegionAdvisor().getBucket(bucketId);
			if (bucket instanceof ProxyBucketRegion == false) {
				if (bucket instanceof BucketRegion) {
					BucketRegion bucketRegion = (BucketRegion) bucket;
					Set keySet = bucketRegion.keySet();
					for (Iterator iterator = keySet.iterator(); iterator.hasNext();) {
						Object key = iterator.next();
						bucketRegion.remove(key);
					}
				}
			}
		}
	}
}
