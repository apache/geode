package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import java.util.HashMap;
import java.util.TreeMap;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;
import com.gemstone.gemfire.internal.tools.gfsh.util.RegionUtil;

public class which implements ServerExecutable
{
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;
	
	static String major;
	static String minor;
	static String update;
	static String build;
	
	static {
		determineGemFireVersion();
	}
	
	private static void determineGemFireVersion()
	{
		String gemfireVersion = GemFireVersion.getGemFireVersion();
		String split[] = gemfireVersion.split("\\.");
		
		for (int i = 0;i < split.length; i++) {
			switch (i) {
			case 0:
				major = split[i];
				break;
			case 1:
				minor = split[i];
				break;
			case 2:
				update = split[i];
				break;
			case 3:
				build = split[i];
				break;
			}
		}
	}
	
	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		GfshData data = new GfshData(null);
		Cache cache = CacheFactory.getAnyInstance();
		
		Object args[] = (Object[])arg;
		if (args.length < 0) {
			code = AggregateResults.CODE_ERROR;
			codeMessage = "Key not specified";
			return data;
		}
		Object key = args[0];
		boolean recursive = false;
		if (args.length > 1) {
			recursive = (Boolean)args[1];
		}		
		
		try {
			TreeMap<String, Object> map = new TreeMap();
			
			if (recursive == false) {	
				
				// Find the value from the partitioned region
				if (regionPath == null || regionPath.equals("/")) {
					code = AggregateResults.CODE_ERROR;
					codeMessage = "Invalid region path " + regionPath;
					return data;
				}
				Region region = cache.getRegion(regionPath);
				if (region == null) {
					code = AggregateResults.CODE_ERROR;
					codeMessage = "Undefined region " + regionPath;
					return data;
				}
				
				Object value;
				DistributedMember primaryMember = null;
				if (region instanceof PartitionedRegion) {
					PartitionedRegion pr = (PartitionedRegion)region;
					Region localRegion = PartitionRegionHelper.getLocalData((PartitionedRegion)region);
					value = localRegion.get(key);
					primaryMember = PartitionRegionHelper.getPrimaryMemberForKey(region, key);
					
					// 6.0 - Note that this may or may not work
//					int bucketId = PartitionedRegionHelper.getHashKey(pr, Operation.GET, key, null);
					
					// 6.5
					int bucketId = pr.getKeyInfo(key).getBucketId();
					
					DistributedMember member = cache.getDistributedSystem().getDistributedMember();
					boolean isPrimary = member == primaryMember;
					HashMap<String, Object> prInfoMap = new HashMap<String, Object>();
					prInfoMap.put("BucketId", bucketId);
					prInfoMap.put("IsPrimary", isPrimary);
					data.setUserData(prInfoMap);
				} else {
					value = region.get(key);
				}
				if (value != null) {
					map.put(regionPath, value);
				}
				
			} else {
				
				// Recursively find the keys starting from the specified region path.
				String regionPaths[] = RegionUtil.getAllRegionPaths(cache, true);
				for (int i = 0; i < regionPaths.length; i++) {
					if (regionPaths[i].startsWith(regionPath)) {
						Object value = null;
						Region region = cache.getRegion(regionPaths[i]);
												
						if (region instanceof PartitionedRegion) {
							PartitionedRegion pr = (PartitionedRegion)region;
							Region localRegion = PartitionRegionHelper.getLocalData(pr);
							value = localRegion.get(key);
						} else {
							value = region.get(key);
						}
						if (value != null) {
							map.put(regionPaths[i], value);
						}
					}
				}
				
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
}
