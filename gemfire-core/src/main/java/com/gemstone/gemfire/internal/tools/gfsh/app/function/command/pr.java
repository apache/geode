package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;

public class pr implements ServerExecutable
{
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;
	
	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		Cache cache = CacheFactory.getAnyInstance();
		
		Object args[] = (Object[])arg;
		
		GfshData data = new GfshData(null);
		try {
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
			if (region instanceof PartitionedRegion == false) {
				code = AggregateResults.CODE_ERROR;
				codeMessage = "Not a partitioned region: " + regionPath;
				return data;
			}
		
			DistributedMember member = cache.getDistributedSystem().getDistributedMember();
			
//			PartitionRegionInfoImpl info = (PartitionRegionInfoImpl)PartitionRegionHelper.getPartitionRegionInfo(region);
//			info.getLowRedundancyBucketCount();
			PartitionedRegion pr = (PartitionedRegion)region;
			if (pr.getDataStore() == null) {
				// PROXY - no data store
				code = AggregateResults.CODE_NORMAL;
				codeMessage = "No data store: " + regionPath;
				data.setUserData(pr.getPartitionAttributes().getTotalNumBuckets());
				return data;
			}
			Set<BucketRegion> set2 = pr.getDataStore().getAllLocalBucketRegions();
//			FindBugs - Unused
//			TreeMap primaryMap = new TreeMap();
//			TreeMap redundantMap = new TreeMap();
//			for (BucketRegion br : set2) {
//				TreeMap map = new TreeMap();
//				map.put("Size", br.size());
//				map.put("Bytes", br.getTotalBytes());
//				InternalDistributedMember m = pr.getBucketPrimary(br.getId());
//				if (m.getId().equals(member.getId())) {
//					primaryMap.put(br.getId(), map);
//				} else {
//					redundantMap.put(br.getId(), map);
//				}
//			}
			List<Mappable> primaryList = new ArrayList<Mappable>();
			List<Mappable> redundantList = new ArrayList<Mappable>();
			for (BucketRegion br : set2) {
				MapMessage map = new MapMessage();
				map.put("BucketId", br.getId());
				map.put("Size", br.size());
				map.put("Bytes", br.getTotalBytes());
				InternalDistributedMember m = pr.getBucketPrimary(br.getId());
				if (m.getId().equals(member.getId())) {
					primaryList.add(map);
				} else {
					redundantList.add(map);
				}
			}
			
			TreeMap map = new TreeMap();
//			map.put("Primary", primaryMap);
//			map.put("Redundant", redundantMap);
			map.put("Primary", primaryList);
			map.put("Redundant", redundantList);
			data.setDataObject(map);
			data.setUserData(pr.getPartitionAttributes().getTotalNumBuckets());
			
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
