package com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.functions.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;

public class LocalRegionInfoFunction implements AggregateFunction, DataSerializable
{
	private static final long serialVersionUID = 1L;

	private String regionPath;

	private static boolean priorTo6011 = true;

	static {
		priorTo6011 = isPriorTo6011();
	}

	static boolean isPriorTo6011()
	{
		String gemfireVersion = GemFireVersion.getGemFireVersion();
		String split[] = gemfireVersion.split("\\.");
		int major = 0;
		int minor = 0;
		int update = 0;
		int update2 = 0;
		for (int i = 0; i < split.length; i++) {
			switch (i) {
			case 0:
				major = Integer.parseInt(split[i]);
				break;
			case 1:
				try {
					minor = Integer.parseInt(split[i]);
				} catch (NumberFormatException ex) {
					minor = Integer.parseInt(split[i].substring(0, 1));
				}
				break;
			case 2:
        try {
          update = Integer.parseInt(split[i]);
        } catch (NumberFormatException ex) {
          // non-number. ignore
        }
				break;
			case 3:
        try {
          update2 = Integer.parseInt(split[i]);
        } catch (NumberFormatException ex) {
          // non-number. ignore.
        }
				break;
			}
		}

		if (major < 6) {
			return true; // 7
		} else if (minor > 0) {
			return false; // 6.1
		} else if (update < 1) {
			return true; // 6.0.0
		} else if (update > 1) {
			return false; // 6.0.2
		} else if (update2 <= 0) {
			return true; // 6.0.1.0
		} else {
			return false; // 6.0.1.1
		}

	}

	public LocalRegionInfoFunction()
	{
	}

	public LocalRegionInfoFunction(String regionPath)
	{
		this.regionPath = regionPath;
	}

	public String getRegionPath()
	{
		return regionPath;
	}

	public void setRegionPath(String regionPath)
	{
		this.regionPath = regionPath;
	}

	public AggregateResults run(FunctionContext context)
	{
		AggregateResults results = new AggregateResults();
		Cache cache = CacheFactory.getAnyInstance();
		DistributedSystem ds = cache.getDistributedSystem();
		DistributedMember member = ds.getDistributedMember();

		Region region = cache.getRegion(regionPath);
		if (region == null) {
			results.setCode(AggregateResults.CODE_ERROR);
			results.setCodeMessage("Undefined region: " + regionPath);
			return results;
		}

		MapMessage message = new MapMessage();
		message.put("MemberId", member.getId());
		message.put("MemberName", ds.getName());
		message.put("Host", member.getHost());
		// message.put("IpAddress",
		// dataSet.getNode().getMemberId().getIpAddress().getHostAddress());
		// message.put("Port", parent.getNode().getMemberId().getPort());
		message.put("Pid", member.getProcessId());
		message.put("RegionPath", regionPath);

		boolean isPR = region instanceof PartitionedRegion;
		message.put("IsPR", isPR);
		if (isPR) {
			PartitionedRegion pr = (PartitionedRegion) region;
			SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy h:mm:ss.SSS a");
			message.put("LastAccessedTime", format.format(new Date(pr.getLastAccessedTime())));
			message.put("LastModifiedTime", format.format(new Date(pr.getLastModifiedTime())));

			// total local primary bucket size
			int totalRegionSize = 0;
			// if (priorTo6011) { // 5.7 - 6.0.1
			// The following call is not returning the primary bucket regions.
			// totalRegionSize = PartitionRegionHelper.getLocalData(pr).size();

			// getDataStore() is null if peer client
			if (pr.getDataStore() == null) {
				message.put("IsPeerClient", true);
			} else {
				List<Integer> bucketIdList = pr.getDataStore().getLocalPrimaryBucketsListTestOnly();
				for (Integer bucketId : bucketIdList) {
					BucketRegion bucketRegion;
					try {
						bucketRegion = pr.getDataStore().getInitializedBucketForId(null, bucketId);
						totalRegionSize += bucketRegion.size();
					} catch (ForceReattemptException e) {
						// ignore
					}
				}
				message.put("IsPeerClient", false);
			}

			// } else {
			// // The following call is not returning the primary bucket
			// regions.
			// // totalRegionSize =
			// PartitionRegionHelper.getLocalData(pr).size();
			//				
			// Region localRegion = new LocalDataSet(pr,
			// pr.getDataStore().getAllLocalPrimaryBucketIds());
			// totalRegionSize = localRegion.size();
			// }
			message.put("RegionSize", totalRegionSize);

		} else {
			message.put("IsPeerClient", false);
			message.put("RegionSize", region.size());
			message.put("Scope", region.getAttributes().getScope().toString());
		}
		message.put("DataPolicy", region.getAttributes().getDataPolicy().toString());

		// info.evictionPolicy =
		// region.getAttributes().getEvictionAttributes().getAlgorithm();

		results.setDataObject(message);
		return results;
	}

	/**
	 * Returns a java.util.List of LocalRegionInfo objects;
	 */
	public Object aggregate(List list)
	{
		ArrayList resultList = new ArrayList();
		for (int i = 0; i < list.size(); i++) {
			AggregateResults results = (AggregateResults) list.get(i);
			if (results.getCode() == AggregateResults.CODE_ERROR) {
				// MapMessage info = new MapMessage();
				// info.put("Code", results.getCode());
				// info.put("CodeMessage", results.getCodeMessage());
				// info.put("RegionPath", regionPath);
				// resultList.add(info);
				// break;
				// ignore - occurs only if undefined region
				continue;
			}
			if (results.getDataObject() != null) {
				resultList.add(results.getDataObject());
			}
		}
		return resultList;
	}

	public Object aggregateDistributedSystems(Object[] results)
	{
		ArrayList list = new ArrayList();
		for (int i = 0; i < results.length; i++) {
			list.add(results[i]);
		}
		return list;
	}

	public void fromData(DataInput input) throws IOException, ClassNotFoundException
	{
		regionPath = DataSerializer.readString(input);
	}

	public void toData(DataOutput output) throws IOException
	{
		DataSerializer.writeString(regionPath, output);
	}
}
