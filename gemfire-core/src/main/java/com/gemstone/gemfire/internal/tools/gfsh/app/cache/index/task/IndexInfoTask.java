package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.AggregatorException;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.AggregatorPeer;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.IndexInfo;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.Indexer;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.IndexerManager;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.DataSerializerEx;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

public class IndexInfoTask implements CommandTask, AggregateFunction
{
	private static final long serialVersionUID = 1L;

  private transient volatile boolean aggregationExecuted = false;
  private final Object aggregationExecutedLock = new Object();
	
	private String regionPath;

	public IndexInfoTask() {}

	public IndexInfoTask(String regionPath)
	{
		this.regionPath = regionPath;
	}

	public CommandResults runTask(Object userData)
	{
		Cache cache = CacheFactory.getAnyInstance();
		cache.getLogger().fine("IndexInfoTask.runTask(): regionPath = " + regionPath);
		
		CommandResults results = new CommandResults();
		
		Region region = cache.getRegion(regionPath);
		if (region == null) {
			results.setCode(QueryTask.ERROR_REGION_UNDEFINED);
			results.setCodeMessage("The specified region " + regionPath + " is undefined.");
			return results;
		}
		
		if (region instanceof PartitionedRegion) {	

			// Partitioned Region
			AggregatorPeer aggregator = new AggregatorPeer((PartitionedRegion)region);
			try {
				Object obj = aggregator.aggregate(this);
				results.setCode(QueryTask.SUCCESS_PR);
				results.setDataObject(obj);
			} catch (AggregatorException ex) {
				results.setCode(QueryTask.ERROR_AGGREGATOR);
				results.setCodeMessage("Unabled to create aggregator: " + ex.getMessage());
				ex.printStackTrace();
			}
			
		} else {
			
			// Replicated Region
			results.setCode(QueryTask.SUCCESS_RR);
			results.setDataObject(getIndexInfo());
			
		}

		return results;
	}

	public AggregateResults run(FunctionContext context)
	{
		AggregateResults results = null;
		synchronized (aggregationExecutedLock) {
			if (aggregationExecuted == false) {
				results = new AggregateResults();
				results.setDataObject(getIndexInfo());
				aggregationExecuted = true;
			}
		}
		return results;
	}

	public Object aggregate(List list)
	{
		ArrayList<IndexInfo> aggregateList = null;
		for (Iterator<AggregateResults> iterator = list.iterator(); iterator.hasNext();) {
			AggregateResults results = iterator.next();
			if (results != null) {
				if (aggregateList == null) {
					aggregateList = new ArrayList(list.size());
				}
				aggregateList.add((IndexInfo)results.getDataObject());
			}
		}
		return aggregateList;
	}

	public Object aggregateDistributedSystems(Object[] results)
	{
		return null;
	}
	
	private IndexInfo getIndexInfo()
	{
		Indexer indexer = IndexerManager.getIndexerManager().getIndexer(regionPath);
		return indexer.getIndexInfo();
	}

	public void fromData(DataInput in) throws IOException,
			ClassNotFoundException
	{
		regionPath = (String) DataSerializerEx.readUTF(in);
	}

	public void toData(DataOutput out) throws IOException
	{
		DataSerializerEx.writeUTF(regionPath, out);
	}

}
