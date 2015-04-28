package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.AggregatorException;
import com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.AggregatorPeer;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.Indexer;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.IndexerManager;
import com.gemstone.gemfire.internal.tools.gfsh.app.misc.util.DataSerializerEx;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

public class QueryTask implements CommandTask, AggregateFunction
{
	private static final long serialVersionUID = 1L;
	
	public final static byte TYPE_KEYS = 0;
	public final static byte TYPE_VALUES = 1;
	public final static byte TYPE_KEYS_VALUES = 2;
	
	public final static byte ERROR_NO_ERROR = CommandResults.CODE_NORMAL;
	public final static byte ERROR_REGION_UNDEFINED = -2;
	public final static byte ERROR_INDEX_UNDEFINED = -3;
	public final static byte ERROR_AGGREGATOR = -3;
	
	public final static byte SUCCESS_RR = 1; //CommandResults.CODE_NORMAL + 1;
	public final static byte SUCCESS_PR = 2; //CommandResults.CODE_NORMAL + 2;
	
	private transient volatile boolean aggregationExecuted = false;
	private final Object aggregationExecutedLock = new Object();
	
	private String regionPath;
	private Object queryKey;
	private byte queryType = TYPE_KEYS_VALUES;

//	private AggregatorPeer aggregator;
	
	private static Set<Integer> routingKeySet;
	
	static {
		int numVMs = Integer.getInteger("indexer.aggregator.routingKeySize", 4);
		routingKeySet = new CopyOnWriteArraySet<Integer>();
		for (int i = 0; i < numVMs; i++) {
			routingKeySet.add(i);
		}
	}
	
	public QueryTask() {}

	public QueryTask(String regionPath, Object queryKey, byte queryType)
	{
		this.regionPath = regionPath;
		this.queryKey = queryKey;
		this.queryType = queryType;	
	}

	public CommandResults runTask(Object userData)
	{
		Cache cache = CacheFactory.getAnyInstance();
		cache.getLogger().fine("QueryTask.runTask(): regionPath = " + regionPath + ", type = " + queryType + ", queryKey = " + queryKey);
		
		CommandResults results = new CommandResults();
		
		Region region = cache.getRegion(regionPath);
		if (region == null) {
			results.setCode(ERROR_REGION_UNDEFINED);
			results.setCodeMessage("The specified region " + regionPath + " is undefined.");
			return results;
		}
		
		if (region instanceof PartitionedRegion) {	

			// Partitioned Region
			AggregatorPeer aggregator = new AggregatorPeer((PartitionedRegion)region);
			try {
				Object obj = aggregator.aggregate(this);
				results.setCode(SUCCESS_PR);
				results.setDataObject(obj);
			} catch (AggregatorException ex) {
				results.setCode(ERROR_AGGREGATOR);
				results.setCodeMessage("Unabled to create aggregator: " + ex.getMessage());
				ex.printStackTrace();
			}
			
		} else {
			
			// Replicated Region
			Indexer indexer = IndexerManager.getIndexerManager().getIndexer(regionPath);
			if (indexer == null) {
				results.setCode(ERROR_INDEX_UNDEFINED);
				results.setCodeMessage("The indexer for the specified region " + regionPath + " is undefined.");
				return results;
			}
			results.setCode(SUCCESS_RR);
			results.setDataObject(run(indexer));
			
		}
		return results;
	}
	
	public AggregateResults run(FunctionContext context)
	{
		AggregateResults results = null;
		synchronized (aggregationExecutedLock) {
			if (aggregationExecuted == false) {
				results = new AggregateResults();
				Indexer indexer = IndexerManager.getIndexerManager().getIndexer(regionPath);
				results.setDataObject(run(indexer));
				aggregationExecuted = true;
			}
		}
		return results;
	}
	
	public Object aggregate(List list)
	{	
		Object aggregateResults = null;
		
		switch (queryType) {
		
		// Set
		case TYPE_KEYS:
			try {
				Set aggregateSet = null;
				Iterator iterator = list.iterator();
				while (iterator.hasNext()) {
					AggregateResults results = (AggregateResults)iterator.next();
					byte[] blob = (byte[])results.getDataObject();
					if (blob != null) {
						Set set = (Set)BlobHelper.deserializeBlob(blob);
						if (aggregateSet == null) {
							aggregateSet = set;
						} else {
							aggregateSet.addAll(set);
						}
					}
				}
				aggregateResults = aggregateSet;
			} catch (Exception ex) {
				CacheFactory.getAnyInstance().getLogger().warning("Error occurred while deserializing to blob: " + ex.getMessage(), ex);
			}
			break;//FindBugs - Usually you need to end this 'case' with a break or return.
			
		// Collection
		case TYPE_VALUES:
			try {
				Collection aggregateCollection = null;
				Iterator iterator = list.iterator();
				while (iterator.hasNext()) {
					AggregateResults results = (AggregateResults)iterator.next();
					byte[] blob = (byte[])results.getDataObject();
					if (blob != null) {
						Collection collection = (Collection)BlobHelper.deserializeBlob(blob);
						if (aggregateCollection == null) {
							aggregateCollection = collection;
						} else {
							aggregateCollection.addAll(collection);
						}
					}
				}
				aggregateResults = aggregateCollection;
			} catch (Exception ex) {
				CacheFactory.getAnyInstance().getLogger().warning("Error occurred while deserializing to blob: " + ex.getMessage(), ex);
			}
			break;//FindBugs - Usually you need to end this 'case' with a break or return.
		
		// Map
		case TYPE_KEYS_VALUES:
		default:
			{
				List aggregateList = new ArrayList(list.size());
				Iterator iterator = list.iterator();
				while (iterator.hasNext()) {
					AggregateResults results = (AggregateResults)iterator.next();
					if (results != null) {
						byte[] blob = (byte[])results.getDataObject();
						if (blob != null) {
							aggregateList.add(blob);
						}
					}
				}
				aggregateResults = aggregateList;
				break;
			}
		}
		
//		byte blob[] = null;
//		if (aggregateResults != null) {
//			try {
//				blob = BlobHelper.serializeToBlob(aggregateResults);
//			} catch (IOException ex) {
//				CacheFactory.getAnyInstance().getLogger().warning("Error occurred while serializing to blob: " + ex.getMessage(), ex);
//			}
//		}
//		
//		return blob;
		
		return aggregateResults;
	}

	public Object aggregateDistributedSystems(Object[] results)
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	private Object run(Indexer indexer)
	{
		Object dataObject = null;
		
		switch (queryType) {
		case TYPE_KEYS:
			dataObject = queryKeys(indexer);
			break;
		case TYPE_VALUES:
			dataObject = queryValues(indexer);
			break;
		case TYPE_KEYS_VALUES:
		default:
			dataObject = queryKeysValues(indexer);
			break;
		}	
		return dataObject;
	}

	
	private Object queryKeys(Indexer indexer)
	{
		byte[] blob = null;

		Map map = indexer.query(queryKey);
		if (map != null) {
			try {
				// Need to serialize from here because "set" is
				// synchronized in Indexer.
				synchronized (map) {
					blob = BlobHelper.serializeToBlob(new HashSet(map.keySet()));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		map = null; // gc
		
		return blob;
	}
	
	private Object queryValues(Indexer indexer)
	{
		byte[] blob = null;
		
		Map map = indexer.query(queryKey);
		if (map != null) {
			try {
				// Need to serialize from here because "set" is
				// synchronized in Indexer.
				synchronized (map) {
					blob = BlobHelper.serializeToBlob(new HashSet(map.values()));
				}	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		map = null; // gc
		return blob;
	}
	
	private Object queryKeysValues(Indexer indexer)
	{	
		byte[] blob = null;
		
		Map map = indexer.query(queryKey);
		if (map != null) {
			try {
				// Need to serialize from here because "set" is
				// synchronized in Indexer.
				synchronized (map) {
					blob = BlobHelper.serializeToBlob(map);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return blob;
	}

	public void fromData(DataInput in) throws IOException,
			ClassNotFoundException
	{
		regionPath = DataSerializerEx.readUTF(in);
		queryKey = DataSerializer.readObject(in);
		queryType = in.readByte();
	}

	public void toData(DataOutput out) throws IOException
	{
		DataSerializerEx.writeUTF(regionPath, out);
		DataSerializer.writeObject(queryKey, out);
		out.writeByte(queryType);
	}
}
