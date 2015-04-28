package com.gemstone.gemfire.internal.tools.gfsh.app.aggregator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregatorPartitionFunction;

/**
 * AggregatorPeer is experimental use only. If the application is a peer member 
 * hosting the specified region, then this class may provide better performance
 * than Aggregator, which uses the client/server topology. It also does not 
 * require the command region to be created unless the method 
 * aggregate(AggregateFunction function, Aggregator aggregatorClient) is used.
 * In which case, the command region is created by Aggregator.
 * 
 * @author dpark
 *
 */
public class AggregatorPeer
{
	PartitionedRegion region;
	Execution execution;
	
	// 30 sec timeout;
	private long timeout = 30000;
	private Set routingKeySet;
	
	/**
	 * Creates a AggregatorPeer object with the specified region path.
	 * @param regionFullPath The full path of the partitioned region on 
	 * 						which aggregation operations are to be executed. 
	 */
	public AggregatorPeer(String regionFullPath)
	{
		Cache cache = CacheFactory.getAnyInstance();
		region = (PartitionedRegion)cache.getRegion(regionFullPath);
		init();
	}
	
	/**
	 * Creates an AggregatorPeer object with the specified partitioned region.
	 * @param region The partitioned region on which aggregation operations 
	 * 				are to be executed.
	 */
	public AggregatorPeer(Region region)
	{
		this.region = (PartitionedRegion)region;
		init();
	}
	
	private void init()
	{
		execution = FunctionService.onMembers(region.getCache().getDistributedSystem());
		int totalNumBuckets = region.getPartitionAttributes().getTotalNumBuckets();
		routingKeySet = new CopyOnWriteArraySet();
		for (int i = 0; i < totalNumBuckets; i++) {
			routingKeySet.add(i);
		}
	}
	
	public void setRoutingKeys(Set routingKeySet)
	{
		this.routingKeySet = routingKeySet;
	}
	
	public Set getRoutingKeys()
	{
		return routingKeySet;
	}
	
	public Region getRegion()
	{
		return region;
	}
	
	public Object aggregate(AggregateFunction function) throws AggregatorException
	{
		return aggregate(function, routingKeySet);
	}
	
	/**
	 * Executes the specified function in each of the partition buckets identified
	 * by the specified routingKeySet. 
	 * @param function aggregate function to execute.
	 * @param routingKeySet A set of RoutingKey objects that identify the
	 *                      partition buckets in which the function to be executed. 
	 * @return Returns aggregate results. The result type is specified by the function 
	 * 	       passed in. 
	 * @throws AggregatorException
	 */
	private Object aggregate(AggregateFunction function, Set routingKeySet) throws AggregatorException
	{
		try {
			Object obj = execution.withArgs(function).execute(AggregatorPartitionFunction.ID).getResult();
			if (obj instanceof List) {
				List list = (List)obj;
				return function.aggregate(list);
			} else if (obj instanceof Map) {
				// GFE 6.3 support
				Map map = (Map)obj;
				ArrayList list = new ArrayList();
				Collection<List> col = map.values();
				for (List list2 : col) {
					list.addAll(list2);
				}
				return function.aggregate(list);
			} else {
				throw new AggregatorException("Unsupported aggregate result type: " + obj.getClass().getName());
			}
		} catch (Exception ex) {
			throw new AggregatorException(ex);
		}
	}

	public synchronized Object aggregate(AggregateFunction function, Aggregator aggregatorClient)  throws AggregatorException
	{
		if (aggregatorClient == null) {
			return aggregate(function);
		} else {
			ArrayList resultsList = new ArrayList();
			ArrayList exceptionList = new ArrayList();
			long count = 2;
			new Thread(new LocalAggregator(function, resultsList, exceptionList)).start();
			
			for (int i = 0; i < count - 1; i++) {
				new Thread(new DSAggregator(function, aggregatorClient, resultsList, exceptionList)).start();
			}
			boolean allResponded = false;
			long startTime = System.currentTimeMillis();
			do {
				try {
					wait(timeout);
					synchronized (resultsList) {
						allResponded = resultsList.size() == count;
						if (allResponded == false) {
							if (exceptionList.isEmpty() == false) {
								break;
							}
						}
					}
					if (allResponded == false && System.currentTimeMillis() - startTime >= timeout) {
						break;
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} while (allResponded == false);
			
			if (allResponded == false) {
				if (exceptionList.isEmpty() == false) {
					
					throw new AggregatorException("Distributed System Error. Errors from " 
							+ exceptionList.size() 
							+ " distributed system(s). See getClientExceptions()", 
							(Throwable[])exceptionList.toArray(new Throwable[0]));
				} else {
					throw new AggregatorException("The aggregate operation timedout. Not all distributed systems responded within the timeout period of " + timeout + " msec.");
				}
			} else {
				Object results[] = resultsList.toArray();
				return function.aggregateDistributedSystems(results);
			}
		}
	}
	
	public long getTimeout() 
	{
		return timeout;
	}

	public void setTimeout(long timeout) 
	{
		this.timeout = timeout;
	}
	
	private synchronized void notifyResults()
	{
		notify();
	}
	
	class LocalAggregator implements Runnable
	{
		private AggregateFunction function;
		private ArrayList resultsList;
		private ArrayList exceptionList;
		
		LocalAggregator(AggregateFunction function, ArrayList resultsList, ArrayList exceptionList)
		{
			this.function = function;
			this.resultsList = resultsList;
			this.exceptionList = exceptionList;
		}
		
		public void run()
		{
			try {
				Object results = aggregate(function);
				synchronized (resultsList) {
					resultsList.add(results);
				}
				notifyResults();
			} catch (AggregatorException ex) {
				synchronized (resultsList) {
					exceptionList.add(ex);
				}
				notifyResults();
			}
		}
	}
	
	class DSAggregator implements Runnable
	{
		private AggregateFunction function;
		private Aggregator aggregatorClient;
		private ArrayList resultsList;
		private ArrayList exceptionList;
		
		DSAggregator(AggregateFunction function, Aggregator aggregatorClient, ArrayList resultsList, ArrayList exceptionList)
		{
			this.function = function;
			this.aggregatorClient = aggregatorClient;
			this.resultsList = resultsList;
			this.exceptionList = exceptionList;
		}
		
		public void run()
		{
			try {
				Object results = aggregatorClient.aggregate(function, region.getFullPath());
				synchronized (resultsList) {
					resultsList.add(results);
				}
				notifyResults();
			} catch (AggregatorException ex) {
				synchronized (resultsList) {
					exceptionList.add(ex);
				}
				notifyResults();
			}
		}
	}
}
