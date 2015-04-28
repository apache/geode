package com.gemstone.gemfire.internal.tools.gfsh.app.aggregator;

import java.util.ArrayList;
import java.util.HashMap;

import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.CommandClient;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.CommandException;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;


/**
 * Aggregator invokes a specified aggregate function on a specified region.
 * Aggregator works only in the client/server topology. As such, it requires endpoints
 * that specifies one or more cache servers to connect to. It also requires 
 * the full path of a command region with which it will communicate with the
 * cache server(s) listed in the endpoints. The command region path must be
 * unique per distributed system. If there are multiple distributed systems
 * that an aggregation needs to be performed on, then the command region
 * path must be unique for each distributed system. For example, given DS1
 * and DS2, DS1 can be assigned to "/command1" and DS2 to "/command2". It is
 * not permitted to share, however, "/command1" with both distributed systems. 
 * <p>
 * Aggregator internally keeps track of all aggregators created. If an 
 * aggregator already exists under a given commandRegionFullPath, 
 * then the endpoints of the existing aggregator will be used for all 
 * subsequently created aggregators. This also applies to the aggregators created
 * by the addAggregator() method. 
 * <p>
 * To remove all aggregators, invoke the static method closeAll(). Use this 
 * method with a care. All aggregators will be closed, and hence, none of them will
 * be valid after this call.
 * 
 * @author dpark
 *
 */
public class Aggregator
{
	/**
	 * The hashmap that contains the global aggregator counts. 
	 */
	private static HashMap allAggregatorsMap = new HashMap();
	
	/**
	 * SingleAggregator owned by this aggregator.
	 */
	private SingleAggregator thisAggregator;
	
	/**
	 * All SingleAggregator objects added by addAggregator() and the constructor.
	 */
	private SingleAggregator singleAggregators[];
	
	/**
	 * The aggregator map that contains all SingleAggregator objects added by 
	 * addAggregator() and the constructor. it contains 
	 * (commandRegionFullPath, SingleAggregator) pairs.
	 */
	private HashMap aggregatorMap = new HashMap(3);
	
	/**
	 * The aggregate invokation timeout in msec. The default value is 30000 msec (or 30 sec).
	 */
	private long timeout = 30000;
	
	/**
	 * Creates an aggregator with the specified command region path and endpoints.
	 * The cache servers specified in the endpoints must have also defined 
	 * the specified command region, otherwise, aggregate() will throw an exception.
	 * 
	 * @param commandRegionFullPath The full path of the command region used to
	 *                              communicate with the cache servers listed in 
	 *                              the endpoints. The cache servers must pre-define 
	 *                              the command region, otherwise, aggregate() will 
	 *                              throw an exception.
	 * @param endpoints The endpoints of cache server(s) that host the command region.
	 *                  The endpoints format is "end1=host1:port1,end2=host2:port2".
	 */
	public Aggregator(String commandRegionFullPath, String endpoints)
	{
		commandRegionFullPath = getCanonicalRegionPath(commandRegionFullPath);
		thisAggregator = new SingleAggregator(commandRegionFullPath, endpoints);
		synchronized (aggregatorMap) {
			singleAggregators = new SingleAggregator[1];
			singleAggregators[0] = thisAggregator;
			aggregatorMap.put(commandRegionFullPath, thisAggregator);
			allAggregatorsMap.put(commandRegionFullPath, new AggregatorCount(thisAggregator));
			incrementCount(commandRegionFullPath);
		}
	}
	
	/**
	 * Creates an aggregator with the specified command client.
	 * The cache servers must have also defined the command region defined,
	 * otherwise, aggregate() will throw an exception.
	 * 
	 * @param commandClient The command client to be used for sending aggregate requests
	 *                      to the cache servers.
	 */
	public Aggregator(CommandClient commandClient)
	{
		thisAggregator = new SingleAggregator(commandClient);
		String commandRegionFullPath = commandClient.getOutboxRegionFullPath();
		synchronized (aggregatorMap) {
			singleAggregators = new SingleAggregator[1];
			singleAggregators[0] = thisAggregator;
			aggregatorMap.put(commandRegionFullPath, thisAggregator);
			allAggregatorsMap.put(commandRegionFullPath, new AggregatorCount(thisAggregator));
			incrementCount(commandRegionFullPath);
		}
	}
	
	/**
	 * Increments the reference count of the specified aggregator.
	 * @param commandRegionFullPath The full path of the command region. 
	 * @return The incremented count. Returns -1 if the specified aggregator
	 *         does not exist.
	 */
	private static int incrementCount(String commandRegionFullPath)
	{
		AggregatorCount ac = (AggregatorCount)allAggregatorsMap.get(commandRegionFullPath);
		if (ac == null) {
			return -1;
		}
		ac.count++;
		return ac.count;
	}
	
	/**
	 * Decrements the reference count of the specified aggregator.
	 * If the decremented count is 0, then the aggregator is removed 
	 * from allAggregateorsMap. The caller must close the aggregator
	 * if the decremented count is 0.
	 * @param commandRegionFullPath The full path of the command region. 
	 * @return The decremented count. Returns -1 if the specified aggregator
	 *         does not exist.
	 */
	private static int decrementCount(String commandRegionFullPath)
	{
		AggregatorCount ac = (AggregatorCount)allAggregatorsMap.get(commandRegionFullPath);
		if (ac == null) {
			return -1;
		}
		ac.count--;
		if (ac.count <= 0) {
			allAggregatorsMap.remove(commandRegionFullPath);
		}
		
		return ac.count;
	}
	
	/**
	 * Returns the reference count of the specified aggregator.
	 * @param commandRegionFullPath The full path of the command region. 
	 */
	private static int getCount(String commandRegionFullPath)
	{
		AggregatorCount ac = (AggregatorCount)allAggregatorsMap.get(commandRegionFullPath);
		if (ac == null) {
			return 0;
		} 
		
		return ac.count;
	}
	
	/**
	 * Adds an aggregator. If the specified commandRegionFullPath has already been added
	 * in this aggregator, this call is silently ignored. It is important to note that 
	 * the specified endpoints is honored per unique commandRegionFullPath. That means 
	 * if another aggregator is already created or added with the same commandRegionFullPath,
	 * then that aggregator is used instead. A new aggregator will be created only if
	 * there is no aggregator found with the same commandRegionFullPath. In other words,
	 * the endpoints will not be assigned if there exist another aggregator that has 
	 * the same commandRegionFullPath. It is ignored silently and the exiting aggregator
	 * is used instead. Note that the exiting aggregator might have been assigned
	 * to a different endpoints.  
	 * 
	 * @param commandRegionFullPath The full path of the command region used to
	 *                              communicate with the cache servers listed in 
	 *                              the endpoints. The cache servers must pre-define 
	 *                              the command region, otherwise, aggregate() will 
	 *                              throw an exception.
	 * @param endpoints The endpoints of cache server(s) that host the command region.
	 *                  The endpoints format is "end1=host1:port1,end2=host2:port2".
	 * @throws AggregatorException Thrown if there is a cache related error. 
	 */
	public void addAggregator(String commandRegionFullPath, String endpoints) throws AggregatorException
	{
		if (isClosed()) {
			throw new AggregatorException("Aggregator closed. Unable to add the specified aggregator. Please create a new Aggregator first.");
		}
		
		synchronized (aggregatorMap) {
			commandRegionFullPath = getCanonicalRegionPath(commandRegionFullPath);
			SingleAggregator aggregator = (SingleAggregator)aggregatorMap.get(commandRegionFullPath);
			if (aggregator == null) {
				aggregator = new SingleAggregator(commandRegionFullPath, endpoints);
				aggregatorMap.put(commandRegionFullPath, aggregator);
				incrementCount(commandRegionFullPath);
				allAggregatorsMap.put(commandRegionFullPath, new AggregatorCount(aggregator));
				singleAggregators = (SingleAggregator[])aggregatorMap.values().toArray(new SingleAggregator[0]);
			}
		}
	}
	
	/**
	 * Removes the aggregator identified by the commandRegionFullPath from this aggregator.
	 * @param commandRegionFullPath The full path of the command region used to
	 *                              communicate with the cache servers listed in 
	 *                              the endpoints.
	 * @throws AggregatorException Thrown if there is a cache related error or
	 *                             this aggregator's commandRegionFullPath is same
	 *                             as the specified commandRegionFullPath. To remove
	 *                             this aggregator, invoke close() instead.
	 */
	public void removeAggregator(String commandRegionFullPath) throws AggregatorException
	{
		commandRegionFullPath = getCanonicalRegionPath(commandRegionFullPath);
		if (thisAggregator.getCommandRegionFullPath().equals(commandRegionFullPath)) {
			throw new AggregatorException("Removing the primary (this) aggregator is not allowed. Please use close() instead.");
		}
		
		remove(commandRegionFullPath);
	}
	
	/**
	 * Removes the specified aggregator. It closes the aggregator if the
	 * reference count is 0.
	 * 
	 * @param commandRegionFullPath The full path of the command region used to
	 *                              communicate with the cache servers listed in 
	 *                              the endpoints.
	 * @throws AggregatorException
	 */
	private void remove(String commandRegionFullPath) throws AggregatorException
	{
		synchronized (aggregatorMap) {
			SingleAggregator aggregator = (SingleAggregator)aggregatorMap.remove(commandRegionFullPath);
			if (aggregator != null) {
				decrementCount(commandRegionFullPath);
				if (getCount(commandRegionFullPath) <= 0) {
					aggregator.close();
				}
			}
		}
	}
	
	/**
	 * Closes this aggregator and removes all added aggregator. This aggregator
	 * is empty upon return of this call and no longer valid. The aggregate() 
	 * method will throw an exception if close() has been invoked.
	 * 
	 * @throws AggregatorException Thrown if there is a cache related error.
	 */
	public void close() throws AggregatorException
	{
		synchronized (aggregatorMap) {
			
			String paths[] = (String[])aggregatorMap.keySet().toArray(new String[0]);
			for (int i = 0; i < paths.length; i++) {
				remove(paths[i]);
			}
			aggregatorMap.clear();
			singleAggregators = new SingleAggregator[0];
			thisAggregator = null;
		}
	}
	
	/**
	 * Returns true if this aggregator is closed. If true, then this
	 * aggregator is no longer valid. All references to this object
	 * should be set to null so that it can be garbage collected.
	 * @return whether aggregator is closed
	 */
	public boolean isClosed()
	{
		return thisAggregator == null;
	}
	
	/**
	 * Closes all aggregators. All aggregators will be no longer valid
	 * after this call.
	 */
	public static void closeAll()
	{
		AggregatorCount acs[] = (AggregatorCount[])allAggregatorsMap.keySet().toArray(new AggregatorCount[0]);
		for (int i = 0; i < acs.length; i++) {
			try {
				acs[i].aggregator.close();
			} catch (AggregatorException e) {
				// ignore - do not throw an exception
				// because one of them failed. continue
				// closing all others.
			}
		}
		allAggregatorsMap.clear();
	}
	
	/**
	 * Executes the specified function and returns an aggregated result defined
	 * by the function. 
	 * 
	 * @param function The aggregate function to execute.
	 * @param regionFullPath The region on which the aggregate function to be
	 * 						 performed.
	 * @return Returns the result from the specified function. See the function
	 * 			definition for the return type.
	 * @throws AggregatorException Thrown if there is a cache related error.
	 */
	public synchronized Object aggregate(AggregateFunction function, String regionFullPath) throws AggregatorException
	{
		if (isClosed()) {
			throw new AggregatorException("Aggregator closed. Unable to aggregate. Please create a new Aggregator.");
		}
		
		SingleAggregator aggregators[] = this.singleAggregators;

		// If only one aggregator then no need to use separate threads.
		// Return the result using the current thread. 
		if (aggregators.length == 1) {
			return aggregators[0].aggregate(function, regionFullPath);
		}

		// Need to spawn threads to parallelize the aggregate fuction
		// execution on different distributed systems. Assign
		// a thread per aggregator (or distributed system).
		ArrayList resultsList = new ArrayList();
		ArrayList exceptionList = new ArrayList();
		long count = aggregators.length;
		for (int i = 0; i < count; i++) {
			new Thread(new DSAggregator(function, aggregators[i], regionFullPath, resultsList, exceptionList)).start();
		}
		
		// Wait and collect results returned by all aggregators.
		// resultsList contains results from all distributed systems.
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
				// ignore
			}
		} while (allResponded == false);
		
		// If all responded then aggregate the results by invoking the
		// AggregateFunction.aggregateDistributedSystems(), which is
		// responsible for aggregating the results.
		if (allResponded == false) {
			
			// Throw exception if not all responded
			if (exceptionList.isEmpty() == false) {
				
				throw new AggregatorException("Distributed System Error. Errors from " 
						+ exceptionList.size() 
						+ " distributed system(s). See getClientExceptions()", 
						(Throwable[])exceptionList.toArray(new Throwable[0]));
			} else {
				throw new AggregatorException("The aggregate operation timed out. Not all distributed systems responded within the timeout period of " + timeout + " msec.");
			}
		} else {
			Object results[] = resultsList.toArray();
			return function.aggregateDistributedSystems(results);
		}
	}
	
	/**
	 * Returns the timeout value in msec. The default value is 30000 msec (or 30 seconds)
	 */
	public long getTimeout() 
	{
		return timeout;
	}

	/**
	 * Sets the timeout in msec. The default value is 30000 msec (or 30 seconds)
	 * @param timeout The timeout value in msec.
	 */
	public void setTimeout(long timeout) 
	{
		this.timeout = timeout;
	}
	
	/**
	 * Returns the full path of the command region.
	 */
	public String getCommandRegionFullPath()
	{
		return thisAggregator.getCommandRegionFullPath();
	}
	
	/**
	 * Returns the endpoints.
	 */
	public String getEndpoints()
	{
		return thisAggregator.getEndpoints();
	}
	
	/**
	 * Returns the canonical region path.
	 * @param regionPath The region path to convert to a canonical form.
	 */
	private String getCanonicalRegionPath(String regionPath)
	{
		// Remove leading and trailing spaces.
		regionPath = regionPath.trim();
		
		// must begin with '/'.
		if (regionPath.startsWith("/") == false) {
			regionPath = "/" + regionPath;
		}
		
		return regionPath;
	}
	
	/**
	 * Notifies the results.
	 */
	private synchronized void notifyResults()
	{
		notify();
	}
	
	/**
	 * DSAggregator is a Runnable that each aggregate thread uses
	 * to keep aggregate context information separate from others.
	 */
	class DSAggregator implements Runnable
	{
		private AggregateFunction function;
		private SingleAggregator aggregator;
		private ArrayList resultsList;
		private ArrayList exceptionList;
		private String regionFullPath;
		
		DSAggregator(AggregateFunction function, SingleAggregator aggregator, String regionFullPath, ArrayList resultsList, ArrayList exceptionList)
		{
			this.function = function;
			this.aggregator = aggregator;
			this.regionFullPath = regionFullPath;
			this.resultsList = resultsList;
			this.exceptionList = exceptionList;
		}
		
		public void run()
		{
			try {
				Object results = aggregator.aggregate(function, regionFullPath);
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
	
	//FindBugs - make static inner class
	static class AggregatorCount
	{
		public int count = 0;
		public SingleAggregator aggregator;
		
		AggregatorCount(SingleAggregator aggregator)
		{
			this.aggregator = aggregator;
		}
	}
}

/**
 * SingleAggregator holds CommandClient information of an aggregator.
 */
class SingleAggregator
{
	private CommandClient commandClient;
	
	SingleAggregator(String commandRegionFullPath, String endpoints)
	{
		commandClient = new CommandClient(commandRegionFullPath, endpoints);
	}
	
	SingleAggregator(CommandClient commandClient)
	{
		this.commandClient = commandClient;
	}
	
	/**
	 * 
	 * @param function
	 * @param regionFullPath
	 * @return Returns null of isListenerEnabled() is true. In that case, the
	 *         aggregated results are delivered to the registered AggregatedDataListeners.
	 * @throws AggregatorException
	 */
	Object aggregate(AggregateFunction function, String regionFullPath) throws AggregatorException
	{
		try {
			CommandResults results = commandClient.execute(new AggregateFunctionTask(function, regionFullPath));
			if (results.getCode() != CommandResults.CODE_NORMAL) {
				throw new AggregatorException(results.getCodeMessage(), results.getException());
			}
			return results.getDataObject();
		} catch (Exception ex) {
			throw new AggregatorException(ex);
		}
	}
	
	String getCommandRegionFullPath()
	{
		return commandClient.getOutboxRegionFullPath();
	}
	
	String getEndpoints()
	{
		return commandClient.getEndpoints();
	}
	
	public void close() throws AggregatorException
	{
		try {
			commandClient.close();
		} catch (CommandException ex) {
			throw new AggregatorException(ex);
		}
	}
	
	public boolean isClosed()
	{
		return commandClient.isClosed();
	}
}

