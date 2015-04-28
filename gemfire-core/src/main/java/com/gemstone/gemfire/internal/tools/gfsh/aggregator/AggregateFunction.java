package com.gemstone.gemfire.internal.tools.gfsh.aggregator;

import java.io.Serializable;
import java.util.List;

import com.gemstone.gemfire.cache.execute.FunctionContext;

/**
 * All aggregate functions must implement AggregateFunction and use
 * Aggregator.aggregate() to execute them. The client application
 * submit a AggregateFunction via Aggregator.aggregate(), which in
 * turn sends it to the server(s) to be executed.
 * 
 * @author dpark
 *
 */
public interface AggregateFunction extends Serializable
{
	/**
	 * This is the first method invoked by each partition that has
	 * the required data to execute the function. Use 
	 * PartitionFunctionExecutionContext.getPartition() to get
	 * the partitioned region that contains the data that the function
	 * needs to execute on. The region can be iterated or queried to 
	 * get the results. The results must be Serializable and returned 
	 * in AggregateResults. Use AggregateResults.setDataObject
	 * (Object dataObject) to set the results. Any errors and exceptions
	 * can be set using AggregateResults.
	 *  
	 * @param context The ParitionFunction execution context.
	 * @return The results from this particular partition.
	 */
	AggregateResults run(FunctionContext context);
	
	/**
	 * Once all of the participating partitions returns AggregateResults, i.e.,
	 * the run(PartitionFunctionExecutionContext context) method is invoked
	 * in all participating partitions, this method is invoked with a list of
	 * AggregateResults objects. This method should iterate thru the list to
	 * perform a final aggregation. Each AggregateResults contains data 
	 * object generated from the run(PartitionFunctionExecutionContext context)
	 * method.
	 * 
	 * @param list The list of AggregateResults objects collected from all participating
	 *             partitions.
	 * @return The aggregated data. The return type is determined by each function.
	 *         As such, each function must document the return type.
	 */
	Object aggregate(List list);
	
	/**
	 * This method is invoked when an Aggregator has one or more aggregators
	 * in addition to itself, i.e., Aggregator.addAggregator() is invoked.
	 * It should iterate thru the passed-in results to perform a final aggregation.
	 * Each object in the results array represents the object returned by
	 * the aggregate(List list) method. 
	 *  
	 * @param results The array of AggregateResults objects collected from
	 *                the participating partitions.
	 * @return The final aggregated results representing the multiple distributed
	 *         systems.
	 */
	Object aggregateDistributedSystems(Object results[]);
}



