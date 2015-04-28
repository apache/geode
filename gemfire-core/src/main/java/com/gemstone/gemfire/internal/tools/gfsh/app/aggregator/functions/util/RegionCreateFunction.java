package com.gemstone.gemfire.internal.tools.gfsh.app.aggregator.functions.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.RegionCreateTask;

public class RegionCreateFunction implements AggregateFunction, DataSerializable
{	
	private static final long serialVersionUID = 1L;

	private RegionCreateTask regionCreateTask;
	
	public RegionCreateFunction()
	{
	}
	
	public RegionCreateFunction(RegionCreateTask regionCreateTask)
	{
		this.regionCreateTask = regionCreateTask;
	}
	
	public RegionCreateTask getRegionCreateAllTask() 
	{
		return regionCreateTask;
	}

	public void setRegionCreateAllTask(RegionCreateTask regionCreateAllTask) 
	{
		this.regionCreateTask = regionCreateAllTask;
	}
	
	public AggregateResults run(FunctionContext context) 
	{
		AggregateResults results = new AggregateResults();
		results.setDataObject(regionCreateTask.runTask(null));
		return results;
	}

	/**
	 * Returns a java.util.List of LocalRegionInfo objects;
	 */
	public Object aggregate(List list)
	{
		ArrayList resultList = new ArrayList();
		for (int i = 0; i < list.size(); i++) {
			AggregateResults results = (AggregateResults)list.get(i);
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
		regionCreateTask = (RegionCreateTask)DataSerializer.readObject(input);
	}

	public void toData(DataOutput output) throws IOException 
	{
		DataSerializer.writeObject(regionCreateTask, output);
	}
}
