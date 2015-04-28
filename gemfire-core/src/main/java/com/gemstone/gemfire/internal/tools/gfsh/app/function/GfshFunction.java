package com.gemstone.gemfire.internal.tools.gfsh.app.function;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.command.task.data.MemberInfo;

public class GfshFunction implements AggregateFunction, DataSerializable
{	
	private static final long serialVersionUID = 1L;

	private String command; 
	private String regionPath;
	private Object arg;
	
	public GfshFunction()
	{
	}
	
	public GfshFunction(String command, String regionPath, Object arg)
	{
		this.command = command;
		this.regionPath = regionPath;
		this.arg = arg;
	}

	public String getCommand()
	{
		return command;
	}

	public void setCommand(String command)
	{
		this.command = command;
	}

	public Object getArg()
	{
		return arg;
	}

	public void setArg(Object arg)
	{
		this.arg = arg;
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
		try {
			String split[] = command.split(" ");
			String className = split[0].trim();
			Class clas = Class.forName("com.gemstone.gemfire.internal.tools.gfsh.app.function.command." + className);
			ServerExecutable se = (ServerExecutable)clas.newInstance();
			Object obj = se.execute(command, regionPath, arg);
			results.setDataObject(obj);
			results.setCode(se.getCode());
			results.setCodeMessage(se.getCodeMessage());
		} catch (Exception ex) {
			results.setCode(AggregateResults.CODE_ERROR);
			results.setCodeMessage(getCauseMessage(ex));
			results.setException(ex);
		}
		return results;
	}
	
	private String getCauseMessage(Throwable ex)
	{
		Throwable cause = ex.getCause();
		String causeMessage = null;
		if (cause != null) {
			causeMessage = getCauseMessage(cause);
		} else {
			causeMessage = ex.getClass().getSimpleName();
			causeMessage += " -- " + ex.getMessage();
		}
		return causeMessage;
	}

	/**
	 * Returns a java.util.List of LocalRegionInfo objects;
	 */
	public synchronized Object aggregate(List list)
	{
		// 5.7: each bucket returns results. Discard all but one that is success
		MemberInfo info;
		HashMap map = new HashMap();
		for (int i = 0; i < list.size(); i++) {
			AggregateResults results = (AggregateResults)list.get(i);
			GfshData data = (GfshData)results.getDataObject();
			if (data == null) {
				data = new GfshData(null);
			}
			info = data.getMemberInfo();
			AggregateResults mapResults = (AggregateResults)map.get(info.getMemberId());
			if (mapResults == null) {
				map.put(info.getMemberId(), results);
			} else if (mapResults.getCode() != AggregateResults.CODE_NORMAL) {
				map.put(info.getMemberId(), results);
			}
		}
		
		return new ArrayList(map.values());
	}
	
	public synchronized Object aggregateDistributedSystems(Object[] results)
	{
		ArrayList list = new ArrayList();
		for (int i = 0; i < results.length; i++) {
			list.add(results[i]);
		}
		return list;
	}
	
	public void fromData(DataInput input) throws IOException, ClassNotFoundException 
	{
		command = DataSerializer.readString(input);
		regionPath = DataSerializer.readString(input);
		arg = DataSerializer.readObject(input);
	}

	public void toData(DataOutput output) throws IOException 
	{
		DataSerializer.writeString(command, output);
		DataSerializer.writeString(regionPath, output);
		DataSerializer.writeObject(arg, output);
	}
	
}
