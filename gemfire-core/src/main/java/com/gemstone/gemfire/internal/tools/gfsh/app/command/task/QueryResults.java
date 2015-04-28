package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * QueryResults contains query results executed by QueryTask.
 * 
 * @author dpark
 * 
 */
public class QueryResults implements DataSerializable
{
	private static final long serialVersionUID = 1L;

	public static final byte ERROR_NONE = 0;
	public static final byte ERROR_QUERY = 1;

	private Object results;
	private int actualSize;
	private int fetchSize;
	private int returnedSize;
	private boolean isPR;
	
	public QueryResults() {}

	// Default constructor required for serialization
	public QueryResults(Object results, int actualSize, int fetchSize, int returnedSize) 
	{
		this(results, actualSize, fetchSize, returnedSize, false);
	}
	
	public QueryResults(Object results, int actualSize, int fetchSize, int returnedSize, boolean isPR) 
	{
		this.results = results;
		this.actualSize = actualSize;
		this.fetchSize = fetchSize;
		this.returnedSize = returnedSize;
		this.isPR = isPR;
	}

	/**
	 * Returns the fetch size. The default is 1000. If -1, fetches
	 * all.
	 * @return fetch size
	 */
	public int getFetchSize()
	{
		return fetchSize;
	}

	/**
	 * Sets the fetch size. The default is 1000. 
	 * @param fetchSize The fetch size. If -1, fetches all.
	 */
	public void setFetchSize(int fetchSize)
	{
		this.fetchSize = fetchSize;
	}
	
	public int getActualSize()
	{
		return actualSize;
	}

	public void setActualSize(int actualSize)
	{
		this.actualSize = actualSize;
	}
	
	public Object getResults()
	{
		return results;
	}

	public void setResults(Object results)
	{
		this.results = results;
	}

	public int getReturnedSize()
	{
		return returnedSize;
	}

	public void setReturnedSize(int returnedSize)
	{
		this.returnedSize = returnedSize;
	}

	public boolean isPR()
	{
		return isPR;
	}

	public void setPR(boolean isPR)
	{
		this.isPR = isPR;
	}

	public void fromData(DataInput input) throws IOException,
			ClassNotFoundException 
	{
		results = DataSerializer.readObject(input);
		actualSize = input.readInt();
		fetchSize = input.readInt();
		returnedSize = input.readInt();
		isPR = input.readBoolean();
	}

	public void toData(DataOutput output) throws IOException 
	{
		DataSerializer.writeObject(results, output);
		output.writeInt(actualSize);
		output.writeInt(fetchSize);
		output.writeInt(returnedSize);
		output.writeBoolean(isPR);
	}
}
