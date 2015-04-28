package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class ForceGCTask implements CommandTask
{
	private static final long serialVersionUID = 1L;
	
	public ForceGCTask() {}
	
	@SuppressFBWarnings(value="DM_GC",justification="This is the desired functionality")
	public CommandResults runTask(Object userData)
	{
		Runtime.getRuntime().gc();
		return new CommandResults();
	}

	public void fromData(DataInput in) throws IOException,
			ClassNotFoundException
	{
	}

	public void toData(DataOutput out) throws IOException
	{
	}
}
