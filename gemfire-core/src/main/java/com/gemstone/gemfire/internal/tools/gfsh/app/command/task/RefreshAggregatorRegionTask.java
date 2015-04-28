package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.command.AbstractCommandTask;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;

public class RefreshAggregatorRegionTask extends AbstractCommandTask {
	private static final long serialVersionUID = 1L;

	// Default constructor required for serialization
	public RefreshAggregatorRegionTask() 
	{
	}
	
	private void initRegion()
	{
		PartitionedRegion pr = (PartitionedRegion)getCommandRegion().getSubregion("pr");
		if (pr == null) {
			return;
		}
		int totalBuckets = pr.getAttributes().getPartitionAttributes().getTotalNumBuckets();
		for (int i = 0; i < totalBuckets; i++) {
			pr.put(i, i);
			pr.remove(i);
		}
	}
	
	@Override
	public CommandResults runTask(Object userData)
	{
		new Thread(new Runnable() {
			public void run()
			{
				initRegion();
			}
		}).start();
		return null;
	}
	
	public void fromData(DataInput input) throws IOException,
			ClassNotFoundException 
	{
		super.fromData(input);
	}

	public void toData(DataOutput output) throws IOException {
		super.toData(output);
	}
}
