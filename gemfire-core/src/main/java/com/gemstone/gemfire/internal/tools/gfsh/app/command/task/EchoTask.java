package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

/**
 * EchoTask returns itself back to the caller. CommandResults.getDataObject()
 * returns EchoTask.
 * @author dpark
 *
 */
public class EchoTask implements CommandTask {
	private static final long serialVersionUID = 1L;

	public static final byte ERROR_REGION_DESTROY = 1;
	
	private String message;

	public EchoTask() {
	}

	public EchoTask(String message) {
		this.message = message;
	}

	public CommandResults runTask(Object userData) {
		CommandResults results = new CommandResults();
		results.setDataObject(this);
		return results;
	}

	private void writeUTF(String value, DataOutput output) throws IOException
	{
		if (value == null) {
			output.writeUTF("\0");
		} else {
			output.writeUTF(value);
		}
	}

	private String readUTF(DataInput in) throws IOException
	{
		String value = in.readUTF();
		if (value.equals("\0")) {
			value = null;
		}
		return value;
	}
	
	public void fromData(DataInput input) throws IOException,
			ClassNotFoundException {
		message = readUTF(input);
	}

	public void toData(DataOutput output) throws IOException {
		writeUTF(message, output);
	}

}
