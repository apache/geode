package com.gemstone.gemfire.internal.tools.gfsh.command;

import com.gemstone.gemfire.DataSerializable;

/**
 * CommandTask is an interface that must be implemented for executing
 * command task. The server invokes CommandTask.runTask() and returns
 * its results to the client.
 * @author dpark
 *
 */
public interface CommandTask extends DataSerializable
{	
	/**
	 * Runs this task. A client executes CommandTak by calling
	 * CommandClient.execute(CommandTask task).
	 * @param userData The userData optionally provided by the cache server. The
	 *                 cache server may pass any user data to the command task.
	 * @return Returns the task results.
	 */
    public CommandResults runTask(Object userData);

}
