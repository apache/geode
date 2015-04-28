package com.gemstone.gemfire.internal.tools.gfsh.app.command;

import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;


/**
 * CommandResultsListener asynchronously receives CommandResults sent by 
 * AbstractCommandTask.sendResults().
 * @author dpark
 *
 */
public interface CommandResultsListener
{
    void commandResultsReceived(CommandResults commandResults);
}
