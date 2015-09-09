package com.gemstone.gemfire.internal.redis;


/**
 * Interface for executors of a {@link Command}
 * 
 * @author Vitaliy Gavrilov
 *
 */
public interface Executor {

  /**
   * This method executes the command and sets the response. Any runtime errors
   * from this execution should be handled by caller to ensure the client gets 
   * a response
   * 
   * @param command The command to be executed
   * @param context The execution context by which this command is to be executed
   */
  public void executeCommand(Command command, ExecutionHandlerContext context);
  
}
