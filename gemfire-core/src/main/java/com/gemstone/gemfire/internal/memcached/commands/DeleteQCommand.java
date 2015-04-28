package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class DeleteQCommand extends DeleteCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
