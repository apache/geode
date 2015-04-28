package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class DecrementQCommand extends DecrementCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
