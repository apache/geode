package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class IncrementQCommand extends IncrementCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
