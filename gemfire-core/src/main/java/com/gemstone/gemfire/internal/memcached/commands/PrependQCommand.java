package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class PrependQCommand extends PrependCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
