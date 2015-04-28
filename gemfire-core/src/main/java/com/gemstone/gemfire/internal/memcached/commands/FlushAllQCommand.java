package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class FlushAllQCommand extends FlushAllCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
