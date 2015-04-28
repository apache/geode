package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class GATQCommand extends GATCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
