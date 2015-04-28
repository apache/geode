package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class GATCommand extends TouchCommand {

  @Override
  protected boolean sendValue() {
    return true;
  }
}
