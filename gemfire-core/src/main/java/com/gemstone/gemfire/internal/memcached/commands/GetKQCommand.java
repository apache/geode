package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 *
 */
public class GetKQCommand extends GetKCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
