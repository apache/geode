package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 *
 */
public class AddQCommand extends AddCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
