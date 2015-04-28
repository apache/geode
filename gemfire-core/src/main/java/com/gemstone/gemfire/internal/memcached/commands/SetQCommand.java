package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 *
 */
public class SetQCommand extends SetCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
