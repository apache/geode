package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class QuitQCommand extends QuitCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
