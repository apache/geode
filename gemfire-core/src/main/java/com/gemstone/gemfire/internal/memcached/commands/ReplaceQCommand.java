package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class ReplaceQCommand extends ReplaceCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
