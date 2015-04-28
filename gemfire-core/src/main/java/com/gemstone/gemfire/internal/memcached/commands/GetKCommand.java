package com.gemstone.gemfire.internal.memcached.commands;

/**
 * 
 * @author Swapnil Bawaskar
 */
public class GetKCommand extends GetCommand {

  @Override
  protected boolean sendKeysInResponse() {
    return true;
  }
  
}
