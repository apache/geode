package com.gemstone.gemfire.internal.redis;

/**
 * This defines a command that can be extended,d there may need some level of abstraction
 * 
 * @author Vitaliy Gavrilov
 *
 */
public interface Extendable {

  /**
   * 
   * @return
   */
  public String getArgsError();
  
}
