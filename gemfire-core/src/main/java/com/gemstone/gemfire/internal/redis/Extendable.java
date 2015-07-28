package com.gemstone.gemfire.internal.redis;

/**
 * This defines a command that can be extended, and there may need some level of abstraction
 * 
 * @author Vitaliy Gavrilov
 *
 */
public interface Extendable {

  /**
   * Getter for error message in case of argument arity mismatch
   * @return Error string
   */
  public String getArgsError();
  
}
