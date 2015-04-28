package com.gemstone.gemfire.internal.util;

/**
 * Similar to the Trove TObjectProcedure, this is used in iterating over some
 * GemFire collections
 * 
 * @author bschuchardt
 *
 */
public interface ObjectIntProcedure {

  public boolean executeWith(Object a, int b);
  
}
