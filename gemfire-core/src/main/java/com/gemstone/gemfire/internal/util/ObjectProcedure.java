package com.gemstone.gemfire.internal.util;

/**
 * Similar to the Trove TObjectProcedure, this is used in iterating over some
 * GemFire collections
 * 
 * @author bschuchardt
 *
 */
public interface ObjectProcedure {

  public boolean executeWith(Object entry);
  
}
