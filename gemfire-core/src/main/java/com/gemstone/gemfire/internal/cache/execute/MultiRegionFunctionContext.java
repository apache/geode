/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;

/**
 * Context available when called using
 * {@link InternalFunctionService#onRegions(Set)}
 * 
 * @author ymahajan
 * 
 * @since 6.5
 * 
 */
public interface MultiRegionFunctionContext extends FunctionContext {

  public Set<Region> getRegions();
  
  /**
   * Returns a boolean to identify whether this is a re-execute. Returns true if
   * it is a re-execute else returns false
   * 
   * @return a boolean (true) to identify whether it is a re-execute (else
   *         false)
   * 
   * @since 6.5
   * @see Function#isHA()
   */
  public boolean isPossibleDuplicate();

}
