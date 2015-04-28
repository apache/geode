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
import com.gemstone.gemfire.cache.execute.ResultSender;

/**
 * Context available when called using
 * {@link InternalFunctionService#onRegions(Set)}
 * 
 * @author ymahajan
 * 
 * @since 6.5
 * 
 */
public class MultiRegionFunctionContextImpl extends FunctionContextImpl
    implements MultiRegionFunctionContext {

  private Set<Region> regions = null;
  
  private final boolean isPossibleDuplicate;

  public MultiRegionFunctionContextImpl(final String functionId,
      final Object args, ResultSender resultSender, Set<Region> regions,
      boolean isPossibleDuplicate) {
    super(functionId, args, resultSender);
    this.regions = regions;
    this.isPossibleDuplicate = isPossibleDuplicate;
  }

  public Set<Region> getRegions() {
    return regions;
  }
  
  public boolean isPossibleDuplicate() {
    return isPossibleDuplicate;
  }

}
