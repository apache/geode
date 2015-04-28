/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.execute;

import java.util.Set;

import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

/**
 * Defines the execution context of a data dependent {@link Function}.
 * 
 * Applications can use the methods provided to retrieve the Region 
 * and context specific routing objects.  When the function is executed 
 * using {@link FunctionService#onRegion(Region)}, the {@link Function#execute(FunctionContext) FunctionContext}
 * can be typecast to this type.
 * </p>
 * <p>
 * If the region is a partitioned region,
 * {@link PartitionRegionHelper} can be used to get local and 
 * {@linkplain PartitionAttributesFactory#setColocatedWith(String) colocated} data references.
 * </p>
 * 
 * @author Yogesh Mahajan
 * @author Mitch Thomas
 * 
 * @since 6.0
 * 
 * @see FunctionContext
 * @see PartitionRegionHelper
 */
public interface RegionFunctionContext extends FunctionContext {

  /**
   * Returns subset of keys (filter) provided by the invoking thread (aka routing
   * objects). The set of filter keys are locally present in the datastore 
   * on the executing cluster member.
   * 
   * @see Execution#withFilter(Set)
   * 
   * @return the objects that caused the function to be routed to this cluster member
   * @since 6.0
   */
  public Set<?> getFilter();

  /**
   * Returns the reference to the Region on which the function is executed
   * 
   * @see FunctionService#onRegion(Region)
   * 
   * @return returns the Region on which the function is executed
   * 
   * @since 6.0
   */
  public <K, V> Region<K, V> getDataSet();
  
}
