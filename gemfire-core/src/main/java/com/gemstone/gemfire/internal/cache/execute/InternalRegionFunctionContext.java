/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.execute;

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.internal.cache.LocalDataSet;

/**
 * Internal interface used to provide for some essential functionality for
 * {@link RegionFunctionContext} invoked by {@link PartitionRegionHelper}.
 * SQLFabric provides its own implementation when using function messages
 * instead of {@link Function}s so {@link PartitionRegionHelper} should not
 * depend on casting to {@link RegionFunctionContextImpl} directly rather should
 * use this interface.
 * 
 * @author swale
 */
public interface InternalRegionFunctionContext extends RegionFunctionContext {

  /**
   * Return a region providing read access limited to the local data set
   * corresponding to the routing keys as specified by the {@link #getFilter()}
   * method of the function context for the given partitioned region.
   * <p>
   * Writes using this Region have no constraints and behave the same as the
   * partitioned region.
   * 
   * @param r
   *          region to get the local data set for
   * 
   * @return a region for efficient reads or null if the region is not a
   *         partitioned region
   */
  public <K, V> Region<K, V> getLocalDataSet(Region<K, V> r);

  /**
   * Return a map of {@link PartitionAttributesFactory#setColocatedWith(String)}
   * colocated Regions with read access limited to the routing keys as specified
   * by the {@link #getFilter()} method of the function context.
   * <p>
   * Writes using these Region have no constraints and behave the same as a
   * partitioned Region.
   * <p>
   * If there are no colocated regions, return an empty map.
   * 
   * @return an unmodifiable map of {@linkplain Region#getFullPath() region
   *         name} to {@link Region}
   */
  public Map<String, LocalDataSet> getColocatedLocalDataSets();

  /**
   * Get the set of bucket IDs for this node as specified by the
   * {@link #getFilter()} method of the function context for the given region.
   * 
   * @param region
   *          region to get the local bucket IDs for
   * 
   * @return the set of bucket IDs for this node in this function context for
   *         the given region
   */
  public <K, V> Set<Integer> getLocalBucketSet(Region<K, V> region);
}
