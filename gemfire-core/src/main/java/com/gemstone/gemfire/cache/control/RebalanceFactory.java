/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.control;

import java.util.Set;

/**
 * Factory for defining and starting a {@link RebalanceOperation}.
 * 
 * @since 6.0
 */
public interface RebalanceFactory {
  
  /**
   * Specify which regions to include in the rebalance operation. The default,
   * <code>null<code>, means
   * all regions should be rebalanced. Includes take precedence over excludes.
   * 
   * @param regions
   *          A set containing the names of regions to include.
   * @since 6.5
   */
  RebalanceFactory includeRegions(Set<String> regions);
  
  /**
   * Exclude specific regions from the rebalancing operation. The default,
   * <code>null<code>, means
   * don't exclude any regions.
   * 
   * @param regions
   *          A set containing the names of regions to exclude.
   * @since 6.5
   */
  RebalanceFactory excludeRegions(Set<String> regions);
  
  /**
   * Asynchronously starts a new rebalance operation. Only the GemFire
   * controlled cache resources used by this member will be rebalanced.
   * Operation may queue as needed for resources in contention by other
   * active rebalance operations.
   */
  public RebalanceOperation start();

  /**
   * Simulates a rebalance of the GemFire controlled cache resources on this
   * member. This operation will not make any actual changes. It will only
   * produce a report of what the results would have been had this been a real
   * rebalance operation.
   */
  public RebalanceOperation simulate();
}
