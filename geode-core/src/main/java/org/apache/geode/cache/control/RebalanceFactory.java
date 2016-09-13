/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache.control;

import java.util.Set;

/**
 * Factory for defining and starting a {@link RebalanceOperation}.
 * 
 * @since GemFire 6.0
 */
public interface RebalanceFactory {
  
  /**
   * Specify which regions to include in the rebalance operation. The default,
   * <code>null<code>, means
   * all regions should be rebalanced. Includes take precedence over excludes.
   * 
   * @param regions
   *          A set containing the names of regions to include.
   * @since GemFire 6.5
   */
  RebalanceFactory includeRegions(Set<String> regions);
  
  /**
   * Exclude specific regions from the rebalancing operation. The default,
   * <code>null<code>, means
   * don't exclude any regions.
   * 
   * @param regions
   *          A set containing the names of regions to exclude.
   * @since GemFire 6.5
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
