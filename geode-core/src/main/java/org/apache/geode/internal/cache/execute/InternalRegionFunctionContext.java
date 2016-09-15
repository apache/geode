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

package org.apache.geode.internal.cache.execute;

import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.LocalDataSet;

/**
 * Internal interface used to provide for some essential functionality for
 * {@link RegionFunctionContext} invoked by {@link PartitionRegionHelper}.
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
