/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache.control;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Factory for defining and starting a {@link CompletableFuture} that returns
 * {@link RestoreRedundancyResults}.
 */
public interface RestoreRedundancyBuilder {
  /**
   * Specify which regions to include in the restore redundancy operation. The default,
   * <code>null<code>, means all regions should be included. Includes take precedence over
   * excludes.
   *
   * @param regions A set containing the names of regions to include.
   */
  RestoreRedundancyBuilder includeRegions(Set<String> regions);

  /**
   * Exclude specific regions from the restore redundancy operation. The default,
   * <code>null<code>, means don't exclude any regions.
   *
   * @param regions A set containing the names of regions to exclude.
   */
  RestoreRedundancyBuilder excludeRegions(Set<String> regions);

  /**
   * Set whether the restore redundancy operation should reassign primary buckets. The default,
   * <code>true</code>, will result in primary buckets being reassigned for better load balancing
   * across members.
   *
   * @param shouldReassign A boolean indicating whether or not the operation created by this
   *        class should reassign primary bucket hosts.
   */
  RestoreRedundancyBuilder setReassignPrimaries(boolean shouldReassign);

  /**
   * Asynchronously starts a new restore redundancy operation. Only the cache resources used by this
   * member will have redundancy restored. The operation may queue as needed for resources in
   * contention by other active restore redundancy operations.
   *
   * @return a {@link CompletableFuture} which will return the results of the restore redundancy
   *         operation started by this method.
   */
  CompletableFuture<RestoreRedundancyResults> start();
}
