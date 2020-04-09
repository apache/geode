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

import org.apache.geode.cache.partition.PartitionRebalanceInfo;

/**
 * The results of rebalancing <code>Cache</code> resources.
 *
 * @since GemFire 6.0
 */
public interface RebalanceResults {

  /**
   * Returns a <code>Set</code> of detailed information about each partitioned region that was
   * rebalanced.
   *
   * @return a <code>Set</code> of detailed information about each partitioned region that was
   *         rebalanced
   */
  Set<PartitionRebalanceInfo> getPartitionRebalanceDetails();

  /**
   * Returns the total time, in milliseconds, that the rebalance operation took.
   *
   * @return the total time, in milliseconds, that the rebalance operation took
   */
  long getTotalTime();

  /**
   * Returns the total number of buckets created during the rebalance operation.
   *
   * @return the total number of buckets created during the rebalance operation
   */
  int getTotalBucketCreatesCompleted();

  /**
   * Returns the total size, in bytes, of all of the buckets that were created as part of the
   * rebalance operation.
   *
   * @return the total size, in bytes, of all of the buckets that were created as part of the
   *         rebalance operation
   */
  long getTotalBucketCreateBytes();

  /**
   * Returns the total time, in milliseconds, taken to create buckets.
   *
   * @return the total time, in milliseconds, taken to create buckets
   */
  long getTotalBucketCreateTime();

  /**
   * Returns the total number of buckets transferred.
   *
   * @return the total number of buckets transferred
   */
  int getTotalBucketTransfersCompleted();

  /**
   * Returns the total size, in bytes, of buckets that were transferred.
   *
   * @return the total size, in bytes, of buckets that were transferred
   */
  long getTotalBucketTransferBytes();

  /**
   * Returns the total amount of time, in milliseconds, it took to transfer buckets.
   *
   * @return the total amount of time, in milliseconds, it took to transfer buckets
   */
  long getTotalBucketTransferTime();

  /**
   * Returns the total number of primaries that were transferred.
   *
   * @return the total number of primaries that were transferred
   */
  int getTotalPrimaryTransfersCompleted();

  /**
   * Returns the total time, in milliseconds, spent transferring primaries.
   *
   * @return the total time, in milliseconds, spent transferring primaries
   */
  long getTotalPrimaryTransferTime();

  /**
   * Returns the total number of members on which command is executed.
   *
   * @return the total number of members on which command is executed
   */
  int getTotalMembersExecutedOn();
}
