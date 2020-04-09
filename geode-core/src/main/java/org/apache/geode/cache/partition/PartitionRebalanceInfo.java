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

package org.apache.geode.cache.partition;

import java.util.Set;

/**
 * The detailed results of rebalancing a partitioned region.
 *
 * @since GemFire 6.0
 */
public interface PartitionRebalanceInfo {

  /**
   * Returns the {@link org.apache.geode.cache.Region#getFullPath() full path} of the partitioned
   * region that these details describe.
   *
   * @return the full name of partioned region that these details describe.
   */
  String getRegionPath();

  /**
   * Returns a <code>Set</code> of detailed information about each member that had rebalancable
   * resources at the time that the rebalance started.
   *
   * @return a <code>Set</code> of detailed information about each member that had rebalancable
   *         resources at the time that the rebalance started
   */
  Set<PartitionMemberInfo> getPartitionMemberDetailsBefore();

  /**
   * Returns a <code>Set</code> of detailed information about each member that had rebalancable
   * resources at the time that the rebalance completed.
   *
   * @return a <code>Set</code> of detailed information about each member that had rebalancable
   *         resources at the time that the rebalance completed
   */
  Set<PartitionMemberInfo> getPartitionMemberDetailsAfter();

  /**
   * Returns the time, in milliseconds, that the rebalance operation took for this region.
   *
   * @return the time, in milliseconds, that the rebalance operation took for this region.
   */
  long getTime();

  /**
   * Returns the number of buckets created during the rebalance operation.
   *
   * @return the number of buckets created during the rebalance operation
   */
  int getBucketCreatesCompleted();

  /**
   * Returns the size, in bytes, of all of the buckets that were created as part of the rebalance
   * operation.
   *
   * @return the size, in bytes, of all of the buckets that were created as part of the rebalance
   *         operation
   */
  long getBucketCreateBytes();

  /**
   * Returns the time, in milliseconds, taken to create buckets for this region.
   *
   * @return the time, in milliseconds, taken to create buckets for this region
   */
  long getBucketCreateTime();

  /**
   * Returns the number of buckets removed during the rebalance operation.
   *
   * @return the number of buckets removed during the rebalance operation
   */
  int getBucketRemovesCompleted();

  /**
   * Returns the size, in bytes, of all of the buckets that were removed as part of the rebalance
   * operation.
   *
   * @return the size, in bytes, of all of the buckets that were removed as part of the rebalance
   *         operation
   */
  long getBucketRemoveBytes();

  /**
   * Returns the time, in milliseconds, taken to remove buckets for this region.
   *
   * @return the time, in milliseconds, taken to remove buckets for this region
   */
  long getBucketRemoveTime();

  /**
   * Returns the number of buckets transferred for this region.
   *
   * @return the number of buckets transferred for this region
   */
  int getBucketTransfersCompleted();

  /**
   * Returns the size, in bytes, of buckets that were transferred for this region.
   *
   * @return the size, in bytes, of buckets that were transferred for this region
   */
  long getBucketTransferBytes();

  /**
   * Returns the amount of time, in milliseconds, it took to transfer buckets for this region.
   *
   * @return the amount of time, in milliseconds, it took to transfer buckets for this region
   */
  long getBucketTransferTime();

  /**
   * Returns the number of primaries that were transferred for this region.
   *
   * @return the number of primaries that were transferred for this region
   */
  int getPrimaryTransfersCompleted();

  /**
   * Returns the time, in milliseconds, spent transferring primaries for this region.
   *
   * @return the time, in milliseconds, spent transferring primaries for this region
   */
  long getPrimaryTransferTime();

  /**
   * Returns the total number of members on which command is executed.
   *
   * @return the total number of members on which command is executed
   */
  int getNumberOfMembersExecutedOn();
}
