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
package org.apache.geode.management.runtime;

import java.io.Serializable;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.JsonSerializable;

/**
 * Details of the rebalancing work performed for a partitioned region.
 */
@Experimental
public interface RebalanceRegionResult extends JsonSerializable, Serializable {
  /**
   * Returns the name of this region
   */
  String getRegionName();

  /**
   * Returns the size, in bytes, of all of the buckets that were created as part of the rebalance
   * operation for this region
   */
  long getBucketCreateBytes();

  /**
   * Returns the time, in milliseconds, taken to create buckets for this region
   */
  long getBucketCreateTimeInMilliseconds();

  /**
   * Returns the number of buckets created during the rebalance operation
   */
  int getBucketCreatesCompleted();

  /**
   * Returns the size, in bytes, of buckets that were transferred for this region.
   */
  long getBucketTransferBytes();

  /**
   * Returns the amount of time, in milliseconds, it took to transfer buckets for this region.
   */
  long getBucketTransferTimeInMilliseconds();

  /**
   * Returns the number of buckets transferred for this region.
   */
  int getBucketTransfersCompleted();

  /**
   * Returns the time, in milliseconds, spent transferring primaries for this region.
   */
  long getPrimaryTransferTimeInMilliseconds();

  /**
   * Returns the number of primaries that were transferred for this region.
   */
  int getPrimaryTransfersCompleted();

  /**
   * Returns the time, in milliseconds, that the rebalance operation took for this region.
   */
  long getTimeInMilliseconds();

  /**
   * Returns the number of members on which rebalance operation is executed.
   */
  int getNumOfMembers();
}
