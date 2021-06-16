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
package org.apache.geode.internal.cache.partitioned;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionStats;
import org.apache.geode.internal.cache.PrimaryBucketException;

public interface PartitionMessageDistribution {
  /**
   * Creates the key/value pair into the remote target that is managing the key's bucket.
   *
   * @param partitionedRegion the region that owns the entry
   * @param prStats the stats for the region
   * @param recipient member id of the recipient of the operation
   * @param event the event prompting this request
   * @param requireOldValue true indicates to propagate the old value on the event
   * @throws ForceReattemptException if the peer is no longer available
   */
  boolean createRemotely(PartitionedRegion partitionedRegion, PartitionedRegionStats prStats,
      DistributedMember recipient, EntryEventImpl event, boolean requireOldValue)
      throws ForceReattemptException;

  /**
   * Puts the key/value pair into the remote target that is managing the key's bucket.
   *
   * @param partitionedRegion the region that owns the entry
   * @param prStats the stats for the region
   * @param recipient the member to receive the message
   * @param event the event prompting this action
   * @param ifNew the mysterious ifNew parameter
   * @param ifOld the mysterious ifOld parameter
   * @param requireOldValue true indicates to propagate the old value on the event
   * @return whether the operation succeeded
   * @throws PrimaryBucketException if the remote bucket was not the primary
   * @throws ForceReattemptException if the peer is no longer available
   */
  boolean putRemotely(PartitionedRegion partitionedRegion, PartitionedRegionStats prStats,
      DistributedMember recipient, EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue)
      throws ForceReattemptException;
}
