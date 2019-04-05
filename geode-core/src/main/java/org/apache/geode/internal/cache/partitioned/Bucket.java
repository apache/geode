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

import java.util.Set;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.CacheDistributionAdvisee;
import org.apache.geode.internal.cache.HasDiskRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.persistence.PersistenceAdvisor;

/**
 * Represents a storage or meta-data container for a <code>PartitionedRegion</code>.
 */
public interface Bucket extends CacheDistributionAdvisee, HasDiskRegion {

  /**
   * Returns the distribution and metadata <code>BucketAdvisor</code> for this bucket.
   */
  BucketAdvisor getBucketAdvisor();

  /**
   * Returns the serial number which identifies the static order in which this bucket was created in
   * relation to other regions or other instances of this bucket during the life of this JVM.
   */
  @Override
  int getSerialNumber();

  /**
   * Returns true if this member is the primary for this bucket.
   */
  boolean isPrimary();

  /**
   * Returns true if this bucket is currently backed by a
   * {@link org.apache.geode.internal.cache.BucketRegion}.
   */
  boolean isHosting();

  /**
   * Returns the bucket id used to uniquely identify the bucket in its partitioned region
   *
   * @return the unique identity of the bucket
   */
  int getId();

  /**
   * Report members that are currently hosting the bucket
   *
   * @return set of members
   * @since GemFire 5.9
   */
  Set<InternalDistributedMember> getBucketOwners();

  PersistenceAdvisor getPersistenceAdvisor();

  /**
   * Returns the parent {@link PartitionedRegion} of this bucket.
   */
  PartitionedRegion getPartitionedRegion();
}
