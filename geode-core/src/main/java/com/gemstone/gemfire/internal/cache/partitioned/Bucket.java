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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.util.Set;

import com.gemstone.gemfire.internal.cache.BucketAdvisor;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisee;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.persistence.PersistenceAdvisor;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * Represents a storage or meta-data container for a 
 * <code>PartitionedRegion</code>.
 */
public interface Bucket extends CacheDistributionAdvisee {
  
  /**
   * Returns the distribution and metadata <code>BucketAdvisor</code> for this
   * bucket.
   */
  public BucketAdvisor getBucketAdvisor();
  
  /** 
   * Returns the serial number which identifies the static order in which this 
   * bucket was created in relation to other regions or other instances of 
   * this bucket during the life of this JVM.
   */
  public int getSerialNumber();
  
  /**
   * Returns true if this member is the primary for this bucket.
   */
  public boolean isPrimary();
  
  /**
   * Returns true if this bucket is currently backed by a {@link 
   * com.gemstone.gemfire.internal.cache.BucketRegion}.
   */
  public boolean isHosting();
  
  /**
   * Returns the bucket id used to uniquely identify the bucket in its
   * partitioned region
   * @return the unique identity of the bucket
   */
  public int getId();
  
  /**
   * Report members that are currently hosting the bucket 
   * @return set of members 
   * @since GemFire 5.9
   */
  public Set/*InternalDistributedMembers*/ getBucketOwners();

  public PersistenceAdvisor getPersistenceAdvisor();

  public DiskRegion getDiskRegion();

  /**
   * Returns the parent {@link PartitionedRegion} of this bucket.
   */
  public PartitionedRegion getPartitionedRegion();
}
