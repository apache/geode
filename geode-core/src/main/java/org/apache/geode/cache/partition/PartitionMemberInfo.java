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

package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Describes a member that has been configured to provide storage space for
 * a partitioned region.
 * <p>
 * This is an immutable snapshot of the details.
 * 
 * @since GemFire 6.0
 */
public interface PartitionMemberInfo {
  
  /**
   * Identifies the member for which these details pertain to.
   * 
   * @return the member for which these details pertain to
   */
  public DistributedMember getDistributedMember();
  
  /**
   * Returns the {@link 
   * com.gemstone.gemfire.cache.PartitionAttributes#getLocalMaxMemory() max 
   * memory} in bytes that the member was configured to provide for storage
   * of data for the partitioned region.
   * 
   * @return the max memory in bytes that the member was configured to
   * provide for storage
   */
  public long getConfiguredMaxMemory(); // in bytes
  
  /**
   * The total size in bytes of memory being used by the member for storage
   * of actual data in the partitioned region.
   *  
   * @return size in bytes of memory being used by the member for storage
   */
  public long getSize(); // in bytes
  
  /**
   * Returns the number of buckets hosted within the member's partition space
   * for the partitioned region.
   * 
   * @return the number of buckets hosted within the member
   */
  public int getBucketCount();
  
  /**
   * The number of hosted buckets for which the member is hosting the primary
   * copy. Other copies are known as redundant backup copies.
   * 
   * @return the number of hosted buckets for which the member is hosting the 
   * primary copy
   */
  public int getPrimaryCount();
}
