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

import java.util.Set;

import com.gemstone.gemfire.cache.control.ResourceManager;

/**
 * Information describing the data storage and distribution of a 
 * partitioned region. The {@link PartitionRegionHelper} is used to gather 
 * <code>PartitionRegionInfo</code>. Each instance describes a single 
 * partitioned region identified by {@link #getRegionPath()}.
 * <p>
 * This is an immutable snapshot of the information.
 * 
 * @since GemFire 6.0
 */
public interface PartitionRegionInfo {

  /**
   * Returns the {@link com.gemstone.gemfire.cache.Region#getFullPath() 
   * full path} of the partitioned region that this object describes.
   * 
   * @return the full path of the partitioned region that this info describes
   */
  public String getRegionPath();
  
  /**
   * Returns an immutable set of <code>PartitionMemberInfo</code> 
   * representing every member that is configured to provide storage space to
   * the partitioned region.
   * 
   * @return set of member info configured for storage space
   */
  public Set<PartitionMemberInfo> getPartitionMemberInfo();
  
  /**
   * Returns the {@link 
   * com.gemstone.gemfire.cache.PartitionAttributes#getTotalNumBuckets()
   * configured number of buckets} for the partitioned region.
   * 
   * @return the configured number of buckets
   */
  public int getConfiguredBucketCount();
  
  /**
   * Returns the number of actual buckets that have been created to hold data
   * for the partitioned region. This is less than or equal to {#link 
   * #getConfiguredBucketCount()}.
   * 
   * @return the current number of actual buckets that have been created
   */
  public int getCreatedBucketCount();
  
  /**
   * Returns the number of created buckets that have fewer than the {@link 
   * #getConfiguredRedundantCopies() configured redundant copies} for this
   * partitioned region.
   * 
   * @return the number of created buckets that have fewer than the configured 
   * redundant copies
   * @see #getActualRedundantCopies()
   */
  public int getLowRedundancyBucketCount();
  
  /**
   * Returns the number of {@link 
   * com.gemstone.gemfire.cache.PartitionAttributes#getRedundantCopies()
   * redundant copies} the partitioned region was configured for.
   * 
   * @return the number of redundant copies the partitioned region was 
   * configured for
   */
  public int getConfiguredRedundantCopies();
  
  /**
   * Returns the lowest number of redundant copies for any bucket holding
   * data for the partitioned region. If all data is currently at full
   * redundancy then this will return the same value as {@link 
   * #getConfiguredRedundantCopies}.
   * 
   * @return the lowest number of redundant copies for any bucket of the
   * partitioned region.
   */
  public int getActualRedundantCopies();
  
  /**
   * Returns the {@link com.gemstone.gemfire.cache.Region#getFullPath() 
   * full path} of the partitioned region that this region has been configured 
   * to be {@link com.gemstone.gemfire.cache.PartitionAttributes#getColocatedWith() 
   * colocated with} or null if it is not colocated.
   * 
   * @return the full path of the partitioned region that the region is 
   * colocated with or null if there is none.
   */
  public String getColocatedWith();
}
