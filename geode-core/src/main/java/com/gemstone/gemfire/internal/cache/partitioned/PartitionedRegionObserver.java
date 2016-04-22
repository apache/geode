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

import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * This interface is used by testing/debugging code to be notified of different
 * events. See the documentation for class PartitionedRegionObserverHolder for
 * details.
 * 
 * 
 */

public interface PartitionedRegionObserver {

  /**
   * This callback is called just before calculating starting bucket id on
   * datastore
   */
  public void beforeCalculatingStartingBucketId();
  
  public void beforeBucketCreation(PartitionedRegion region, int bucketId);
  /**
   * Called after a bucket region is created, but before it is added to the 
   * map of buckets.
   */
  public void beforeAssignBucket(PartitionedRegion partitionedRegion, int bucketId);
}
