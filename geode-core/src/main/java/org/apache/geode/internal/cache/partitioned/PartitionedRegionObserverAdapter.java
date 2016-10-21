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

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.ProxyBucketRegion;

/**
 * This class provides 'do-nothing' implementations of all of the methods of interface
 * PartitionedRegionObserver. See the documentation for class PartitionedRegionObserverHolder for
 * details.
 * 
 */

public class PartitionedRegionObserverAdapter implements PartitionedRegionObserver {

  /**
   * This callback is called just before calculating starting bucket id on datastore
   */

  public void beforeCalculatingStartingBucketId() {}

  @Override
  public void beforeBucketCreation(PartitionedRegion region, int bucketId) {}

  @Override
  public void beforeAssignBucket(PartitionedRegion partitionedRegion, int bucketId) {}
}
