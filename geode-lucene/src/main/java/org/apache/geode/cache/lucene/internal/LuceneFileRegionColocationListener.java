/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal;

import org.apache.geode.internal.cache.ColocationListener;

public class LuceneFileRegionColocationListener implements ColocationListener {
  private final PartitionedRepositoryManager partitionedRepositoryManager;
  private final Integer bucketID;

  public LuceneFileRegionColocationListener(
      PartitionedRepositoryManager partitionedRepositoryManager, Integer bucketID) {
    this.partitionedRepositoryManager = partitionedRepositoryManager;
    this.bucketID = bucketID;
  }


  @Override
  public void afterColocationCompleted() {
    partitionedRepositoryManager.computeRepository(bucketID);
  }

  // Current implementation will allow only one LuceneFileRegionColocationListener to be
  // added to the PartitionRegion colocationListener set.
  @Override
  public int hashCode() {
    return bucketID.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return (obj != null && obj instanceof LuceneFileRegionColocationListener
        && ((LuceneFileRegionColocationListener) obj).bucketID != null
        && ((LuceneFileRegionColocationListener) obj).bucketID.equals(bucketID));
  }
}
