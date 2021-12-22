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

package org.apache.geode.internal.cache;

/**
 * Class <code>PartitionedRegionStatus</code> provides information about
 * <code>PartitionedRegion</code>s. This class is used by the monitoring tool.
 *
 *
 * @since GemFire 5.1
 */
public class PartitionedRegionStatus extends RegionStatus {
  private static final long serialVersionUID = -6755318987122602065L;

  protected int numberOfLocalEntries;

  public PartitionedRegionStatus(PartitionedRegion region) {
    initialize(region);
  }

  @Override
  public long getHeapSize() {
    return heapSize;
  }

  private void setHeapSize(long heapSize) {
    this.heapSize = heapSize;
  }

  private void initialize(PartitionedRegion region) {
    setNumberOfEntries(region.size());

    // If there is a data store (meaning that the PR has storage
    // in this VM), get the number of entries and heap size. Else,
    // set these to 0.
    PartitionedRegionDataStore ds = region.getDataStore();
    long heapSize = 0;
    if (ds != null) {
      heapSize = ds.currentAllocatedMemory();
    }
    setHeapSize(heapSize);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("PartitionedRegionStatus[").append("numberOfEntries=")
        .append(numberOfEntries).append("; numberOfLocalEntries=")
        .append(numberOfLocalEntries).append("; heapSize=").append(heapSize).append("]");
    return buffer.toString();
  }
}
