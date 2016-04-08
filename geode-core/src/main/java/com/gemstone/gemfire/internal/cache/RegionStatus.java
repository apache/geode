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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;

import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;

import java.io.Serializable;

/**
 * Class <code>RegionStatus</code> provides information about
 * <code>Region</code>s. This class is used by the monitoring tool.
 *
 *
 * @since 5.1
 */
public class RegionStatus implements Serializable {
  private static final long serialVersionUID = 3442040750396350302L;

  protected int numberOfEntries;
  protected long heapSize;

  public RegionStatus() {}

  public RegionStatus(Region region) {
    initialize(region);
  }

  public int getNumberOfEntries() {
    return this.numberOfEntries;
  }

  protected void setNumberOfEntries(int numberOfEntries) {
    this.numberOfEntries = numberOfEntries;
  }

  public long getHeapSize() {
    return this.heapSize;
  }

  private void setHeapSize(long heapSize) {
    this.heapSize = heapSize;
  }

  private void initialize(Region region) {
    setNumberOfEntries(region.size());

    EvictionAttributes ea = region.getAttributes().getEvictionAttributes();
    if (ea != null && ea.getAlgorithm().isLRUMemory()) {
      LocalRegion lr = (LocalRegion) region;
      LRUStatistics stats = ((AbstractLRURegionMap) lr.getRegionMap())._getLruList().stats();
      setHeapSize(stats.getCounter());
    } else {
      setHeapSize(-1);
    }
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("RegionStatus[")
      .append("numberOfEntries=")
      .append(this.numberOfEntries)
      .append("; heapSize=")
      .append(this.heapSize)
      .append("]");
    return buffer.toString();
  }
}
