/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author Barry Oglesby
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
