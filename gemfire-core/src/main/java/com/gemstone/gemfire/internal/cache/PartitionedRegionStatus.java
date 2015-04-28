/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

/**
 * Class <code>PartitionedRegionStatus</code> provides information about
 * <code>PartitionedRegion</code>s. This class is used by the monitoring tool.
 *
 * @author Barry Oglesby
 *
 * @since 5.1
 */
public class PartitionedRegionStatus extends RegionStatus {
  private static final long serialVersionUID = -6755318987122602065L;

  protected int numberOfLocalEntries;

  public PartitionedRegionStatus(PartitionedRegion region) {
    initialize(region);
  }

  public int getNumberOfLocalEntries() {
    return this.numberOfLocalEntries;
  }

  protected void setNumberOfLocalEntries(int numberOfLocalEntries) {
    this.numberOfLocalEntries = numberOfLocalEntries;
  }

  @Override
  public long getHeapSize() {
    return this.heapSize;
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
    int numLocalEntries = 0;
    long heapSize = 0;
    if (ds != null) {
      CachePerfStats cpStats = ds.getCachePerfStats();
      numLocalEntries = (int) cpStats.getEntries();
      heapSize = ds.currentAllocatedMemory();
    }
    setNumberOfLocalEntries(numLocalEntries);
    setHeapSize(heapSize);
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("PartitionedRegionStatus[")
      .append("numberOfEntries=")
      .append(this.numberOfEntries)
      .append("; numberOfLocalEntries=")
      .append(this.numberOfLocalEntries)
      .append("; heapSize=")
      .append(this.heapSize)
      .append("]");
    return buffer.toString();
  }
}
