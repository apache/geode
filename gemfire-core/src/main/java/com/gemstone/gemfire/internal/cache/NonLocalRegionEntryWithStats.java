/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.cache.StatisticsDisabledException;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalStatisticsDisabledException;

public class NonLocalRegionEntryWithStats extends NonLocalRegionEntry {
  private long hitCount;
  private long missCount;
  private long lastAccessed;
  
  public NonLocalRegionEntryWithStats(RegionEntry re, LocalRegion br, boolean allowTombstones) {
    super(re, br, allowTombstones);
    try {
      this.lastAccessed = re.getLastAccessed();
      this.hitCount = re.getHitCount();
      this.missCount = re.getMissCount();
    } catch (InternalStatisticsDisabledException unexpected) {
      Assert.assertTrue(false, "Unexpected " + unexpected); 
    }
  }
  
  @Override
  public boolean hasStats() {
    return true;
  }

  @Override
  public long getLastAccessed() throws StatisticsDisabledException {
    return this.lastAccessed;
  }

  @Override
  public long getHitCount() throws StatisticsDisabledException {
    return this.hitCount;
  }

  @Override
  public long getMissCount() throws StatisticsDisabledException {
    return this.missCount;
  }

  public NonLocalRegionEntryWithStats() {
    // for fromData
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(this.lastAccessed);
    out.writeLong(this.hitCount);
    out.writeLong(this.missCount);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.lastAccessed = in.readLong();
    this.hitCount = in.readLong();
    this.missCount = in.readLong();
  }
}