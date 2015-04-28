/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.*;
import java.io.*;

/**
 * This class represents a snapshot of a {@link com.gemstone.gemfire.cache.CacheStatistics}
 * from a remote vm
 */
public class RemoteCacheStatistics implements CacheStatistics, DataSerializable {
  private static final long serialVersionUID = 53585856563375154L;
  private long lastModified;
  private long lastAccessed;
  private long hitCount;
  private long missCount;
  private float hitRatio;

  public RemoteCacheStatistics(CacheStatistics stats) {
    this.lastModified = stats.getLastModifiedTime();
    this.lastAccessed = stats.getLastAccessedTime();
    this.hitCount = stats.getHitCount();
    this.missCount = stats.getMissCount();
    this.hitRatio = stats.getHitRatio();    
  }

  /**
   * For use only by DataExternalizable mechanism
   */
  public RemoteCacheStatistics() {}

  public long getLastModifiedTime() {
    return lastModified;
  }

  public long getLastAccessedTime() {
    return lastAccessed;
  }

  public long getHitCount() {
    return hitCount;
  }

  public long getMissCount() {
    return missCount;
  }

  public float getHitRatio() {
    return hitRatio;
  }

  public void resetCounts() {
    throw new UnsupportedOperationException();
  }

  public void toData(DataOutput out) throws IOException {
    out.writeLong(lastModified);
    out.writeLong(lastAccessed);
    out.writeLong(hitCount);
    out.writeLong(missCount);
    out.writeFloat(hitRatio);
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    lastModified = in.readLong();
    lastAccessed = in.readLong();
    hitCount = in.readLong();
    missCount = in.readLong();
    hitRatio = in.readFloat();
  }
}
