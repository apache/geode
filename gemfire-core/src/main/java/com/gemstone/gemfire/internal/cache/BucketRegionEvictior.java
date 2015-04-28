/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.concurrent.Callable;
import com.gemstone.gemfire.cache.Region;

/**
 * 
 * Takes delta to be evicted and tries to evict the least no of LRU entry which
 * would make evictedBytes more than or equal to the delta
 * 
 * @author Suranjan Kumar
 * @author Amardeep Rajpal
 * @since 6.0
 * 
 */
public class BucketRegionEvictior implements Callable<Object> {
  private LocalRegion region;

  private int bytesToEvict;

  public BucketRegionEvictior(LocalRegion region, int bytesToEvict) {
    this.bytesToEvict = bytesToEvict;
    this.region = region;
  }

  public Region getRegion() {
    return this.region;
  }

  public int getDelta() {
    return this.bytesToEvict;
  }

  public void setRegion(Region reg) {
    this.region = (LocalRegion)reg;
  }

  public void setDelta(int bytes) {
    this.bytesToEvict = bytes;
  }
  
  public Object call() throws Exception {
    ((AbstractLRURegionMap)region.entries).lruUpdateCallback(bytesToEvict);
    return null;
  }

}
