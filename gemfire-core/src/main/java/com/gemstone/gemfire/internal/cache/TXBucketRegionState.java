/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * File comment
 */
package com.gemstone.gemfire.internal.cache;

/**
 * @author mthomas
 *
 */
public class TXBucketRegionState extends TXRegionState {
  private final PartitionedRegion pr;

  public TXBucketRegionState(BucketRegion r,TXState txs) {
    super(r,txs);
    this.pr = r.getPartitionedRegion();
  }
  
  public PartitionedRegion getPartitionedRegion() {
    return this.pr;
  }
}
