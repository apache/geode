/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;

/**
 * @author dsmith
 *
 */
public class PersistentPartitionedRegionOldConfigDUnitTest extends
    PersistentPartitionedRegionDUnitTest {

  public PersistentPartitionedRegionOldConfigDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected RegionAttributes getPersistentPRAttributes(
      final int redundancy, final int recoveryDelay, Cache cache, int numBuckets,
      boolean synchronous) {
    AttributesFactory af = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy);
    paf.setRecoveryDelay(recoveryDelay);
    paf.setTotalNumBuckets(numBuckets);
    af.setPartitionAttributes(paf.create());
    af.setDataPolicy(DataPolicy.PERSISTENT_PARTITION);
    af.setDiskDirs(getDiskDirs());
    RegionAttributes attr = af.create();
    return attr;
  }
}
