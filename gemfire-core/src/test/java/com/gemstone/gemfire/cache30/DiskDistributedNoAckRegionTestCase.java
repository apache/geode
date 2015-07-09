/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;

/**
 *
 * @author Eric Zoerner
 *
 */
public abstract class DiskDistributedNoAckRegionTestCase extends DistributedNoAckRegionDUnitTest {
  final protected DiskRegionTestImpl regionTestImpl;
  
  /** Creates a new instance of DiskDistributedNoAckRegionTest */
  public DiskDistributedNoAckRegionTestCase(String name) {
    super(name);
    regionTestImpl = new DiskRegionTestImpl(this);
  }
  
  public void testCreateDiskRegion() throws CacheException {
    this.regionTestImpl.testCreateDiskRegion();
  }
  
  public void testBackupFillInValues() throws CacheException {
        this.regionTestImpl.testBackupFillValues();
  }
}
