/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** 
 * Test the distribution limitations of transactions.  Other tests can be found in
 * <code>MultiVMRegionTestCase</code>.
 * 
 *
 * @author Mitch Thomas
 * @since 4.0
 * @see MultiVMRegionTestCase
 *
 */

package com.gemstone.gemfire.cache30;


import com.gemstone.gemfire.cache.*;

import java.io.File;
import dunit.*;
import com.gemstone.gemfire.internal.OSProcess;

public class TXRestrictionsDUnitTest extends CacheTestCase {
  public TXRestrictionsDUnitTest(String name) {
    super(name);
  }

  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    return factory.create();
  }

  protected RegionAttributes getDiskRegionAttributes() {
    AttributesFactory factory = new AttributesFactory(getRegionAttributes());
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + OSProcess.getId());
    diskDirs[0].mkdirs();
    factory.setDiskStoreName(getCache().createDiskStoreFactory()
                             .setDiskDirs(diskDirs)
                             .setTimeInterval(1000)
                             .setQueueSize(0)
                             .create("TXRestrictionsDUnitTest")
                             .getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }

  /** 
   * Check that remote persistent regions cause conflicts
   */
  public void testPersistentRestriction() throws Exception {
    final CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();
    final String misConfigRegionName = getUniqueName();
    Region misConfigRgn = getCache().createRegion(misConfigRegionName, getDiskRegionAttributes());
    invokeInEveryVM(new SerializableRunnable("testPersistentRestriction: Illegal Region Configuration") {
        public void run() {
          try {
            getCache().createRegion(misConfigRegionName, getDiskRegionAttributes());
            // rgn1.put("misConfigKey", "oldmisConfigVal");
          } catch (CacheException e) {
            fail("While creating region", e);
          }
        }
      });
    misConfigRgn.put("misConfigKey", "oldmisConfigVal");

    txMgr.begin();
    
    try {
      misConfigRgn.put("misConfigKey", "newmisConfigVal");
      fail("Expected an IllegalStateException with information about misconfigured regions");
    } catch (UnsupportedOperationException expected) {
      getSystem().getLogWriter().info("Expected exception: " + expected);
      txMgr.rollback();
    }
    misConfigRgn.destroyRegion();
  }
}
