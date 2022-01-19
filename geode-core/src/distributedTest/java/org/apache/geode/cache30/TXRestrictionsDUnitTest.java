/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * Test the distribution limitations of transactions. Other tests can be found in
 * <code>MultiVMRegionTestCase</code>.
 *
 *
 * @since GemFire 4.0
 * @see MultiVMRegionTestCase
 *
 */
package org.apache.geode.cache30;

import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class TXRestrictionsDUnitTest extends JUnit4CacheTestCase {

  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    return factory.create();
  }

  protected <K, V> RegionAttributes<K, V> getDiskRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>(getRegionAttributes());
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("diskRegionDirs/" + OSProcess.getId());
    diskDirs[0].mkdirs();
    factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(diskDirs)
        .setTimeInterval(1000).setQueueSize(0).create("TXRestrictionsDUnitTest").getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return factory.create();
  }

  /**
   * Check that remote persistent regions cause conflicts
   */
  @Test
  public void testPersistentRestriction() throws Exception {
    final CacheTransactionManager txMgr = getCache().getCacheTransactionManager();
    final String misConfigRegionName = getUniqueName();
    Region misConfigRgn = getCache().createRegion(misConfigRegionName, getDiskRegionAttributes());
    Invoke.invokeInEveryVM(
        new SerializableRunnable("testPersistentRestriction: Illegal Region Configuration") {
          @Override
          public void run() {
            try {
              getCache().createRegion(misConfigRegionName, getDiskRegionAttributes());
              // rgn1.put("misConfigKey", "oldmisConfigVal");
            } catch (CacheException e) {
              Assert.fail("While creating region", e);
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
