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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

/**
 * TRAC #33726: afterRegionCreate event delivered before region initialization occurs
 */

public class AfterRegionCreateBeforeInitializationRegressionTest extends DistributedTestCase {

  private static final boolean[] flags = new boolean[2];
  private static Cache cache = null;
  private static DistributedSystem ds = null;
  private static boolean isOK = false;

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testAfterRegionCreate() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(() -> createCacheAndPopulateRegion1());
    vm1.invoke(() -> createCacheAndRegion2());
    boolean pass = vm1.invoke(() -> testFlag());
    assertTrue("The test failed", pass);

  }

  private void createCacheAndPopulateRegion1() {
    ds = getSystem();
    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attr = factory.create();
    Region region = cache.createRegion("testRegion", attr);
    Region subRegion = region.createSubregion("testSubRegion", attr);
    for (int i = 1; i < 100; i++) {
      region.put(i, i);
      subRegion.put(i, i);
    }
  }

  private void createCacheAndRegion2() {
    ds = getSystem();
    cache = CacheFactory.create(ds);
    AttributesFactory factory = new AttributesFactory();
    factory.setCacheListener(new TestCacheListener());
    factory.setScope(Scope.GLOBAL);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attr = factory.create();
    Region region = cache.createRegion("testRegion", attr);
    region.createSubregion("testSubRegion", attr);
  }

  private boolean testFlag() {
    if (isOK) {
      return isOK;
    } else {
      synchronized (AfterRegionCreateBeforeInitializationRegressionTest.class) {
        if (isOK) {
          return isOK;
        } else {
          try {
            AfterRegionCreateBeforeInitializationRegressionTest.class.wait(120000);
          } catch (InterruptedException ie) {
            fail("interrupted");
          }
        }
      }
      return isOK;
    }
  }

  private static class TestCacheListener extends CacheListenerAdapter {

    @Override
    public void afterRegionCreate(RegionEvent event) {
      Region region = event.getRegion();
      if (((LocalRegion) region).isInitialized()) {
        String regionPath = event.getRegion().getFullPath();
        if (regionPath.contains(SEPARATOR + "testRegion" + SEPARATOR + "testSubRegion")) {
          flags[1] = true;
        } else if (regionPath.contains(SEPARATOR + "testRegion")) {
          flags[0] = true;
        }

      }
      if (flags[0] && flags[1]) {
        isOK = true;
        synchronized (AfterRegionCreateBeforeInitializationRegressionTest.class) {
          AfterRegionCreateBeforeInitializationRegressionTest.class.notify();
        }
      }
    }
  }
}
