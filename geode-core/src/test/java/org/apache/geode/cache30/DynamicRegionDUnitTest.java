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
package org.apache.geode.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.io.File;
import java.util.Properties;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;

/**
 * Test to make sure dynamic regions work
 *
 * @since GemFire 4.3
 */
@Category(DistributedTest.class)
public class DynamicRegionDUnitTest extends JUnit4CacheTestCase {

  // Specify oplog size in MB
  private static final int OPLOG_SIZE = 1;

  public DynamicRegionDUnitTest() {
    super();
  }

  // this test has special config of its distributed system so
  // the setUp and tearDown methods need to make sure we don't
  // use the ds from previous test and that we don't leave ours around
  // for the next test to use.

  @Override
  public final void preSetUp() throws Exception {
    try {
      disconnectAllFromDS();
    } finally {
      File d = new File("DynamicRegionData" + OSProcess.getId());
      d.mkdirs();
      DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(d, null));
    }
  }

  /**
   * Tear down the test suite.
   * <p>
   * <H1>IMPORTANT NOTE:</H1> Never throw an exception from this method as it will mask any
   * exception thrown in a test.
   * </p>
   */
  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    LogWriterUtils.getLogWriter().info("Running tearDown in " + this);
    try {
      // Asif destroy dynamic regions at the end of the test
      CacheSerializableRunnable destroyDynRegn =
          new CacheSerializableRunnable("Destroy Dynamic regions") {
            public void run2() throws CacheException {
              Region dr = getCache().getRegion("__DynamicRegions");
              if (dr != null) {
                dr.localDestroyRegion();
              }
            }
          };
      getOtherVm().invoke(destroyDynRegn);
      Region dr = getCache().getRegion("__DynamicRegions");
      if (dr != null) {
        dr.localDestroyRegion();
      }
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      LogWriterUtils.getLogWriter().severe("tearDown in " + this + " failed due to " + t);
    } finally {
      try {
        disconnectAllFromDS();
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable t) {
        LogWriterUtils.getLogWriter()
            .severe("tearDown in " + this + " failed to disconnect all DS due to " + t);
      }
    }
    if (!DynamicRegionFactory.get().isClosed()) {
      LogWriterUtils.getLogWriter().severe("DynamicRegionFactory not closed!", new Exception());
    }
  }

  ////////////////////// Test Methods //////////////////////

  private VM getOtherVm() {
    Host host = Host.getHost(0);
    return host.getVM(0);
  }

  private void doParentCreateOtherVm(final Properties p, final boolean persist) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("create root") {
      public void run2() throws CacheException {
        File d = new File("DynamicRegionData" + OSProcess.getId());
        d.mkdirs();
        DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(d, null));
        getSystem(p);
        assertEquals(true, DynamicRegionFactory.get().isOpen());
        createParentRegion("parent", persist);
      }
    });
  }

  private void recreateOtherVm() {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("recreate") {
      public void run2() throws CacheException {
        beginCacheXml();
        {
          File d = new File("DynamicRegionData" + OSProcess.getId());
          d.mkdirs();
          CacheCreation cc = (CacheCreation) getCache();
          cc.setDynamicRegionFactoryConfig(new DynamicRegionFactory.Config(d, null));
        }
        createParentRegion("parent", true);
        finishCacheXml("dynamicRegionDUnitTest");
        // now make sure we recovered from disk ok
        assertEquals(true, DynamicRegionFactory.get().isOpen());
      }
    });
  }

  private void checkForRegionOtherVm(final String fullPath, final boolean shouldExist) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("checkForRegion") {
      public void run2() throws CacheException {
        Region r = getCache().getRegion(fullPath);
        if (shouldExist) {
          if (r == null) {
            fail("region " + fullPath + " does not exist");
          }
          // assertNotSame(r.getParentRegion().getAttributes().getCapacityController(),
          // r.getAttributes().getCapacityController());
          assertEquals(true, r.containsKey("key1"));
          assertEquals(true, r.containsValueForKey("key1"));
          assertEquals("value1", r.getEntry("key1").getValue());
        } else {
          assertEquals(null, r);
        }
      }
    });
  }

  private void checkForSubregionOtherVm(final String fullPath, final boolean shouldExist) {
    VM vm = getOtherVm();
    vm.invoke(new CacheSerializableRunnable("checkForRegion") {
      public void run2() throws CacheException {
        Region r = getCache().getRegion(fullPath);
        if (shouldExist) {
          if (r == null) {
            fail("region " + fullPath + " does not exist");
          }
        } else {
          assertEquals(null, r);
        }
      }
    });
  }

  /**
   * @param persist added this param to fix bug 37439
   */
  protected Region createParentRegion(String name, boolean persist) throws CacheException {
    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    File d = new File("DynamicRegionData" + OSProcess.getId());
    factory.setDiskStoreName(getCache().createDiskStoreFactory().setDiskDirs(new File[] {d})
        .setMaxOplogSize(OPLOG_SIZE).create("DynamicRegionDUnitTest").getName());
    if (persist) {
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    }
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    final Region r = createRootRegion(name, factory.create());
    return r;
  }

  /**
   * Make sure dynamic regions work on peers
   */
  @Test
  public void testPeerRegion() {
    assertEquals(true, DynamicRegionFactory.get().isOpen());
    createParentRegion("parent", true);

    Properties p = new Properties();
    doParentCreateOtherVm(p, false);
    Region dr = DynamicRegionFactory.get().createDynamicRegion("parent", "dynamicRegion1");
    String drFullPath = dr.getFullPath();
    dr.put("key1", "value1");
    // test for bug 35528 - support for dynamic subregions of dynamic regions
    for (int i = 0; i < 10; i++) {
      DynamicRegionFactory.get().createDynamicRegion(drFullPath, "subregion" + i);
    }

    LogWriterUtils.getLogWriter()
        .info("testPeerRegion - check #1 make sure other region has new dynamic subregion");
    checkForRegionOtherVm(drFullPath, true);

    // spot check the subregions
    checkForSubregionOtherVm(drFullPath + "/subregion7", true);

    // now see if OTHER can recreate which should fetch meta-info from controller
    recreateOtherVm();

    LogWriterUtils.getLogWriter().info(
        "testPeerRegion - check #2 make sure other region has dynamic region after restarting through getInitialImage");
    checkForRegionOtherVm(drFullPath, true);

    // now close the controller and see if OTHER can still fetch meta-info from disk
    closeCache();
    recreateOtherVm();
    LogWriterUtils.getLogWriter()
        .info("testPeerRegion - check #3 make sure dynamic region can be recovered from disk");
    checkForRegionOtherVm(drFullPath, true);
    for (int i = 0; i < 10; i++) {
      checkForSubregionOtherVm(drFullPath + "/subregion" + i, true);
    }

    // now start our cache back up and see if we still have the dynamic regions
    // even if we don't have disk on the controller to recover from.
    // This means we will get them from the peer
    {
      assertEquals(true, DynamicRegionFactory.get().isClosed());
      DynamicRegionFactory.get().open(new DynamicRegionFactory.Config());
      beginCacheXml();
      createParentRegion("parent", true);
      finishCacheXml("dynamicRegionCTRDUnitTest");
      assertEquals(true, DynamicRegionFactory.get().isOpen());
      assertEquals(true, DynamicRegionFactory.get().isActive());
      Cache c = getCache();

      // verify that controller has all dynamic regions
      assertEquals(true, c.getRegion(drFullPath) != null);

      // now make sure we can destroy dynamic regions
      for (int i = 0; i < 10; i++) {
        String regName = drFullPath + "/subregion" + i;
        assertEquals(true, c.getRegion(regName) != null);
        DynamicRegionFactory.get().destroyDynamicRegion(regName);
        assertEquals(null, c.getRegion(regName));
        checkForSubregionOtherVm(regName, false);
      }

      // make sure that we can explicitly destroy a region and then still
      // ask the factory to destroy it.
      c.getRegion(drFullPath).localDestroyRegion();
      checkForRegionOtherVm(drFullPath, true);
      DynamicRegionFactory.get().destroyDynamicRegion(drFullPath);
      assertEquals(null, c.getRegion(drFullPath));
      checkForRegionOtherVm(drFullPath, false);
    }
  }
}
