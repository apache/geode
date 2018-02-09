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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests the functionality of cache regions whose contents may be written to disk.
 *
 *
 * @since GemFire 3.2
 */
@Category(DistributedTest.class)
public class DiskRegionDistributedTest extends JUnit4CacheTestCase {

  /**
   * Returns the <code>EvictionStatistics</code> for the given region
   */
  private EvictionCounters getLRUStats(Region region) {
    final LocalRegion l = (LocalRegion) region;
    return l.getEvictionController().getCounters();
  }

  /**
   * Makes sure that updates from other VMs cause existing entries to be written to disk.
   */
  @Test
  public void testRemoteUpdates() {
    final String name = this.getUniqueName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_NO_ACK);
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUMemoryAttributes(2, null, EvictionAction.OVERFLOW_TO_DISK));
        File d = new File("DiskRegions" + OSProcess.getId());
        d.mkdirs();
        DiskStoreFactory dsf = getCache().createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {d});
        DiskStore ds = dsf.create(name);
        factory.setDiskStoreName(ds.getName());
        createRegion(name, factory.create());
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Fill Region") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        EvictionCounters lruStats = getLRUStats(region);
        int i;
        for (i = 0; lruStats.getEvictions() <= 0; i++) {
          region.put(new Integer(i), new short[250]);
        }
        assertTrue(i > 5);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Update Region") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        // EvictionStatistics lruStats = getLRUStats(region);
        for (int i = 0; i < 10; i++) {
          region.put(new Integer(i), new int[250]);
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Verify overflow") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        final EvictionCounters lruStats = getLRUStats(region);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return lruStats.getEvictions() > 6;
          }

          public String description() {
            return "waiting for evictions to exceed 6";
          }
        };
        Wait.waitForCriterion(ev, 5 * 1000, 200, true);
        // DiskRegionStats diskStats = dr.getStats();
        // assertTrue(diskStats.getWrites() > 6);
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Populate with byte[]") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        // EvictionStatistics lruStats = getLRUStats(region);
        for (int i = 0; i < 10000; i++) {
          region.put(String.valueOf(i), String.valueOf(i).getBytes());
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Get with byte[]") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        // EvictionStatistics lruStats = getLRUStats(region);
        for (int i = 0; i < 10000; i++) {
          byte[] bytes = (byte[]) region.get(String.valueOf(i));
          assertEquals(String.valueOf(i), new String(bytes));
        }
      }
    });
  }

  /**
   * Tests overflow with mirrored regions. Note that we have to use <code>byte</code> array values
   * in this test. Otherwise, the size of the data in the "puter" VM would be different from the
   * size of the data in the receiver VM, thus cause the two VMs to have different LRU eviction
   * behavior.
   */
  @Test
  public void testOverflowMirror() throws Exception {
    final String name = this.getUniqueName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setEarlyAck(false);
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
        factory.setDataPolicy(DataPolicy.REPLICATE);
        File d = new File("DiskRegions" + OSProcess.getId());
        d.mkdirs();
        DiskStoreFactory dsf = getCache().createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {d});
        factory.setDiskSynchronous(true);
        DiskStore ds = dsf.create(name);
        factory.setDiskStoreName(ds.getName());
        createRegion(name, factory.create());
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Fill Region") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        EvictionCounters lruStats = getLRUStats(region);
        for (int i = 0; lruStats.getEvictions() < 10; i++) {
          LogWriterUtils.getLogWriter().info("Put " + i);
          region.put(new Integer(i), new byte[1]);
        }

        assertEquals(10, lruStats.getEvictions());
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Verify overflow") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        EvictionCounters lruStats = getLRUStats(region);
        assertEquals(10, lruStats.getEvictions());

        // Because we are DISTRIBUTED_ACK, we can rely on the order
        // in which messages arrive and hence the order of the LRU
        // entries.
        for (int i = 0; i < 10; i++) {
          region.get(new Integer(i));
          assertEquals("No eviction for " + i, 10 + 1 + i, lruStats.getEvictions());
        }
      }
    });

  }

  /**
   * Tests that invalidates and updates received from different VMs are handled appropriately by
   * overflow regions.
   */
  @Test
  public void testDistributedInvalidate() throws Exception {
    final String name = this.getUniqueName();

    SerializableRunnable create = new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setEarlyAck(false);
        factory.setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
        File d = new File("DiskRegions" + OSProcess.getId());
        d.mkdirs();
        DiskStoreFactory dsf = getCache().createDiskStoreFactory();
        dsf.setDiskDirs(new File[] {d});
        DiskStore ds = dsf.create(name);
        factory.setDiskStoreName(ds.getName());
        createRegion(name, factory.create());
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    vm0.invoke(new CacheSerializableRunnable("Fill Region") {
      public void run2() throws CacheException {
        LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);
        // DiskRegion dr = region.getDiskRegion();
        EvictionCounters lruStats = getLRUStats(region);
        for (int i = 0; lruStats.getEvictions() < 10; i++) {
          LogWriterUtils.getLogWriter().info("Put " + i);
          region.put(new Integer(i), new byte[1]);
        }

        assertEquals(10, lruStats.getEvictions());
      }
    });

    final Object key = new Integer(20);

    vm1.invoke(new CacheSerializableRunnable("Invalidate entry") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        assertNotNull(region.get(key));
        region.invalidate(key);
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Verify invalidate") {
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(name);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return region.get(key) == null;
          }

          public String description() {
            return "value for key remains: " + key;
          }
        };
        Wait.waitForCriterion(ev, 500, 200, true);
      }
    });

    final String newValue = "NEW VALUE";

    vm1.invoke(new CacheSerializableRunnable("Update entry") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(name);
        region.put(key, newValue);
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Verify update") {
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(name);
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return newValue.equals(region.get(key));
          }

          public String description() {
            return "verify update";
          }
        };
        Wait.waitForCriterion(ev, 500, 200, true);
      }
    });
  }

  @Test
  public void testPersistentReplicateB4NonPersistent() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    final String regionName = getName();

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        RegionFactory rf = getCache().createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT);
        Region r = rf.create(regionName);
        assertTrue(r.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    });
    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        RegionFactory rf = getCache().createRegionFactory(RegionShortcut.REPLICATE);
        Region region = rf.create(regionName);
        assertTrue(region.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    });
    vm3.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        RegionFactory rf = cache.createRegionFactory(af.create());
        Region r = rf.create(regionName);
        assertNotNull(r);
        assertTrue(r.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    });
  }

  @Test
  public void testRRProxyWithPersistentReplicates() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    final String regionName = getName();
    SerializableCallable createRRProxy = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r =
            getCache().createRegionFactory(RegionShortcut.REPLICATE_PROXY).create(regionName);
        assertNotNull(r);
        return null;
      }
    };
    SerializableCallable createPersistentRR = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r =
            getCache().createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create(regionName);
        assertNotNull(r);
        return null;
      }
    };
    vm1.invoke(createRRProxy);
    vm2.invoke(createPersistentRR);
    vm3.invoke(createRRProxy);

    SerializableCallable assertConcurrency = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        assertTrue(r.getAttributes().getConcurrencyChecksEnabled());
        return null;
      }
    };

    vm1.invoke(assertConcurrency);
    vm3.invoke(assertConcurrency);
  }
}
