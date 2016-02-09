/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache30;

import java.io.File;
import java.lang.reflect.Array;
import java.util.BitSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.DiskRegion;
import com.gemstone.gemfire.internal.cache.DiskRegionStats;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.lru.LRUCapacityController;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * Tests the functionality of cache regions whose contents may be
 * written to disk.
 *
 * @author David Whitlock
 *
 * @since 3.2
 */
public class DiskRegionDUnitTest extends CacheTestCase {

  /**
   * Creates a new <code>DiskRegionDUnitTest</code>
   */
  public DiskRegionDUnitTest(String name) {
    super(name);
  }

//   public RegionAttributes getRegionAttributes() {
//     AttributesFactory factory = new AttributesFactory();
//     factory.setScope(Scope.DISTRIBUTED_ACK);
//     factory.setEarlyAck(false);
//     factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
//     DiskStoreFactory dsf = getCache().createDiskStoreFactory();
//     factory.setDiskSynchronous(true);    
//     factory.setDiskWriteAttributes(dwaf.create());
//     File d = new File("DiskRegions" + OSProcess.getId());
//     d.mkdirs();
//     factory.setDiskDirs(new File[]{d});
//     DiskStore ds = dsf.create(regionName);
//     factory.setDiskStoreName(ds.getName());
//     return factory.create();
//   }

  /**
   * Returns the <code>LRUStatistics</code> for the given region
   */
  protected LRUStatistics getLRUStats(Region region) {
    final LocalRegion l = (LocalRegion) region;
    return l.getEvictionController().getLRUHelper().getStats();
  }

  ////////  Test Methods

  /**
   * Tests that data overflows correctly to a disk region
   */
  public void testDiskRegionOverflow() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();

    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    factory.setDiskSynchronous(true);
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());

    Region region =
      createRegion(name, factory.create());
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    assertNotNull(dr);

    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);
    assertNotNull(diskStats);
    assertNotNull(lruStats);

    flush(region);
    
    assertEquals(0, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, lruStats.getEvictions());
    
    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 0; total++) {
      //getLogWriter().info("DEBUG: total " + total + ", evictions " + lruStats.getEvictions());
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

    flush(region);
    
    LogWriterUtils.getLogWriter().info("DEBUG: writes=" + diskStats.getWrites()
        + " reads=" + diskStats.getReads()
        + " evictions=" + lruStats.getEvictions()
        + " total=" + total
        + " numEntriesInVM=" + diskStats.getNumEntriesInVM()
        + " numOverflows=" + diskStats.getNumOverflowOnDisk());

    assertEquals(1, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(1, lruStats.getEvictions());
    assertEquals(1, diskStats.getNumOverflowOnDisk());
    assertEquals(total - 1, diskStats.getNumEntriesInVM());

    Object value = region.get(new Integer(0));
    flush(region);
    
    assertNotNull(value);
    assertEquals(0, ((int[]) value)[0]);

    LogWriterUtils.getLogWriter().info("DEBUG: writes=" + diskStats.getWrites()
        + " reads=" + diskStats.getReads()
        + " evictions=" + lruStats.getEvictions()
        + " total=" + total
        + " numEntriesInVM=" + diskStats.getNumEntriesInVM()
        + " numOverflows=" + diskStats.getNumOverflowOnDisk());

    assertEquals(2, diskStats.getWrites());
    assertEquals(1, diskStats.getReads());
    assertEquals(2, lruStats.getEvictions());

    for (int i = 0 ; i < total; i++) {
      int[] array = (int[]) region.get(new Integer(i));
      assertNotNull(array);
      assertEquals(i, array[0]);
    }
  }

  /**
   * Makes sure that updates from other VMs cause existing entries to
   * be written to disk.
   */
  public void testRemoteUpdates() throws Exception {
    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_NO_ACK);
            factory.setEvictionAttributes(EvictionAttributes
                .createLRUMemoryAttributes(2, null, EvictionAction.OVERFLOW_TO_DISK));
            File d = new File("DiskRegions" + OSProcess.getId());
            d.mkdirs();
            DiskStoreFactory dsf = getCache().createDiskStoreFactory();
            dsf.setDiskDirs(new File[]{d});
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
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
          LRUStatistics lruStats = getLRUStats(region);
          int i;
          for (i = 0; lruStats.getEvictions() <= 0; i++) {
            region.put(new Integer(i), new short[250]);
          }
          assertTrue(i > 5);
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Update Region") {
        public void run2() throws CacheException {
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
//          LRUStatistics lruStats = getLRUStats(region);
          for (int i = 0; i < 10; i++) {
            region.put(new Integer(i), new int[250]);
          }
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Verify overflow") {
        public void run2() throws CacheException {
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
          final LRUStatistics lruStats = getLRUStats(region);
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return lruStats.getEvictions() > 6;
            }
            public String description() {
              return "waiting for evictions to exceed 6";
            }
          };
          Wait.waitForCriterion(ev, 5 * 1000, 200, true);
          //DiskRegionStats diskStats = dr.getStats();
          //assertTrue(diskStats.getWrites() > 6);
        }
      });

    vm0.invoke(new CacheSerializableRunnable("Populate with byte[]") {
        public void run2() throws CacheException {
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
//          LRUStatistics lruStats = getLRUStats(region);
          for (int i = 0; i < 10000; i++) {
            region.put(String.valueOf(i),
                       String.valueOf(i).getBytes());
          }
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Get with byte[]") {
        public void run2() throws CacheException {
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
//          LRUStatistics lruStats = getLRUStats(region);
          for (int i = 0; i < 10000; i++) {
            byte[] bytes = (byte[]) region.get(String.valueOf(i));
            assertEquals(String.valueOf(i), new String(bytes));
          }
        }
      });
  }

  /**
   * Overflows a region and makes sure that gets of recently-used
   * objects do not cause faults.
   */
  public void testNoFaults() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region =
      createRegion(name, factory.create());
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);
    
    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 20; total++) {
//       System.out.println("total " + total + ", evictions " +
//                          lruStats.getEvictions());
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

    assertTrue(total > 40);
    long firstEvictions = lruStats.getEvictions();
    long firstReads = diskStats.getReads();
    
    for (int i = 1; i <= 40; i++) {
      int key = total - i;
      region.get(new Integer(key));
      assertEquals("Key " + key + " caused an eviction",
                   firstEvictions, lruStats.getEvictions());
      assertEquals("Key " + key + " caused an eviction",
                   firstReads, diskStats.getReads());
    }
  }

  /**
   * Tests overflow with mirrored regions.  Note that we have to use
   * <code>byte</code> array values in this test.  Otherwise, the size
   * of the data in the "puter" VM would be different from the size of
   * the data in the receiver VM, thus cause the two VMs to have
   * different LRU eviction behavior.
   */
  public void testOverflowMirror() throws Exception {
    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setEarlyAck(false);
            factory.setEvictionAttributes(EvictionAttributes
					  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
            factory.setDataPolicy(DataPolicy.REPLICATE);
            File d = new File("DiskRegions" + OSProcess.getId());
            d.mkdirs();
            DiskStoreFactory dsf = getCache().createDiskStoreFactory();
            dsf.setDiskDirs(new File[]{d});
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
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
          LRUStatistics lruStats = getLRUStats(region);
          for (int i = 0; lruStats.getEvictions() < 10; i++) {
            LogWriterUtils.getLogWriter().info("Put " + i);
            region.put(new Integer(i), new byte[1]);
          }

          assertEquals(10, lruStats.getEvictions());
        }
      });

    vm1.invoke(new CacheSerializableRunnable("Verify overflow") {
        public void run2() throws CacheException {
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
          LRUStatistics lruStats = getLRUStats(region);
          assertEquals(10, lruStats.getEvictions());

          // Because we are DISTRIBUTED_ACK, we can rely on the order
          // in which messages arrive and hence the order of the LRU
          // entries.
          for (int i = 0; i < 10; i++) {
            region.get(new Integer(i));
            assertEquals("No eviction for " + i,
                         10 + 1 + i, lruStats.getEvictions());
          }
        }
      });

  }

  /**
   * Tests destroying entries in an overflow region
   */
  public void testDestroy() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();

    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    factory.setDiskSynchronous(true);
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    
    Region region =
      createRegion(name, factory.create());
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);

    int total;
    for (total = 0; lruStats.getEvictions() < 40; total++) {
      region.put(new Integer(total), new byte[1000]);
    }
    assertEquals(0, diskStats.getRemoves());

    long evictions = lruStats.getEvictions();

    LogWriterUtils.getLogWriter().info("Destroying memory resident entries");
    // Destroying each of these guys should have no effect on the disk
    for (int i = total - 1; i >= evictions; i--) {
      region.destroy(new Integer(i));
      flush(region);
      assertEquals(0, diskStats.getRemoves());
      assertEquals(evictions, lruStats.getEvictions());
    }

//    long startRemoves = diskStats.getRemoves();

    LogWriterUtils.getLogWriter().info("Destroying disk-resident entries.  evictions=" + evictions);
    
    // Destroying each of these guys should cause a removal from disk
    for (int i = ((int) evictions) - 1; i >= 0; i--) {
      region.destroy(new Integer(i));
      flush(region);
      
      assertEquals((evictions - i), diskStats.getRemoves());
    }

    assertEquals(evictions, lruStats.getEvictions());
    
    LogWriterUtils.getLogWriter().info("keys remaining in region: " + region.keys().size());
    assertEquals(0, region.keys().size());
  }

  /**
   * Tests cache listeners in an overflow region are invoked and that
   * their events are reasonable.
   */
  public void testCacheEvents() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    TestCacheListener listener = new TestCacheListener() {
        public void afterCreate2(EntryEvent event) {

        }
      };
    factory.addCacheListener(listener);

    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    factory.setDiskSynchronous(true);
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());

    Region region = createRegion(name, factory.create());
//    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
//    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);

    int total;
    for (total = 0; lruStats.getEvictions() < 20; total++) {
      region.put(new Integer(total), String.valueOf(total));
      assertEquals(String.valueOf(total),
                   region.get(new Integer(total)));
    }

    assertTrue(listener.wasInvoked());

    listener = new TestCacheListener() {
        public void close2() { }
      };

    region.getAttributesMutator().setCacheListener(listener);

    for (int i = 0; i < total; i++) {
      String value = (String) region.get(new Integer(i));
      assertNotNull(value);
      assertEquals(String.valueOf(i), value);
    }

    assertFalse(listener.wasInvoked());

    listener = new TestCacheListener() {
        public void afterUpdate2(EntryEvent event) {
          Integer key = (Integer) event.getKey();
          assertEquals(null, event.getOldValue());
          assertEquals(false, event.isOldValueAvailable());
          byte[] value = (byte[]) event.getNewValue();
          assertEquals(key.intValue(), value.length);
        }
      };
    
    region.getAttributesMutator().setCacheListener(listener);

    for (int i = 0; i < 20; i++) {
      region.put(new Integer(i), new byte[i]);
    }

    assertTrue(listener.wasInvoked());
  }
  

  /**
   * Tests that an {@link IllegalStateException} is thrown when the
   * region is full of keys and entries.
   */
  public void fillUpOverflowRegion() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region =
      createRegion(name, factory.create());
    
    for (int i = 0; i < 10000; i++) {
      int[] array = new int[1000];
      array[0] = i;
      try {
        region.put(array, new Integer(i));

      } catch (IllegalStateException ex) {
        String message = ex.getMessage();
        assertTrue(message.indexOf("is full with") != -1);
        return;
      }
    }

    fail("Should have thrown an IllegalStateException");
  }

  /**
   * Tests iterating over all of the values when some have been
   * overflowed.
   */
  public void testValues() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region =
      createRegion(name, factory.create());
//    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
//    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);
    
    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 0; total++) {
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

    BitSet bits = new BitSet();
    Collection values = region.values();
    assertEquals(total, values.size());

    for(Iterator iter = values.iterator(); iter.hasNext(); ) {
      Object value = iter.next();
      assertNotNull(value);
      int[] array = (int[]) value;
      int i = array[0];
      assertFalse("Bit " + i + " is already set", bits.get(i));
      bits.set(i);
    }
  }

  /**
   * Tests for region.evictValue().
   */
  public void testRegionEvictValue() throws Exception {
    final String name = this.getUniqueName()+ "testRegionEvictValue";
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    //factory.setEvictionAttributes(EvictionAttributes.createLIFOEntryAttributes(capacity, EvictionAction.OVERFLOW_TO_DISK));
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region = createRegion(name, factory.create());

    int size = 200;
    for (int i=0; i < size; i++){
      region.put("Key-" + i, new Integer(i));
    }

    // Evict alternate values.
    for (int i = 0; i < size/2; i++) {
      if (i%2 == 0){
        ((LocalRegion)region).evictValue("Key-" + i);
      }
    }    

    // Check if its moved to disk.
    for (int i = 0; i < size/2; i++) {
      if (i%2 == 0){
        try {
          Object value = ((LocalRegion)region).getValueInVM("Key-" + i);
          if (value != null){
            fail("The values should have been evicted to disk, for key: " + "Key-" + i);
          }
        }
        catch (EntryNotFoundException e) {
          fail("Entry not found not expected but occured ");
        }
      }
    }
   }

  /**
   * Tests calling region.evictValue() on region with eviction-attribute set.
   */
  public void testEvictValueOnRegionWithEvictionAttributes() throws Exception {
    final String name = this.getUniqueName()+ "testRegionEvictValue";
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region = createRegion(name, factory.create());

    int size = 200;
    for (int i=0; i < size; i++){
      region.put("Key-" + i, new Integer(i));
    }
    
    // Evict alternate values.
    for (int i = 0; i < size/4; i++) {
      try {
        ((LocalRegion)region).evictValue("Key-" + i);
        fail("Should have thrown exception with, evictValue not supported on region with eviction attributes.");
      } catch (Exception ex){
        // Expected exception.
        // continue.
      }
    }    

  }

  /**
   * Tests that the disk region statistics are updated correctly for
   * persist backup regions.
   */
  public void testBackupStatistics() throws CacheException {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    final int total = 10;

    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    factory.setDiskSynchronous(true);
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());

    Region region =
      createRegion(name, factory.create());
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    assertNotNull(dr);

    DiskRegionStats diskStats = dr.getStats();

    assertEquals(0, diskStats.getWrites());
    assertEquals(0, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, diskStats.getNumOverflowOnDisk());

    for (int i=0; i < total; i++) {
      String s = String.valueOf(i);
      region.put(s, s);

      assertEquals(i+1, diskStats.getWrites());
      assertEquals(i+1, diskStats.getNumEntriesInVM());
      assertEquals(0, diskStats.getReads());
      assertEquals(0, diskStats.getNumOverflowOnDisk());
    }

    region.put("foobar", "junk");

    assertEquals(total+1, diskStats.getWrites());
    assertEquals(total+1, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, diskStats.getNumOverflowOnDisk());

    region.localDestroy("foobar");

    // destroy becomes a tombstone
    assertEquals(total+2, diskStats.getWrites());
    assertEquals(total+0, diskStats.getNumEntriesInVM());
    assertEquals(0, diskStats.getReads());

    region.put("foobar2", "junk");
    flush(region);
    region.localDestroy("foobar2");
    assertEquals(total, region.keys().size());
  }

  public void assertArrayEquals(Object expected, Object v) {
    assertEquals(expected.getClass(), v.getClass());
    int vLength = Array.getLength(v);
    assertEquals(Array.getLength(expected), vLength);
    for (int i=0; i < vLength; i++) {
      assertEquals(Array.get(expected, i), Array.get(v, i));
    }
  }
  
  public void testBackup() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    factory.setDiskSynchronous(true);
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    int total = 10;
    {
      Region region =
        createRegion(name, factory.create());

      for (int i=0; i < total; i++) {
        String s = String.valueOf(i);
        region.put(s, s);
      }
      region.put("foobar", "junk");
      region.localDestroy("foobar");
      region.put("foobar2", "junk");
      flush(region);
      region.localDestroy("foobar2");
      // test invalidate
      region.put("invalid", "invalid");
      flush(region);
      region.invalidate("invalid");
      flush(region);
      assertTrue(region.containsKey("invalid") && !region.containsValueForKey("invalid"));
      total++;
      // test local-invalidate
      region.put("localinvalid", "localinvalid");
      flush(region);
      region.localInvalidate("localinvalid");
      flush(region);
      assertTrue(region.containsKey("localinvalid") && !region.containsValueForKey("localinvalid"));
      total++;
      // test byte[] values
      region.put("byteArray", new byte[0]);
      flush(region);
      assertArrayEquals(new byte[0], region.get("byteArray"));
      total++;
      // test modification
      region.put("modified", "originalValue");
      flush(region);
      region.put("modified", "modified");
      flush(region);
      assertEquals("modified", region.get("modified"));
      total++;
      assertEquals(total, region.keys().size());
    }
    closeCache();  // @todo need to do a close that does not remove disk files
    getCache();
    {
      dsf = getCache().createDiskStoreFactory();
      dsf.setDiskDirs(new File[]{d});
      dsf.create(name);
      Region region =
        createRegion(name, factory.create());
      assertEquals(total, region.keys().size());
      assertTrue(region.containsKey("invalid") && !region.containsValueForKey("invalid"));
      region.localDestroy("invalid");
      total--;
      assertTrue(region.containsKey("localinvalid") && !region.containsValueForKey("localinvalid"));
      region.localDestroy("localinvalid");
      total--;
      assertArrayEquals(new byte[0], region.get("byteArray"));
      region.localDestroy("byteArray");
      total--;
      assertEquals("modified", region.get("modified"));
      region.localDestroy("modified");
      total--;
    }
  }

  // testSwitchOut is no longer valid
  // the test was not written correctly to recover
  // and if it was it would now fail with a split brain

  // testSwitchIn is no longer valid
  // we no longer switchIn files if GII aborts.
  
  /**
   * Tests getting the {@linkplain
   * com.gemstone.gemfire.cache.Region.Entry#getValue values} of
   * region entries that have been overflowed.
   */
  public void testRegionEntryValues() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region =
      createRegion(name, factory.create());
//    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
//    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);
    
    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 0; total++) {
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

//    BitSet bits = new BitSet();
    Set values = region.entries(false);
    assertEquals(total, values.size());

    for(Iterator iter = values.iterator(); iter.hasNext(); ) {
      Region.Entry entry = (Region.Entry) iter.next();
      Integer key = (Integer) entry.getKey();
      int[] value = (int[]) entry.getValue();
      assertNotNull(value);
      assertEquals("Key/value" + key, key.intValue(), value[0]);
    }
  }

  /**
   * Tests that once an overflowed entry is {@linkplain
   * Region#invalidate invalidated} its value is gone.
   */
  public void testInvalidate() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region =
      createRegion(name, factory.create());
//    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
//    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);
    
    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 10; total++) {
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

    region.invalidate(new Integer(0));
    assertNull(region.get(new Integer(0)));
  }

  /**
   * Tests that invalidates and updates received from different VMs
   * are handled appropriately by overflow regions.
   */
  public void testDistributedInvalidate() throws Exception {
    final String name = this.getUniqueName();

    SerializableRunnable create =
      new CacheSerializableRunnable("Create region") {
          public void run2() throws CacheException {
            AttributesFactory factory = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setEarlyAck(false);
            factory.setEvictionAttributes(EvictionAttributes
					  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
            File d = new File("DiskRegions" + OSProcess.getId());
            d.mkdirs();
            DiskStoreFactory dsf = getCache().createDiskStoreFactory();
            dsf.setDiskDirs(new File[]{d});
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
          LocalRegion region =
            (LocalRegion) getRootRegion().getSubregion(name);
//          DiskRegion dr = region.getDiskRegion();
          LRUStatistics lruStats = getLRUStats(region);
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

  /**
   * Tests that the updated value gets overflowed
   */
  public void testOverflowUpdatedValue() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region =
      createRegion(name, factory.create());
//    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
//    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);

    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 10; total++) {
      int[] array = new int[250];
      array[0] = total;
      region.put(new Integer(total), array);
    }

    // Update a value
    final Object newValue = "NEW VALUE";
    final Object key = new Integer(0);
    region.put(key, newValue);
    assertEquals(newValue, region.get(key));

    // Iterate over a bunch of stuff to cause the updated entry to be
    // overflowed 
    for (int i = 1; i < total; i++) {
      region.get(new Integer(i));
    }

    // Make sure that the updated value got written to disk
    assertEquals(newValue, region.get(key));
  }

  /**
   * Flushing all pending writes to disk.
   */
  private static void flush(Region region) {
	 ((LocalRegion)region).getDiskRegion().flushForTesting();
  }

  /**
   * Tests that the "test hook" {@link DiskRegionStats} work as
   * advertised. 
   */
  public void testTestHookStatistics() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
//    factory.setConcurrencyChecksEnabled(false);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK));
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    factory.setDiskSynchronous(true);
    
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    LocalRegion region = (LocalRegion)
      createRegion(name, factory.create());
    DiskRegion dr = region.getDiskRegion();
    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);
    
    // Put in stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 0; total++) {
      int[] array = new int[1];
      array[0] = total;
      region.put(new Integer(total), array);
      if (lruStats.getEvictions() <= 0) {
        assertEquals(total + 1, diskStats.getNumEntriesInVM());
      }
    }

    assertEquals(1, diskStats.getNumOverflowOnDisk());
    
    // Net change of zero
    region.get(new Integer(0));
    assertEquals(region.entryCount(), diskStats.getNumEntriesInVM() +
                 diskStats.getNumOverflowOnDisk());
    assertEquals(total - 1, diskStats.getNumEntriesInVM());
    assertEquals(1, diskStats.getNumOverflowOnDisk());

    // Kick out 4 entries
    region.put(new Integer(total + 10), new int[1]);
    region.put(new Integer(total + 11), new int[1]);
    region.put(new Integer(total + 12), new int[1]);
    region.put(new Integer(total + 13), new int[1]);
    assertEquals(region.entryCount(), diskStats.getNumEntriesInVM() +
                 diskStats.getNumOverflowOnDisk());
    assertEquals(total - 1, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Make sure invalidate doesn't change anything
    region.invalidate(new Integer(total+10));
    assertEquals(region.entryCount(), diskStats.getNumEntriesInVM() +
                 diskStats.getNumOverflowOnDisk());
    assertEquals(total - 1, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Make sure local-invalidate doesn't change anything
    region.localInvalidate(new Integer(total+11));
    assertEquals(region.entryCount(), diskStats.getNumEntriesInVM() +
                 diskStats.getNumOverflowOnDisk());
    assertEquals(total - 1, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Make sure destroy does
    region.destroy(new Integer(total+10));
    ((LocalRegion)region).dumpBackingMap();
    assertEquals(region.entryCount(), diskStats.getNumEntriesInVM() +
                 diskStats.getNumOverflowOnDisk());
    assertEquals(total - 2, diskStats.getNumEntriesInVM());
    assertEquals(5, diskStats.getNumOverflowOnDisk());

    // Destroy an entry that has been overflowed
    region.destroy(new Integer(3));
    assertEquals(region.entryCount(), diskStats.getNumEntriesInVM() +
                 diskStats.getNumOverflowOnDisk());
    assertEquals(total - 2, diskStats.getNumEntriesInVM());
    assertEquals(4, diskStats.getNumOverflowOnDisk());
  }

  /**
   * Tests the {@link LocalRegion#getValueInVM getValueInVM} and
   * {@link LocalRegion#getValueOnDisk getValueOnDisk} methods that
   * were added for testing.
   */
  public void testLowLevelGetMethods() throws Exception {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(100,EvictionAction.OVERFLOW_TO_DISK));
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    factory.setDiskSynchronous(true);
    
    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    LocalRegion region = (LocalRegion)
      createRegion(name, factory.create());
//    DiskRegion dr = region.getDiskRegion();
//    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);
    
    // Put in larger stuff until we start evicting
    int total;
    for (total = 0; lruStats.getEvictions() <= 2; total++) {
      int[] array = new int[250];
      array[0] = total;
      Integer key = new Integer(total);
      region.put(key, array);
      array = (int[]) region.getValueInVM(key);
      assertNotNull(array);
      assertEquals(total, array[0]);
      assertNull(region.getValueOnDisk(key));
    }

    Integer key = new Integer(1);
    assertNull(region.getValueInVM(key));
    int[] array = (int[]) region.getValueOnDisk(key);
    assertNotNull(array);
    assertEquals(1, array[0]);

    region.get(key);

    CachedDeserializable cd = (CachedDeserializable)region.getValueInVM(key);
    array = (int[])cd.getValue();
    assertNotNull(array);
    assertEquals(1, array[0]);

    array = (int[]) region.getValueOnDisk(key);
    assertNotNull(array);
    assertEquals(1, array[0]);
  }

  /**
   * Tests disk overflow with an entry-based {@link
   * LRUCapacityController}. 
   */
  public void testLRUCapacityController() throws CacheException {
    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
				  .createLRUEntryAttributes(1000, EvictionAction.OVERFLOW_TO_DISK));
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    factory.setDiskSynchronous(true);    

    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());
    Region region =
      createRegion(name, factory.create());
    DiskRegion dr = ((LocalRegion) region).getDiskRegion();
    DiskRegionStats diskStats = dr.getStats();
    LRUStatistics lruStats = getLRUStats(region);

    flush(region);
    
    assertEquals(0, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, lruStats.getEvictions());
    
    // Put in larger stuff until we start evicting
    for (int i = 1; i <= 1000; i++) {
//       System.out.println("total " + i + ", evictions " +
//                          lruStats.getEvictions());
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(i, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
      assertEquals("On iteration " + i,
                   0, diskStats.getWrites());
      assertEquals(0, diskStats.getReads());
      assertEquals(0, diskStats.getNumOverflowOnDisk());
    }

    assertEquals(0, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(0, diskStats.getNumOverflowOnDisk());

    // Add a new value
    region.put(new Integer(1000 + 1), String.valueOf(1000 + 1));
    assertEquals(1000, lruStats.getCounter());
    assertEquals(1, lruStats.getEvictions());
    assertEquals(1, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(1, diskStats.getNumOverflowOnDisk());
    assertEquals(1000, diskStats.getNumEntriesInVM());

    // Add another new value
    region.put(new Integer(1000 + 2), String.valueOf(1000 + 2));
    assertEquals(1000, lruStats.getCounter());
    assertEquals(2, lruStats.getEvictions());
    assertEquals(2, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(2, diskStats.getNumOverflowOnDisk());
    assertEquals(1000, diskStats.getNumEntriesInVM());

    // Replace a value
    region.put(new Integer(1000), String.valueOf(1000));
    assertEquals(1000, lruStats.getCounter());
    assertEquals(2, lruStats.getEvictions());
    assertEquals(2, diskStats.getWrites());
    assertEquals(0, diskStats.getReads());
    assertEquals(2, diskStats.getNumOverflowOnDisk());
    assertEquals(1000, diskStats.getNumEntriesInVM());
  }

  /**
   * Tests a disk-based region with an {@link LRUCapacityController}
   * with size 1 and an eviction action of "overflow".
   */
  public void testLRUCCSizeOne() throws CacheException {
    int threshold = 1;

    final String name = this.getUniqueName();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setEvictionAttributes(EvictionAttributes
        .createLRUEntryAttributes(threshold, EvictionAction.OVERFLOW_TO_DISK));
    factory.setCacheLoader(new CacheLoader() {
        public Object load(LoaderHelper helper)
          throws CacheLoaderException {
          return "LOADED VALUE";
        }

        public void close() { }

      });
    DiskStoreFactory dsf = getCache().createDiskStoreFactory();
    factory.setDiskSynchronous(true);    

    File d = new File("DiskRegions" + OSProcess.getId());
    d.mkdirs();
    dsf.setDiskDirs(new File[]{d});
    DiskStore ds = dsf.create(name);
    factory.setDiskStoreName(ds.getName());

    Region region =
      createRegion(name, factory.create());
    LRUStatistics lruStats = getLRUStats(region);
    assertNotNull(lruStats);

    for (int i = 1; i <= 1; i++) {
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(1, lruStats.getCounter());
      assertEquals(0, lruStats.getEvictions());
    }

    for (int i = 2; i <= 10; i++) {
      Object key = new Integer(i);
      Object value = String.valueOf(i);
      region.put(key, value);
      assertEquals(1, lruStats.getCounter());
      assertEquals(i - 1, lruStats.getEvictions());
    }

    for (int i = 11; i <= 20; i++) {
      Object key = new Integer(i);
//      Object value = String.valueOf(i);
      // Invoke loader
      region.get(key);
      assertEquals(1, lruStats.getCounter());
      assertEquals(i - 1, lruStats.getEvictions());
    }
  }


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

  public void testRRProxyWithPersistentReplicates() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    final String regionName = getName();
    SerializableCallable createRRProxy = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE_PROXY).create(regionName);
        assertNotNull(r);
        return null;
      }
    };
    SerializableCallable createPersistentRR = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().createRegionFactory(RegionShortcut.REPLICATE_PERSISTENT).create(regionName);
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
  
  // I think this test is no longer valid; at least the part
  // about doing concurrent ops while disk recovery is in progress.
  // @todo rewrite the test but only expect nb ops during the gii part.
//   private static final int NB1_CHUNK_SIZE = 500 * 1024; // == InitialImageOperation.CHUNK_SIZE_IN_BYTES
//   private static final int NB1_NUM_ENTRIES = 50000;
//   private static final int NB1_VALUE_SIZE = NB1_CHUNK_SIZE * 10 / NB1_NUM_ENTRIES;
//   private static final int MIN_NB_PUTS = 300;
    
//   protected static volatile int numPutsDuringRecovery = 0;
//   protected static volatile boolean stopPutting = false;
//   protected static volatile boolean stoppedPutting = false;
  
//   /**
//    * Tests that distributed ack operations do not block while
//    * another cache is doing a getInitialImage with disk recovery.
//    */
//   public void testNbPutsDuringRecovery() throws Throwable {
    
//     final String name = this.getUniqueName();
//     final byte[][] values = new byte[NB1_NUM_ENTRIES][];
    
//     for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
//       values[i] = new byte[NB1_VALUE_SIZE];
//       Arrays.fill(values[i], (byte)0x42);
//     }
    
//     Host host = Host.getHost(0);
//     VM vm0 = host.getVM(0);
//     VM vm2 = host.getVM(2);
        
//     // Set up recovery scenario in vm2
//     vm2.invoke(new CacheSerializableRunnable("Create Disk Region and close cache") {
//       public void run2() throws CacheException {        
//         getLogWriter().info("DEBUG nbput: start phase one");
//         AttributesFactory factory =
//           new AttributesFactory(getRegionAttributes());
//         DiskStoreFactory dsf = getCache().createDiskStoreFactory();
//         factory.setDiskSynchronous(true);
//         factory.setDiskWriteAttributes(dwaf.create());
//         Region rgn = createRegion(name, factory.create());
//         for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
//           rgn.put(new Integer(i), values[i]);
//         }
//         assertEquals(NB1_NUM_ENTRIES, rgn.keys().size());
//         //close and create to ensure that all data will go to htree 
//         //TODO: Mitul : remove this later to fine tune test to also take oplogs recovery into account
//         rgn.close();
//         rgn = createRegion(name, factory.createRegionAttributes());
//         closeCache();
//         getCache();
//         getLogWriter().info("DEBUG nbput: finished phase one");
//       }
//     });

//     // start asynchronous process that does updates to the data
//     AsyncInvocation async = vm0.invokeAsync(new CacheSerializableRunnable("Do Nonblocking Operations") {
//       public void run2() throws CacheException {
//         boolean sawRecoveringVM = false;
//         // non-mirrored region to force recovery
//         AttributesFactory factory = new AttributesFactory();
//         factory.setScope(Scope.DISTRIBUTED_ACK);
//         factory.setEarlyAck(false);
//         factory.setPersistBackup(false);
//         Region region = createRegion(name, factory.create());
//         // keep putting until told to stop
//         getLogWriter().info("DEBUG nbput: started async putter");
//         int putCount = 0;
//         for (int i = 0; ; i++) {
//           if (stopPutting) break;
//           Object key = new Integer(i);
//           Object value = new Long(System.currentTimeMillis());
//           getLogWriter().info("DEBUG nbput(" + key + ", " + value + ")");
//           region.put(key, value);
//           putCount++;

//           {
//             boolean allDone = false;
//             boolean stillRecovering = false;
//             DistributedRegion drgn = (DistributedRegion)region;
//             CacheDistributionAdvisor advisor = drgn.getCacheDistributionAdvisor();
//             Set idSet = advisor.adviseGeneric();
//             for (Iterator itr = idSet.iterator(); itr.hasNext();) {
//               CacheDistributionAdvisor.CacheProfile profile =
//                 (CacheDistributionAdvisor.CacheProfile)advisor.getProfile((InternalDistributedMember)itr.next());
//               if (profile.inRecovery) {
//                 sawRecoveringVM = true;
//                 stillRecovering = true;
//                 synchronized (DiskRegionDUnitTest.class) {
//                   numPutsDuringRecovery++;
//                   if (numPutsDuringRecovery >= MIN_NB_PUTS) {
//                     allDone = true;
//                   }
//                 }
//                 break;
//               }
//             }
//             if (allDone) {
//               // get out of the for loop since we have done enough puts
//               getLogWriter().info("DEBUG nbput: allDone");
//               break;
//             }
//             if (sawRecoveringVM && !stillRecovering) {
//               // get out of the for loop further puts will not
//               // happen during recovery
//               getLogWriter().info("DEBUG nbput: sawRecoveringVM and not stillRecovering");
//               break;
//             }
//           }
//         }
//         stoppedPutting = true;
//         getLogWriter().info("DEBUG nbput: stopped async putter who did " + putCount + " puts");
//       }
//     });

//     // in the meantime, do recovery in vm2 
//     AsyncInvocation async2 = vm2.invokeAsync(new CacheSerializableRunnable("Do Recovery") {
//       public void run2() throws CacheException {
//         AttributesFactory factory =
//           new AttributesFactory(getRegionAttributes());
//         DiskRegion.recoverDelay = 10; // artificially slow down recovery
//         getLogWriter().info("DEBUG nbput: started recovery");
//         try {
//           createRegion(name, factory.create());
//         }
//         finally {
//           DiskRegion.recoverDelay = 0;
//           getLogWriter().info("DEBUG nbput: finished recovery");
//         }
//       }
//     });

//     boolean spedUpRecovery = false;
//     while (async2.isAlive()) {
//       // still doing recovery
//       if (!spedUpRecovery && !async.isAlive()) {
//         // done doing puts so speed up recovery
//         spedUpRecovery = true;
//         getLogWriter().info("DEBUG nbput: telling recovery to speed up");
//         vm2.invoke(new CacheSerializableRunnable("Speed up recovery") {
//             public void run2() throws CacheException {
//               DiskRegion.recoverDelay = 0;
//             }
//           });
//       }
//       async2.join(5 * 1000); // yes, call join here.
//     }
//     if (async.isAlive()) {
//       // tell putter to stop putting
//       vm0.invoke(new SerializableRunnable() {
//           public void run() {
//             getLogWriter().info("DEBUG nbput: telling putter to stop");
//             stopPutting = true;
//             int reps = 0;
//             while (!stoppedPutting && reps < 20) {
//               reps++;
//               try {
//                 Thread.sleep(1000);
//               }
//               catch (InterruptedException ie) {
//                 getLogWriter().warning("Someone interrupted this thread while it" +
//                                 "was trying to stop the nb putter");
//                 fail("interrupted");
//                 return;
//               }
//             }
//           }
//         });
    
//       // wait for nonblocking operations to complete
//       getLogWriter().info("DEBUG nbput: waiting for putter thread to finish");
//       DistributedTestCase.join(async, 30 * 1000, getLogWriter());
//       getLogWriter().info("DEBUG nbput: done waiting for putter thread to finish");
//     }
    
//     if (async2.exceptionOccurred()) {
//       fail("async2 failed", async2.getException());
//     }
    
//     if (async.exceptionOccurred()) {
//       fail("async failed", async.getException());
//     }
        
//     vm0.invoke(new CacheSerializableRunnable("Verify number of puts during recovery") {
//       public void run2() throws CacheException {
//         getLogWriter().info(name + ": " + numPutsDuringRecovery + " entries out of " + NB1_NUM_ENTRIES +
//         " were updated concurrently with recovery");
//         // make sure at least some of them were concurrent
//         assertTrue("Not enough updates concurrent with getInitialImage occurred to my liking. "
//           + numPutsDuringRecovery + " entries out of " + NB1_NUM_ENTRIES +
//           " were updated concurrently with getInitialImage, and I'd expect at least " +
//           MIN_NB_PUTS + " or so", numPutsDuringRecovery >= MIN_NB_PUTS);
//       }
//     });    
//   }  
}
