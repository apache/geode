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

import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.PartitionedRegionException;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.PureLogWriter;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * This class tests the functionality of a cache {@link Region region} that has a scope of
 * {@link Scope#DISTRIBUTED_ACK distributed ACK} and {@link PartitionAttributes
 * partition-attributes}.
 *
 * @since GemFire 5.1
 */

public class PartitionedRegionDUnitTest extends MultiVMRegionTestCase {

  public static boolean InvalidateInvoked = false;

  static int oldLogLevel;

  @Override
  protected boolean supportsSubregions() {
    return false;
  }

  @Override
  protected boolean supportsNetLoad() {
    return false;
  }

  @Override
  protected boolean supportsReplication() {
    return false;
  }

  @Override
  protected boolean supportsTransactions() {
    return false;
  }

  protected boolean supportsLocalDestroyAndLocalInvalidate() {
    return false;
  }

  @Ignore("TODO: test is not implemented for partioned regions")
  @Override
  @Test
  public void testCacheLoaderModifyingArgument() {
    // TODO, implement a specific PR related test that properly reflects primary allocation
    // and event deliver based on that allocation
  }

  @Ignore("TODO: test is not implemented for partioned regions")
  @Override
  @Test
  public void testLocalAndRemoteCacheWriters() {
    // TODO, implement a specific PR related test that properly reflects primary allocation
    // and event deliver based on that allocation
  }

  @Ignore("TODO: test is not implemented for partioned regions")
  @Override
  @Test
  public void testLocalCacheLoader() {
    // TODO, implement a specific PR related test that properly reflects primary allocation
    // and event deliver based on that allocation
  }

  /**
   * Returns region attributes for a partitioned region with distributed-ack scope
   */
  @Override
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setEarlyAck(false);
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
    return factory.create();
  }

  /**
   * Returns region attributes with a distributed-ack scope
   */
  protected RegionAttributes getNonPRRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEarlyAck(false);
    return factory.create();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache30.PartitionedRegionDUnitTest$PoisonedKey");
    return properties;
  }

  public static int setLogLevel(LogWriter l, int logLevl) {
    int ret = -1;
    if (l instanceof PureLogWriter) {
      PureLogWriter pl = (PureLogWriter) l;
      ret = pl.getLogWriterLevel();
      pl.setLevel(logLevl);
    }
    return ret;
  }

  void setVMInfoLogLevel() {
    SerializableRunnable runnable = new SerializableRunnable() {
      public void run() {
        oldLogLevel = setLogLevel(getCache().getLogger(), InternalLogWriter.INFO_LEVEL);
      }
    };
    for (int i = 0; i < 4; i++) {
      Host.getHost(0).getVM(i).invoke(runnable);
    }
  }

  void resetVMLogLevel() {
    SerializableRunnable runnable = new SerializableRunnable() {
      public void run() {
        setLogLevel(getCache().getLogger(), oldLogLevel);
      }
    };
    for (int i = 0; i < 4; i++) {
      Host.getHost(0).getVM(i).invoke(runnable);
    }
  }

  /**
   * Bug #47235 concerns assertion failures being thrown when there is a member that receives
   * adjunct messages (as in a WAN gateway, a peer with clients, etc).
   */
  @Test
  public void testRegionInvalidationWithAdjunctMessages() throws Exception {
    final String name = getUniqueName();
    VM vm1 = Host.getHost(0).getVM(1);
    Cache cache = getCache();
    RegionFactory fact = getCache().createRegionFactory(RegionShortcut.PARTITION);
    Region pr = fact.create(name + "Region");
    pr.put("Object1", "Value1");

    vm1.invoke(new SerializableRunnable("create PR") {
      @Override
      public void run() {
        RegionFactory fact = getCache().createRegionFactory(RegionShortcut.PARTITION);
        fact.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        fact.addCacheListener(new CacheListenerAdapter() {
          @Override
          public void afterInvalidate(EntryEvent event) {
            org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
                .info("afterInvalidate invoked with " + event);
            InvalidateInvoked = true;
          }
        });
        fact.create(name + "Region");
      }
    });
    try {
      pr.invalidateRegion();
      assertTrue("vm1 should have invoked the listener for an invalidateRegion operation",
          (Boolean) vm1.invoke(new SerializableCallable("getStatus") {
            public Object call() {
              return InvalidateInvoked;
            }
          }));
    } finally {
      disconnectAllFromDS();
    }
  }

  /**
   * Tests the compatibility of creating certain kinds of subregions of a local region.
   */
  @Test
  public void testIncompatibleSubregions() throws CacheException, InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final String name = this.getUniqueName() + "-PR";
    vm0.invoke(new SerializableRunnable("Create partitioned Region") {
      public void run() {
        try {

          createRegion(name, "INCOMPATIBLE_ROOT", getRegionAttributes());
        } catch (CacheException ex) {
          Assert.fail("While creating Partitioned region", ex);
        }
      }
    });

    vm1.invoke(new SerializableRunnable("Create non-partitioned Region") {
      public void run() {
        try {
          AttributesFactory factory = new AttributesFactory(getNonPRRegionAttributes());
          try {
            createRegion(name, "INCOMPATIBLE_ROOT", factory.create());
            fail("Should have thrown an IllegalStateException");
          } catch (IllegalStateException ex) {
            // pass...
          }

        } catch (CacheException ex) {
          Assert.fail("While creating Partitioned Region", ex);
        }
      }
    });
  }

  private void setupExtendedTest(final String regionName, final int numVals) {
    Host host = Host.getHost(0);
    SerializableRunnable createPR = new SerializableRunnable("createPartitionedRegion") {
      public void run() {
        try {
          createRegion(regionName, "root", getRegionAttributes());
        } catch (CacheException ex) {
          Assert.fail("While creating Partitioned region", ex);
        }
      }
    };
    for (int i = 1; i < 4; i++) {
      host.getVM(i).invoke(createPR);
    }
    VM vm0 = host.getVM(0);
    vm0.invoke(new SerializableRunnable("Populate Partitioned Region") {
      public void run() {
        Region region = null;
        try {
          region = createRegion(regionName, "root", getRegionAttributes());
          // since random keys are being used, we might hit duplicates
          region.getCache().getLogger().info("<ExpectedException action=add>"
              + "org.apache.geode.cache.EntryExistsException" + "</ExpectedException>");
          java.util.Random rand = new java.util.Random(System.currentTimeMillis());
          for (int i = 0; i < numVals; i++) {
            boolean created = false;
            while (!created) {
              try {
                int val = rand.nextInt(100000000);
                String key = String.valueOf(val);
                region.create(key, new Integer(val));
                created = true;
              } catch (EntryExistsException eee) {
                // loop to try again
              }
            }
          }
        } catch (Exception ex) {
          Assert.fail("while creating or populating partitioned region", ex);
        } finally {
          if (region != null) {
            region.getCache().getLogger().info("<ExpectedException action=remove>"
                + "org.apache.geode.cache.EntryExistsException" + "</ExpectedException>");
          }
        }
      }
    });
  }

  /**
   * test with multiple vms and a decent spread of keys
   */
  @Test
  public void testExtendedKeysValues() {
    final String regionName = getUniqueName();
    final int numEntries = 20000;

    // since this test has to create a lot of entries, info log level is used.
    // comment out the setting of this and rerun if there are problems
    setVMInfoLogLevel();
    try {
      setupExtendedTest(regionName, numEntries);

      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      vm0.invoke(new SerializableRunnable("exercise Region.values") {
        public void run() {
          try {
            Region region = getRootRegion().getSubregion(regionName);
            Collection values = region.values();
            Set keys = region.keySet();
            Set entries = region.entrySet();
            assertEquals("value collection size was not the expected value", numEntries,
                values.size());
            assertEquals("key set size was not the expected value", numEntries, keys.size());
            assertEquals("entry set size was not the expected value", numEntries, entries.size());
            assertEquals("region size was not the expected value", numEntries, region.size());
            Iterator valuesIt = values.iterator();
            Iterator keysIt = keys.iterator();
            Iterator entriesIt = entries.iterator();
            for (int i = 0; i < numEntries; i++) {
              assertTrue(valuesIt.hasNext());
              Integer value = (Integer) valuesIt.next();
              assertNotNull("value was null", value);

              assertTrue(keysIt.hasNext());
              String key = (String) keysIt.next();
              assertNotNull("key was null", key);

              assertTrue(entriesIt.hasNext());
              Region.Entry entry = (Region.Entry) entriesIt.next();
              assertNotNull("entry was null", entry);
              assertNotNull("entry key was null", entry.getKey());
              assertNotNull("entry value was null", entry.getValue());
            }
            assertTrue("should have been end of values iteration", !valuesIt.hasNext());
            assertTrue("should have been end of keys iteration", !keysIt.hasNext());
            assertTrue("should have been end of entries iteration", !entriesIt.hasNext());
          } catch (Exception ex) { // TODO: remove all of this and just disconnect DS in tear down
            try {
              getRootRegion().getSubregion(regionName).destroyRegion();
            } catch (Exception ex2) {
            }
            Assert.fail("Unexpected exception", ex);
          }
        }
      });
    } finally {
      resetVMLogLevel();
    }
  }

  // these tests make no sense for partitioned regions

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testDefinedEntryUpdated() {}

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testRemoteCacheListener() {}

  // user attributes aren't supported in partitioned regions at this time (5.1)

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testEntryUserAttribute() {}

  // these tests require misc Region operations not currently supported by PRs

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testInvalidateRegion() {}

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testLocalDestroyRegion() {}

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testLocalInvalidateRegion() {}

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testSnapshot() {}

  @Ignore("Not implemented for partitioned regions")
  @Override
  @Test
  public void testRootSnapshot() {}

  static class PoisonedKey implements Serializable {
    static volatile boolean poisoned = false;
    static volatile boolean poisonDetected = false;

    /**
     * Accessed via reflection
     *
     * @return true if poison found
     */
    public static boolean poisonFound() {
      boolean result = poisonDetected;
      poisonDetected = false; // restore default static value
      return result;
    }

    @Override
    public int hashCode() {
      int result = k.hashCode();
      synchronized (PoisonedKey.class) {
        if (poisoned) {
          result += (new Random()).nextInt();
        }
      }
      return result;
    }

    final String k;

    PoisonedKey(String s) {
      this.k = s;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      if (!(o instanceof PoisonedKey)) {
        return false;
      }
      PoisonedKey po = (PoisonedKey) o;
      if (k == null) {
        return po.k == null;
      }
      return k.equals(po.k);
    }
  }

  @Test
  public void testBadHash() {
    final String regionName = getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    SerializableRunnable createPR = new SerializableRunnable("createPartitionedRegion") {
      public void run() {
        try {
          createRegion(regionName, "root", getRegionAttributes());
        } catch (CacheException ex) {
          Assert.fail("While creating Partitioned region", ex);
        }
      }
    };
    vm0.invoke(createPR);
    vm1.invoke(createPR);

    vm0.invoke(new SerializableRunnable("Populate 1") {
      public void run() {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < 10; i++) {
          String st = Integer.toString(i);
          PoisonedKey pk = new PoisonedKey(st);
          region.create(pk, st);
        }
      }
    });

    // Verify values are readily accessible
    vm1.invoke(new SerializableRunnable("Read 1") {
      public void run() {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < 10; i++) {
          String st = Integer.toString(i);
          PoisonedKey pk = new PoisonedKey(st);
          assertTrue("Keys screwed up too early", region.get(pk).equals(st));
        }
      }
    });

    // Bucket ID's will be screwed up with these creates.
    vm0.invoke(new SerializableRunnable("Populate 2") {
      public void run() {
        Region region = getRootRegion().getSubregion(regionName);
        PoisonedKey.poisoned = true;
        try {
          for (int i = 10; i < 20; i++) {
            String st = Integer.toString(i);
            PoisonedKey pk = new PoisonedKey(st);
            region.create(pk, st);
          }
        } catch (PartitionedRegionException e) {
          PoisonedKey.poisonDetected = true;
        } finally {
          PoisonedKey.poisoned = false; // restore default static value
        }
      }
    });

    boolean success = vm0.invoke(() -> PoisonedKey.poisonFound());
    assertTrue("Hash mismatch not found", success);
  }
}
