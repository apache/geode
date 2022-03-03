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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.GemFireException;
import org.apache.geode.cache.AttributesFactory;
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
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * This class tests the functionality of a cache {@link Region region} that has a scope of {@link
 * Scope#DISTRIBUTED_ACK distributed ACK} and {@link PartitionAttributes partition-attributes}.
 *
 * @since GemFire 5.1
 */
public class PartitionedRegionDUnitTest extends MultiVMRegionTestCase {

  private static boolean InvalidateInvoked = false;

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

  @Override
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
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
    return factory.create();
  }


  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache30.PartitionedRegionDUnitTest$PoisonedKey");
    return properties;
  }


  /**
   * Bug #47235 concerns assertion failures being thrown when there is a member that receives
   * adjunct messages (as in a WAN gateway, a peer with clients, etc).
   */
  @Test
  public void testRegionInvalidationWithAdjunctMessages() {
    final String name = getUniqueName();
    VM vm1 = VM.getVM(1);
    RegionFactory<String, String> fact = getCache().createRegionFactory(RegionShortcut.PARTITION);
    Region<String, String> pr = fact.create(name + "Region");
    pr.put("Object1", "Value1");

    vm1.invoke("create PR", new SerializableRunnable() {
      @Override
      public void run() {
        RegionFactory<String, String> fact =
            getCache().createRegionFactory(RegionShortcut.PARTITION);
        fact.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        fact.addCacheListener(new CacheListenerAdapter<String, String>() {
          @Override
          public void afterInvalidate(EntryEvent event) {
            logger
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
          vm1.invoke("getStatus", () -> InvalidateInvoked));
    } finally {
      disconnectAllFromDS();
    }
  }

  /**
   * Tests the compatibility of creating certain kinds of subregions of a local region.
   */
  @Test
  public void testIncompatibleSubregions() throws CacheException {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);

    final String name = getUniqueName() + "-PR";
    vm0.invoke("Create partitioned Region", new SerializableRunnable() {
      @Override
      public void run() {
        try {

          createRegion(name, "INCOMPATIBLE_ROOT", getRegionAttributes());
        } catch (CacheException ex) {
          fail("While creating Partitioned region", ex);
        }
      }
    });

    vm1.invoke("Create non-partitioned Region", new SerializableRunnable() {
      @Override
      public void run() {
        try {
          RegionFactory<String, String> factory = getCache().createRegionFactory();
          try {
            createRegion(name, "INCOMPATIBLE_ROOT", factory);
            fail("Should have thrown an IllegalStateException");
          } catch (IllegalStateException ex) {
            // pass...
          }

        } catch (CacheException ex) {
          fail("While creating Partitioned Region", ex);
        }
      }
    });
  }

  private void setupExtendedTest(final String regionName) {
    SerializableRunnable createPR = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          createRegion(regionName, "root", getRegionAttributes());
        } catch (CacheException ex) {
          fail("While creating Partitioned region", ex);
        }
      }
    };
    for (int i = 1; i < 4; i++) {
      VM.getVM(i).invoke("createPartitionedRegion", createPR);
    }
    VM vm0 = VM.getVM(0);
    vm0.invoke("Populate Partitioned Region", new SerializableRunnable() {
      @Override
      public void run() {
        Region<Object, Object> region = null;
        try {
          region = createRegion(regionName, "root", getRegionAttributes());
          // since random keys are being used, we might hit duplicates
          logger.info("<ExpectedException action=add>"
              + "org.apache.geode.cache.EntryExistsException" + "</ExpectedException>");
          Random rand = new Random(System.currentTimeMillis());
          for (int i = 0; i < 20000; i++) {
            boolean created = false;
            while (!created) {
              try {
                int val = rand.nextInt(100000000);
                String key = String.valueOf(val);
                region.create(key, val);
                created = true;
              } catch (EntryExistsException eee) {
                // loop to try again
              }
            }
          }
        } catch (GemFireException ex) {
          fail("while creating or populating partitioned region", ex);
        } finally {
          if (region != null) {
            logger.info("<ExpectedException action=remove>"
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
    setupExtendedTest(regionName);

    VM vm0 = VM.getVM(0);
    vm0.invoke("exercise Region.values", new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Region<Object, Object> region = getRootRegion().getSubregion(regionName);
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
            assertThat(valuesIt.hasNext()).isTrue();
            Integer value = (Integer) valuesIt.next();
            assertNotNull("value was null", value);

            assertThat(keysIt.hasNext()).isTrue();
            String key = (String) keysIt.next();
            assertNotNull("key was null", key);

            assertThat(entriesIt.hasNext()).isTrue();
            Region.Entry entry = (Region.Entry) entriesIt.next();
            assertNotNull("entry was null", entry);
            assertNotNull("entry key was null", entry.getKey());
            assertNotNull("entry value was null", entry.getValue());
          }
          assertThat(!valuesIt.hasNext()).describedAs("should have been end of values iteration")
              .isTrue();
          assertThat(!keysIt.hasNext()).describedAs("should have been end of keys iteration")
              .isTrue();
          assertThat(!entriesIt.hasNext()).describedAs("should have been end of entries iteration")
              .isTrue();
        } catch (Exception ex) { // TODO: remove all of this and just disconnect DS in tear down
          try {
            getRootRegion().getSubregion(regionName).destroyRegion();
          } catch (Exception ignored) {
          }
          fail("Unexpected exception", ex);
        }
      }
    });
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
    static boolean poisonFound() {
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
      k = s;
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
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    SerializableRunnable createPR = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          createRegion(regionName, "root", getRegionAttributes());
        } catch (CacheException ex) {
          fail("While creating Partitioned region", ex);
        }
      }
    };
    vm0.invoke("createPartitionedRegion", createPR);
    vm1.invoke("createPartitionedRegion", createPR);

    vm0.invoke("Populate 1", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < 10; i++) {
        String st = Integer.toString(i);
        PoisonedKey pk = new PoisonedKey(st);
        region.create(pk, st);
      }
    });

    // Verify values are readily accessible
    vm1.invoke("Read 1", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i < 10; i++) {
        String st = Integer.toString(i);
        PoisonedKey pk = new PoisonedKey(st);
        assertThat(region.get(pk).equals(st)).describedAs("Keys screwed up too early").isTrue();
      }
    });

    // Bucket ID's will be screwed up with these creates.
    vm0.invoke("Populate 2", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(regionName);
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
    });

    boolean success = vm0.invoke(PoisonedKey::poisonFound);
    assertThat(success).describedAs("Hash mismatch not found").isTrue();
  }
}
