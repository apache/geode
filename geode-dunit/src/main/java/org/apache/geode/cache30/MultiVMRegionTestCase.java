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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.lang.ThrowableUtils.hasCauseMessage;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionEvent;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.TransactionListener;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.TxEventTestUtil;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.EntryExpiryTask;
import org.apache.geode.internal.cache.ExpiryTask;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.TombstoneService;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * Abstract superclass of {@link Region} tests that involve more than one VM.
 */
public abstract class MultiVMRegionTestCase extends RegionTestCase {
  private static final long POLL_INTERVAL_MILLIS = 50;

  private static final int PUT_RANGE_1_START = 1;
  private static final int PUT_RANGE_1_END = 5;
  private static final int PUT_RANGE_2_START = 6;
  private static final int PUT_RANGE_2_END = 10;

  private static TestCacheListener testCacheListener;
  private static TestCacheLoader testCacheLoader;
  private static TestCacheWriter testCacheWriter;

  private static int afterCreates;

  static LocalRegion CCRegion;
  static int distributedSystemID;
  protected VM vm0;
  protected VM vm1;
  protected VM vm2;
  protected VM vm3;


  private static <K, V> TestCacheListener<K, V> listener() {
    return testCacheListener;
  }

  private static <K, V> void setListener(TestCacheListener<K, V> listener) {
    testCacheListener = listener;
  }

  protected static <K, V> TestCacheLoader<K, V> loader() {
    return testCacheLoader;
  }

  protected static <K, V> void setLoader(TestCacheLoader<K, V> loader) {
    testCacheLoader = loader;
  }

  private static <K, V> TestCacheWriter<K, V> writer() {
    return testCacheWriter;
  }

  private static <K, V> void setWriter(TestCacheWriter<K, V> writer) {
    testCacheWriter = writer;
  }

  private static void cleanup() {
    testCacheListener = null;
    testCacheLoader = null;
    testCacheWriter = null;
    CCRegion = null;
  }

  @Before
  public void setup() {
    vm0 = VM.getVM(0);
    vm1 = VM.getVM(1);
    vm2 = VM.getVM(2);
    vm3 = VM.getVM(3);
  }

  @After
  public void caseTearDown() {
    disconnectAllFromDS();
  }

  @SuppressWarnings("RedundantThrows") // Subclasses may want to throw
  @Override
  protected final void postTearDownRegionTestCase() throws Exception {
    cleanup();
    invokeInEveryVM(MultiVMRegionTestCase::cleanup);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache30.MultiVMRegionTestCase$DeltaValue");
    return properties;
  }

  /**
   * This is a for the ConcurrentMap operations. 4 VMs are used to create the region and operations
   * are performed on one of the nodes
   */
  @Test
  public void testConcurrentOperations() {

    // create the VM(0 - 4)
    for (int i = 0; i < 4; i++) {
      VM vm = VM.getVM(i);
      vm.invoke("createRegion", () -> {
        Cache cache1 = getCache();
        RegionAttributes<?, ?> regionAttribs = getRegionAttributes();
        cache1.createRegionFactory(regionAttribs).create("R1");
      });
    }

    concurrentMapTest(SEPARATOR + "R1");
  }

  /**
   * Do putIfAbsent(), replace(Object, Object), replace(Object, Object, Object), remove(Object,
   * Object) operations
   */
  private void concurrentMapTest(final String rName) {

    vm0.invoke("doConcurrentMapOperations", () -> {
      Cache cache = getCache();
      final Region<String, String> pr = cache.getRegion(rName);
      assertThat(pr).describedAs(rName + " not created").isNotNull();

      // test successful putIfAbsent
      for (int i = PUT_RANGE_1_START; i <= PUT_RANGE_1_END; i++) {
        Object putResult = pr.putIfAbsent(Integer.toString(i), Integer.toString(i));
        assertThat(putResult).describedAs("Expected null, but got " + putResult + " for key " + i)
            .isNull();
      }
      int size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value")
          .isEqualTo(PUT_RANGE_1_END);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isFalse();

      // test unsuccessful putIfAbsent
      for (int i = PUT_RANGE_1_START; i <= PUT_RANGE_1_END; i++) {
        Object putResult = pr.putIfAbsent(Integer.toString(i), Integer.toString(i + 1));
        assertThat(putResult).describedAs("for i=" + i).isEqualTo(Integer.toString(i));
        assertThat(pr.get(Integer.toString(i))).describedAs("for i=" + i).isEqualTo(
            Integer.toString(i));
      }
      size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value")
          .isEqualTo(PUT_RANGE_1_END);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isFalse();

      // test successful replace(key, oldValue, newValue)
      for (int i = PUT_RANGE_1_START; i <= PUT_RANGE_1_END; i++) {
        boolean replaceSucceeded =
            pr.replace(Integer.toString(i), Integer.toString(i), "replaced" + i);
        assertThat(replaceSucceeded).describedAs("for i=" + i).isTrue();
        assertThat(pr.get(Integer.toString(i))).describedAs("for i=" + i).isEqualTo(
            ("replaced" + i));
      }
      size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value").isEqualTo(
          PUT_RANGE_1_END);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isFalse();

      // test unsuccessful replace(key, oldValue, newValue)
      for (int i = PUT_RANGE_1_START; i <= PUT_RANGE_2_END; i++) {
        boolean replaceSucceeded = pr.replace(Integer.toString(i), Integer.toString(i), // wrong
            // expected
            // old
            // value
            "not" + i);
        assertThat(replaceSucceeded).describedAs("for i=" + i).isFalse();
        Object expected1 = i <= PUT_RANGE_1_END ? "replaced" + i : null;
        assertThat(pr.get(Integer.toString(i))).describedAs("for i=" + i).isEqualTo(
            expected1);
      }
      size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value").isEqualTo(
          PUT_RANGE_1_END);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isFalse();

      // test successful replace(key, value)
      for (int i = PUT_RANGE_1_START; i <= PUT_RANGE_1_END; i++) {
        Object replaceResult = pr.replace(Integer.toString(i), "twice replaced" + i);
        assertThat(replaceResult).describedAs("for i=" + i).isEqualTo(("replaced" + i));
        assertThat(pr.get(Integer.toString(i))).describedAs("for i=" + i).isEqualTo(
            ("twice replaced" + i));
      }
      size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value").isEqualTo(
          PUT_RANGE_1_END);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isFalse();

      // test unsuccessful replace(key, value)
      for (int i = PUT_RANGE_2_START; i <= PUT_RANGE_2_END; i++) {
        Object replaceResult = pr.replace(Integer.toString(i), "thrice replaced" + i);
        assertThat(replaceResult).describedAs("for i=" + i).isNull();
        assertThat(pr.get(Integer.toString(i))).describedAs("for i=" + i).isNull();
      }
      size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value").isEqualTo(
          PUT_RANGE_1_END);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isFalse();

      // test unsuccessful remove(key, value)
      for (int i = PUT_RANGE_1_START; i <= PUT_RANGE_2_END; i++) {
        boolean removeResult = pr.remove(Integer.toString(i), Integer.toString(-i));
        assertThat(removeResult).describedAs("for i=" + i).isFalse();
        Object expected1 =
            i <= PUT_RANGE_1_END ? "twice replaced" + i : null;
        assertThat(pr.get(Integer.toString(i))).describedAs("for i=" + i).isEqualTo(
            expected1);
      }
      size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value").isEqualTo(
          PUT_RANGE_1_END);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isFalse();

      // test successful remove(key, value)
      for (int i = PUT_RANGE_1_START; i <= PUT_RANGE_1_END; i++) {
        boolean removeResult = pr.remove(Integer.toString(i), "twice replaced" + i);
        assertThat(removeResult).describedAs("for i=" + i).isTrue();
        assertThat(pr.get(Integer.toString(i))).describedAs("for i=" + i).isNull();
      }
      size = pr.size();
      assertThat(size).describedAs("Size doesn't return expected value").isEqualTo(0);
      assertThat(pr.isEmpty()).describedAs("isEmpty doesn't return proper state of the region")
          .isTrue();
    });


    /*
     * destroy the Region.
     */
    vm0.invoke("destroyRegionOp", () -> {
      Cache cache = getCache();
      Region<?, ?> pr = cache.getRegion(rName);
      assertThat(pr).describedAs("Region already destroyed.").isNotNull();
      pr.destroyRegion();
      assertThat(pr.isDestroyed()).describedAs("Region isDestroyed false").isTrue();
      assertThat(cache.getRegion(rName)).describedAs("Region not destroyed.").isNull();
    });
  }

  /**
   * Tests that doing a {@link Region#put put} in a distributed region one VM updates the value in
   * another VM.
   */
  @Test
  public void testDistributedUpdate() {
    boolean condition = getRegionAttributes().getScope().isDistributed();
    assertThat(condition).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";

    vm0.invoke("Put key/value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, oldValue);
      flushIfNecessary(region);
    });
    vm1.invoke("Put key/value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, oldValue);
      flushIfNecessary(region);
    });

    vm0.invoke("Update", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, newValue);
      flushIfNecessary(region);
    });

    vm1.invoke("Validate update", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Region.Entry entry = region.getEntry(key);
      assertThat(entry).isNotNull();
      assertThat(entry.getValue()).isEqualTo(newValue);
    });
  }

  /**
   * Verifies that distributed updates are delivered in order.
   *
   * <p>
   * Note that this test does not make sense for regions that are {@link Scope#DISTRIBUTED_NO_ACK}
   * for which we do not guarantee the ordering of updates for a single producer/single consumer.
   */
  @Test
  public void testOrderedUpdates() throws Exception {
    assumeThat(getRegionAttributes().getScope()).isNotEqualTo(Scope.DISTRIBUTED_NO_ACK);

    final String regionName = getUniqueName();
    final String key = "KEY";
    final int lastValue = 10;

    vm0.invoke("Create region", () -> {
      createRegion(regionName);
    });

    vm1.invoke("Create region and region entry", () -> {
      Region<String, Integer> region = createRegion(regionName);
      region.create(key, null);
    });

    vm1.invoke("Set listener", () -> {
      Region<String, Integer> region = getRootRegion().getSubregion(regionName);
      region.setUserAttribute(new LinkedBlockingQueue<Integer>());

      region.getAttributesMutator().addCacheListener(new CacheListenerAdapter<String, Integer>() {
        @Override
        public void afterUpdate(EntryEvent<String, Integer> event) {
          Region<String, Integer> region = event.getRegion();
          @SuppressWarnings("unchecked")
          BlockingQueue<Integer> queue = (BlockingQueue<Integer>) region.getUserAttribute();
          int value = event.getNewValue();
          try {
            queue.put(value);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      flushIfNecessary(region);
    });

    AsyncInvocation verify = vm1.invokeAsync("Verify", () -> {
      Region<String, Integer> region = getRootRegion().getSubregion(regionName);
      @SuppressWarnings("unchecked")
      BlockingQueue<Integer> queue = (BlockingQueue<Integer>) region.getUserAttribute();
      for (int i = 0; i <= lastValue; i++) {
        try {
          int value = queue.take();
          assertThat(value).isEqualTo(i);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    AsyncInvocation populate = vm0.invokeAsync("Populate", () -> {
      Region<String, Integer> region = getRootRegion().getSubregion(regionName);
      for (int i = 0; i <= lastValue; i++) {
        region.put(key, i);
      }
    });

    populate.await();
    verify.await();
  }

  /**
   * Tests that doing a distributed get results in a <code>netSearch</code>.
   */
  @Test
  public void testDistributedGet() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Populate region", () -> {
      Region<Object, Object> region = createRegion(name);
      region.put(key, value);
    });

    vm1.invoke("Distributed get", () -> {
      Region<Object, Object> region = createRegion(name);
      assertThat(region.get(key)).isEqualTo(value);
    });
  }

  /**
   * Tests that doing a {@link Region#put put} on a distributed region in one VM does not effect a
   * region in a different VM that does not have that key defined.
   */
  @Test
  public void testDistributedPutNoUpdate() throws Exception {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    Thread.sleep(250);

    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Put key/value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, value);
    });

    Thread.sleep(250);

    vm1.invoke("Verify no update", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Region.Entry entry = region.getEntry(key);
      if (getRegionAttributes().getDataPolicy().withReplication()
          || getRegionAttributes().getPartitionAttributes() != null) {
        assertThat(region.get(key)).isEqualTo(value);
      } else {
        assertThat(entry).isNull();
      }
    });
  }

  /**
   * Two VMs create a region. One populates a region entry. The other VM defines that entry. The
   * first VM updates the entry. The second VM should see the updated value.
   */
  @Test
  public void testDefinedEntryUpdated() {
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm0.invoke("Create and populate", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, oldValue);
    });

    vm1.invoke("Define entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        region.create(key, null);
      }
    });

    vm0.invoke("Update entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, newValue);
    });

    vm1.invoke("Get entry", repeatingIfNecessary(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get(key)).isEqualTo(newValue);
    }));
  }


  /**
   * Tests that {@linkplain Region#destroy destroying} an entry is propagated to all VMs that define
   * that entry.
   */
  @Test
  public void testDistributedDestroy() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = createRegion(name);
        assertThat(region.isDestroyed()).isFalse();
        Region root = region.getParentRegion();
        assertThat(root.isDestroyed()).isFalse();
      }
    };

    vm0.invoke("Create Region", create);
    vm1.invoke("Create Region", create);
    vm2.invoke("Create Region", create);

    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable put = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.put(key, value);
        assertThat(region.isDestroyed()).isFalse();
        assertThat(region.getParentRegion().isDestroyed()).isFalse();
        flushIfNecessary(region);
      }
    };

    vm0.invoke("Put key/value", put);
    vm1.invoke("Put key/value", put);
    vm2.invoke("Put key/value", put);

    SerializableRunnable verifyPut = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<?, ?> root = getRootRegion();
        assertThat(root.isDestroyed()).isFalse();
        Region<Object, Object> region = root.getSubregion(name);
        assertThat(region.isDestroyed()).isFalse();
        assertThat(region.getEntry(key).getValue()).isEqualTo(value);
      }
    };

    vm0.invoke("Verify Put", verifyPut);
    vm1.invoke("Verify Put", verifyPut);
    vm2.invoke("Verify Put", verifyPut);

    vm0.invoke("Destroy Entry", () -> {
      Region<?, ?> region = getRootRegion().getSubregion(name);
      region.destroy(key);
      flushIfNecessary(region);
    });

    CacheSerializableRunnable verifyDestroy =
        new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {
            Region<?, ?> root = getRootRegion();
            assertThat(root.isDestroyed()).isFalse();
            Region<Object, Object> region = root.getSubregion(name);
            assertThat(region.isDestroyed()).isFalse();
            assertThat(region.getEntry(key)).isNull();
          }
        };
    vm0.invoke("Verify entry destruction", verifyDestroy);
    vm1.invoke("Verify entry destruction", verifyDestroy);
    vm2.invoke("Verify entry destruction", verifyDestroy);
  }

  /**
   * Tests that {@linkplain Region#destroy destroying} a region is propagated to all VMs that define
   * that region.
   */
  @Test
  public void testDistributedRegionDestroy() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    invokeInEveryVM("Create Region", () -> {
      createRegion(name);
    });

    vm0.invoke("Destroy Region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroyRegion();
      flushIfNecessary(region);
    });

    invokeInEveryVM("Verify region destruction", () -> await()
        .until(() -> getRootRegion().getSubregion(name) == null));
  }

  /**
   * Tests that a {@linkplain Region#localDestroy} does not effect other VMs that define that
   * entry.
   */
  @Test
  public void testLocalDestroy() throws Exception {
    assumeThat(supportsLocalDestroyAndLocalInvalidate()).isTrue();

    // test not valid for persistBackup region since they have to be
    // mirrored KEYS_VALUES
    if (getRegionAttributes().getDataPolicy().withPersistence()) {
      return;
    }

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    Thread.sleep(250);

    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Put key/value", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(name);
      region1.put(key, value);
    });
    vm1.invoke("Put key/value", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(name);
      region1.put(key, value);
    });

    Thread.sleep(250);

    vm0.invoke("Local Destroy Entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroy(key);
    });

    Thread.sleep(250);

    vm1.invoke("Verify entry existence", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.getEntry(key)).isNotNull();
    });
  }

  /**
   * Tests that a {@link Region#localDestroyRegion} is not propagated to other VMs that define that
   * region.
   */
  @Test
  public void testLocalRegionDestroy() throws Exception {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    Thread.sleep(250);

    vm0.invoke("Local Destroy Region",
        () -> getRootRegion().getSubregion(name).localDestroyRegion());

    Thread.sleep(250);

    vm1.invoke("Verify region existence", () -> {
      assertThat(getRootRegion().getSubregion(name)).isNotNull();
    });
  }

  /**
   * Tests that {@linkplain Region#invalidate invalidating} an entry is propagated to all VMs that
   * define that entry.
   */
  @Test
  public void testDistributedInvalidate() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    // vm2 is on a different gemfire system
    vm2.invoke("Create Region", () -> {
      createRegion(name);
    });

    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable put = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.put(key, value);
        flushIfNecessary(region);
      }
    };

    vm0.invoke("Put key/value", put);
    vm1.invoke("Put key/value", put);
    vm2.invoke("Put key/value", put);

    vm0.invoke("Invalidate Entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.invalidate(key);
      flushIfNecessary(region);
    });

    CacheSerializableRunnable verify = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        Region.Entry entry = region.getEntry(key);
        assertThat(entry).isNotNull();
        if (entry.getValue() != null) {
          // changed from severe to fine because it is possible
          // for this to return non-null on d-no-ack
          // that is was invokeRepeatingIfNecessary is called
          logger.debug("invalidated entry has value of " + entry.getValue());
        }
        assertThat(entry.getValue()).isNull();
      }
    };

    vm1.invoke("Verify entry invalidation", verify);
    vm2.invoke("Verify entry invalidation", verify);
  }

  /**
   * Tests that {@linkplain Region#invalidate invalidating} an entry in multiple VMs does not cause
   * any problems.
   */
  @Test
  public void testDistributedInvalidate4() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    int vmCount = VM.getVMCount();
    for (int i = 0; i < vmCount; i++) {
      VM vm0 = VM.getVM(i);
      vm0.invoke("Create Region", () -> {
        createRegion(name);
      });
    }

    for (int i = 0; i < vmCount; i++) {
      VM vm = VM.getVM(i);
      vm.invoke("put entry", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.put(key, value);
        flushIfNecessary(region);
      });
    }

    for (int i = 0; i < vmCount; i++) {
      VM vm = VM.getVM(i);
      vm.invoke("Invalidate Entry", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.invalidate(key);
        flushIfNecessary(region);
      });
    }

    for (int i = 0; i < vmCount; i++) {
      VM vm0 = VM.getVM(i);
      vm0.invoke("Verify entry invalidation", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        Region.Entry entry = region.getEntry(key);
        assertThat(entry).isNotNull();
        assertThat(entry.getValue()).isNull();
      });
    }
  }

  /**
   * Tests that {@linkplain Region#invalidateRegion invalidating} a region is propagated to all VMs
   * that define that entry.
   */
  @Test
  public void testDistributedRegionInvalidate() {
    assumeThat(supportsSubregions()).isTrue();

    final String name = this.getUniqueName();
    final String subname = "sub";
    final boolean useSubs = getRegionAttributes().getPartitionAttributes() == null;

    invokeInEveryVM("Create Region", () -> {
      Region<Object, Object> region1;
      region1 = createRegion(name);
      if (useSubs) {
        getCache().createRegionFactory(region1.getAttributes()).createSubregion(region1, subname);
      }
    });

    final Object key = "KEY";
    final Object value = "VALUE";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";

    invokeInEveryVM("Put key/value", () -> {
      Region<Object, Object> region1 = getRootRegion().getSubregion(name);
      region1.put(key, value);
      region1.put(key2, value2);
      flushIfNecessary(region1);

      if (useSubs) {
        Region<Object, Object> subregion = region1.getSubregion(subname);
        subregion.put(key, value);
        subregion.put(key2, value2);
        flushIfNecessary(subregion);
      }
    });

    vm0.invoke("Invalidate Region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.invalidateRegion();
    });

    invokeInEveryVM(repeatingIfNecessary(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Region.Entry<Object, Object> entry = region.getEntry(key);
      assertThat(entry).isNotNull();
      Object v = entry.getValue();
      assertThat(v).isNull();

      entry = region.getEntry(key2);
      assertThat(entry).isNotNull();
      assertThat(entry.getValue()).isNull();

      if (useSubs) {
        Region<Object, Object> subregion = region.getSubregion(subname);
        entry = subregion.getEntry(key);
        assertThat(entry).isNotNull();
        assertThat(entry.getValue()).isNull();

        entry = subregion.getEntry(key2);
        assertThat(entry).isNotNull();
        assertThat(entry.getValue()).isNull();
      }
    }));
  }

  /**
   * Tests that a {@link CacheListener} is invoked in a remote VM.
   */
  @Test
  public void testRemoteCacheListener() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";

    vm0.invoke("Create Region and Put", () -> {
      Region<Object, Object> region1 = createRegion(name);
      region1.put(key, oldValue);
    });
    vm1.invoke("Create Region and Put", () -> {
      Region<Object, Object> region1 = createRegion(name);
      region1.put(key, oldValue);
    });

    vm1.invoke("Set listener", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setListener(new TestCacheListener<Object, Object>() {
        @Override
        public void afterUpdate2(EntryEvent<Object, Object> event) {
          assertThat(event.getOperation()).isEqualTo(Operation.UPDATE);
          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getDistributedMember()).isEqualTo(event.getCallbackArgument());
          assertThat(event.getKey()).isEqualTo(key);
          assertThat(event.getOldValue()).isEqualTo(oldValue);
          assertThat(event.getNewValue()).isEqualTo(newValue);
          assertThat(event.getOperation().isLoad()).isFalse();
          assertThat(event.getOperation().isLocalLoad()).isFalse();
          assertThat(event.getOperation().isNetLoad()).isFalse();
          assertThat(event.getOperation().isNetSearch()).isFalse();
          if (event.getRegion().getAttributes().getOffHeap()) {
            // since off heap always serializes the old value is serialized and available
            assertThat(event.getSerializedOldValue().getDeserializedValue()).isEqualTo(oldValue);
          } else {
            assertThat(event.getSerializedOldValue()).isNull(); // since it was put originally in
            // this VM
          }
          DataInputStream dis = new DataInputStream(
              new ByteArrayInputStream(event.getSerializedNewValue().getSerializedValue()));
          try {
            assertThat(DataSerializer.<Object>readObject(dis)).isEqualTo(newValue);
          } catch (IOException | ClassNotFoundException e) {
            fail("Unexpected Exception", e);
          }
        }
      });
      region.getAttributesMutator().addCacheListener(listener());
    });

    // I see no reason to pause here.
    // The test used to pause here but only if no-ack.
    // But we have no operations to wait for.
    // The last thing we did was install a listener in vm1
    // and it is possible that vm0 does not yet know we have
    // a listener but for this test it does not matter.
    // So I'm commenting out the following pause:
    // pauseIfNecessary();
    // If needed then do a flushIfNecessary(region) after adding the cache listener

    vm0.invoke("Update", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, newValue, getSystem().getDistributedMember());
    });

    vm1.invoke("Verify Update", () -> {
      listener().waitForInvocation(3000, 10);

      // Setup listener for next test
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setListener(new TestCacheListener<Object, Object>() {
        @Override
        public void afterInvalidate2(EntryEvent<Object, Object> event) {
          assertThat(event.getOperation()).isEqualTo(Operation.INVALIDATE);
          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getDistributedMember()).isEqualTo(event.getCallbackArgument());
          assertThat(event.getKey()).isEqualTo(key);
          assertThat(event.getOldValue()).isEqualTo(newValue);
          assertThat(event.getNewValue()).isNull();
          assertThat(event.getOperation().isLoad()).isFalse();
          assertThat(event.getOperation().isLocalLoad()).isFalse();
          assertThat(event.getOperation().isNetLoad()).isFalse();
          assertThat(event.getOperation().isNetSearch()).isFalse();
          assertThat(event.getSerializedNewValue()).isNull();
          DataInputStream dis = new DataInputStream(
              new ByteArrayInputStream(event.getSerializedOldValue().getSerializedValue()));
          try {
            assertThat(DataSerializer.<Object>readObject(dis)).isEqualTo(newValue);
          } catch (IOException | ClassNotFoundException e) {
            fail("Unexpected Exception", e);
          }
        }
      });
      region.getAttributesMutator().addCacheListener(listener());
    });

    vm0.invoke("Invalidate", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.invalidate(key, getSystem().getDistributedMember());
    });

    vm1.invoke("Verify Invalidate", () -> {
      listener().waitForInvocation(3000, 10);

      // Setup listener for next test
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setListener(new TestCacheListener<Object, Object>() {
        @Override
        public void afterDestroy2(EntryEvent<Object, Object> event) {
          assertThat(event.getOperation().isDestroy()).isTrue();
          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getDistributedMember()).isEqualTo(event.getCallbackArgument());
          assertThat(event.getKey()).isEqualTo(key);
          assertThat(event.getOldValue()).isNull();
          assertThat(event.getNewValue()).isNull();
          assertThat(event.getOperation().isLoad()).isFalse();
          assertThat(event.getOperation().isLocalLoad()).isFalse();
          assertThat(event.getOperation().isNetLoad()).isFalse();
          assertThat(event.getOperation().isNetSearch()).isFalse();
          assertThat(event.getSerializedOldValue()).isNull();
          assertThat(event.getSerializedNewValue()).isNull();
        }
      });
      region.getAttributesMutator().addCacheListener(listener());
    });

    vm0.invoke("Destroy", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroy(key, getSystem().getDistributedMember());
    });

    vm1.invoke("Verify Destroy", () -> {
      listener().waitForInvocation(3000, 10);

      // Setup listener for next test
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setListener(new TestCacheListener<Object, Object>() {
        @Override
        public void afterRegionInvalidate2(RegionEvent<Object, Object> event) {
          assertThat(event.getOperation()).isEqualTo(Operation.REGION_INVALIDATE);
          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getDistributedMember()).isEqualTo(event.getCallbackArgument());
        }
      });
      region.getAttributesMutator().addCacheListener(listener());
    });

    vm0.invoke("Invalidate Region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.invalidateRegion(getSystem().getDistributedMember());
    });

    vm1.invoke("Verify Invalidate Region", () -> {
      listener().waitForInvocation(3000, 10);

      // Setup listener for next test
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setListener(new TestCacheListener<Object, Object>() {
        @Override
        public void afterRegionDestroy2(RegionEvent<Object, Object> event) {
          assertThat(event.getOperation()).isEqualTo(Operation.REGION_DESTROY);
          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getDistributedMember()).isEqualTo(event.getCallbackArgument());
        }
      });
      region.getAttributesMutator().addCacheListener(listener());
    });

    vm0.invoke("Destroy Region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroyRegion(getSystem().getDistributedMember());
    });

    vm1.invoke("Verify Destroy Region", () -> {
      listener().waitForInvocation(3000, 10);
    });
  }

  /**
   * Tests that a {@link CacheListener} is invoked in a remote VM.
   */
  @Test
  public void testRemoteCacheListenerInSubregion() {
    assumeThat(supportsSubregions()).isTrue();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Root", (SerializableRunnableIF) this::createRootRegion);

    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm1.invoke("Set listener", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setListener(new TestCacheListener<Object, Object>() {
        @Override
        public void afterRegionInvalidate2(RegionEvent<Object, Object> event) {
          assertThat(event.getOperation()).isEqualTo(Operation.REGION_INVALIDATE);
          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getDistributedMember()).isEqualTo(event.getCallbackArgument());
        }
      });
      region.getAttributesMutator().addCacheListener(listener());
    });

    vm0.invoke("Invalidate Root Region",
        () -> getRootRegion().invalidateRegion(getSystem().getDistributedMember()));

    vm1.invoke("Verify Invalidate Region", () -> {
      listener().waitForInvocation(3000, 10);

      // Setup listener for next test
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setListener(new TestCacheListener<Object, Object>() {
        @Override
        public void afterRegionDestroy2(RegionEvent<Object, Object> event) {
          assertThat(event.getOperation()).isEqualTo(Operation.REGION_DESTROY);
          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getDistributedMember()).isEqualTo(event.getCallbackArgument());
        }
      });
      region.getAttributesMutator().addCacheListener(listener());
    });

    vm0.invoke("Destroy Root Region",
        () -> getRootRegion().destroyRegion(getSystem().getDistributedMember()));

    vm1.invoke("Verify Destroy Region", () -> {
      listener().waitForInvocation(3000, 10);
    });
  }


  /**
   * Indicate whether this region supports netload
   *
   * @return true if it supports netload
   */
  protected boolean supportsNetLoad() {
    return true;
  }

  /**
   * Tests that a {@link CacheLoader} is invoked in a remote VM. This essentially tests
   * <code>netLoad</code>.
   */
  @Test
  public void testRemoteCacheLoader() {
    assumeThat(supportsNetLoad()).isTrue();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm1.invoke("Set CacheLoader", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
          assertThat(helper.getRegion()).isEqualTo(region);
          assertThat(helper.getKey()).isEqualTo(key);
          assertThat(helper.getArgument()).isNull();

          return value;
        }
      });
      region.getAttributesMutator().setCacheLoader(loader());
    });

    vm0.invoke("Remote load", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get(key)).isEqualTo(value);
    });

    vm1.invoke("Verify loader", () -> {
      assertThat(loader().wasInvoked()).isTrue();
    });
  }

  /**
   * Tests that the parameter passed to a remote {@link CacheLoader} is actually passed.
   */
  @Test
  public void testRemoteCacheLoaderArg() {
    assumeThat(supportsNetLoad()).isTrue();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final String arg = "ARG";

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm1.invoke("Set CacheLoader", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
          assertThat(helper.getRegion()).isEqualTo(region);
          assertThat(helper.getKey()).isEqualTo(key);
          assertThat(helper.getArgument()).isEqualTo(arg);

          return value;
        }
      });
      region.getAttributesMutator().setCacheLoader(loader());
      flushIfNecessary(region);
    });

    vm0.invoke("Remote load", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);

      try {
        // Use a non-serializable arg object
        region.get(key, new Object() {});
        fail("Should have thrown an IllegalArgumentException");

      } catch (IllegalArgumentException ex) {
        // pass...
      }
      assertThat(region.getEntry(key)).isNull();
      try {
        assertThat(region.get(key, arg)).isEqualTo(value);
      } catch (IllegalArgumentException ignored) {
      }
    });

    vm1.invoke("Verify loader", () -> {
      assertThat(loader().wasInvoked()).isTrue();
    });
  }

  /**
   * Tests that a remote {@link CacheLoader} that throws a {@link CacheLoaderException} results is
   * propagated back to the caller.
   */
  @Test
  public void testRemoteCacheLoaderException() {
    assumeThat(supportsNetLoad()).isTrue();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm1.invoke("Set CacheLoader", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
          assertThat(helper.getRegion()).isEqualTo(region);
          assertThat(helper.getKey()).isEqualTo(key);
          assertThat(helper.getArgument()).isNull();

          String s = "Test Exception";
          throw new CacheLoaderException(s);
        }
      });
      region.getAttributesMutator().setCacheLoader(loader());
      flushIfNecessary(region);
    });

    vm0.invoke("Remote load", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      try {
        region.get(key);
        fail("Should have thrown a CacheLoaderException");

      } catch (CacheLoaderException ex) {
        // pass...
      }
    });

    vm1.invoke("Verify loader", () -> {
      assertThat(loader().wasInvoked()).isTrue();
    });
  }

  @Test
  public void testCacheLoaderWithNetSearch() {
    assumeThat(supportsNetLoad()).isTrue();

    // some tests use mirroring by default (e.g. persistBackup regions)
    // if so, then this test won't work right
    assumeThat(getRegionAttributes().getDataPolicy().withReplication()).isFalse();
    assumeThat(getRegionAttributes().getDataPolicy().withPreloaded()).isFalse();

    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = 42;

    // use vm on other gemfire system

    vm1.invoke("set remote value", () -> {
      Region<Object, Object> rgn = createRegion(name);
      rgn.put(key, value);
      flushIfNecessary(rgn);
    });

    final TestCacheLoader<Object, Object> loader1 = new TestCacheLoader<Object, Object>() {
      @Override
      public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {

        assertThat(helper.getKey()).isEqualTo(key);
        assertThat(helper.getRegion().getName()).isEqualTo(name);

        try {
          helper.getRegion().getAttributes();
          Object result = helper.netSearch(false);
          assertThat(result).isEqualTo(value);
          return result;
        } catch (TimeoutException ex) {
          fail("Why did I time out?", ex);
        }
        return null;
      }
    };

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    factory.setCacheLoader(loader1);
    Region<Object, Object> region = createRegion(name, factory);

    loader1.wasInvoked();

    Region.Entry entry = region.getEntry(key);
    assertThat(entry).isNull();
    region.create(key, null);

    entry = region.getEntry(key);
    assertThat(entry).isNotNull();
    assertThat(entry.getValue()).isNull();

    // make sure value is still there in vm1
    vm1.invoke("verify remote value", () -> {
      Region rgn = getRootRegion().getSubregion(name);
      assertThat(rgn.getEntry(key).getValue()).isEqualTo(value);
    });

    assertThat(region.get(key)).isEqualTo(value);
    // if global scope, then a netSearch is done BEFORE the loader is invoked,
    // so we get the value but the loader is never invoked.
    if (region.getAttributes().getScope().isGlobal()) {
      assertThat(loader1.wasInvoked()).isFalse();
    } else {
      assertThat(loader1.wasInvoked()).isTrue();
    }
    assertThat(region.getEntry(key).getValue()).isEqualTo(value);
  }

  @Test
  public void testCacheLoaderWithNetLoad() {
    // replicated regions and partitioned regions make no sense for this
    // test
    assumeThat(getRegionAttributes().getDataPolicy().withReplication()).isFalse();
    assumeThat(getRegionAttributes().getDataPolicy().withPreloaded()).isFalse();
    assumeThat(getRegionAttributes().getPartitionAttributes())
        .withFailMessage("the region has partition attributes").isNull();

    final String name = this.getUniqueName();
    final Object key = this.getUniqueName();
    final Object value = 42;

    vm1.invoke("set up remote loader", () -> {
      final TestCacheLoader<Object, Object> remoteLoader = new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
          assertThat(helper.getKey()).isEqualTo(key);
          assertThat(helper.getRegion().getName()).isEqualTo(name);
          return value;
        }
      };

      RegionFactory<Object, Object> factory =
          getCache().createRegionFactory(getRegionAttributes());
      factory.setCacheLoader(remoteLoader);
      createRegion(name, factory);
    });

    final TestCacheLoader<Object, Object> loader1 = new TestCacheLoader<Object, Object>() {
      @Override
      public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {

        assertThat(helper.getKey()).isEqualTo(key);
        assertThat(helper.getRegion().getName()).isEqualTo(name);

        try {
          helper.getRegion().getAttributes();
          Object result = helper.netSearch(true);
          assertThat(result).isEqualTo(value);
          return result;
        } catch (TimeoutException ex) {
          fail("Why did I time out?", ex);
        }
        return null;
      }
    };

    RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
    factory.setCacheLoader(loader1);
    Region<Object, Object> region = createRegion(name, factory);

    loader1.wasInvoked();

    Region.Entry entry = region.getEntry(key);
    assertThat(entry).isNull();

    region.create(key, null);

    entry = region.getEntry(key);
    assertThat(entry).isNotNull();
    assertThat(entry.getValue()).isNull();

    assertThat(region.get(key)).isEqualTo(value);

    assertThat(loader1.wasInvoked()).isTrue();
    assertThat(region.getEntry(key).getValue()).isEqualTo(value);
  }


  /**
   * Tests that {@link Region#get} returns <code>null</code> when there is no remote loader.
   */
  @Test
  public void testNoRemoteCacheLoader() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm0.invoke("Remote load", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get(key)).isNull();
    });
  }

  /**
   * Tests that a remote <code>CacheLoader</code> is not invoked if the remote region has an invalid
   * entry (that is, a key, but no value).
   */
  @Test
  public void testNoLoaderWithInvalidEntry() {
    assumeThat(supportsNetLoad()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = createRegion(name);
        setLoader(new TestCacheLoader<Object, Object>() {
          @Override
          public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
            return value;
          }
        });
        region.getAttributesMutator().setCacheLoader(loader());
      }
    };

    vm0.invoke("Create Region", create);
    vm1.invoke("Create Region", create);

    vm1.invoke("Create invalid entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.create(key, null);
    });

    vm0.invoke("Remote get", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get(key)).isEqualTo(value);
      assertThat(loader().wasInvoked()).isTrue();
    });

    vm1.invoke("Verify loader", () -> {
      assertThat(loader().wasInvoked()).isFalse();
    });
  }

  /**
   * Tests that a remote {@link CacheWriter} is invoked and that <code>CacheWriter</code> arguments
   * and {@link CacheWriterException}s are propagated appropriately.
   */
  @Test
  public void testRemoteCacheWriter() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";
    final Object arg = "ARG";
    final Object exception = "EXCEPTION";

    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = createRegion(name);

        // Put key2 in the region before any callbacks are
        // registered, so it can be destroyed later
        region.put(key2, value2);
        assertThat(region.size()).isEqualTo(1);
        if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
          GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
          MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
          LocalRegion reRegion;
          reRegion = (LocalRegion) region;
          RegionEntry re = reRegion.getRegionEntry(key2);
          StoredObject so = (StoredObject) re.getValue();
          assertThat(so.getRefCount()).isEqualTo(1);
          assertThat(ma.getStats().getObjects()).isEqualTo(1);
        }
      }
    };

    vm0.invoke("Create Region", create);
    vm1.invoke("Create Region", create);

    //////// Create

    vm1.invoke("Set Writer", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setWriter(new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeCreate2(EntryEvent<Object, Object> event) throws CacheWriterException {

          if (exception.equals(event.getCallbackArgument())) {
            String s = "Test Exception";
            throw new CacheWriterException(s);
          }

          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isCreate()).isTrue();
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getKey()).isEqualTo(key);
          assertThat(event.getOldValue()).isNull();
          assertThat(event.getNewValue()).isEqualTo(oldValue);
          assertThat(event.getOperation().isLoad()).isFalse();
          assertThat(event.getOperation().isLocalLoad()).isFalse();
          assertThat(event.getOperation().isNetLoad()).isFalse();
          assertThat(event.getOperation().isNetSearch()).isFalse();

        }
      });
      region.getAttributesMutator().setCacheWriter(writer());
      flushIfNecessary(region);
    });

    vm0.invoke("Create with Exception", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      try {
        region.put(key, oldValue, exception);
        fail("Should have thrown a CacheWriterException");

      } catch (CacheWriterException ex) {
        assertThat(region.getEntry(key)).isNull();
        assertThat(region.size()).isEqualTo(1);
        if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
          GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
          MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
          assertThat(ma.getStats().getObjects()).isEqualTo(1);
        }
      }
    });

    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });

    vm0.invoke("Create with Argument", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, oldValue, arg);
      assertThat(region.size()).isEqualTo(2);
      if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
        assertThat(ma.getStats().getObjects()).isEqualTo(2);
        LocalRegion reRegion;
        reRegion = (LocalRegion) region;
        StoredObject so = (StoredObject) reRegion.getRegionEntry(key).getValue();
        assertThat(so.getRefCount()).isEqualTo(1);
      }
    });
    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });

    //////// Update

    vm1.invoke("Set Writer", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setWriter(new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeUpdate2(EntryEvent<Object, Object> event) throws CacheWriterException {

          Object argument = event.getCallbackArgument();
          if (exception.equals(argument)) {
            String s = "Test Exception";
            throw new CacheWriterException(s);
          }

          assertThat(argument).isEqualTo(arg);

          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isUpdate()).isTrue();
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getKey()).isEqualTo(key);
          assertThat(event.getOldValue()).isEqualTo(oldValue);
          assertThat(event.getNewValue()).isEqualTo(newValue);
          assertThat(event.getOperation().isLoad()).isFalse();
          assertThat(event.getOperation().isLocalLoad()).isFalse();
          assertThat(event.getOperation().isNetLoad()).isFalse();
          assertThat(event.getOperation().isNetSearch()).isFalse();

        }
      });
      region.getAttributesMutator().setCacheWriter(writer());
    });

    vm0.invoke("Update with Exception", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      try {
        region.put(key, newValue, exception);
        fail("Should have thrown a CacheWriterException");

      } catch (CacheWriterException ex) {
        Region.Entry entry = region.getEntry(key);
        assertThat(entry.getValue()).isEqualTo(oldValue);
        assertThat(region.size()).isEqualTo(2);
        if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
          GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
          MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
          assertThat(ma.getStats().getObjects()).isEqualTo(2);
          LocalRegion reRegion;
          reRegion = (LocalRegion) region;
          StoredObject so = (StoredObject) reRegion.getRegionEntry(key).getValue();
          assertThat(so.getRefCount()).isEqualTo(1);
        }
      }
    });
    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });

    vm0.invoke("Update with Argument", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, newValue, arg);
      assertThat(region.size()).isEqualTo(2);
      if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
        assertThat(ma.getStats().getObjects()).isEqualTo(2);
      }
    });
    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });

    //////// Destroy

    vm1.invoke("Set Writer", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setWriter(new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeDestroy2(EntryEvent<Object, Object> event) throws CacheWriterException {

          Object argument = event.getCallbackArgument();
          if (exception.equals(argument)) {
            String s = "Test Exception";
            throw new CacheWriterException(s);
          }

          assertThat(argument).isEqualTo(arg);

          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isDestroy()).isTrue();
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
          assertThat(event.getKey()).isEqualTo(key);
          assertThat(event.getOldValue()).isEqualTo(newValue);
          assertThat(event.getNewValue()).isNull();
          assertThat(event.getOperation().isLoad()).isFalse();
          assertThat(event.getOperation().isLocalLoad()).isFalse();
          assertThat(event.getOperation().isNetLoad()).isFalse();
          assertThat(event.getOperation().isNetSearch()).isFalse();
        }
      });
      region.getAttributesMutator().setCacheWriter(writer());
    });

    vm0.invoke("Destroy with Exception", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      try {
        region.destroy(key, exception);
        fail("Should have thrown a CacheWriterException");

      } catch (CacheWriterException ex) {
        assertThat(region.getEntry(key)).isNotNull();
        assertThat(region.size()).isEqualTo(2);
        if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
          GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
          MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
          assertThat(ma.getStats().getObjects()).isEqualTo(2);
        }
      }
    });
    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });

    vm0.invoke("Destroy with Argument", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroy(key, arg);
      assertThat(region.size()).isEqualTo(1);
      if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
        assertThat(ma.getStats().getObjects()).isEqualTo(1);
      }
    });
    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });

    //////// Region Destroy

    vm1.invoke("Set Writer", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setWriter(new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeRegionDestroy2(RegionEvent<Object, Object> event)
            throws CacheWriterException {

          Object argument = event.getCallbackArgument();
          if (exception.equals(argument)) {
            String s = "Test Exception";
            throw new CacheWriterException(s);
          }

          assertThat(argument).isEqualTo(arg);

          assertThat(event.getRegion()).isEqualTo(region);
          assertThat(event.getOperation().isRegionDestroy()).isTrue();
          assertThat(event.getOperation().isDistributed()).isTrue();
          assertThat(event.getOperation().isExpiration()).isFalse();
          assertThat(event.isOriginRemote()).isTrue();
        }
      });
      region.getAttributesMutator().setCacheWriter(writer());
    });

    vm0.invoke("Destroy with Exception", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      try {
        region.destroyRegion(exception);
        fail("Should have thrown a CacheWriterException");

      } catch (CacheWriterException ex) {
        if (region.isDestroyed()) {
          fail("should not have an exception if region is destroyed", ex);
        }
        assertThat(region.size()).isEqualTo(1);
        if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
          GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
          MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
          assertThat(ma.getStats().getObjects()).isEqualTo(1);
        }
      }
    });
    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });

    vm0.invoke("Destroy with Argument", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.size()).isEqualTo(1);
      if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
        assertThat(ma.getStats().getObjects()).isEqualTo(1);
      }
      region.destroyRegion(arg);
      if (region.getAttributes().getOffHeap() && !(region instanceof PartitionedRegion)) {
        GemFireCacheImpl gfc = (GemFireCacheImpl) getCache();
        final MemoryAllocatorImpl ma = (MemoryAllocatorImpl) gfc.getOffHeapStore();
        await("waiting for off-heap object count go to zero")
            .until(() -> ma.getStats().getObjects(), equalTo(0));

      }
    });
    vm1.invoke("Verify callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });
  }

  /**
   * Tests that, when given a choice, a local <code>CacheWriter</code> is invoked instead of a
   * remote one.
   */
  @Test
  public void testLocalAndRemoteCacheWriters() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object oldValue = "OLD_VALUE";
    final Object newValue = "NEW_VALUE";

    vm0.invoke("Create \"Local\" Region", () -> {
      Region<Object, Object> region = createRegion(name);
      setWriter(new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeUpdate2(EntryEvent<Object, Object> event) throws CacheWriterException {}

        @Override
        public void beforeCreate2(EntryEvent<Object, Object> event) throws CacheWriterException {}

        @Override
        public void beforeDestroy2(EntryEvent<Object, Object> event)
            throws CacheWriterException {}

        @Override
        public void beforeRegionDestroy2(RegionEvent<Object, Object> event)
            throws CacheWriterException {}
      });
      region.getAttributesMutator().setCacheWriter(writer());
    });

    vm1.invoke("Create \"Local\" Region", () -> {
      Region<Object, Object> region1 = createRegion(name);
      setWriter(new TestCacheWriter<Object, Object>() {});
      region1.getAttributesMutator().setCacheWriter(writer());
    });

    vm0.invoke("Create entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, oldValue);
      assertThat(writer().wasInvoked()).isTrue();
    });
    vm1.invoke("Verify no callback", () -> {
      assertThat(writer().wasInvoked()).isFalse();
    });

    vm0.invoke("Update entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, newValue);
      assertThat(writer().wasInvoked()).isTrue();
    });
    vm1.invoke("Verify no callback", () -> {
      assertThat(writer().wasInvoked()).isFalse();
    });

    vm0.invoke("Destroy entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroy(key);
      assertThat(writer().wasInvoked()).isTrue();
    });
    vm1.invoke("Verify no callback", () -> {
      assertThat(writer().wasInvoked()).isFalse();
    });

    vm0.invoke("Destroy region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.destroyRegion();
      assertThat(writer().wasInvoked()).isTrue();
    });
    vm1.invoke("Verify no callback", () -> {
      assertThat(writer().wasInvoked()).isFalse();
    });
  }

  /**
   * Tests that when a <code>CacheLoader</code> modifies the callback argument in place, the change
   * is visible to the <code>CacheWriter</code> even if it is in another VM.
   */
  @Test
  public void testCacheLoaderModifyingArgument() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";
    final Object one = "ONE";
    final Object two = "TWO";

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    vm0.invoke("Set CacheLoader", () -> {
      final Region<Object, Object> region1 = getRootRegion().getSubregion(name);
      setLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {

          Object[] array1 = (Object[]) helper.getArgument();
          assertThat(array1[0]).isEqualTo(one);
          array1[0] = two;
          return value;
        }
      });
      region1.getAttributesMutator().setCacheLoader(loader());
      flushIfNecessary(region1);
    });

    // if this is a partitioned region, we need the loader in both vms
    vm1.invoke("Conditionally create second loader", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      if (region.getAttributes().getPartitionAttributes() != null) {
        setLoader(new TestCacheLoader<Object, Object>() {
          @Override
          public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {

            Object[] array = (Object[]) helper.getArgument();
            assertThat(array[0]).isEqualTo(one);
            array[0] = two;
            return value;
          }
        });
        region.getAttributesMutator().setCacheLoader(loader());
      }
    });

    vm1.invoke("Set CacheWriter", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      setWriter(new TestCacheWriter<Object, Object>() {
        @Override
        public void beforeCreate2(EntryEvent<Object, Object> event) throws CacheWriterException {

          Object[] array = (Object[]) event.getCallbackArgument();
          assertThat(array[0]).isEqualTo(two);
        }
      });
      region.getAttributesMutator().setCacheWriter(writer());
      flushIfNecessary(region);
    });

    vm0.invoke("Create entry", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Object[] array = {one};
      Object result = region.get(key, array);
      assertThat(loader().wasInvoked()).isTrue();
      assertThat(result).isEqualTo(value);
    });

    vm1.invoke("Validate callback", () -> {
      assertThat(writer().wasInvoked()).isTrue();
    });
  }

  /**
   * Tests that invoking <code>netSearch</code> in a remote loader returns <code>null</code> instead
   * of causing infinite recursion.
   */
  @Test
  public void testRemoteLoaderNetSearch() {
    assumeThat(supportsNetLoad()).isTrue();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Create Region", () -> {
      Region<Object, Object> region = createRegion(name);
      region.getAttributesMutator().setCacheLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {

          try {
            assertThat(helper.netSearch(true)).isNull();

          } catch (TimeoutException ex) {
            fail("Why did I time out?", ex);
          }
          return value;
        }
      });
    });

    vm1.invoke("Get value", () -> {
      Region<Object, Object> region = createRegion(name);
      assertThat(region.get(key)).isEqualTo(value);
    });
  }

  /**
   * Tests that a local loader is preferred to a remote one
   */
  @Test
  public void testLocalCacheLoader() {
    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Create \"local\" region", () -> {
      Region<Object, Object> region = createRegion(name);
      region.getAttributesMutator().setCacheLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
          return value;
        }
      });
    });

    vm1.invoke("Create \"remote\" region", () -> {
      Region<Object, Object> region1 = createRegion(name);
      setLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
          if (helper.getRegion().getAttributes().getPartitionAttributes() == null) {
            fail("Should not be invoked");
            return null;
          } else {
            return value;
          }
        }
      });
      region1.getAttributesMutator().setCacheLoader(loader());
    });

    vm0.invoke("Get", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get(key)).isEqualTo(value);
    });
    vm1.invoke("Verify loader not invoked", () -> {
      assertThat(loader().wasInvoked()).isFalse();
    });
  }

  /**
   * Tests that an entry update is propagated to other caches that have that same entry defined.
   */
  @Test
  public void testDistributedPut() {
    final String rgnName = getUniqueName();

    SerializableRunnable newKey = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          if (!getRegionAttributes().getDataPolicy().withReplication()
              && getRegionAttributes().getPartitionAttributes() == null) {
            Region<?, ?> root = getRootRegion("root");
            Region<Object, Object> rgn = root.getSubregion(rgnName);
            rgn.create("key", null);
            getSystem().getLogWriter().info("testDistributedPut: Created Key");
          }
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };

    vm0.invoke("testDistributedPut: Create Region", () -> {
      createRegion(rgnName);
    });
    vm0.invoke("testDistributedPut: Create Key", newKey);
    int vmCount = VM.getVMCount();

    for (int i = 1; i < vmCount; i++) {
      VM vm = VM.getVM(i);
      vm.invoke("testDistributedPut: Create Region", () -> {
        createRegion(rgnName);
      });
      if (!getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm.invoke("testDistributedPut: Create Key", newKey);
      }
    }

    try {
      Region<String, String> rgn = createRegion(rgnName);

      rgn.put("key", "value");
      getSystem().getLogWriter().info("testDistributedPut: Put Value");

      invokeInEveryVM("testDistributedPut: Verify Received Value", repeatingIfNecessary(() -> {
        Region<String, String> rgn1 = getRootRegion().getSubregion(rgnName);
        String description = "Could not find entry for 'key'";
        Object actual = rgn1.getEntry("key");
        assertThat(actual).describedAs(description).isNotNull();
        assertThat(rgn1.getEntry("key").getValue()).isEqualTo("value");
      }));

    } catch (Exception e) {
      getCache().close();
      getSystem().getLogWriter().fine("testDistributedPut: Caused exception in createRegion");
      throw e;
    }

  }


  /**
   * Indicate whether replication/GII supported
   *
   * @return true if replication is supported
   */
  protected boolean supportsReplication() {
    return true;
  }

  /**
   * Tests that keys and values are pushed with {@link DataPolicy#REPLICATE}.
   */
  @Test
  public void testReplicate() {
    assumeThat(supportsReplication()).isTrue();

    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";

    Object[] v = new Object[3000];
    Arrays.fill(v, 0xCAFE);

    final Object value2 = Arrays.asList(v);
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        RegionFactory<?, ?> regionFactory = getBasicRegionFactory();
        createRegion(name, regionFactory);
      }
    };

    vm0.invoke("Create Mirrored Region", create);
    vm2.invoke("Create Mirrored Region", create);

    vm0.invoke("Put data", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key1, value1);
      region.put(key2, value2);
      region.put(key3, value3);
      flushIfNecessary(region);
    });

    vm2.invoke("Wait for update", repeatingIfNecessary(new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        assertThat(region.getEntry(key1)).isNotNull();
        assertThat(region.getEntry(key2)).isNotNull();
        assertThat(region.getEntry(key3)).isNotNull();
      }
    }));

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm0.invoke("Remove local entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
      flushIfNecessary(region);
    });

    vm2.invoke("Verify keys", repeatingIfNecessary(new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);

        // small
        Region.Entry<Object, Object> entry1 = region.getEntry(key1);
        assertThat(entry1).isNotNull();
        assertThat(entry1.getValue()).isEqualTo(value1);

        // large
        Region.Entry<Object, Object> entry2 = region.getEntry(key2);
        assertThat(entry2).isNotNull();
        assertThat(entry2.getValue()).isEqualTo(value2);

        // small
        Region.Entry<Object, Object> entry3 = region.getEntry(key3);
        assertThat(entry3).isNotNull();
        assertThat(entry3.getValue()).isEqualTo(value3);
      }
    }));
  }

  /**
   * Tests that a newly-created mirrored region contains all of the entries of another region.
   */
  @Test
  public void testGetInitialImage() {
    assumeThat(supportsReplication()).isTrue();

    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        RegionAttributes<?, ?> ra = getRegionAttributes();
        RegionFactory<?, ?> factory = getCache().createRegionFactory(ra);
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          factory.setDiskStoreName(null);
        }
        factory.setDataPolicy(DataPolicy.REPLICATE);
        createRegion(name, factory);
      }
    };

    vm0.invoke("Create Mirrored Region", create);

    vm0.invoke("Put data", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key1, value1);
      region.put(key2, value2);
      region.put(key3, value3);
    });

    vm2.invoke("Create Mirrored Region", create);

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm0.invoke("Remove local entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });

    vm2.invoke("Verify keys/values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);

      Region.Entry entry1 = region.getEntry(key1);
      assertThat(entry1).isNotNull();
      assertThat(entry1.getValue()).isEqualTo(value1);

      Region.Entry entry2 = region.getEntry(key2);
      assertThat(entry2).isNotNull();
      assertThat(entry2.getValue()).isEqualTo(value2);

      Region.Entry entry3 = region.getEntry(key3);
      assertThat(entry3).isNotNull();
      assertThat(entry3.getValue()).isEqualTo(value3);
    });
  }

  private static final int CHUNK_SIZE = 500 * 1024; // == InitialImageOperation.CHUNK_SIZE_IN_BYTES
  private static final int NUM_ENTRIES = 100;
  private static final int VALUE_SIZE = CHUNK_SIZE * 10 / NUM_ENTRIES;

  /**
   * Tests that a newly-created mirrored region contains all of the entries of another region, with
   * a large quantity of data.
   */
  @Test
  public void testLargeGetInitialImage() {
    assumeThat(supportsReplication()).isTrue();

    final String name = this.getUniqueName();
    final Integer[] keys = new Integer[NUM_ENTRIES];
    final byte[][] values = new byte[NUM_ENTRIES][];

    for (int i = 0; i < NUM_ENTRIES; i++) {
      keys[i] = i;
      values[i] = new byte[VALUE_SIZE];
      Arrays.fill(values[i], (byte) 0x42);
    }

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        RegionAttributes<?, ?> ra = getRegionAttributes();
        RegionFactory<?, ?> factory = getCache().createRegionFactory(ra);
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          factory.setDiskStoreName(null);
        }
        factory.setDataPolicy(DataPolicy.REPLICATE);
        createRegion(name, factory);
      }
    };

    vm0.invoke("Create Mirrored Region", create);

    vm0.invoke("Put data", () -> {
      Region<Integer, byte[]> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < NUM_ENTRIES; i++) {
        region.put(keys[i], values[i]);
      }
    });

    vm2.invoke("Create Mirrored Region", create);

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm0.invoke("Remove local entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });

    vm2.invoke("Verify keys/values", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.entrySet(false).size()).isEqualTo(NUM_ENTRIES);
      for (int i = 0; i < NUM_ENTRIES; i++) {
        Region.Entry entry = region.getEntry(keys[i]);
        assertThat(entry).isNotNull();
        if (!(entry.getValue() instanceof byte[])) {
          fail("getValue returned a " + entry.getValue().getClass()
              + " instead of the expected byte[]");
        }
        assertThat(Arrays.equals(values[i], (byte[]) entry.getValue())).isTrue();
      }
    });
  }

  /**
   * Tests that a mirrored region gets data pushed to it from a non-mirrored region and the
   * afterCreate event is invoked on a listener.
   */
  @Test
  public void testMirroredDataFromNonMirrored() {
    assumeThat(supportsReplication()).isTrue();

    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    class MirroredDataFromNonMirroredListener extends TestCacheListener<Object, Object> {
      // use modifiable ArrayLists
      private final List<Object> expectedKeys = new ArrayList<>(Arrays.asList(key1, key2, key3));
      private final List<Object> expectedValues =
          new ArrayList<>(Arrays.asList(value1, value2, value3));

      @Override
      public synchronized void afterCreate2(EntryEvent<Object, Object> event) {
        int index = expectedKeys.indexOf(event.getKey());
        assertThat(index >= 0).isTrue();
        assertThat(event.getNewValue()).isEqualTo(expectedValues.remove(index));
        expectedKeys.remove(index);
        logger.info("afterCreate called in "
            + "MirroredDataFromNonMirroredListener for key:" + event.getKey());
      }
    }

    vm0.invoke("Create Mirrored Region", () -> {
      RegionAttributes<Object, Object> ra = getRegionAttributes();
      RegionFactory<Object, Object> factory = getCache().createRegionFactory(ra);
      if (ra.getEvictionAttributes() == null
          || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
        factory.setDiskStoreName(null);
      }
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.addCacheListener(new MirroredDataFromNonMirroredListener());
      createRegion(name, factory);
    });

    vm2.invoke("Populate non-mirrored region", () -> {
      RegionAttributes<Object, Object> ra = getRegionAttributes();
      RegionFactory<Object, Object> regionFactory = getCache().createRegionFactory(ra);
      if (ra.getEvictionAttributes() == null
          || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
        regionFactory.setDiskStoreName(null);
      }
      regionFactory.setDataPolicy(DataPolicy.NORMAL);

      Region<Object, Object> region1 = createRegion(name, regionFactory);
      region1.put(key1, value1);
      region1.put(key2, value2);
      region1.put(key3, value3);
      flushIfNecessary(region1);
    });

    // Destroy the local entries so we know that they are not found by
    // a netSearch
    vm2.invoke("Remove local entries", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroy(key1);
      region.localDestroy(key2);
      region.localDestroy(key3);
      flushIfNecessary(region);
    });

    vm0.invoke("Verify keys/values and listener", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);

      Region.Entry entry1 = region.getEntry(key1);
      assertThat(entry1).isNotNull();
      assertThat(entry1.getValue()).isEqualTo(value1);

      Region.Entry entry2 = region.getEntry(key2);
      assertThat(entry2).isNotNull();
      assertThat(entry2.getValue()).isEqualTo(value2);

      Region.Entry entry3 = region.getEntry(key3);
      assertThat(entry3).isNotNull();
      assertThat(entry3.getValue()).isEqualTo(value3);

      MirroredDataFromNonMirroredListener mirroredDataFromNonMirroredListener =
          (MirroredDataFromNonMirroredListener) region.getAttributes().getCacheListeners()[0];

      assertThat(mirroredDataFromNonMirroredListener.wasInvoked()).isTrue();
      assertThat(mirroredDataFromNonMirroredListener.expectedKeys.isEmpty()).describedAs(
          "expectedKeys should be empty, but was: "
              + mirroredDataFromNonMirroredListener.expectedKeys)
          .isTrue();
    });
  }

  private <K, V> RegionFactory<K, V> getBasicRegionFactory(CacheLoader<K, V> cacheLoader) {
    RegionFactory<K, V> regionFactory = getBasicRegionFactory();
    regionFactory.setCacheLoader(cacheLoader);
    return regionFactory;
  }

  private <K, V> RegionFactory<K, V> getBasicRegionFactory() {
    RegionAttributes<K, V> ra = getRegionAttributes();
    RegionFactory<K, V> regionFactory = getCache().createRegionFactory(ra);
    if (ra.getEvictionAttributes() == null
        || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
      regionFactory.setDiskStoreName(null);
    }
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    return regionFactory;
  }

  /**
   * Tests that a mirrored region does not push data to a non-mirrored region.
   */
  @Test
  public void testNoMirroredDataToNonMirrored() {
    assumeThat(supportsReplication()).isTrue();

    final String name = this.getUniqueName();
    final Object key1 = "KEY1";
    final Object value1 = "VALUE1";
    final Object key2 = "KEY2";
    final Object value2 = "VALUE2";
    final Object key3 = "KEY3";
    final Object value3 = "VALUE3";

    vm0.invoke("Create Non-mirrored Region", () -> {
      createRegion(name, getRegionAttributes());
    });

    vm2.invoke("Populate mirrored region", () -> {
      RegionFactory<Object, Object> regionFactory = getBasicRegionFactory();
      Region<Object, Object> region1 = createRegion(name, regionFactory);
      region1.put(key1, value1);
      region1.put(key2, value2);
      region1.put(key3, value3);
      flushIfNecessary(region1);
    });

    // Make sure that data wasn't pushed
    vm0.invoke("Verify keys/values", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      Region.Entry entry1 = region.getEntry(key1);
      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        if (entry1 != null) {
          logger.info("found entry " + entry1);
        }
        assertThat(entry1).isNull();
      } else {
        assertThat(entry1).isNotNull();
      }

      Region.Entry entry2 = region.getEntry(key2);
      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        assertThat(entry2).isNull();
      } else {
        assertThat(entry2).isNotNull();
      }

      Region.Entry entry3 = region.getEntry(key3);
      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        assertThat(entry3).isNull();
      } else {
        assertThat(entry3).isNotNull();
      }
    });
  }

  /**
   * Tests that a local load occurs, even with mirroring
   */
  @Test
  public void testMirroredLocalLoad() {
    assumeThat(supportsReplication()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Create region with loader", () -> {
      RegionFactory<Object, Object> regionFactory =
          getBasicRegionFactory(new TestCacheLoader<Object, Object>() {
            @Override
            public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
              return value;
            }
          });
      createRegion(name, regionFactory);
    });

    vm2.invoke("Create region with bad loader", () -> {
      setLoader(new TestCacheLoader<Object, Object>() {
        @Override
        public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {

          fail("Should not be invoked");
          return null;
        }
      });
      RegionFactory<Object, Object> regionFactory = getBasicRegionFactory(loader());
      createRegion(name, regionFactory);
    });

    vm0.invoke("Get", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get(key)).isEqualTo(value);
    });

    vm2.invoke("Verify no load", () -> {
      assertThat(loader().wasInvoked()).isFalse();
    });
  }

  /**
   * Tests sure that a <code>netLoad</code> occurs, even with mirroring
   */
  @Test
  public void testMirroredNetLoad() {
    assumeThat(supportsReplication()).isTrue();

    final String name = this.getUniqueName();
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Create region with bad loader", () -> {
      RegionFactory<Object, Object> regionFactory = getBasicRegionFactory();
      createRegion(name, regionFactory);
    });

    vm2.invoke("Create region with loader", () -> {
      RegionFactory<Object, Object> regionFactory =
          getBasicRegionFactory(new TestCacheLoader<Object, Object>() {
            @Override
            public Object load2(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
              return value;
            }
          });
      createRegion(name, regionFactory);
    });

    vm0.invoke("Get", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      assertThat(region.get(key)).isEqualTo(value);
    });
  }

  /**
   * Tests that a region is not kept alive
   */
  @Test
  public void testNoRegionKeepAlive() {
    final String name = this.getUniqueName();
    final Object key = "KEEP_ALIVE_KEY";
    final Object value = "VALUE";

    vm0.invoke("Create region", () -> {
      createRegion(name);
    });

    vm0.invoke("Populate region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, value);
      assertThat(region.get(key)).isEqualTo(value);
    });
    vm0.invoke("Close cache", JUnit4CacheTestCase::closeCache);

    vm0.invoke("Re-create cache", () -> {
      Region<Object, Object> region = createRegion(name);
      // if this is a backup region, then it will find the data
      // otherwise it should not
      if (region.getAttributes().getDataPolicy().withPersistence()) {
        assertThat(region.get(key)).isEqualTo(value);
      } else {
        assertThat(region.get(key)).isNull();
      }
    });
  }

  @Test
  public void testNetSearchObservesTtl() {
    assumeThat(getRegionAttributes().getPartitionAttributes())
        .withFailMessage("the region has partition attributes").isNull();

    final String name = this.getUniqueName();
    final int shortTimeout = 10; // ms
    final int longTimeout = 1000000; // ms
    final Object key = "KEY";
    final Object value = "VALUE";

    vm1.invoke("Create with TTL", () -> {
      RegionFactory<Object, Object> factory1 =
          getCache().createRegionFactory(getRegionAttributes());
      factory1.setStatisticsEnabled(true);
      ExpirationAttributes expire1 =
          new ExpirationAttributes(longTimeout, ExpirationAction.DESTROY);
      factory1.setEntryTimeToLive(expire1);
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        Region<Object, Object> region1 = createRegion(name, factory1);
        region1.create(key, value);
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    });

    // vm0 - Create region, short timeout
    vm0.invoke("Create with TTL", () -> {
      RegionAttributes<?, ?> ra = getRegionAttributes();
      AttributesFactory<?, ?> factory = new AttributesFactory<>(ra);
      final boolean partitioned =
          ra.getPartitionAttributes() != null || ra.getDataPolicy().withPartitioning();
      // MUST be non-mirrored, so turn off persistBackup if this is a disk region test
      if (!partitioned) {
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          factory.setDiskStoreName(null);
        }
        factory.setDataPolicy(DataPolicy.NORMAL);
      }
      factory.setStatisticsEnabled(true);
      ExpirationAttributes expire =
          new ExpirationAttributes(shortTimeout, ExpirationAction.DESTROY);
      factory.setEntryTimeToLive(expire);
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        createRegion(name, factory.create());
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    });

    // Even though netSearch finds vm1's entry is not expired, it is considered
    // expired with respect to vm0's attributes
    vm0.invoke("get(key), expect null", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      await().pollDelay(5, SECONDS).untilAsserted(() -> assertThat(region.get(key)).isNull());
    });

    // We see the object is actually still there
    vm1.invoke("get(key), expect value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Object got = region.get(key);
      assertThat(got).isEqualTo(value);
    });
  }

  @Test
  public void testNetSearchObservesIdleTime() {
    assumeThat(getRegionAttributes().getPartitionAttributes())
        .withFailMessage("the region has partition attributes").isNull();

    final String name = this.getUniqueName();
    final int shortTimeout = 10; // ms
    final int longTimeout = 10000; // ms
    final Object key = "KEY";
    final Object value = "VALUE";

    // if using shared memory, make sure we use two VMs on different
    // gemfire systems

    vm1.invoke("Create with IdleTimeout", () -> {
      RegionFactory<Object, Object> factory1 =
          getCache().createRegionFactory(getRegionAttributes());
      factory1.setStatisticsEnabled(true);
      ExpirationAttributes expire1 =
          new ExpirationAttributes(longTimeout, ExpirationAction.DESTROY);
      factory1.setEntryIdleTimeout(expire1);
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        Region<Object, Object> region1 = createRegion(name, factory1);
        region1.create(key, value);
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    });

    // vm0 - Create region, short timeout
    vm0.invoke("Create with IdleTimeout", () -> {
      RegionAttributes<?, ?> ra = getRegionAttributes();
      AttributesFactory<?, ?> factory = new AttributesFactory<>(ra);
      final boolean partitioned =
          ra.getPartitionAttributes() != null || ra.getDataPolicy().withPartitioning();
      // MUST be nonmirrored, so turn off persistBackup if this is a disk region test
      if (!partitioned) {
        if (ra.getEvictionAttributes() == null
            || !ra.getEvictionAttributes().getAction().isOverflowToDisk()) {
          factory.setDiskStoreName(null);
        }
        factory.setDataPolicy(DataPolicy.NORMAL);
      }
      factory.setStatisticsEnabled(true);
      ExpirationAttributes expire =
          new ExpirationAttributes(shortTimeout, ExpirationAction.DESTROY);
      factory.setEntryIdleTimeout(expire);
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        createRegion(name, factory.create());
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    });

    // Even though netSearch finds vm1's entry is not expired, it is considered
    // expired with respect to vm0's attributes
    vm0.invoke("get(key), expect null", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      await().pollDelay(1, SECONDS).untilAsserted(() -> assertThat(region.get(key)).isNull());
    });

    // We see the object is actually still there
    vm1.invoke("get(key), expect value", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      await().untilAsserted(() -> assertThat(region.get(key)).isEqualTo(value));
    });
  }

  private static TestCacheListener<Object, Object> destroyListener = null;

  /**
   * Tests that an entry in a distributed region that expires with a distributed destroy causes an
   * event in other VM with isExpiration flag set.
   */
  @Test
  public void testEntryTtlDestroyEvent() {
    assumeThat(getRegionAttributes().getPartitionAttributes())
        .withFailMessage("the region has partition attributes").isNull();

    final String name = this.getUniqueName();
    final int timeout = 22; // ms
    final Object key = "KEY";
    final Object value = "VALUE";

    class DestroyListener extends TestCacheListener<Object, Object> {
      private boolean eventIsExpiration = false;

      @Override
      public void afterDestroyBeforeAddEvent(EntryEvent<Object, Object> event) {
        eventIsExpiration = event.getOperation().isExpiration();
      }

      @Override
      public void afterDestroy2(EntryEvent<Object, Object> event) {
        if (event.isOriginRemote()) {
          assertThat(event.getDistributedMember().equals(getSystem().getDistributedMember()))
              .isFalse();
        } else {
          assertThat(event.getDistributedMember()).isEqualTo(
              getSystem().getDistributedMember());
        }
        assertThat(event.getOperation()).isEqualTo(Operation.EXPIRE_DESTROY);
        assertThat(event.getOldValue()).isEqualTo(value);
        eventIsExpiration = event.getOperation().isExpiration();
      }

      @Override
      public void afterCreate2(EntryEvent<Object, Object> ignored) {}

      @Override
      public void afterUpdate2(EntryEvent<Object, Object> ignored) {}
    }

    vm1.invoke("Create with Listener", () -> {
      RegionFactory<Object, Object> factory1 =
          getCache().createRegionFactory(getRegionAttributes());
      factory1.addCacheListener(destroyListener = new DestroyListener());
      createRegion(name, factory1);
    });

    vm0.invoke("Create with TTL", () -> {
      AttributesFactory<?, ?> factory = new AttributesFactory<>(getRegionAttributes());
      factory.setStatisticsEnabled(true);
      ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
      factory.setEntryTimeToLive(expire);
      if (!getRegionAttributes().getDataPolicy().withReplication()) {
        factory.setDataPolicy(DataPolicy.NORMAL);
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      }
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        createRegion(name, factory.create());
        ExpiryTask.suspendExpiration();
        // suspend to make sure we can see that the put is distributed to this member
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    });

    try {

      // let region create finish before doing put
      vm1.invoke(() -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        DestroyListener dl = (DestroyListener) region.getAttributes().getCacheListeners()[0];
        dl.enableEventHistory();
        region.put(key, value);
        // reset listener after create event
        assertThat(dl.wasInvoked()).isTrue();
        List<CacheEvent<Object, Object>> history = dl.getEventHistory();
        CacheEvent<Object, Object> ce = history.get(0);
        dl.disableEventHistory();
        assertThat(ce.getOperation()).isEqualTo(Operation.CREATE);
      });
      vm0.invoke("Check create received from vm1", () -> {
        final Region<Object, Object> region = getRootRegion().getSubregion(name);
        await("never saw create of " + key)
            .until(() -> region.getEntry(key) != null);
      });

    } finally {
      vm0.invoke("resume expiration", ExpiryTask::permitExpiration);
    }

    // now wait for it to expire
    vm0.invoke("Check local destroy", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      await("never saw expire of " + key)
          .until(() -> region.getEntry(key) == null);
    });

    vm1.invoke("Verify destroyed and event", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      await("never saw expire of " + key)
          .until(() -> region.getEntry(key) == null);

      assertThat(destroyListener.waitForInvocation(555)).isTrue();
      assertThat(((DestroyListener) destroyListener).eventIsExpiration).isTrue();
    });
  }

  /**
   * Tests that an entry in a distributed region expires with a local destroy after a given time to
   * live.
   */
  @Test
  public void testEntryTtlLocalDestroy() throws Exception {
    assumeThat(getRegionAttributes().getPartitionAttributes())
        .withFailMessage("the region has partition attributes").isNull();

    final boolean mirrored = getRegionAttributes().getDataPolicy().withReplication();
    final boolean partitioned = getRegionAttributes().getPartitionAttributes() != null
        || getRegionAttributes().getDataPolicy().withPartitioning();
    if (!mirrored) {
      // This test fails intermittently because the DSClock we inherit from the existing
      // distributed system is stuck in the "stopped" state.
      // The DSClock is going away when java groups is merged and at that
      // time this following can be removed.
      disconnectAllFromDS();
    }

    final String name = this.getUniqueName();
    final int timeout = 10; // ms
    final String key = "KEY";
    final String value = "VALUE";

    vm1.invoke("Populate", () -> {
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        createRegion(name);
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    });

    vm0.invoke("Create with TTL", () -> {
      RegionFactory<Object, Object> factory =
          getCache().createRegionFactory(getRegionAttributes());
      factory.setStatisticsEnabled(true);
      ExpirationAttributes expire =
          new ExpirationAttributes(timeout, ExpirationAction.LOCAL_DESTROY);
      factory.setEntryTimeToLive(expire);
      if (!mirrored) {
        // make it cached all events so that remote creates will also
        // be created here
        if (!partitioned) {
          factory.setDataPolicy(DataPolicy.NORMAL);
        }
        factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
        factory.addCacheListener(new CountingDistCacheListener<>());
      }
      // Crank up the expiration so test runs faster. This property only needs to be set while the
      // region is created
      System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
      try {
        createRegion(name, factory);
        if (mirrored) {
          fail("Should have thrown an IllegalStateException");
        }
      } catch (IllegalStateException e) {
        if (!mirrored) {
          throw e;
        }
      } finally {
        System.getProperties().remove(LocalRegion.EXPIRY_MS_PROPERTY);
      }
    });
    if (mirrored) {
      return;
    }

    vm1.invoke(() -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, value);
    });

    vm0.invoke("Check local destroy", () -> {
      final Region<Object, Object> region = getRootRegion().getSubregion(name);
      // make sure we created the entry
      CountingDistCacheListener<Object, Object> l =
          (CountingDistCacheListener<Object, Object>) region.getAttributes()
              .getCacheListeners()[0];
      await()
          .untilAsserted(() -> l.assertCount(1, 0, 0, 0));

      // now make sure it expires
      // this should happen really fast since timeout is 10 ms.
      // But it may take longer in some cases because of thread
      // scheduling delays and machine load (see GEODE-410).
      // The previous code would fail after 100ms; now we wait 3000ms.

      try {
        await()
            .until(() -> {
              Region.Entry re = region.getEntry(key);
              if (re != null) {
                EntryExpiryTask eet = getEntryExpiryTask(region);
                if (eet != null) {
                  long stopTime =
                      getCache().getInternalDistributedSystem().getClock().getStopTime();
                  logger.info("DEBUG: waiting for expire destroy expirationTime= "
                      + eet.getExpirationTime() + " now=" + eet.calculateNow() + " stopTime="
                      + stopTime + " currentTimeMillis=" + System.currentTimeMillis());
                } else {
                  logger.info("DEBUG: waiting for expire destroy but expiry task is null");
                }
              }
              return re == null;
            });
      } catch (ConditionTimeoutException timeoutEx) {
        // Catching in order to inform about the final state
        fail(determineExpiryFailureMessage(region), timeoutEx);
      }

      assertThat(region.getEntry(key)).isNull();
    });

    vm1.invoke("Verify local", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      Region.Entry entry = region.getEntry(key);
      assertThat(entry.getValue()).isEqualTo(value);
    });
  }

  private String determineExpiryFailureMessage(Region region) {
    String expiryInfo = "";
    try {
      EntryExpiryTask eet = getEntryExpiryTask(region);
      if (eet != null) {
        expiryInfo = "expirationTime= " + eet.getExpirationTime()
            + " now=" + eet.calculateNow()
            + " currentTimeMillis=" + System.currentTimeMillis();
      }
    } catch (EntryNotFoundException ex) {
      expiryInfo = "EntryNotFoundException when getting expiry task";
    }
    return "Entry for key " + "KEY" + " never expired (since it still exists) "
        + expiryInfo;
  }

  private static EntryExpiryTask getEntryExpiryTask(Region r) {
    EntryExpiryTask result = null;
    try {
      LocalRegion lr = (LocalRegion) r;
      result = lr.getEntryExpiryTask("KEY");
    } catch (EntryNotFoundException ignore) {
    }
    return result;
  }

  /**
   * Tests to makes sure that a distributed update resets the expiration timer.
   */
  @Test
  public void testUpdateResetsIdleTime() {
    final String name = this.getUniqueName();
    // test no longer waits for this timeout to expire
    final int timeout = 90; // seconds
    final Object key = "KEY";
    final Object value = "VALUE";

    vm0.invoke("Create with Idle", () -> {
      RegionFactory<Object, Object> regionFactory =
          getCache().createRegionFactory(getRegionAttributes());
      regionFactory.setStatisticsEnabled(true);
      ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
      regionFactory.setEntryIdleTimeout(expire);
      LocalRegion region = (LocalRegion) createRegion(name, regionFactory);
      if (region.getDataPolicy().withPartitioning()) {
        // Force all buckets to be created locally so the
        // test will know that the create happens in this vm
        // and the update (in vm1) is remote.
        PartitionRegionHelper.assignBucketsToPartitions(region);
      }
      region.create(key, null);
      EntryExpiryTask eet = region.getEntryExpiryTask(key);
      region.create("createExpiryTime", eet.getExpirationTime());
      Wait.waitForExpiryClockToChange(region);
    });

    vm1.invoke("Create Region " + name, () -> {
      AttributesFactory<?, ?> regionFactory = new AttributesFactory<>(getRegionAttributes());
      regionFactory.setStatisticsEnabled(true);
      ExpirationAttributes expire = new ExpirationAttributes(timeout, ExpirationAction.DESTROY);
      regionFactory.setEntryIdleTimeout(expire);
      if (getRegionAttributes().getPartitionAttributes() != null) {
        createRegion(name, regionFactory.create());
      } else {
        createRegion(name);
      }
    });

    vm1.invoke("Update entry", () -> {
      final Region<Object, Object> r = getRootRegion().getSubregion(name);
      assertThat(r).isNotNull();
      r.put(key, value);
    });

    vm0.invoke("Verify reset", () -> {
      final LocalRegion region = (LocalRegion) getRootRegion().getSubregion(name);

      // wait for update to reach us from vm1 (needed if no-ack)
      await("never saw key " + key + "equal to value " + value)
          .until(() -> value.equals(region.get(key)));

      EntryExpiryTask eet = region.getEntryExpiryTask(key);
      long createExpiryTime = (Long) region.get("createExpiryTime");
      long updateExpiryTime = eet.getExpirationTime();
      if (updateExpiryTime - createExpiryTime <= 0L) {
        fail("update did not reset the expiration time. createExpiryTime=" + createExpiryTime
            + " updateExpiryTime=" + updateExpiryTime);
      }
    });
  }

  private static final int NB1_CHUNK_SIZE = 500 * 1024; // ==
  private static final int NB1_NUM_ENTRIES = 1000;
  private static final int NB1_VALUE_SIZE = NB1_CHUNK_SIZE * 10 / NB1_NUM_ENTRIES;

  /**
   * Tests that distributed ack operations do not block while another cache is doing a
   * getInitialImage.
   */
  @Test
  public void testNonblockingGetInitialImage() throws InterruptedException {
    assumeThat(supportsReplication()).isTrue();
    // don't run this test if global scope since its too difficult to predict
    // how many concurrent operations will occur
    assumeThat(getRegionAttributes().getScope().isGlobal()).isFalse();

    final String name = this.getUniqueName();
    final byte[][] values = new byte[NB1_NUM_ENTRIES][];

    for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
      values[i] = new byte[NB1_VALUE_SIZE];
      Arrays.fill(values[i], (byte) 0x42);
    }

    vm0.invoke("Create Nonmirrored Region", () -> {
      { // root region must be DACK because its used to sync up async subregions
        AttributesFactory<?, ?> factory = new AttributesFactory<>();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setDataPolicy(DataPolicy.EMPTY);
        createRootRegion(factory.create());
      }
      {
        AttributesFactory<?, ?> factory = new AttributesFactory<>(getRegionAttributes());
        createRegion(name, factory.create());
      }
      // reset slow
      InitialImageOperation.slowImageProcessing = 0;
    });

    vm0.invoke("Put initial data", () -> {
      Region<Integer, byte[]> region = getRootRegion().getSubregion(name);
      for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
        region.put(i, values[i]);
      }
      assertThat(region.keySet().size()).isEqualTo(NB1_NUM_ENTRIES);
    });

    // start asynchronous process that does updates to the data
    AsyncInvocation async =
        vm0.invokeAsync("Do Nonblocking Operations", () -> {
          Region<Object, Object> region = getRootRegion().getSubregion(name);

          // wait for profile of getInitialImage cache to show up
          final org.apache.geode.internal.cache.CacheDistributionAdvisor adv =
              ((org.apache.geode.internal.cache.DistributedRegion) region)
                  .getCacheDistributionAdvisor();
          final int expectedProfiles = 1;

          await("replicate count never reached " + expectedProfiles)
              .until(() -> {
                DataPolicy currentPolicy = getRegionAttributes().getDataPolicy();
                if (currentPolicy == DataPolicy.PRELOADED) {
                  return (adv.advisePreloadeds().size()
                      + adv.adviseReplicates().size()) >= expectedProfiles;
                } else {
                  return adv.adviseReplicates().size() >= expectedProfiles;
                }
              });

          DataPolicy currentPolicy = getRegionAttributes().getDataPolicy();
          int numProfiles;
          if (currentPolicy == DataPolicy.PRELOADED) {
            numProfiles = adv.advisePreloadeds().size() + adv.adviseReplicates().size();
          } else {
            numProfiles = adv.adviseReplicates().size();
          }
          assertThat(numProfiles >= expectedProfiles).isTrue();

          // operate on every odd entry with different value, alternating between
          // updates, invalidates, and destroys. These operations are likely
          // to be nonblocking if a sufficient number of updates get through
          // before the get initial image is complete.
          for (int i = 1; i < NB1_NUM_ENTRIES; i += 2) {
            Object key = i;
            logger.info("Operation #" + i + " on key " + key);
            switch (i % 6) {
              case 1: // UPDATE
                // use the current timestamp so we know when it happened
                // we could have used last modification timestamps, but
                // this works without enabling statistics
                Object value = System.currentTimeMillis();
                region.put(key, value);
                // no longer safe since get is not allowed to member doing GII
                // if (getRegionAttributes().getScope().isDistributedAck()) {
                // // do a nonblocking netSearch
                // region.localInvalidate(key);
                // assertIndexDetailsEquals(value, region.get(key));
                // }
                break;
              case 3: // INVALIDATE
                region.invalidate(key);
                if (getRegionAttributes().getScope().isDistributedAck()) {
                  // do a nonblocking netSearch
                  assertThat(region.get(key)).isNull();
                }
                break;
              case 5: // DESTROY
                region.destroy(key);
                if (getRegionAttributes().getScope().isDistributedAck()) {
                  // do a nonblocking netSearch
                  assertThat(region.get(key)).isNull();
                }
                break;
              default:
                fail("unexpected modulus result: " + i);
                break;
            }
          }

          // add some new keys
          for (int i = NB1_NUM_ENTRIES; i < NB1_NUM_ENTRIES + 200; i++) {
            region.create(i, System.currentTimeMillis());
          }
          // now do a put and our DACK root region which will not complete
          // until processed on otherside which means everything done before this
          // point has been processed
          getRootRegion().put("DONE", "FLUSH_OPS");
        });

    // in the meantime, do the get initial image in vm2
    // slow down image processing to make it more likely to get async updates
    if (!getRegionAttributes().getScope().isGlobal()) {
      vm2.invoke("Set slow image processing", () -> {
        // if this is a no_ack test, then we need to slow down more because of the
        // pauses in the nonblocking operations
        InitialImageOperation.slowImageProcessing = 200;
      });
    }

    SerializableRunnable create = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        beginCacheXml();
        { // root region must be DACK because its used to sync up async subregions
          AttributesFactory<?, ?> factory = new AttributesFactory<>();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.NORMAL);
          factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
          createRootRegion(factory.create());
        }
        {
          AttributesFactory<?, ?> factory = new AttributesFactory<>(getRegionAttributes());
          factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
          if (getRegionAttributes().getDataPolicy() == DataPolicy.NORMAL) {
            factory.setDataPolicy(DataPolicy.PRELOADED);
          }
          createRegion(name, factory.create());
        }
        finishCacheXml(name);
        // reset slow
        org.apache.geode.internal.cache.InitialImageOperation.slowImageProcessing = 0;
      }
    };

    AsyncInvocation asyncGII = vm2.invokeAsync("Create Mirrored Region", create);

    if (!getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      async.get();
      vm2.invoke("Set fast image processing", () -> {
        InitialImageOperation.slowImageProcessing = 0;
      });
    }

    // wait for GII to complete
    asyncGII.get();
    final long iiComplete = System.currentTimeMillis();

    if (getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      async.get();
    }

    // Locally destroy the region in vm0 so we know that they are not found by
    // a netSearch
    vm0.invoke("Locally destroy region", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.localDestroyRegion();
    });

    // invoke repeating so noack regions wait for all updates to get processed
    vm2.invoke("Verify entryCount", repeatingIfNecessary(5000, () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      // expected entry count (subtract entries destroyed)
      int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
      int actualCount = region.entrySet(false).size();

      assertThat(actualCount).isEqualTo(entryCount);
    }));

    vm2.invoke("Verify keys/values & Nonblocking", () -> {
      Region<Object, Object> region = getRootRegion().getSubregion(name);
      // expected entry count (subtract entries destroyed)
      int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
      assertThat(region.entrySet(false).size()).isEqualTo(entryCount);
      // determine how many entries were updated before getInitialImage
      // was complete
      int numConcurrent = 0;
      numConcurrent = verifyEntryUpdateCounts(region, numConcurrent, values, iiComplete);

      // make sure at least some of them were concurrent
      if (region.getAttributes().getScope().isGlobal()) {
        assertThat(numConcurrent < 10).describedAs(
            "Too many concurrent updates when expected to block: " + numConcurrent).isTrue();
      } else {
        int min = 30;
        assertThat(numConcurrent >= min).describedAs(
            "Not enough updates concurrent with getInitialImage occurred to my liking. "
                + numConcurrent + " entries out of " + entryCount
                + " were updated concurrently with getInitialImage, and I'd expect at least "
                + min
                + " or so")
            .isTrue();
      }
    });
  }

  /**
   * Tests that distributed ack operations do not block while another cache is doing a
   * getInitialImage.
   */
  @Test
  public void testTXNonblockingGetInitialImage() {
    assumeThat(supportsReplication()).isTrue();
    assumeThat(supportsTransactions()).isTrue();
    // don't run this test if global scope since its too difficult to predict
    // how many concurrent operations will occur
    assumeThat(getRegionAttributes().getScope().isGlobal()).isFalse();
    assumeThat(getRegionAttributes().getDataPolicy().withPersistence()).isFalse();

    final String name = this.getUniqueName();
    final byte[][] values = new byte[NB1_NUM_ENTRIES][];

    for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
      values[i] = new byte[NB1_VALUE_SIZE];
      Arrays.fill(values[i], (byte) 0x42);
    }

    SerializableRunnable create = new CacheSerializableRunnable("Create Mirrored Region") {
      @Override
      public void run2() throws CacheException {
        beginCacheXml();
        { // root region must be DACK because its used to sync up async subregions
          AttributesFactory<?, ?> factory = new AttributesFactory<>();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.NORMAL);
          factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
          createRootRegion(factory.create());
        }
        {
          AttributesFactory<?, ?> factory = new AttributesFactory<>(getRegionAttributes());
          if (getRegionAttributes().getDataPolicy() == DataPolicy.NORMAL) {
            factory.setDataPolicy(DataPolicy.PRELOADED);
          }
          factory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
          createRegion(name, factory.create());
        }
        finishCacheXml(name);
        // reset slow
        org.apache.geode.internal.cache.InitialImageOperation.slowImageProcessing = 0;
      }
    };

    vm0.invoke("Create Nonmirrored Region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        { // root region must be DACK because its used to sync up async subregions
          AttributesFactory<?, ?> factory = new AttributesFactory<>();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.EMPTY);
          createRootRegion(factory.create());
        }
        {
          AttributesFactory<?, ?> factory = new AttributesFactory<>(getRegionAttributes());
          createRegion(name, factory.create());
        }
        // reset slow
        org.apache.geode.internal.cache.InitialImageOperation.slowImageProcessing = 0;
      }
    });

    vm0.invoke("Put initial data", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        for (int i = 0; i < NB1_NUM_ENTRIES; i++) {
          region.put(i, values[i]);
        }
        assertThat(region.keySet().size()).isEqualTo(NB1_NUM_ENTRIES);
      }
    });

    // start asynchronous process that does updates to the data
    AsyncInvocation async =
        vm0.invokeAsync("Do Nonblocking Operations", new CacheSerializableRunnable() {
          @Override
          public void run2() throws CacheException {
            Region<Object, Object> region = getRootRegion().getSubregion(name);

            // wait for profile of getInitialImage cache to show up
            final org.apache.geode.internal.cache.CacheDistributionAdvisor adv =
                ((org.apache.geode.internal.cache.DistributedRegion) region)
                    .getCacheDistributionAdvisor();
            final int expectedProfiles = 1;

            await("replicate count never reached " + expectedProfiles)
                .until(() -> {
                  DataPolicy currentPolicy = getRegionAttributes().getDataPolicy();
                  if (currentPolicy == DataPolicy.PRELOADED) {
                    return (adv.advisePreloadeds().size()
                        + adv.adviseReplicates().size()) >= expectedProfiles;
                  } else {
                    return adv.adviseReplicates().size() >= expectedProfiles;
                  }
                });
            // operate on every odd entry with different value, alternating between
            // updates, invalidates, and destroys. These operations are likely
            // to be nonblocking if a sufficient number of updates get through
            // before the get initial image is complete.
            CacheTransactionManager txMgr = getCache().getCacheTransactionManager();
            for (int i = 1; i < NB1_NUM_ENTRIES; i += 2) {
              Object key = i;
              switch (i % 6) {
                case 1: // UPDATE
                  // use the current timestamp so we know when it happened
                  // we could have used last modification timestamps, but
                  // this works without enabling statistics
                  Object value = System.currentTimeMillis();
                  txMgr.begin();
                  region.put(key, value);
                  txMgr.commit();
                  // no longer safe since get is not allowed to member doing GII
                  // if (getRegionAttributes().getScope().isDistributedAck()) {
                  // // do a nonblocking netSearch
                  // region.localInvalidate(key);
                  // assertIndexDetailsEquals(value, region.get(key));
                  // }
                  break;
                case 3: // INVALIDATE
                  txMgr.begin();
                  region.invalidate(key);
                  txMgr.commit();
                  if (getRegionAttributes().getScope().isDistributedAck()) {
                    // do a nonblocking netSearch
                    assertThat(region.get(key)).isNull();
                  }
                  break;
                case 5: // DESTROY
                  txMgr.begin();
                  region.destroy(key);
                  txMgr.commit();
                  if (getRegionAttributes().getScope().isDistributedAck()) {
                    // do a nonblocking netSearch
                    assertThat(region.get(key)).isNull();
                  }
                  break;
                default:
                  fail("unexpected modulus result: " + i);
                  break;
              }
            }
            // add some new keys
            for (int i = NB1_NUM_ENTRIES; i < NB1_NUM_ENTRIES + 200; i++) {
              txMgr.begin();
              region.create(i, System.currentTimeMillis());
              txMgr.commit();
            }
            // now do a put and our DACK root region which will not complete
            // until processed on otherside which means everything done before this
            // point has been processed
            getRootRegion().put("DONE", "FLUSH_OPS");
          }
        });

    // in the meantime, do the get initial image in vm2
    // slow down image processing to make it more likely to get async updates
    if (!getRegionAttributes().getScope().isGlobal()) {
      vm2.invoke("Set slow image processing", new SerializableRunnable() {
        @Override
        public void run() {
          // if this is a no_ack test, then we need to slow down more because of the
          // pauses in the nonblocking operations
          org.apache.geode.internal.cache.InitialImageOperation.slowImageProcessing = 200;
        }
      });
    }

    AsyncInvocation asyncGII = vm2.invokeAsync(create);

    if (!getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);

      vm2.invoke("Set fast image processing", new SerializableRunnable() {
        @Override
        public void run() {
          org.apache.geode.internal.cache.InitialImageOperation.slowImageProcessing = 0;
        }
      });
      logger.info("after async nonblocking ops complete");
    }

    // wait for GII to complete
    ThreadUtils.join(asyncGII, 30 * 1000);
    final long iiComplete = System.currentTimeMillis();
    logger.info("Complete GetInitialImage at: " + System.currentTimeMillis());
    if (getRegionAttributes().getScope().isGlobal()) {
      // wait for nonblocking operations to complete
      ThreadUtils.join(async, 30 * 1000);
    }

    if (async.exceptionOccurred()) {
      fail("async failed", async.getException());
    }
    if (asyncGII.exceptionOccurred()) {
      fail("asyncGII failed", asyncGII.getException());
    }

    // Locally destroy the region in vm0 so we know that they are not found by
    // a netSearch
    vm0.invoke("Locally destroy region", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.localDestroyRegion();
      }
    });

    // invoke repeating so noack regions wait for all updates to get processed
    vm2.invoke(repeatingIfNecessary(5000,
        new CacheSerializableRunnable("Verify entryCount") {
          boolean entriesDumped = false;

          @Override
          public void run2() throws CacheException {
            Region<Object, Object> region = getRootRegion().getSubregion(name);
            // expected entry count (subtract entries destroyed)
            int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
            int actualCount = region.entrySet(false).size();
            if (actualCount == NB1_NUM_ENTRIES + 200) {
              // entries not destroyed, dump entries that were supposed to have been destroyed
              dumpDestroyedEntries(region);
            }
            assertThat(actualCount).isEqualTo(entryCount);
          }

          private void dumpDestroyedEntries(Region region) throws EntryNotFoundException {
            if (entriesDumped) {
              return;
            }
            entriesDumped = true;

            logger.info("DUMPING Entries with values in VM that should have been destroyed:");
            for (int i = 5; i < NB1_NUM_ENTRIES; i += 6) {
              logger.info(i + "-->" + ((org.apache.geode.internal.cache.LocalRegion) region)
                  .getValueInVM(i));
            }
          }
        }));

    vm2.invoke("Verify keys/values & Nonblocking", new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        // expected entry count (subtract entries destroyed)
        int entryCount = NB1_NUM_ENTRIES + 200 - NB1_NUM_ENTRIES / 6;
        assertThat(region.entrySet(false).size()).isEqualTo(entryCount);
        // determine how many entries were updated before getInitialImage
        // was complete
        int numConcurrent = 0;
        numConcurrent = verifyEntryUpdateCounts(region, numConcurrent, values, iiComplete);

        logger.info(name + ": " + numConcurrent
            + " entries out of " + entryCount + " were updated concurrently with getInitialImage");
        // make sure at least some of them were concurrent
        {
          int min = 30;
          String description =
              "Not enough updates concurrent with getInitialImage occurred to my liking. "
                  + numConcurrent + " entries out of " + entryCount
                  + " were updated concurrently with getInitialImage, and I'd expect at least "
                  + min
                  + " or so";
          assertThat(numConcurrent >= min).describedAs(description).isTrue();
        }
      }
    });
  }

  private int verifyEntryUpdateCounts(Region<Object, Object> region, int numConcurrent,
      byte[][] values, long iiComplete) {
    for (int i = 0; i < NB1_NUM_ENTRIES + 200; i++) {
      Region.Entry entry = region.getEntry(i);
      Object v = entry == null ? null : entry.getValue();
      if (i < NB1_NUM_ENTRIES) { // old keys
        switch (i % 6) {
          // even keys are originals
          case 0:
          case 2:
          case 4:
            assertThat(entry).isNotNull();
            assertThat(Arrays.equals(values[i], (byte[]) v)).isTrue();
            break;
          case 1: // updated
            assertThat(v).isNotNull();
            boolean condition = v instanceof Long;
            assertThat(condition).describedAs(
                "Value for key " + i + " is not a Long, is a " + v.getClass().getName()).isTrue();
            Long timestamp = (Long) entry.getValue();
            if (timestamp < iiComplete) {
              numConcurrent++;
            }
            break;
          case 3: // invalidated
            assertThat(entry).isNotNull();
            assertThat(v).describedAs("Expected value for " + i + " to be null, but was " + v)
                .isNull();
            break;
          case 5: // destroyed
            assertThat(entry).isNull();
            break;
          default:
            fail("unexpected modulus result: " + (i % 6));
            break;
        }
      } else { // new keys
        assertThat(v).isNotNull();
        boolean condition = v instanceof Long;
        assertThat(condition).describedAs(
            "Value for key " + i + " is not a Long, is a " + v.getClass().getName()).isTrue();
        Long timestamp = (Long) entry.getValue();
        if (timestamp < iiComplete) {
          numConcurrent++;
        }
      }
    }
    return numConcurrent;
  }

  /**
   * Tests what happens when one VM attempts to read an object for which it does not have a
   * registered <code>DataSerializer</code>.
   *
   * @since GemFire 3.5
   */
  @Test
  public void testNoDataSerializer() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    final Object key = "KEY";
    final Object key2 = "KEY2";
    final int intValue = 3452;
    final long longValue = 213421;
    // final boolean[] wasInvoked = new boolean[1];

    vm2.invoke("Disconnect from DS", JUnit4DistributedTestCase::disconnectFromDS);

    try {
      vm0.invoke("Put int", () -> {
        Class c = IntWrapper.IntWrapperSerializer.class;
        IntWrapper.IntWrapperSerializer serializer =
            (IntWrapper.IntWrapperSerializer) DataSerializer.register(c);

        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.put(key, new IntWrapper(intValue));

        flushIfNecessary(region);
        assertThat(serializer.wasInvoked).isTrue();
      });

      SerializableRunnable get = new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          // wait a while for the serializer to be registered
          // A race condition exists in the product in which
          // this thread can be stuck waiting in getSerializer
          // for 60 seconds. So it only calls getSerializer once
          // causing it to fail intermittently (see GEODE-376).
          // To workaround this the test wets WAIT_MS to 1 ms.
          // So the getSerializer will only block for 1 ms.
          // This allows the WaitCriterion to make multiple calls
          // of getSerializer and the subsequent calls will find
          // the DataSerializer.
          final int savVal = InternalDataSerializer.GetMarker.WAIT_MS;
          InternalDataSerializer.GetMarker.WAIT_MS = 1;
          try {
            await("DataSerializer with id 120 was never registered")
                .until(() -> InternalDataSerializer.getSerializer((byte) 120) != null);
          } finally {
            InternalDataSerializer.GetMarker.WAIT_MS = savVal;
          }
          IntWrapper value = (IntWrapper) region.get(key);
          assertThat(InternalDataSerializer.getSerializer((byte) 120)).isNotNull();
          assertThat(value).isNotNull();
          assertThat(value.intValue).isEqualTo(intValue);
        }
      };
      vm1.invoke("Get int", get);

      // Make sure that VMs that connect after registration can get the
      // serializer
      vm2.invoke("Connect to DS", () -> {
        // Register a DataSerializer before connecting to system
        Class c = LongWrapper.LongWrapperSerializer.class;
        DataSerializer.register(c);

        getSystem();
      });
      vm2.invoke("Create Region", () -> {
        createRegion(name);
      });

      // Make sure vm0 and vm1 have both registered the LongWrapperSerializer after vm2 reconnects
      // and before vm2 puts any instance of LongWrapper in the region
      // This is to avoid async disk store FlusherThread serializing any instance of LongWrapper
      // before the LongWrapperSerializer is registered
      vm0.invoke("waitForLongWrapperSerializerRegistration",
          this::waitForLongWrapperSerializerRegistration);

      // see the comments in "get" CacheSerializableRunnable above
      vm1.invoke("waitForLongWrapperSerializerRegistration",
          this::waitForLongWrapperSerializerRegistration);

      vm2.invoke("Put long", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.put(key2, new LongWrapper(longValue));

        flushIfNecessary(region);

        LongWrapper.LongWrapperSerializer serializer =
            (LongWrapper.LongWrapperSerializer) InternalDataSerializer.getSerializer((byte) 121);
        assertThat(serializer.wasInvoked).isTrue();
      });
      vm2.invoke("Get int", get);

      SerializableRunnable get2 = new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          LongWrapper value = (LongWrapper) region.get(key2);
          assertThat(InternalDataSerializer.getSerializer((byte) 121)).isNotNull();
          assertThat(value).isNotNull();
          assertThat(value.longValue).isEqualTo(longValue);
        }
      };
      vm0.invoke("Get long", get2);
      vm1.invoke("Get long", get2);

      // wait a little while for other netsearch requests to return
      // before unregistering the serializers that will be needed to process these
      // responses.
    } finally {
      Wait.pause(1500);
      vm0.invoke("Flush disk store",
          () -> flushDiskStore(name));
      vm1.invoke("Flush disk store",
          () -> flushDiskStore(name));
      vm2.invoke("Flush disk store",
          () -> flushDiskStore(name));
      unregisterAllSerializers();
    }
  }

  private void flushDiskStore(String name) {
    if (getRootRegion() != null && getRootRegion().getSubregion(name) != null) {
      DiskStoreImpl diskStore = ((LocalRegion) getRootRegion().getSubregion(name)).getDiskStore();
      if (diskStore != null) {
        diskStore.flush();
      }
    }
  }

  private void waitForLongWrapperSerializerRegistration() {
    // see the comments in "get" CacheSerializableRunnable in testNoDataSerializer()
    final int savVal = InternalDataSerializer.GetMarker.WAIT_MS;
    InternalDataSerializer.GetMarker.WAIT_MS = 1;
    try {
      await("DataSerializer with id 121 was never registered")
          .until(() -> InternalDataSerializer.getSerializer((byte) 121) != null);
    } finally {
      InternalDataSerializer.GetMarker.WAIT_MS = savVal;
    }
  }

  /**
   * Tests what happens when one VM attempts to read an object for which it does not have a
   * registered <code>Instantiator</code>.
   *
   * @since GemFire 3.5
   */
  @Test
  public void testNoInstantiator() {
    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    final String name = this.getUniqueName();

    vm0.invoke("Create Region", () -> {
      createRegion(name);
    });
    vm1.invoke("Create Region", () -> {
      createRegion(name);
    });

    final Object key = "KEY";
    final Object key2 = "KEY2";
    final int intValue = 7201;
    final long longValue = 123612;
    // final boolean[] wasInvoked = new boolean[1];

    vm2.invoke("Disconnect from DS", JUnit4DistributedTestCase::disconnectFromDS);

    vm0.invoke("Put int", () -> {
      Instantiator.register(new DSIntWrapper.DSIntWrapperInstantiator());

      Region<Object, Object> region = getRootRegion().getSubregion(name);
      region.put(key, new DSIntWrapper(intValue));

      flushIfNecessary(region);
    });

    SerializableRunnable get = new CacheSerializableRunnable() {
      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        DSIntWrapper.DSIntWrapperInstantiator inst = new DSIntWrapper.DSIntWrapperInstantiator();
        assertThat(InternalInstantiator.getInstantiator(inst.getId())).isNotNull();
        DSIntWrapper value = (DSIntWrapper) region.get(key);
        assertThat(value).isNotNull();
        assertThat(value.intValue).isEqualTo(intValue);
      }
    };
    try {
      vm1.invoke("Get int", get);

      // Make sure that VMs that connect after registration can get the
      // serializer
      vm2.invoke("Connect to DS", () -> {
        // Register a Instantiator before connecting to system
        Instantiator.register(new DSLongWrapper.DSLongWrapperInstantiator());

        getSystem();
      });

      vm2.invoke("Create Region", () -> {
        createRegion(name);
      });

      vm2.invoke("Put long", () -> {
        Region<Object, Object> region = getRootRegion().getSubregion(name);
        region.put(key2, new DSLongWrapper(longValue));

        flushIfNecessary(region);
      });
      vm2.invoke("Get int", get);

      SerializableRunnable get2 = new CacheSerializableRunnable() {
        @Override
        public void run2() throws CacheException {
          Region<Object, Object> region = getRootRegion().getSubregion(name);
          DSLongWrapper.DSLongWrapperInstantiator inst =
              new DSLongWrapper.DSLongWrapperInstantiator();
          assertThat(InternalInstantiator.getInstantiator(inst.getId())).isNotNull();
          LongWrapper value = (LongWrapper) region.get(key2);
          assertThat(value).isNotNull();
          assertThat(value.longValue).isEqualTo(longValue);

          inst = (DSLongWrapper.DSLongWrapperInstantiator) InternalInstantiator
              .getInstantiator(inst.getId());
          assertThat(inst).isNotNull();
          assertThat(inst.wasInvoked).isTrue();
        }
      };
      vm0.invoke("Get long", get2);
      vm1.invoke("Get long", get2);

    } finally {
      // wait a little while for other netsearch requests to return
      // before unregistering the serializers that will be needed to process these
      // responses.
      Wait.pause(1500);
      unregisterAllSerializers();
    }
  }

  /**
   * Unregisters all of the <code>DataSerializer</code>s and <code>Instantiators</code> in all of
   * the VMs in the distributed system.
   */
  private static void unregisterAllSerializers() {
    DistributedTestUtils.unregisterAllDataSerializersFromAllVms();
  }

  /**
   * A class used when testing <code>DataSerializable</code> and distribution.
   */
  static class IntWrapper {
    int intValue;

    IntWrapper(int intValue) {
      this.intValue = intValue;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof IntWrapper) {
        return ((IntWrapper) o).intValue == this.intValue;

      } else {
        return false;
      }
    }

    static class IntWrapperSerializer extends DataSerializer {
      boolean wasInvoked = false;

      @Override
      public int getId() {
        return 120;
      }

      @Override
      public Class[] getSupportedClasses() {
        return new Class[] {IntWrapper.class};
      }

      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        if (o instanceof IntWrapper) {
          this.wasInvoked = true;
          IntWrapper iw = (IntWrapper) o;
          out.writeInt(iw.intValue);
          return true;

        } else {
          return false;
        }
      }

      @Override
      public Object fromData(DataInput in) throws IOException {

        return new IntWrapper(in.readInt());
      }
    }
  }

  /**
   * An <code>IntWrapper</code> that is <code>DataSerializable</code>.
   */
  static class DSIntWrapper extends IntWrapper implements DataSerializable {

    DSIntWrapper(int intValue) {
      super(intValue);
    }

    DSIntWrapper() {
      super(0);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(intValue);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      this.intValue = in.readInt();
    }

    static class DSIntWrapperInstantiator extends Instantiator {
      DSIntWrapperInstantiator() {
        this(DSIntWrapper.class, (byte) 76);
      }

      DSIntWrapperInstantiator(Class<? extends DSIntWrapper> c, byte id) {
        super(c, id);
      }

      @Override
      public DataSerializable newInstance() {
        return new DSIntWrapper();
      }
    }
  }

  /**
   * Another class used when testing <code>DataSerializable</code> and distribution.
   */
  static class LongWrapper {
    long longValue;

    LongWrapper(long longValue) {
      this.longValue = longValue;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof LongWrapper) {
        return ((LongWrapper) o).longValue == this.longValue;

      } else {
        return false;
      }
    }

    static class LongWrapperSerializer extends DataSerializer {
      boolean wasInvoked = false;

      @Override
      public int getId() {
        return 121;
      }

      @Override
      public Class[] getSupportedClasses() {
        return new Class[] {LongWrapper.class};
      }

      @Override
      public boolean toData(Object o, DataOutput out) throws IOException {
        if (o instanceof LongWrapper) {
          this.wasInvoked = true;
          LongWrapper iw = (LongWrapper) o;
          out.writeLong(iw.longValue);
          return true;

        } else {
          return false;
        }
      }

      @Override
      public Object fromData(DataInput in) throws IOException {

        return new LongWrapper(in.readLong());
      }
    }
  }

  /**
   * An <code>LongWrapper</code> that is <code>DataSerializable</code>.
   */
  static class DSLongWrapper extends LongWrapper implements DataSerializable {

    DSLongWrapper(long longValue) {
      super(longValue);
    }

    DSLongWrapper() {
      super(0L);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeLong(longValue);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      this.longValue = in.readLong();
    }

    static class DSLongWrapperInstantiator extends Instantiator {
      boolean wasInvoked = false;

      DSLongWrapperInstantiator(Class<? extends DSLongWrapper> c, byte id) {
        super(c, id);
      }

      DSLongWrapperInstantiator() {
        this(DSLongWrapper.class, (byte) 99);
      }

      @Override
      public DataSerializable newInstance() {
        this.wasInvoked = true;
        return new DSLongWrapper();
      }
    }
  }

  private static class MyTransactionListener implements TransactionListener {
    private volatile TransactionId expectedTxId;
    private volatile TransactionEvent lastEvent;
    private volatile int afterCommitCount;
    private volatile int afterFailedCommitCount;
    private volatile int afterRollbackCount;
    private volatile int closeCount;

    @Override
    public synchronized void afterCommit(TransactionEvent event) {
      this.lastEvent = event;
      this.afterCommitCount++;
    }

    @Override
    public synchronized void afterFailedCommit(TransactionEvent event) {
      this.lastEvent = event;
      this.afterFailedCommitCount++;
    }

    @Override
    public synchronized void afterRollback(TransactionEvent event) {
      this.lastEvent = event;
      this.afterRollbackCount++;
    }

    @Override
    public synchronized void close() {
      this.closeCount++;
    }

    synchronized void checkAfterCommitCount(int expected) {
      assertThat(this.afterCommitCount).isEqualTo(expected);
    }

    synchronized void assertCounts() {
      await().untilAsserted(() -> {
        assertThat(afterCommitCount).describedAs("After Commit Count")
            .isEqualTo(1);
        assertThat(afterFailedCommitCount).describedAs("After Failed Commit Count")
            .isEqualTo(0);
        assertThat(afterRollbackCount).describedAs("After Rollback Count")
            .isEqualTo(0);
        assertThat(closeCount).describedAs("Close Count")
            .isEqualTo(0);
      });
    }
  }

  private static class CountingDistCacheListener<K, V> extends CacheListenerAdapter<K, V> {
    private int aCreateCalls, aUpdateCalls, aInvalidateCalls, aDestroyCalls, regionOps;
    private EntryEvent<K, V> lastEvent;

    @Override
    public void close() {}

    @Override
    public void afterCreate(EntryEvent<K, V> e) {
      if (e.isOriginRemote()) {
        synchronized (this) {
          ++this.aCreateCalls;
          this.lastEvent = e;
        }
      }
    }

    @Override
    public void afterUpdate(EntryEvent<K, V> e) {
      if (e.isOriginRemote()) {
        synchronized (this) {
          ++this.aUpdateCalls;
          this.lastEvent = e;
        }
      }
    }

    @Override
    public void afterInvalidate(EntryEvent<K, V> e) {
      if (e.isOriginRemote()) {
        synchronized (this) {
          ++this.aInvalidateCalls;
          this.lastEvent = e;
        }
      }
    }

    @Override
    public void afterDestroy(EntryEvent<K, V> e) {
      if (e.isOriginRemote()) {
        synchronized (this) {
          ++this.aDestroyCalls;
          this.lastEvent = e;
        }
      }
    }

    @Override
    public synchronized void afterRegionInvalidate(RegionEvent<K, V> e) {
      ++this.regionOps;
    }

    @Override
    public synchronized void afterRegionDestroy(RegionEvent<K, V> e) {
      ++this.regionOps;
    }

    synchronized void assertCount(int expectedCreate, int expectedUpdate,
        int expectedInvalidate, int expectedDestroy) {
      assertThat(this.aCreateCalls).isEqualTo(expectedCreate);
      assertThat(this.aUpdateCalls).isEqualTo(expectedUpdate);
      assertThat(this.aInvalidateCalls).isEqualTo(expectedInvalidate);
      assertThat(this.aDestroyCalls).isEqualTo(expectedDestroy);
      assertThat(this.regionOps).isEqualTo(0);
    }

    synchronized EntryEvent<K, V> getEntryEvent() {
      return this.lastEvent;
    }

    synchronized void setEntryEvent() {
      this.lastEvent = null;
    }
  }

  private void assertCacheCallbackEvents(String regionName, TransactionId txId,
      Object oldValue, Object newValue) {
    Region<Object, Object> re = getCache().getRegion("root").getSubregion(regionName);
    MyTransactionListener tl =
        firstTransactionListenerFrom(getCache().getCacheTransactionManager());
    tl.expectedTxId = txId;
    assertThat(re).describedAs("Cannot assert TX Callout Events with a null Region: " + regionName)
        .isNotNull();
    final CountingDistCacheListener<Object, Object> cdcl =
        (CountingDistCacheListener<Object, Object>) re.getAttributes().getCacheListeners()[0];

    await("retry event = null where it should not be")
        .until(() -> cdcl.getEntryEvent() != null);

    EntryEvent<Object, Object> listenEvent = cdcl.getEntryEvent();
    assertThat(listenEvent).describedAs(
        "Cannot assert TX CacheListener Events with a null Entry Event").isNotNull();
    assertThat(listenEvent.getRegion()).isEqualTo(re);
    assertThat(listenEvent.getTransactionId()).isEqualTo(txId);
    assertThat(listenEvent.getKey()).isEqualTo("key");
    assertThat(listenEvent.getOldValue()).isEqualTo(oldValue);
    assertThat(listenEvent.getNewValue()).isEqualTo(newValue);
    assertThat(listenEvent.getCallbackArgument()).isNull();
    assertThat(listenEvent.isCallbackArgumentAvailable()).isTrue();
    assertThat(listenEvent.getOperation().isLoad()).isFalse();
    assertThat(listenEvent.getOperation().isNetLoad()).isFalse();
    assertThat(listenEvent.getOperation().isNetSearch()).isFalse();
    assertThat(listenEvent.getOperation().isLocalLoad()).isFalse();
    assertThat(listenEvent.getOperation().isDistributed()).isTrue();
    assertThat(listenEvent.getOperation().isExpiration()).isFalse();
    assertThat(listenEvent.isOriginRemote()).isTrue();
    cdcl.setEntryEvent();
  }

  /**
   * Tests that an entry update is propagated to other caches that have that same entry defined.
   */
  @Test
  public void testTXSimpleOps() {
    assumeThat(supportsTransactions()).isTrue();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();
    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    if (getRegionAttributes().getScope().isGlobal()
        || getRegionAttributes().getDataPolicy().withPersistence()) {
      // just make sure transactions are not allowed on global or shared regions
      Region<String, String> rgn = createRegion(getUniqueName());
      txMgr.begin();
      assertThatThrownBy(() -> rgn.put("testTXSimpleOpsKey1", "val"))
          .isInstanceOf(UnsupportedOperationException.class);

      txMgr.rollback();
      rgn.localDestroyRegion();
      return;
    }

    final String rgnName = getUniqueName();

    SerializableRunnable create = new SerializableRunnable() {
      @Override
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        setTxListener(txMgr2, tl);
        assertThat(tl.lastEvent).isNull();
        assertThat(tl.afterCommitCount).isEqualTo(0);
        assertThat(tl.afterFailedCommitCount).isEqualTo(0);
        assertThat(tl.afterRollbackCount).isEqualTo(0);
        assertThat(tl.closeCount).isEqualTo(0);
        try {
          Region<Object, Object> rgn = createRegion(rgnName);
          CountingDistCacheListener<Object, Object> cacheListener =
              new CountingDistCacheListener<>();
          rgn.getAttributesMutator().addCacheListener(cacheListener);
          cacheListener.assertCount(0, 0, 0, 0);
          getSystem().getLogWriter().info("testTXSimpleOps: Created region");
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };

    SerializableRunnable newKey =
        new SerializableRunnable() {
          @Override
          public void run() {
            try {
              Region<?, ?> root = getRootRegion();
              Region<Object, Object> rgn = root.getSubregion(rgnName);
              rgn.create("key", null);
              CountingDistCacheListener cacheListener =
                  (CountingDistCacheListener) rgn.getAttributes().getCacheListeners()[0];
              cacheListener.assertCount(0, 0, 0, 0);
              getSystem().getLogWriter().info("testTXSimpleOps: Created Key");
            } catch (CacheException e) {
              fail("While creating region", e);
            }
          }
        };

    VM vm = VM.getVM(0);
    vm.invoke("testTXSimpleOps: Create Region", create);
    vm.invoke("testTXSimpleOps: Create Region & Create Key", newKey);
    int vmCount = VM.getVMCount();
    for (int i = 1; i < vmCount; i++) {
      vm = VM.getVM(i);
      vm.invoke("testTXSimpleOps: Create Region", create);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm.invoke("testTXSimpleOps: Create Region & Create Key", newKey);
      }
    }

    try {
      Region<String, String> rgn = createRegion(rgnName);
      DMStats dmStats = getSystem().getDistributionManager().getStats();
      long cmtMsgs = dmStats.getSentCommitMessages();
      long commitWaits = dmStats.getCommitWaits();

      {
        txMgr.begin();
        rgn.put("key", "value");
        final TransactionId txId = txMgr.getTransactionId();
        txMgr.commit();
        assertThat(dmStats.getSentCommitMessages()).isEqualTo(cmtMsgs + 1);
        if (rgn.getAttributes().getScope().isAck()) {
          assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits + 1);
        } else {
          assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits);
        }
        getSystem().getLogWriter().info("testTXSimpleOps: Create/Put Value");
        invokeInEveryVM(new SerializableRunnable() {
          @Override
          public void run() {
            assertCacheCallbackEvents(rgnName, txId, null, "value");
          }
        });
        invokeInEveryVM("testTXSimpleOps: Verify Received Value", repeatingIfNecessary(
            new CacheSerializableRunnable() {
              @Override
              public void run2() {
                Region<String, String> rgn1 = getRootRegion().getSubregion(rgnName);
                assertThat(rgn1.getEntry("key")).describedAs("Could not find entry for 'key'")
                    .isNotNull();
                assertThat(rgn1.getEntry("key").getValue()).isEqualTo("value");
                CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
                MyTransactionListener tl =
                    firstTransactionListenerFrom(txMgr2);
                tl.checkAfterCommitCount(1);
                assertThat(tl.afterFailedCommitCount).isEqualTo(0);
                assertThat(tl.afterRollbackCount).isEqualTo(0);
                assertThat(tl.closeCount).isEqualTo(0);
                assertThat(tl.lastEvent.getCache()).isEqualTo(rgn1.getRegionService());
                {
                  Collection<EntryEvent<?, ?>> events;
                  RegionAttributes<String, String> attr = getRegionAttributes();
                  if (!attr.getDataPolicy().withReplication()
                      || attr.getConcurrencyChecksEnabled()) {
                    events = TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
                  } else {
                    events = TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
                  }
                  assertThat(events.size()).isEqualTo(1);
                  EntryEvent ev = events.iterator().next();
                  assertThat(ev.getTransactionId()).isEqualTo(tl.expectedTxId);
                  assertThat(rgn1).isSameAs(ev.getRegion());
                  assertThat(ev.getKey()).isEqualTo("key");
                  assertThat(ev.getNewValue()).isEqualTo("value");
                  assertThat(ev.getOldValue()).isNull();
                  assertThat(ev.getOperation().isLocalLoad()).isFalse();
                  assertThat(ev.getOperation().isNetLoad()).isFalse();
                  assertThat(ev.getOperation().isLoad()).isFalse();
                  assertThat(ev.getOperation().isNetSearch()).isFalse();
                  assertThat(ev.getOperation().isExpiration()).isFalse();
                  assertThat(ev.getCallbackArgument()).isNull();
                  assertThat(ev.isCallbackArgumentAvailable()).isTrue();
                  assertThat(ev.isOriginRemote()).isTrue();
                  assertThat(ev.getOperation().isDistributed()).isTrue();
                }
                CountingDistCacheListener<String, String> cdcL =
                    (CountingDistCacheListener<String, String>) rgn1.getAttributes()
                        .getCacheListeners()[0];
                cdcL.assertCount(0, 1, 0, 0);
              }
            }));
      }

      {
        txMgr.begin();
        rgn.put("key", "value2");
        final TransactionId txId = txMgr.getTransactionId();
        txMgr.commit();
        getSystem().getLogWriter().info("testTXSimpleOps: Put(update) Value2");
        invokeInEveryVM(new SerializableRunnable() {
          @Override
          public void run() {
            assertCacheCallbackEvents(rgnName, txId, "value", "value2");
          }
        });
        invokeInEveryVM("testTXSimpleOps: Verify Received Value", repeatingIfNecessary(
            new CacheSerializableRunnable() {
              @Override
              public void run2() {
                Region<String, String> rgn1 = getRootRegion().getSubregion(rgnName);
                assertThat(rgn1.getEntry("key")).describedAs("Could not find entry for 'key'")
                    .isNotNull();
                assertThat(rgn1.getEntry("key").getValue()).isEqualTo("value2");
                CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
                MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
                tl.checkAfterCommitCount(2);
                assertThat(tl.lastEvent.getCache()).isEqualTo(rgn1.getRegionService());
                {
                  Collection<EntryEvent<?, ?>> events =
                      TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
                  assertThat(events.size()).isEqualTo(1);
                  EntryEvent<?, ?> ev = events.iterator().next();
                  assertThat(ev.getTransactionId()).isEqualTo(tl.expectedTxId);
                  assertThat(rgn1).isSameAs(ev.getRegion());
                  assertThat(ev.getKey()).isEqualTo("key");
                  assertThat(ev.getNewValue()).isEqualTo("value2");
                  assertThat(ev.getOldValue()).isEqualTo("value");
                  assertThat(ev.getOperation().isLocalLoad()).isFalse();
                  assertThat(ev.getOperation().isNetLoad()).isFalse();
                  assertThat(ev.getOperation().isLoad()).isFalse();
                  assertThat(ev.getOperation().isNetSearch()).isFalse();
                  assertThat(ev.getOperation().isExpiration()).isFalse();
                  assertThat(ev.getCallbackArgument()).isNull();
                  assertThat(ev.isCallbackArgumentAvailable()).isTrue();
                  assertThat(ev.isOriginRemote()).isTrue();
                  assertThat(ev.getOperation().isDistributed()).isTrue();
                }
                CountingDistCacheListener cdcL =
                    (CountingDistCacheListener) rgn1.getAttributes().getCacheListeners()[0];
                cdcL.assertCount(0, 2, 0, 0);
              }
            }));
      }

      {
        txMgr.begin();
        rgn.invalidate("key");
        final TransactionId txId = txMgr.getTransactionId();
        txMgr.commit();
        getSystem().getLogWriter().info("testTXSimpleOps: invalidate key");
        // validate each of the CacheListeners EntryEvents
        invokeInEveryVM(new SerializableRunnable() {
          @Override
          public void run() {
            assertCacheCallbackEvents(rgnName, txId, "value2", null);
          }
        });
        invokeInEveryVM("testTXSimpleOps: Verify Received Value", repeatingIfNecessary(
            new CacheSerializableRunnable() {
              @Override
              public void run2() {
                Region<String, String> rgn1 = getRootRegion().getSubregion(rgnName);
                assertThat(rgn1.getEntry("key")).describedAs("entry for 'key'")
                    .isNotNull();
                assertThat(rgn1.containsKey("key")).isTrue();
                assertThat(rgn1.containsValueForKey("key")).isFalse();
                CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
                MyTransactionListener tl =
                    firstTransactionListenerFrom(txMgr2);
                tl.checkAfterCommitCount(3);
                assertThat(tl.lastEvent.getCache()).isEqualTo(rgn1.getRegionService());
                {
                  List<EntryEvent<?, ?>> events =
                      TxEventTestUtil.getInvalidateEvents(tl.lastEvent.getEvents());
                  assertThat(events.size()).isEqualTo(1);
                  EntryEvent<?, ?> ev = events.get(0);
                  assertThat(ev.getTransactionId()).isEqualTo(tl.expectedTxId);
                  assertThat(rgn1).isSameAs(ev.getRegion());
                  assertThat(ev.getKey()).isEqualTo("key");
                  assertThat(ev.getNewValue()).isNull();
                  assertThat(ev.getOldValue()).isEqualTo("value2");
                  assertThat(ev.getOperation().isLocalLoad()).isFalse();
                  assertThat(ev.getOperation().isNetLoad()).isFalse();
                  assertThat(ev.getOperation().isLoad()).isFalse();
                  assertThat(ev.getOperation().isNetSearch()).isFalse();
                  assertThat(ev.getOperation().isExpiration()).isFalse();
                  assertThat(ev.getCallbackArgument()).isNull();
                  assertThat(ev.isCallbackArgumentAvailable()).isTrue();
                  assertThat(ev.isOriginRemote()).isTrue();
                  assertThat(ev.getOperation().isDistributed()).isTrue();
                }
                CountingDistCacheListener cdcL =
                    (CountingDistCacheListener) rgn1.getAttributes().getCacheListeners()[0];
                cdcL.assertCount(0, 2, 1, 0);
              }
            }));
      }

      {
        txMgr.begin();
        rgn.destroy("key");
        TransactionId txId = txMgr.getTransactionId();
        txMgr.commit();
        getSystem().getLogWriter().info("testTXSimpleOps: destroy key");
        // validate each of the CacheListeners EntryEvents
        invokeInEveryVM(new SerializableRunnable() {
          @Override
          public void run() {
            assertCacheCallbackEvents(rgnName, txId, null, null);
          }
        });
        invokeInEveryVM("testTXSimpleOps: Verify Received Value", repeatingIfNecessary(
            new CacheSerializableRunnable() {
              @Override
              public void run2() {
                Region<String, String> rgn1 = getRootRegion().getSubregion(rgnName);
                assertThat(rgn1.containsKey("key")).isFalse();
                CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
                MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
                tl.checkAfterCommitCount(4);
                assertThat(tl.lastEvent.getCache()).isEqualTo(rgn1.getRegionService());
                {
                  Collection<EntryEvent<?, ?>> events =
                      TxEventTestUtil.getDestroyEvents(tl.lastEvent.getEvents());
                  assertThat(events.size()).isEqualTo(1);
                  EntryEvent<?, ?> ev = events.iterator().next();
                  assertThat(ev.getTransactionId()).isEqualTo(tl.expectedTxId);
                  assertThat(rgn1).isSameAs(ev.getRegion());
                  assertThat(ev.getKey()).isEqualTo("key");
                  assertThat(ev.getNewValue()).isNull();
                  assertThat(ev.getOldValue()).isNull();
                  assertThat(ev.getOperation().isLocalLoad()).isFalse();
                  assertThat(ev.getOperation().isNetLoad()).isFalse();
                  assertThat(ev.getOperation().isLoad()).isFalse();
                  assertThat(ev.getOperation().isNetSearch()).isFalse();
                  assertThat(ev.getOperation().isExpiration()).isFalse();
                  assertThat(ev.getCallbackArgument()).isNull();
                  assertThat(ev.isCallbackArgumentAvailable()).isTrue();
                  assertThat(ev.isOriginRemote()).isTrue();
                  assertThat(ev.getOperation().isDistributed()).isTrue();
                }
                CountingDistCacheListener cdcL =
                    (CountingDistCacheListener) rgn1.getAttributes().getCacheListeners()[0];
                cdcL.assertCount(0, 2, 1, 1);
              }
            }));
      }
    } catch (Exception e) {
      getCache().close();
      getSystem().getLogWriter().fine("testTXSimpleOps: Caused exception in createRegion");
      throw e;
    }

  }

  /**
   * Indicate whether this region supports transactions
   *
   * @return true if it supports transactions
   */
  protected boolean supportsTransactions() {
    return true;
  }

  /**
   * Tests that the push of a loaded value does not cause a conflict on the side receiving the
   * update
   */
  @Ignore("TODO: this test always hits early out")
  @Test
  public void testTXUpdateLoadNoConflict() {
    assumeThat(supportsTransactions()).isTrue();
    assumeThat(getRegionAttributes().getScope().isGlobal()).isFalse();
    assumeThat(getRegionAttributes().getDataPolicy().withPersistence()).isFalse();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();
    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    final String rgnName = getUniqueName();

    try {
      MyTransactionListener tl = new MyTransactionListener();
      setTxListener(txMgr, tl);
      RegionFactory<String, String> factory = getCache().createRegionFactory(getRegionAttributes());
      factory.setDataPolicy(DataPolicy.REPLICATE);
      Region<String, String> rgn = createRegion(rgnName, factory);

      txMgr.begin();
      TransactionId myTXId = txMgr.getTransactionId();

      rgn.create("key", "txValue");

      vm0.invoke("testTXUpdateLoadNoConflict: Create Region & Load value", () -> {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl1 = new MyTransactionListener();
        setTxListener(txMgr2, tl1);
        try {
          Region<String, String> rgn1 = createRegion(rgnName);
          AttributesMutator<String, String> mutator = rgn1.getAttributesMutator();
          mutator.setCacheLoader(new CacheLoader<String, String>() {
            int count = 0;

            @Override
            public String load(LoaderHelper<String, String> helper)
                throws CacheLoaderException {
              count++;
              return "LV " + count;
            }

            @Override
            public void close() {}
          });
          Object value = rgn1.get("key");
          assertThat(value).isEqualTo("LV 1");
          getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: loaded Key");
          flushIfNecessary(rgn1);
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      });

      {
        TXStateProxy tx = ((TXManagerImpl) txMgr).pauseTransaction();
        assertThat(rgn.containsKey("key")).isTrue();
        assertThat(rgn.getEntry("key").getValue()).isEqualTo("LV 1");
        ((TXManagerImpl) txMgr).unpauseTransaction(tx);
      }
      // make sure transactional view is still correct
      assertThat(rgn.getEntry("key").getValue()).isEqualTo("txValue");

      txMgr.commit();
      getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: did commit");
      assertThat(rgn.getEntry("key").getValue()).isEqualTo("txValue");
      {
        Collection<EntryEvent<?, ?>> events =
            TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
        assertThat(events.size()).isEqualTo(1);
        EntryEvent<?, ?> ev = events.iterator().next();
        assertThat(ev.getTransactionId()).isEqualTo(myTXId);
        assertThat(rgn).isSameAs(ev.getRegion());
        assertThat(ev.getKey()).isEqualTo("key");
        assertThat(ev.getNewValue()).isEqualTo("txValue");
        assertThat(ev.getOldValue()).isNull();
        assertThat(ev.getOperation().isLocalLoad()).isFalse();
        assertThat(ev.getOperation().isNetLoad()).isFalse();
        assertThat(ev.getOperation().isLoad()).isFalse();
        assertThat(ev.getOperation().isNetSearch()).isFalse();
        assertThat(ev.getOperation().isExpiration()).isFalse();
        assertThat(ev.getCallbackArgument()).isNull();
        assertThat(ev.isCallbackArgumentAvailable()).isTrue();
        assertThat(ev.isOriginRemote()).isFalse();
        assertThat(ev.getOperation().isDistributed()).isTrue();
      }

      // Now setup recreate the region in the controller with NONE
      // so test can do local destroys.
      rgn.localDestroyRegion();
      factory.setDataPolicy(DataPolicy.NORMAL);
      rgn = createRegion(rgnName, factory);

      // now see if net loader is working
      Object v2 = rgn.get("key2");
      assertThat(v2).isEqualTo("LV 2");

      // now confirm that netload does not cause a conflict
      txMgr.begin();
      myTXId = txMgr.getTransactionId();

      rgn.create("key3", "txValue3");

      {
        TXStateProxy tx = ((TXManagerImpl) txMgr).pauseTransaction();
        // do a get outside of the transaction to force a net load
        Object v3 = rgn.get("key3");
        assertThat(v3).isEqualTo("LV 3");
        ((TXManagerImpl) txMgr).unpauseTransaction(tx);
      }
      // make sure transactional view is still correct
      assertThat(rgn.getEntry("key3").getValue()).isEqualTo("txValue3");

      txMgr.commit();
      getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: did commit");
      assertThat(rgn.getEntry("key3").getValue()).isEqualTo("txValue3");
      {
        Collection<EntryEvent<?, ?>> events =
            TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
        assertThat(events.size()).isEqualTo(1);
        EntryEvent<?, ?> ev = events.iterator().next();
        assertThat(ev.getTransactionId()).isEqualTo(myTXId);
        assertThat(rgn).isSameAs(ev.getRegion());
        assertThat(ev.getKey()).isEqualTo("key3");
        assertThat(ev.getNewValue()).isEqualTo("txValue3");
        assertThat(ev.getOldValue()).isNull();
        assertThat(ev.getOperation().isLocalLoad()).isFalse();
        assertThat(ev.getOperation().isNetLoad()).isFalse();
        assertThat(ev.getOperation().isLoad()).isFalse();
        assertThat(ev.getOperation().isNetSearch()).isFalse();
        assertThat(ev.getOperation().isExpiration()).isFalse();
        assertThat(ev.getCallbackArgument()).isNull();
        assertThat(ev.isCallbackArgumentAvailable()).isTrue();
        assertThat(ev.isOriginRemote()).isFalse();
        assertThat(ev.getOperation().isDistributed()).isTrue();
      }

      // now see if tx net loader is working

      // now confirm that netload does not cause a conflict
      txMgr.begin();
      Object v4 = rgn.get("key4");
      assertThat(v4).isEqualTo("LV 4");
      assertThat(rgn.get("key4")).isEqualTo("LV 4");
      assertThat(rgn.getEntry("key4").getValue()).isEqualTo("LV 4");
      txMgr.rollback();
      // confirm that netLoad is transactional
      assertThat(rgn.get("key4")).isEqualTo("LV 5");
      assertThat(rgn.getEntry("key4").getValue()).isEqualTo("LV 5");

      // make sure non-tx netsearch works
      assertThat(rgn.get("key")).isEqualTo("txValue");
      assertThat(rgn.getEntry("key").getValue()).isEqualTo("txValue");

      // make sure net-search result does not conflict with commit
      rgn.localInvalidate("key");
      txMgr.begin();
      myTXId = txMgr.getTransactionId();

      rgn.put("key", "new txValue");

      {
        TXStateProxy tx = ((TXManagerImpl) txMgr).pauseTransaction();
        // do a get outside of the transaction to force a netsearch
        assertThat(rgn.get("key")).isEqualTo("txValue");
        assertThat(rgn.getEntry("key").getValue()).isEqualTo("txValue");
        ((TXManagerImpl) txMgr).unpauseTransaction(tx);
      }
      // make sure transactional view is still correct
      assertThat(rgn.getEntry("key").getValue()).isEqualTo("new txValue");

      txMgr.commit();
      flushIfNecessary(rgn); // give other side change to process commit
      getSystem().getLogWriter().info("testTXUpdateLoadNoConflict: did commit");
      assertThat(rgn.getEntry("key").getValue()).isEqualTo("new txValue");
      {
        Collection<EntryEvent<?, ?>> events =
            TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
        assertThat(events.size()).isEqualTo(1);
        EntryEvent<?, ?> ev = events.iterator().next();
        assertThat(ev.getTransactionId()).isEqualTo(myTXId);
        assertThat(rgn).isSameAs(ev.getRegion());
        assertThat(ev.getKey()).isEqualTo("key");
        assertThat(ev.getNewValue()).isEqualTo("new txValue");
        assertThat(ev.getOldValue()).isNull();
        assertThat(ev.getOperation().isLocalLoad()).isFalse();
        assertThat(ev.getOperation().isNetLoad()).isFalse();
        assertThat(ev.getOperation().isLoad()).isFalse();
        assertThat(ev.getOperation().isNetSearch()).isFalse();
        assertThat(ev.getOperation().isExpiration()).isFalse();
        assertThat(ev.getCallbackArgument()).isNull();
        assertThat(ev.isCallbackArgumentAvailable()).isTrue();
        assertThat(ev.isOriginRemote()).isFalse();
        assertThat(ev.getOperation().isDistributed()).isTrue();
      }

      // make sure tx local invalidate allows netsearch
      Object localCmtValue = rgn.getEntry("key").getValue();
      txMgr.begin();
      assertThat(rgn.getEntry("key").getValue()).isSameAs(localCmtValue);
      rgn.localInvalidate("key");
      assertThat(rgn.getEntry("key").getValue()).isNull();
      // now make sure a get will do a netsearch and find the value
      // in the other vm instead of the one in local cmt state
      Object txValue = rgn.get("key");
      assertThat(txValue).isNotSameAs(localCmtValue);
      assertThat(rgn.get("key")).isSameAs(txValue);
      assertThat(rgn.getEntry("key").getValue()).isNotSameAs(localCmtValue);
      // make sure we did a search and not a load
      assertThat(rgn.getEntry("key").getValue()).isEqualTo(localCmtValue);
      // now make sure that if we do a tx distributed invalidate
      // that we will do a load and not a search
      rgn.invalidate("key");
      assertThat(rgn.getEntry("key").getValue()).isNull();
      txValue = rgn.get("key");
      assertThat(txValue).isEqualTo("LV 6");
      assertThat(rgn.get("key")).isSameAs(txValue);
      assertThat(rgn.getEntry("key").getValue()).isEqualTo("LV 6");
      // now make sure after rollback that local cmt state has not changed
      txMgr.rollback();
      assertThat(rgn.getEntry("key").getValue()).isSameAs(localCmtValue);
    } catch (Exception e) {
      getCache().close();
      getSystem().getLogWriter()
          .fine("testTXUpdateLoadNoConflict: Caused exception in createRegion");
      throw e;
    }
  }

  @Test
  public void testTXMultiRegion() {
    assumeThat(supportsTransactions()).isTrue();
    assumeThat(getRegionAttributes().getScope().isGlobal()).isFalse();
    assumeThat(getRegionAttributes().getDataPolicy().withPersistence()).isFalse();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();

    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    final String rgnName1 = getUniqueName() + "MR1";
    final String rgnName2 = getUniqueName() + "MR2";
    final String rgnName3 = getUniqueName() + "MR3";

    SerializableRunnable create1 = new SerializableRunnable() {
      @Override
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        setTxListener(txMgr2, tl);
        try {
          createRegion(rgnName1);
          getSystem().getLogWriter().info("testTXMultiRegion: Created region1");
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };
    SerializableRunnable newKey1 = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Region<String, ?> rgn = getRootRegion("root").getSubregion(rgnName1);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testTXMultiRegion: Created key");
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };

    SerializableRunnable create2 = new SerializableRunnable() {
      @Override
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        setTxListener(txMgr2, tl);
        try {
          createRegion(rgnName2);
          getSystem().getLogWriter().info("testTXMultiRegion: Created region2");
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };
    SerializableRunnable newKey2 = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Region<?, ?> root = getRootRegion("root");
          Region<String, ?> rgn = root.getSubregion(rgnName2);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testTXMultiRegion: Created Key");
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };

    SerializableRunnable create3 = new SerializableRunnable() {
      @Override
      public void run() {
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = new MyTransactionListener();
        setTxListener(txMgr2, tl);
        try {
          createRegion(rgnName3);
          getSystem().getLogWriter().info("testTXMultiRegion: Created Region");
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };

    SerializableRunnable newKey3 = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Region<?, ?> root = getRootRegion("root");
          Region<String, ?> rgn = root.getSubregion(rgnName3);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testTXMultiRegion: Created Key");
        } catch (CacheException e) {
          fail("While creating region", e);
        }
      }
    };

    SerializableRunnable check1_3 = new SerializableRunnable() {
      @Override
      public void run() {
        Region<String, String> rgn1 = getRootRegion().getSubregion(rgnName1);
        {
          assertThat(rgn1.getEntry("key")).describedAs("Could not find entry for 'key'")
              .isNotNull();
          assertThat(rgn1.getEntry("key").getValue()).isEqualTo("value1");
        }
        Region<String, String> rgn3 = getRootRegion().getSubregion(rgnName3);
        {
          assertThat(rgn3.getEntry("key")).describedAs("Could not find entry for 'key'")
              .isNotNull();
          assertThat(rgn3.getEntry("key").getValue()).isEqualTo("value3");
        }
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
        tl.checkAfterCommitCount(1);
        assertThat(tl.afterFailedCommitCount).isEqualTo(0);
        assertThat(tl.afterRollbackCount).isEqualTo(0);
        assertThat(tl.closeCount).isEqualTo(0);
        assertThat(tl.lastEvent.getCache()).isEqualTo(rgn1.getRegionService());
        {
          Collection<EntryEvent<?, ?>> events;
          RegionAttributes<String, String> attr = getRegionAttributes();
          if (!attr.getDataPolicy().withReplication() || attr.getConcurrencyChecksEnabled()) {
            events = TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
          } else {
            events = TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
          }
          assertThat(events.size()).isEqualTo(2);
          List<EntryEvent<?, ?>> eventList = new ArrayList<>(events);
          eventList.sort((o1, o2) -> {
            String s1 = o1.getRegion().getFullPath() + o1.getKey();
            String s2 = o2.getRegion().getFullPath() + o2.getKey();
            return s1.compareTo(s2);
          });

          verifyMirrorRegionEventsMatch(eventList.get(0), rgn1, "value1");
          verifyMirrorRegionEventsMatch(eventList.get(1), rgn3, "value3");
        }
      }
    };
    SerializableRunnable check2_3 = new SerializableRunnable() {
      @Override
      public void run() {
        Region rgn2 = getRootRegion().getSubregion(rgnName2);
        {
          assertThat(rgn2.getEntry("key")).describedAs("Could not find entry for 'key'")
              .isNotNull();
          assertThat(rgn2.getEntry("key").getValue()).isEqualTo("value2");
        }
        Region rgn3 = getRootRegion().getSubregion(rgnName3);
        {
          assertThat(rgn3.getEntry("key")).describedAs("Could not find entry for 'key'")
              .isNotNull();
          assertThat(rgn3.getEntry("key").getValue()).isEqualTo("value3");
        }
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
        tl.checkAfterCommitCount(1);
        assertThat(tl.afterFailedCommitCount).isEqualTo(0);
        assertThat(tl.afterRollbackCount).isEqualTo(0);
        assertThat(tl.closeCount).isEqualTo(0);
        assertThat(tl.lastEvent.getCache()).isEqualTo(rgn2.getRegionService());
        {
          Collection<EntryEvent<?, ?>> events;
          RegionAttributes<String, String> attr = getRegionAttributes();
          if (!attr.getDataPolicy().withReplication() || attr.getConcurrencyChecksEnabled()) {
            events = TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
          } else {
            events = TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
          }
          assertThat(events.size()).isEqualTo(2);
          List<EntryEvent<?, ?>> eventList = new ArrayList<>(events);
          eventList.sort((o1, o2) -> {
            String s1 = o1.getRegion().getFullPath() + o1.getKey();
            String s2 = o2.getRegion().getFullPath() + o2.getKey();
            return s1.compareTo(s2);
          });

          verifyMirrorRegionEventsMatch(eventList.get(0), rgn2, "value2");
          verifyMirrorRegionEventsMatch(eventList.get(1), rgn3, "value3");
        }
      }
    };
    SerializableRunnable check1 = new SerializableRunnable() {
      @Override
      public void run() {
        Region<String, String> rgn = getRootRegion().getSubregion(rgnName1);
        assertThat(rgn.getEntry("key")).describedAs("Could not find entry for 'key'").isNotNull();
        assertThat(rgn.getEntry("key").getValue()).isEqualTo("value1");
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
        tl.checkAfterCommitCount(1);
        assertThat(tl.afterFailedCommitCount).isEqualTo(0);
        assertThat(tl.afterRollbackCount).isEqualTo(0);
        assertThat(tl.closeCount).isEqualTo(0);
        assertThat(tl.lastEvent.getCache()).isEqualTo(rgn.getRegionService());
        {
          Collection<EntryEvent<?, ?>> events;
          RegionAttributes<String, String> attr = getRegionAttributes();
          if (!attr.getDataPolicy().withReplication() || attr.getConcurrencyChecksEnabled()) {
            events = TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
          } else {
            events = TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
          }
          assertThat(events.size()).isEqualTo(1);
          EntryEvent<?, ?> ev = events.iterator().next();
          assertThat(rgn).isSameAs(ev.getRegion());
          assertThat(ev.getKey()).isEqualTo("key");
          assertThat(ev.getNewValue()).isEqualTo("value1");
          assertThat(ev.getOldValue()).isNull();
          assertThat(ev.getOperation().isLocalLoad()).isFalse();
          assertThat(ev.getOperation().isNetLoad()).isFalse();
          assertThat(ev.getOperation().isLoad()).isFalse();
          assertThat(ev.getOperation().isNetSearch()).isFalse();
          assertThat(ev.getOperation().isExpiration()).isFalse();
          assertThat(ev.getCallbackArgument()).isNull();
          assertThat(ev.isCallbackArgumentAvailable()).isTrue();
          assertThat(ev.isOriginRemote()).isTrue();
          assertThat(ev.getOperation().isDistributed()).isTrue();
        }
      }
    };
    SerializableRunnable check2 = new SerializableRunnable() {
      @Override
      public void run() {
        Region<String, String> rgn = getRootRegion().getSubregion(rgnName2);
        assertThat(rgn.getEntry("key")).describedAs("Could not find entry for 'key'").isNotNull();
        assertThat(rgn.getEntry("key").getValue()).isEqualTo("value2");
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
        tl.checkAfterCommitCount(1);
        assertThat(tl.afterFailedCommitCount).isEqualTo(0);
        assertThat(tl.afterRollbackCount).isEqualTo(0);
        assertThat(tl.closeCount).isEqualTo(0);
        assertThat(tl.lastEvent.getCache()).isEqualTo(rgn.getRegionService());
        {
          Collection<EntryEvent<?, ?>> events;
          RegionAttributes<String, String> attr = getRegionAttributes();
          if (!attr.getDataPolicy().withReplication() || attr.getConcurrencyChecksEnabled()) {
            events = TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
          } else {
            events = TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
          }
          assertThat(events.size()).isEqualTo(1);
          EntryEvent<?, ?> ev = events.iterator().next();
          assertThat(rgn).isSameAs(ev.getRegion());
          assertThat(ev.getKey()).isEqualTo("key");
          assertThat(ev.getNewValue()).isEqualTo("value2");
          assertThat(ev.getOldValue()).isNull();
          assertThat(ev.getOperation().isLocalLoad()).isFalse();
          assertThat(ev.getOperation().isNetLoad()).isFalse();
          assertThat(ev.getOperation().isLoad()).isFalse();
          assertThat(ev.getOperation().isNetSearch()).isFalse();
          assertThat(ev.getOperation().isExpiration()).isFalse();
          assertThat(ev.getCallbackArgument()).isNull();
          assertThat(ev.isCallbackArgumentAvailable()).isTrue();
          assertThat(ev.isOriginRemote()).isTrue();
          assertThat(ev.getOperation().isDistributed()).isTrue();
        }
      }
    };
    SerializableRunnable check3 = new SerializableRunnable() {
      @Override
      public void run() {
        Region<String, String> rgn = getRootRegion().getSubregion(rgnName3);
        assertThat(rgn.getEntry("key")).describedAs("Could not find entry for 'key'").isNotNull();
        assertThat(rgn.getEntry("key").getValue()).isEqualTo("value3");
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
        tl.checkAfterCommitCount(1);
        assertThat(tl.afterFailedCommitCount).isEqualTo(0);
        assertThat(tl.afterRollbackCount).isEqualTo(0);
        assertThat(tl.closeCount).isEqualTo(0);
        assertThat(tl.lastEvent.getCache()).isEqualTo(rgn.getRegionService());
        {
          Collection<EntryEvent<?, ?>> events;
          RegionAttributes attr = getRegionAttributes();
          if (!attr.getDataPolicy().withReplication() || attr.getConcurrencyChecksEnabled()) {
            events = TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents());
          } else {
            events = TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
          }
          assertThat(events.size()).isEqualTo(1);
          EntryEvent<?, ?> ev = events.iterator().next();
          assertThat(rgn).isSameAs(ev.getRegion());
          assertThat(ev.getKey()).isEqualTo("key");
          assertThat(ev.getNewValue()).isEqualTo("value3");
          assertThat(ev.getOldValue()).isNull();
          assertThat(ev.getOperation().isLocalLoad()).isFalse();
          assertThat(ev.getOperation().isNetLoad()).isFalse();
          assertThat(ev.getOperation().isLoad()).isFalse();
          assertThat(ev.getOperation().isNetSearch()).isFalse();
          assertThat(ev.getOperation().isExpiration()).isFalse();
          assertThat(ev.getCallbackArgument()).isNull();
          assertThat(ev.isCallbackArgumentAvailable()).isTrue();
          assertThat(ev.isOriginRemote()).isTrue();
          assertThat(ev.getOperation().isDistributed()).isTrue();
        }
      }
    };

    try {
      DMStats dmStats = getSystem().getDistributionManager().getStats();
      Region<String, String> rgn1;
      Region<String, String> rgn2;
      Region<String, String> rgn3;
      long cmtMsgs;
      long commitWaits;

      // vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R2,R3
      vm0.invoke("testTXMultiRegion: Create Region", create1);
      vm0.invoke("testTXMultiRegion: Create Key", newKey1);
      vm0.invoke("testTXMultiRegion: Create Region", create3);
      vm0.invoke("testTXMultiRegion: Create Key", newKey3);

      vm1.invoke("testTXMultiRegion: Create Region", create1);
      vm1.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm1.invoke("testTXMultiRegion: Create Key", newKey1);
        vm1.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      vm2.invoke("testTXMultiRegion: Create Region", create2);
      vm2.invoke("testTXMultiRegion: Create Key", newKey2);

      vm3.invoke("testTXMultiRegion: Create Region", create2);
      vm3.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm3.invoke("testTXMultiRegion: Create Key", newKey2);
        vm3.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");
      // TransactionId txId = txMgr.getTransactionId();
      getSystem().getLogWriter()
          .info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R2,R3");
      txMgr.commit();
      assertThat(dmStats.getSentCommitMessages()).isEqualTo(cmtMsgs + 3);
      if (rgn1.getAttributes().getScope().isAck() || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()) {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits + 1);
      } else {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits);
        // pause to give cmt message a chance to be processed
        // [bruce] changed from 200 to 2000 for mcast testing
        try {
          Thread.sleep(2000);
        } catch (InterruptedException chomp) {
          fail("interrupted");
        }
      }
      vm0.invoke(check1_3);
      vm1.invoke("testTXMultiRegion: check", check1_3);
      vm2.invoke("testTXMultiRegion: check", check2);
      vm3.invoke("testTXMultiRegion: check", check2_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R1,R3 vm2->R2,R3 vm3->R2,R3
      vm0.invoke("testTXMultiRegion: Create Region", create1);
      vm0.invoke("testTXMultiRegion: Create Key", newKey1);
      vm0.invoke("testTXMultiRegion: Create Region", create3);
      vm0.invoke("testTXMultiRegion: Create Key", newKey3);

      vm1.invoke("testTXMultiRegion: Create Region", create1);
      vm1.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm1.invoke("testTXMultiRegion: Create Key", newKey1);
        vm1.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      vm2.invoke("testTXMultiRegion: Create Region", create2);
      vm2.invoke("testTXMultiRegion: Create Key", newKey2);
      vm2.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm2.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      vm3.invoke("testTXMultiRegion: Create Region", create2);
      vm3.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm3.invoke("testTXMultiRegion: Create Key", newKey2);
        vm3.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");

      getSystem().getLogWriter()
          .info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R2,R3 vm3->R2,R3");
      txMgr.commit();
      assertThat(dmStats.getSentCommitMessages()).isEqualTo(cmtMsgs + 2);
      if (rgn1.getAttributes().getScope().isAck() || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()) {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits + 1);
      } else {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits);
        // pause to give cmt message a chance to be processed
        try {
          Thread.sleep(200);
        } catch (InterruptedException chomp) {
          fail("interrupted");
        }
      }
      vm0.invoke("testTXMultiRegion: check", check1_3);
      vm1.invoke("testTXMultiRegion: check", check1_3);
      vm2.invoke("testTXMultiRegion: check", check2_3);
      vm3.invoke("testTXMultiRegion: check", check2_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R1,R3
      vm0.invoke("testTXMultiRegion: Create Region", create1);
      vm0.invoke("testTXMultiRegion: Create Key", newKey1);
      vm0.invoke(create3);
      vm0.invoke("testTXMultiRegion: Create Key", newKey3);

      vm1.invoke("testTXMultiRegion: Create Region", create1);
      vm1.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm1.invoke("testTXMultiRegion: Create Key", newKey1);
        vm1.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      vm2.invoke("testTXMultiRegion: Create Region", create2);
      vm2.invoke("testTXMultiRegion: Create Key", newKey2);

      vm3.invoke("testTXMultiRegion: Create Region", create1);
      vm3.invoke(create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm3.invoke("testTXMultiRegion: Create Key", newKey1);
        vm3.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");

      getSystem().getLogWriter()
          .info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R2 vm3->R1,R3");
      txMgr.commit();
      assertThat(dmStats.getSentCommitMessages()).isEqualTo(cmtMsgs + 2);
      if (rgn1.getAttributes().getScope().isAck() || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()) {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits + 1);
      } else {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits);
        // pause to give cmt message a chance to be processed
        try {
          Thread.sleep(200);
        } catch (InterruptedException chomp) {
          fail("interrupted");
        }
      }
      vm0.invoke("testTXMultiRegion: check", check1_3);
      vm1.invoke("testTXMultiRegion: check", check1_3);
      vm2.invoke("testTXMultiRegion: check", check2);
      vm3.invoke("testTXMultiRegion: check", check1_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1 vm1->R1 vm2->R2 vm3->R3
      vm0.invoke("testTXMultiRegion: Create Region", create1);
      vm0.invoke("testTXMultiRegion: Create Key", newKey1);

      vm1.invoke("testTXMultiRegion: Create Region", create1);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm1.invoke("testTXMultiRegion: Create Key", newKey1);
      }

      vm2.invoke("testTXMultiRegion: Create Region", create2);
      vm2.invoke("testTXMultiRegion: Create Key", newKey2);

      vm3.invoke("testTXMultiRegion: Create Region", create3);
      vm3.invoke("testTXMultiRegion: Create Key", newKey3);

      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");

      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1 vm1->R1 vm2->R2 vm3->R3");
      txMgr.commit();
      assertThat(dmStats.getSentCommitMessages()).isEqualTo(cmtMsgs + 3);
      if (rgn1.getAttributes().getScope().isAck() || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()) {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits + 1);
      } else {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits);
        // pause to give cmt message a chance to be processed
        try {
          Thread.sleep(200);
        } catch (InterruptedException chomp) {
          fail("interrupted");
        }
      }
      vm0.invoke(check1);
      vm1.invoke(check1);
      vm2.invoke("testTXMultiRegion: check", check2);
      vm3.invoke(check3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R2,R3 vm2->R2 vm3->R3
      vm0.invoke("testTXMultiRegion: Create Region", create1);
      vm0.invoke("testTXMultiRegion: Create Key", newKey1);
      vm0.invoke("testTXMultiRegion: Create Region", create3);
      vm0.invoke("testTXMultiRegion: Create Key", newKey3);

      vm1.invoke("testTXMultiRegion: Create Region", create2);
      vm1.invoke("testTXMultiRegion: Create Key", newKey2);
      vm1.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm1.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      vm2.invoke("testTXMultiRegion: Create Region", create2);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm2.invoke("testTXMultiRegion: Create Key", newKey2);
      }

      vm3.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm3.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");

      getSystem().getLogWriter().info("testTXMultiRegion: vm0->R1,R3 vm1->R2,R3 vm2->R2 vm3->R3");
      txMgr.commit();
      assertThat(dmStats.getSentCommitMessages()).isEqualTo(cmtMsgs + 4);
      if (rgn1.getAttributes().getScope().isAck() || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()) {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits + 1);
      } else {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits);
        // pause to give cmt message a chance to be processed
        try {
          Thread.sleep(200);
        } catch (InterruptedException chomp) {
          fail("interrupted");
        }
      }
      vm0.invoke("testTXMultiRegion: check", check1_3);
      vm1.invoke("testTXMultiRegion: check", check2_3);
      vm2.invoke("testTXMultiRegion: check", check2);
      vm3.invoke(check3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();

      // vm0->R1,R3 vm1->R1,R3 vm2->R1,R3 vm3->R1,R3
      vm0.invoke("testTXMultiRegion: Create Region", create1);
      vm0.invoke("testTXMultiRegion: Create Key", newKey1);
      vm0.invoke("testTXMultiRegion: Create Region", create3);
      vm0.invoke("testTXMultiRegion: Create Key", newKey3);

      vm1.invoke("testTXMultiRegion: Create Region", create1);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm1.invoke("testTXMultiRegion: Create Key", newKey1);
      }
      vm1.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm1.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      vm2.invoke("testTXMultiRegion: Create Region", create1);
      vm2.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm2.invoke("testTXMultiRegion: Create Key", newKey1);
        vm2.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      vm3.invoke("testTXMultiRegion: Create Region", create1);
      vm3.invoke("testTXMultiRegion: Create Region", create3);
      if (!getRegionAttributes().getDataPolicy().withReplication()
          && !getRegionAttributes().getDataPolicy().withPreloaded()) {
        vm3.invoke("testTXMultiRegion: Create Key", newKey1);
        vm3.invoke("testTXMultiRegion: Create Key", newKey3);
      }

      rgn1 = createRegion(rgnName1);
      rgn2 = createRegion(rgnName2);
      rgn3 = createRegion(rgnName3);
      cmtMsgs = dmStats.getSentCommitMessages();
      commitWaits = dmStats.getCommitWaits();

      txMgr.begin();
      rgn1.put("key", "value1");
      rgn2.put("key", "value2");
      rgn3.put("key", "value3");

      getSystem().getLogWriter()
          .info("testTXMultiRegion: vm0->R1,R3 vm1->R1,R3 vm2->R1,R3 vm3->R1,R3");
      txMgr.commit();
      assertThat(dmStats.getSentCommitMessages()).isEqualTo(cmtMsgs + 1);
      if (rgn1.getAttributes().getScope().isAck() || rgn2.getAttributes().getScope().isAck()
          || rgn3.getAttributes().getScope().isAck()) {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits + 1);
      } else {
        assertThat(dmStats.getCommitWaits()).isEqualTo(commitWaits);
        // pause to give cmt message a chance to be processed
        try {
          Thread.sleep(200);
        } catch (InterruptedException chomp) {
          fail("interrupted");
        }
      }
      vm0.invoke("testTXMultiRegion: check", check1_3);
      vm1.invoke("testTXMultiRegion: check", check1_3);
      vm2.invoke("testTXMultiRegion: check", check1_3);
      vm3.invoke("testTXMultiRegion: check", check1_3);
      rgn1.destroyRegion();
      rgn2.destroyRegion();
      rgn3.destroyRegion();
    } catch (Exception e) {
      getCache().close();
      getSystem().getLogWriter().fine("testTXMultiRegion: Caused exception in createRegion");
      throw e;
    }
  }

  /**
   * Clear all existing transaction listeners and install the given listener.
   */
  private static void setTxListener(CacheTransactionManager manager,
      TransactionListener listener) {
    Stream.of(manager.getListeners())
        .forEach(manager::removeListener);
    manager.addListener(listener);
  }

  private void verifyMirrorRegionEventsMatch(EntryEvent<?, ?> event, Region region,
      String expectedValue) {
    assertThat(event.getRegion()).isSameAs(region);
    assertThat(event.getKey()).isEqualTo("key");
    assertThat(event.getNewValue()).isEqualTo(expectedValue);
    assertThat(event.getOldValue()).isNull();
    assertThat(event.getOperation().isLocalLoad()).isFalse();
    assertThat(event.getOperation().isNetLoad()).isFalse();
    assertThat(event.getOperation().isLoad()).isFalse();
    assertThat(event.getOperation().isNetSearch()).isFalse();
    assertThat(event.getOperation().isExpiration()).isFalse();
    assertThat(event.getCallbackArgument()).isNull();
    assertThat(event.isCallbackArgumentAvailable()).isTrue();
    assertThat(event.isOriginRemote()).isTrue();
    assertThat(event.getOperation().isDistributed()).isTrue();
  }

  @Test
  public void testTXRmtMirror() {
    assumeThat(supportsTransactions()).isTrue();
    assumeThat(getRegionAttributes().getScope().isGlobal()).isFalse();
    assumeThat(getRegionAttributes().getDataPolicy().withPersistence()).isFalse();

    assertThat(getRegionAttributes().getScope().isDistributed()).isTrue();
    CacheTransactionManager txMgr = this.getCache().getCacheTransactionManager();

    final String rgnName = getUniqueName();

    // Make sure that a remote create done in a tx is created in a remote mirror
    // and dropped in a remote non-mirror

    vm0.invoke("textTXRmtMirror: Create Mirrored Region", () -> {
      CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
      MyTransactionListener tl = new MyTransactionListener();
      setTxListener(txMgr2, tl);
      try {
        RegionFactory<?, ?> factory = getCache().createRegionFactory(getRegionAttributes());
        factory.setDataPolicy(DataPolicy.REPLICATE);
        createRegion(rgnName, factory);
      } catch (CacheException e) {
        fail("While creating region", e);
      }
    });

    vm1.invoke("textTXRmtMirror: Create Mirrored Region", () -> {
      CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
      MyTransactionListener tl = new MyTransactionListener();
      setTxListener(txMgr2, tl);
      try {
        RegionFactory<?, ?> factory = getCache().createRegionFactory(getRegionAttributes());
        factory.setDataPolicy(DataPolicy.NORMAL);
        createRegion(rgnName, factory);
      } catch (CacheException e) {
        fail("While creating region", e);
      }
    });

    Region<String, String> region = createRegion(rgnName);

    txMgr.begin();
    region.create("key", "value");
    logger.info("textTXRmtMirror: create mirror and non-mirror");
    txMgr.commit();

    validateTXRmtMirror(rgnName, vm0, vm1);
  }

  private void validateTXRmtMirror(String rgnName, VM vm0, VM vm1) {

    vm0.invoke("textTXRmtMirror: checkExists", new SerializableRunnable() {
      @Override
      public void run() {
        Region<String, String> rgn = getRootRegion().getSubregion(rgnName);

        if (!rgn.getAttributes().getScope().isAck()) {
          await().untilAsserted(() -> checkCommitAndDataExists(rgn));
        } else {
          checkCommitAndDataExists(rgn);
        }
      }

      private void checkCommitAndDataExists(Region<String, String> rgn) {
        assertThat(rgn.getEntry("key")).describedAs("Could not find entry for 'key'")
            .isNotNull();
        assertThat(rgn.getEntry("key").getValue()).isEqualTo("value");

        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
        tl.assertCounts();
        assertThat(tl.lastEvent.getCache()).isEqualTo(rgn.getRegionService());
        List<EntryEvent<?, ?>> events =
            TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents());
        assertThat(events.size()).isEqualTo(1);
        EntryEvent<?, ?> ev = events.get(0);
        assertThat(rgn).isSameAs(ev.getRegion());
        assertThat(ev.getKey()).isEqualTo("key");
        assertThat(ev.getNewValue()).isEqualTo("value");
        assertThat(ev.getOldValue()).isNull();
        assertThat(ev.getOperation().isLocalLoad()).isFalse();
        assertThat(ev.getOperation().isNetLoad()).isFalse();
        assertThat(ev.getOperation().isLoad()).isFalse();
        assertThat(ev.getOperation().isNetSearch()).isFalse();
        assertThat(ev.getOperation().isExpiration()).isFalse();
        assertThat(ev.getCallbackArgument()).isNull();
        assertThat(ev.isCallbackArgumentAvailable()).isTrue();
        assertThat(ev.isOriginRemote()).isTrue();
        assertThat(ev.getOperation().isDistributed()).isTrue();
      }
    });

    vm1.invoke("textTXRmtMirror: checkNoKey", new SerializableRunnable() {
      @Override
      public void run() {
        Region<String, String> rgn = getRootRegion().getSubregion(rgnName);

        if (!rgn.getAttributes().getScope().isAck()) {
          await().untilAsserted(() -> checkCommitAndNoData(rgn));
        } else {
          checkCommitAndNoData(rgn);
        }
      }

      private void checkCommitAndNoData(Region<String, String> rgn) {
        assertThat(rgn.getEntry("key")).isNull();
        CacheTransactionManager txMgr2 = getCache().getCacheTransactionManager();
        MyTransactionListener tl = firstTransactionListenerFrom(txMgr2);
        tl.assertCounts();
        assertThat(TxEventTestUtil.getCreateEvents(tl.lastEvent.getEvents())).isEmpty();
        assertThat(TxEventTestUtil.getPutEvents(tl.lastEvent.getEvents())).isEmpty();
        assertThat(tl.lastEvent.getCache()).isEqualTo(rgn.getRegionService());
      }
    });
  }

  //////////////////////////////////////////////////////////////////////////////////////
  // M E T H O D S F O R V E R S I O N I N G F O L L O W
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * return the region attributes for the given type of region. See
   * GemFireCache.getRegionAttributes(String).
   * Subclasses are expected to reimplement this method. See
   * DistributedAckRegionCCEDUnitTest.getRegionAttributes(String).
   */
  protected <K, V> RegionAttributes<K, V> getRegionAttributes(String type) {
    throw new IllegalStateException("subclass must reimplement this method");
  }

  /*
   * This test creates a server cache in vm0 and a peer cache in vm1. It then tests to see if GII
   * transferred tombstones to vm1 like it's supposed to. A client cache is created in vm2 and the
   * same sort of check is performed for register-interest.
   */

  protected void versionTestGIISendsTombstones() {

    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();

    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable() {
      @Override
      public void run() {
        RegionFactory<?, ?> f = getCache().createRegionFactory(getRegionAttributes());
        CCRegion = (LocalRegion) f.create(name);
        if (VM.getVMId() == 0) {
          CacheServer bridge = CCRegion.getCache().addCacheServer();
          bridge.setPort(serverPort);
          try {
            bridge.start();
          } catch (IOException ex) {
            fail("While creating bridge", ex);
          }
        }
      }
    };

    SerializableRunnable asserter = new SerializableRunnable() {
      @Override
      public void run() {
        RegionEntry entry = CCRegion.getRegionEntry("object2");
        assertThat(entry).isNotNull();
        assertThat(entry.isTombstone()).isTrue();
      }
    };

    vm0.invoke("Create Region", createRegion);
    vm0.invoke("create some tombstones", () -> {
      CCRegion.put("object1", "value1");
      CCRegion.put("object2", "value2");
      CCRegion.put("object3", "value3");
      CCRegion.destroy("object2");
    });

    try {
      vm0.invoke("ensure tombstone has been received", asserter);
      vm1.invoke("Create Region", createRegion);
      vm1.invoke("ensure tombstone has been received", asserter);

    } finally {
      disconnectAllFromDS();
    }
  }

  private void checkResults(Map... regionContents) {
    for (int i = 0; i < 10; i++) {
      String key = "cckey" + i;
      for (int j = 1; j < regionContents.length; j++) {
        assertThat(regionContents[j].get(key))
            .describedAs("region contents are not consistent for " + key)
            .isEqualTo(regionContents[j - 1].get(key));
      }

      for (int subi = 1; subi < 3; subi++) {
        String subkey = key + "-" + subi;
        if (regionContents[0].containsKey(subkey)) {
          for (int j = 1; j < regionContents.length; j++) {
            assertThat(regionContents[j].get(subkey)).describedAs(
                "region contents are not consistent for " + subkey)
                .isEqualTo(regionContents[j - 1].get(subkey));
          }
        } else {
          for (int j = 1; j < regionContents.length; j++) {
            assertThat(regionContents[j].containsKey(subkey)).isFalse();
          }

        }
      }
    }
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  protected void versionTestConcurrentEvents() throws Exception {
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable() {
      @Override
      public void run() {
        RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
        CCRegion = (LocalRegion) f.create(name);
      }
    };

    vm0.invoke("Create Region", createRegion);
    vm1.invoke("Create Region", createRegion);

    SerializableRunnable performOps = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            fail("sleep was interrupted");
          }
          doOpsLoop(false);
          long events = CCRegion.getCachePerfStats().getConflatedEventsCount();
          if (!CCRegion.getScope().isGlobal()) {
            assertThat(events > 0).describedAs("expected some event conflation").isTrue();
          }
        } catch (CacheException e) {
          fail("while performing concurrent operations", e);
        }
      }
    };

    AsyncInvocation a0 = vm0.invokeAsync("perform concurrent ops", performOps);
    AsyncInvocation a1 = vm1.invokeAsync("perform concurrent ops", performOps);

    vm2.invoke(createRegion);

    boolean a0failed = waitForAsyncProcessing(a0, "expected some event conflation");
    boolean a1failed = waitForAsyncProcessing(a1, "expected some event conflation");

    if (a0failed && a1failed) {
      fail("neither member saw event conflation - check stats for " + name);
    }

    await().untilAsserted(() -> {
      // check consistency of the regions
      Map r0Contents = vm0.invoke(MultiVMRegionTestCase::getCCRegionContents);
      Map r1Contents = vm1.invoke(MultiVMRegionTestCase::getCCRegionContents);
      Map r2Contents = vm2.invoke(MultiVMRegionTestCase::getCCRegionContents);

      checkResults(r0Contents, r1Contents, r2Contents);
    });
    // now we move on to testing deltas

    if (!getRegionAttributes().getScope().isDistributedNoAck()) { // no-ack doesn't support deltas
      vm0.invoke(MultiVMRegionTestCase::clearCCRegion);

      performOps = new SerializableRunnable() {
        @Override
        public void run() {
          try {
            long stopTime = System.currentTimeMillis() + 5000;
            Random ran = new Random(System.currentTimeMillis());
            while (System.currentTimeMillis() < stopTime) {
              for (int i = 0; i < 10; i++) {
                CCRegion.put("cckey" + i, new DeltaValue("ccvalue" + ran.nextInt()));
              }
            }
            long events = CCRegion.getCachePerfStats().getDeltaFailedUpdates();
            assertThat(events > 0).describedAs("expected some failed deltas").isTrue();
          } catch (CacheException e) {
            fail("while performing concurrent operations", e);
          }
        }
      };

      a0 = vm0.invokeAsync("perform concurrent delta ops", performOps);
      a1 = vm1.invokeAsync("perform concurrent delta ops", performOps);

      a0failed = waitForAsyncProcessing(a0, "expected some failed deltas");
      a1failed = waitForAsyncProcessing(a1, "expected some failed deltas");

      if (a0failed && a1failed) {
        fail("neither member saw failed deltas - check stats for " + name);
      }

      // check consistency of the regions
      await().untilAsserted(() -> {
        // check consistency of the regions
        Map r0Contents = vm0.invoke(MultiVMRegionTestCase::getCCRegionContents);
        Map r1Contents = vm1.invoke(MultiVMRegionTestCase::getCCRegionContents);
        Map r2Contents = vm2.invoke(MultiVMRegionTestCase::getCCRegionContents);

        checkResults(r0Contents, r1Contents, r2Contents);
      });

      // The region version vectors should now all be consistent with the version stamps in the
      // entries.

      List<RegionVersionVector<VersionSource<?>>> versionVectors = new ArrayList<>();
      List<InternalDistributedMember> vmIds = new ArrayList<>();
      List<Map<String, VersionTag<VersionSource<?>>>> versions = new ArrayList<>();

      for (VM vm : Arrays.asList(vm0, vm1, vm2)) {
        vmIds.add(vm.invoke(MultiVMRegionTestCase::getMemberId));
        versionVectors.add(getVersionVector(vm));
        versions.add(vm.invoke(MultiVMRegionTestCase::getCCRegionVersions));
      }

      for (int i = 0; i < versions.size(); i++) {
        compareVersions(versions.get(0), vmIds.get(0), vmIds, versionVectors);
      }
    }
  }

  private void compareVersions(Map<String, VersionTag<VersionSource<?>>> versions,
      InternalDistributedMember defaultID,
      List<InternalDistributedMember> vmIds,
      List<RegionVersionVector<VersionSource<?>>> versionVectors) {
    for (Map.Entry<String, VersionTag<VersionSource<?>>> entry : versions.entrySet()) {
      VersionTag<VersionSource<?>> tag = entry.getValue();
      tag.replaceNullIDs(defaultID);
      for (int i = 0; i < vmIds.size(); i++) {
        assertThat(versionVectors.get(i).contains(tag.getMemberID(), tag.getRegionVersion()))
            .describedAs(
                vmIds.get(i) + " should contain " + tag)
            .isTrue();
      }
    }
  }

  private RegionVersionVector<VersionSource<?>> getVersionVector(VM vm) throws Exception {
    byte[] serializedForm = vm.invoke(MultiVMRegionTestCase::getCCRegionVersionVector);
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serializedForm));
    return (RegionVersionVector<VersionSource<?>>) DataSerializer.readObject(dis);
  }

  private AsyncInvocation performOps4ClearWithConcurrentEvents(VM vm) {
    return vm.invokeAsync("perform concurrent ops", () -> {
      try {
        doOpsLoop(true);
      } catch (CacheException e) {
        fail("while performing concurrent operations", e);
      }
    });
  }

  protected void createRegionWithAttribute(VM vm, final String name, final boolean syncDiskWrite) {
    vm.invoke("Create Region", () -> {
      RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
      f.setDiskSynchronous(syncDiskWrite);
      CCRegion = (LocalRegion) f.create(name);
    });
  }

  protected void versionTestClearWithConcurrentEvents() {
    versionTestClearWithConcurrentEvents(true);
  }

  protected void versionTestClearWithConcurrentEventsAsync() {
    versionTestClearWithConcurrentEvents(false);
  }

  private void versionTestClearWithConcurrentEvents(boolean syncDiskWrite) {
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    createRegionWithAttribute(vm0, name, syncDiskWrite);
    createRegionWithAttribute(vm1, name, syncDiskWrite);

    AsyncInvocation a0 = performOps4ClearWithConcurrentEvents(vm0);
    AsyncInvocation a1 = performOps4ClearWithConcurrentEvents(vm1);

    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      fail("sleep was interrupted");
    }

    createRegionWithAttribute(vm2, name, syncDiskWrite);

    waitForAsyncProcessing(a0, "");
    waitForAsyncProcessing(a1, "");

    await().untilAsserted(() -> {
      // check consistency of the regions
      Map r0Contents = vm0.invoke(MultiVMRegionTestCase::getCCRegionContents);
      Map r1Contents = vm1.invoke(MultiVMRegionTestCase::getCCRegionContents);
      Map r2Contents = vm2.invoke(MultiVMRegionTestCase::getCCRegionContents);

      checkResults(r0Contents, r1Contents, r2Contents);
    });

    vm0.invoke(MultiVMRegionTestCase::assertNoClearTimeouts);
    vm1.invoke(MultiVMRegionTestCase::assertNoClearTimeouts);
    vm2.invoke(MultiVMRegionTestCase::assertNoClearTimeouts);
  }

  protected void versionTestClearOnNonReplicateWithConcurrentEvents() {
    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";

    Stream.of(vm0, vm1, vm2, vm3).forEach(vm -> {
      vm.invoke("Create Region", () -> {
        RegionFactory<?, ?> f;
        if (VM.getVMId() == 0) {
          f = getCache().createRegionFactory(
              getRegionAttributes(RegionShortcut.REPLICATE_PROXY.toString()));
        } else if (VM.getVMId() == 1) {
          f = getCache().createRegionFactory(getRegionAttributes());
          f.setDataPolicy(DataPolicy.NORMAL);
        } else {
          f = getCache().createRegionFactory(getRegionAttributes());
        }
        CCRegion = (LocalRegion) f.create(name);
      });
    });


    AsyncInvocation a0 = vm0.invokeAsync("perform concurrent ops", () -> {
      doOpsLoop(true);
      if (CCRegion.getScope().isDistributedNoAck()) {
        sendSerialMessageToAll(); // flush the ops
      }
    });

    AsyncInvocation a1 = vm1.invokeAsync("perform concurrent ops", () -> {

      doOpsLoop(true);
      if (CCRegion.getScope().isDistributedNoAck()) {
        sendSerialMessageToAll(); // flush the ops
      }
    });


    waitForAsyncProcessing(a0, "");
    waitForAsyncProcessing(a1, "");

    await().untilAsserted(() -> {
      Map r2Contents = vm2.invoke(MultiVMRegionTestCase::getCCRegionContents); // replicated
      Map r3Contents = vm3.invoke(MultiVMRegionTestCase::getCCRegionContents); // replicated

      assertThat(r2Contents.size()).isEqualTo(r3Contents.size());

      for (int i = 0; i < 10; i++) {
        String key = "cckey" + i;
        // because datapolicy=normal regions do not accept ops when an entry is not present
        // they may miss invalidates/creates/destroys that other VMs see while applying ops
        // that they originate that maybe should have been elided. For this reason we can't
        // guarantee their consistency and don't check for it here.
        assertThat(r3Contents.get(key)).describedAs("region contents are not consistent for " + key)
            .isEqualTo(
                r2Contents.get(key));
        for (int subi = 1; subi < 3; subi++) {
          String subkey = key + "-" + subi;
          if (r2Contents.containsKey(subkey)) {
            assertThat(r3Contents.get(subkey)).describedAs(
                "region contents are not consistent for " + subkey)
                .isEqualTo(r2Contents.get(subkey));
          }
        }
      }
    });
    // vm0 has no storage, but the others should have hit no timeouts waiting for
    // their region version vector to dominate the vector sent with the clear() operation.
    // If they did, then some ops were not properly recorded in the vector and something
    // is broken.
    vm1.invoke(MultiVMRegionTestCase::assertNoClearTimeouts);
    vm2.invoke(MultiVMRegionTestCase::assertNoClearTimeouts);
    vm3.invoke(MultiVMRegionTestCase::assertNoClearTimeouts);
  }

  private void checkCCRegionTombstoneCount(String msg, int expected) {
    int actual = CCRegion.getTombstoneCount();
    if (expected != actual) {
      assertThat(actual)
          .describedAs(msg + " region tombstone count was " + actual + " expected=" + expected
              + " TombstoneService=" + CCRegion.getCache().getTombstoneService())
          .isEqualTo(expected);
    }
  }

  private void waitForAllTombstonesToExpire(int initialTombstoneCount) {
    try {

      await()
          .until(() -> CCRegion.getTombstoneCount() == 0);
    } catch (ConditionTimeoutException timeout) {
      fail("Timed out waiting for all tombstones to expire.  There are now "
          + CCRegion.getTombstoneCount() + " tombstones left out of " + initialTombstoneCount
          + " initial tombstones. " + CCRegion.getCache().getTombstoneService(), timeout);
    }
  }

  protected void versionTestTombstones() {
    disconnectAllFromDS();
    final int numEntries = 100;

    // create replicated regions in VM 0 and 1, then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent
    final long oldServerTimeout = TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT;
    final long oldClientTimeout = TombstoneService.NON_REPLICATE_TOMBSTONE_TIMEOUT;
    final int oldExpiredTombstoneLimit = TombstoneService.EXPIRED_TOMBSTONE_LIMIT;
    final boolean oldIdleExpiration = TombstoneService.IDLE_EXPIRATION;
    final double oldLimit = TombstoneService.GC_MEMORY_THRESHOLD;
    final long oldMaxSleepTime = TombstoneService.MAX_SLEEP_TIME;
    try {
      SerializableRunnable setTimeout = new SerializableRunnable() {
        @Override
        public void run() {
          TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT = 1000;
          TombstoneService.NON_REPLICATE_TOMBSTONE_TIMEOUT = 900;
          TombstoneService.EXPIRED_TOMBSTONE_LIMIT = numEntries;
          TombstoneService.IDLE_EXPIRATION = true;
          TombstoneService.GC_MEMORY_THRESHOLD = 0; // turn this off so heap profile won't cause
          // test to fail
          TombstoneService.MAX_SLEEP_TIME = 500;
        }
      };
      vm0.invoke(setTimeout);
      vm1.invoke(setTimeout);
      final String name = this.getUniqueName() + "-CC";
      SerializableRunnable createRegion = new SerializableRunnable() {
        @Override
        public void run() {
          RegionFactory f = getCache().createRegionFactory(getRegionAttributes());
          CCRegion = (LocalRegion) f.create(name);
          for (int i = 0; i < numEntries; i++) {
            CCRegion.put("cckey" + i, "ccvalue");
          }
          if (CCRegion.getScope().isDistributedNoAck()) {
            sendSerialMessageToAll(); // flush the ops
          }
        }
      };

      vm0.invoke("Create Region", createRegion);
      vm1.invoke("Create Region", createRegion);

      vm0.invoke("destroy entries and check tombstone count", () -> {
        try {
          for (int i = 0; i < numEntries; i++) {
            CCRegion.destroy("cckey" + i);
            boolean condition1 = !CCRegion.containsKey("cckey" + i);
            assertThat(condition1).describedAs("entry should not exist").isTrue();
            boolean condition = !CCRegion.containsValueForKey("cckey" + i);
            assertThat(condition).describedAs("entry should not contain a value").isTrue();
          }
          checkCCRegionTombstoneCount("after destroys in this vm ", numEntries);
          boolean condition = !CCRegion.containsValue(Token.TOMBSTONE);
          assertThat(condition).describedAs("region should not contain a tombstone").isTrue();
          if (CCRegion.getScope().isDistributedNoAck()) {
            sendSerialMessageToAll(); // flush the ops
          }
        } catch (CacheException e) {
          fail("while performing destroy operations", e);
        }
      });

      vm1.invoke("check tombstone count(2)", () -> {
        checkCCRegionTombstoneCount("after destroys in other vm ", numEntries);
        try {
          waitForAllTombstonesToExpire(numEntries);
        } catch (ConditionTimeoutException e) {
          CCRegion.dumpBackingMap();
          logger.info("tombstone service state: " + CCRegion.getCache().getTombstoneService());
          throw e;
        }
      });

      vm0.invoke("create/destroy entries and check tombstone count", () -> {
        final int origCount = CCRegion.getTombstoneCount();
        try {
          waitForAllTombstonesToExpire(origCount);
          logger.debug("creating tombstones.  current count={}", CCRegion.getTombstoneCount());
          for (int i = 0; i < numEntries; i++) {
            CCRegion.create("cckey" + i, i);
            CCRegion.destroy("cckey" + i);
          }
          logger.debug("done creating tombstones.  current count={}",
              CCRegion.getTombstoneCount());
          checkCCRegionTombstoneCount("after create+destroy in this vm ", numEntries);
          assertThat(CCRegion.size()).isEqualTo(0);
          afterCreates = 0;
          AttributesMutator<Object, Object> m = attributesMutatorFor(CCRegion);
          m.addCacheListener(new CacheListenerAdapter<Object, Object>() {
            @Override
            public void afterCreate(EntryEvent<Object, Object> event) {
              afterCreates++;
            }
          });
          if (CCRegion.getScope().isDistributedNoAck()) {
            sendSerialMessageToAll(); // flush the ops
          }
        } catch (AssertionError e) {
          CCRegion.dumpBackingMap();
          logger.info("tombstone service state: " + CCRegion.getCache().getTombstoneService());
          throw e;
        } catch (CacheException e) {
          fail("while performing create/destroy operations", e);
        }
      });

      vm1.invoke("check tombstone count and install listener", () -> {
        checkCCRegionTombstoneCount("after create+destroy in other vm ", numEntries);
        afterCreates = 0;
        AttributesMutator<Object, Object> m = attributesMutatorFor(CCRegion);
        m.addCacheListener(new CacheListenerAdapter<Object, Object>() {
          @Override
          public void afterCreate(EntryEvent<Object, Object> event) {
            afterCreates++;
          }
        });
      });

      // Now check to see if tombstones are resurrected by a create.
      // The entries should be created okay and the callback should be afterCreate.
      // The tombstone count won't go down until the entries are swept, but then
      // the count should fall to zero.

      vm0.invoke("create entries and check afterCreate and tombstone count", () -> {
        try {
          for (int i = 0; i < numEntries; i++) {
            CCRegion.create("cckey" + i, i);
          }
          checkCCRegionTombstoneCount("after create in this vm", 0);
          assertThat(afterCreates).describedAs("expected " + numEntries + " afterCreates")
              .isEqualTo(
                  numEntries);
          assertThat(CCRegion.size()).isEqualTo(numEntries);
          if (CCRegion.getScope().isDistributedNoAck()) {
            sendSerialMessageToAll(); // flush the ops
          }

          await()
              .until(
                  () -> CCRegion.getCache().getTombstoneService()
                      .getScheduledTombstoneCount() == 0);

        } catch (CacheException e) {
          fail("while performing create operations", e);
        } catch (ConditionTimeoutException timeout) {
          fail("Timed out waiting for all scheduled tombstones to be removed.  There are now "
              + CCRegion.getCache().getTombstoneService().getScheduledTombstoneCount()
              + " tombstones left out of " + numEntries + " initial tombstones. "
              + CCRegion.getCache().getTombstoneService(), timeout);
        }
      });

      vm1.invoke("check afterCreate and tombstone count", () -> {
        checkCCRegionTombstoneCount("after create in other vm", 0);
        assertThat(afterCreates).describedAs("expected " + numEntries + " afterCreates")
            .isEqualTo(
                numEntries);
        assertThat(CCRegion.size()).isEqualTo(numEntries);
        try {
          await()
              .until(() -> CCRegion.getCache().getTombstoneService()
                  .getScheduledTombstoneCount() == 0);
        } catch (ConditionTimeoutException timeout) {
          String message =
              "Timed out waiting for all scheduled tombstones to be removed.  There are now "
                  + CCRegion.getCache().getTombstoneService().getScheduledTombstoneCount()
                  + " tombstones left out of " + numEntries + " initial tombstones. "
                  + CCRegion.getCache().getTombstoneService();
          fail(message, timeout);
        }
      });
    } finally {
      SerializableRunnable resetTimeout = new SerializableRunnable() {
        @Override
        public void run() {
          TombstoneService.REPLICATE_TOMBSTONE_TIMEOUT = oldServerTimeout;
          TombstoneService.NON_REPLICATE_TOMBSTONE_TIMEOUT = oldClientTimeout;
          TombstoneService.EXPIRED_TOMBSTONE_LIMIT = oldExpiredTombstoneLimit;
          TombstoneService.IDLE_EXPIRATION = oldIdleExpiration;
          TombstoneService.GC_MEMORY_THRESHOLD = oldLimit;
          TombstoneService.MAX_SLEEP_TIME = oldMaxSleepTime;
        }
      };
      vm0.invoke(resetTimeout);
      vm1.invoke(resetTimeout);
    }
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  protected void versionTestConcurrentEventsOnEmptyRegion() {
    // create an empty region in vm0 and replicated regions in VM 1 and 3,
    // then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable() {
      @Override
      public void run() {
        // set log-level to fine so we'll see InitialImageOperation keys in the log for
        // GEODE-6326
        System.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        final RegionFactory<?, ?> f;
        if (VM.getVMId() == 0) {
          f = getCache().createRegionFactory(
              getRegionAttributes(RegionShortcut.REPLICATE_PROXY.toString()));
        } else {
          if (VM.getVMId() == 2) {
            InitialImageOperation.slowImageProcessing = 1;
          }
          f = getCache().createRegionFactory(getRegionAttributes());
        }
        CCRegion = (LocalRegion) f.create(name);
      }
    };

    prepConcurrentEventsTest(name, createRegion);

    // For no-ack regions, messages may still be in flight between replicas at this point
    try {
      await("Wait for the members to eventually be consistent")
          .untilAsserted(() -> {
            // check consistency of the regions
            Map r1Contents = vm1.invoke(MultiVMRegionTestCase::getCCRegionContents);
            Map r2Contents = vm2.invoke(MultiVMRegionTestCase::getCCRegionContents);
            Map r3Contents = vm3.invoke(MultiVMRegionTestCase::getCCRegionContents);
            checkResults(r1Contents, r2Contents, r3Contents);
          });
    } finally {
      vm1.invoke("dump region contents", () -> CCRegion.dumpBackingMap());
      vm2.invoke("dump region contents", () -> {
        CCRegion.dumpBackingMap();
        InitialImageOperation.slowImageProcessing = 0;
      });
      vm3.invoke("dump region contents", () -> CCRegion.dumpBackingMap());
    }
  }

  private void prepConcurrentEventsTest(String name, SerializableRunnable createRegion) {
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    vm3.invoke(createRegion);

    SerializableRunnable performOps = new SerializableRunnable() {
      @Override
      public void run() {
        try {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            fail("sleep was interrupted");
          }
          doOpsLoop(false);
          // This send serial message is only going to flush the operations
          // from the empty member to *one* of the replicas, not all of them.
          sendSerialMessageToAll();

          if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
            long events = CCRegion.getCachePerfStats().getConflatedEventsCount();
            assertThat(events > 0).describedAs("expected some event conflation").isTrue();
          }
        } catch (CacheException e) {
          fail("while performing concurrent operations", e);
        }
      }
    };

    AsyncInvocation a0 = vm0.invokeAsync("perform concurrent ops", performOps);
    AsyncInvocation a1 = vm1.invokeAsync("perform concurrent ops", performOps);

    // try {
    // Thread.sleep(500);
    // } catch (InterruptedException e) {
    // fail("sleep was interrupted");
    // }

    vm2.invoke(createRegion);
    boolean a0failed = waitForAsyncProcessing(a0, "expected some event conflation");
    boolean a1failed = waitForAsyncProcessing(a1, "expected some event conflation");
    if (a0failed && a1failed) {
      fail("neither member saw event conflation - check stats for " + name);
    }
  }

  private void doOpsLoop(boolean includeClear) {
    doOpsLoopNoFlush(includeClear, true);
    if (CCRegion.getScope().isDistributedNoAck()) {
      sendSerialMessageToAll(); // flush the ops
    }
  }


  protected static void doOpsLoopNoFlush(boolean includeClear,
      boolean includePutAll) {
    long stopTime = System.currentTimeMillis() + 5000;
    Random ran = new Random(System.currentTimeMillis());
    String key = null;
    String value = null;
    String oldkey;
    String oldvalue;
    while (System.currentTimeMillis() < stopTime) {
      for (int i = 0; i < 10; i++) {
        oldkey = key;
        oldvalue = value;
        int v = ran.nextInt();
        key = "cckey" + i;
        value = "ccvalue" + v;
        try {
          switch (v & 0x7) {
            case 0:
              CCRegion.put(key, value);
              break;
            case 1:
              if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
                if (oldkey != null) {
                  CCRegion.replace(oldkey, oldvalue, value);
                }
                break;
              } // else fall through to putAll
              //$FALL-THROUGH$
            case 2:
              v = ran.nextInt();
              // too many putAlls make this test run too long
              if (includePutAll && (v & 0x7) < 3) {
                Map<String, String> map = new HashMap<>();
                map.put(key, value);
                map.put(key + "-1", value);
                map.put(key + "-2", value);
                CCRegion.putAll(map, "putAllCallback");
              } else {
                CCRegion.put(key, value);
              }
              break;
            case 3:
              if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
                // since we have a known key/value pair that was used before,
                // use the oldkey/oldvalue for remove(k,v)
                if (oldkey != null) {
                  CCRegion.remove(oldkey, oldvalue);
                }
                break;
              } // else fall through to destroy
              //$FALL-THROUGH$
            case 4:
              CCRegion.destroy(key);
              break;
            case 5:
              if (includeClear) {
                CCRegion.clear();
                break;
              } else {
                if (CCRegion.getAttributes().getDataPolicy().withReplication()) {
                  if (oldkey != null) {
                    CCRegion.putIfAbsent(oldkey, value);
                  }
                  break;
                } // else fall through to invalidate
              }
              //$FALL-THROUGH$
            case 6:
              CCRegion.invalidate(key);
              break;
          }
        } catch (EntryNotFoundException e) {
          // expected
        }
      }
    }
  }

  /**
   * This tests the concurrency versioning system to ensure that event conflation happens correctly
   * and that the statistic is being updated properly
   */
  protected void versionTestConcurrentEventsOnNonReplicatedRegion() {

    final boolean noAck = !getRegionAttributes().getScope().isAck();

    // create an empty region in vm0 and replicated regions in VM 1 and 3,
    // then perform concurrent ops
    // on the same key while creating the region in VM2. Afterward make
    // sure that all three regions are consistent

    final String name = this.getUniqueName() + "-CC";
    SerializableRunnable createRegion = new SerializableRunnable() {
      @Override
      public void run() {
        final RegionFactory<?, ?> f;
        if (VM.getVMId() == 0) {
          f = getCache()
              .createRegionFactory(getRegionAttributes(RegionShortcut.LOCAL.toString()));
          f.setScope(getRegionAttributes().getScope());
        } else {
          f = getCache().createRegionFactory(getRegionAttributes());
        }
        CCRegion = (LocalRegion) f.create(name);
      }
    };

    prepConcurrentEventsTest(name, createRegion);

    await().untilAsserted(() -> {
      // check consistency of the regions
      Map r0Contents = vm0.invoke(MultiVMRegionTestCase::getCCRegionContents);
      Map r1Contents = vm1.invoke(MultiVMRegionTestCase::getCCRegionContents);
      Map r2Contents = vm2.invoke(MultiVMRegionTestCase::getCCRegionContents);
      Map r3Contents = vm3.invoke(MultiVMRegionTestCase::getCCRegionContents);

      for (int i = 0; i < 10; i++) {
        String key = "cckey" + i;
        assertThat(r2Contents.get(key)).describedAs("region contents are not consistent").isEqualTo(
            r1Contents.get(key));
        assertThat(r3Contents.get(key)).describedAs("region contents are not consistent").isEqualTo(
            r2Contents.get(key));
        for (int subi = 1; subi < 3; subi++) {
          String subkey = key + "-" + subi;
          if (r1Contents.containsKey(subkey)) {
            assertThat(r2Contents.get(subkey)).describedAs("region contents are not consistent")
                .isEqualTo(
                    r1Contents.get(subkey));
            assertThat(r3Contents.get(subkey)).describedAs("region contents are not consistent")
                .isEqualTo(
                    r2Contents.get(subkey));
            if (r0Contents.containsKey(subkey)) {
              assertThat(r0Contents.get(subkey)).describedAs("region contents are not consistent")
                  .isEqualTo(
                      r1Contents.get(subkey));
            }
          } else {
            assertThat(r2Contents.containsKey(subkey)).isFalse();
            assertThat(r3Contents.containsKey(subkey)).isFalse();
            assertThat(r0Contents.containsKey(subkey)).isFalse();
          }
        }
      }
    });
    // a non-replicate region should not carry tombstones, so we'll check for that.
    // then we'll use a cache loader and make sure that 1-hop is used to put the
    // entry into the cache in a replicate. The new entry should have a version
    // of 1 when it's first created, the tombstone should have version 2 and
    // the loaded value should have version 3
    final String loadKey = "loadKey";
    vm0.invoke("add cache loader and create destroyed entry", () -> {
      AttributesMutator<Object, Object> attributesMutator = attributesMutatorFor(CCRegion);
      attributesMutator.setCacheLoader(new CacheLoader<Object, Object>() {
        @Override
        public void close() {}

        @Override
        public Object load(LoaderHelper<Object, Object> helper) throws CacheLoaderException {
          logger.info("The test CacheLoader has been invoked for key '" + helper.getKey() + "'");
          return "loadedValue";
        }
      });

      CCRegion.put(loadKey, "willbeLoadedInitialValue");
      CCRegion.destroy(loadKey);
      if (noAck) { // flush for validation to work
        sendSerialMessageToAll();
      }
      // this assertion guarantees that non-replicated regions do not create tombstones.
      // this is currently not the case but is an open issue
      // assertTrue(CCRegion.getRegionEntry(loadKey) == null)

    });
    vm1.invoke("confirm tombstone", () -> {
      assertThat(CCRegion.getRegionEntry(loadKey).getValueInVM(CCRegion)).isSameAs(
          Token.TOMBSTONE);
    });
    vm0.invoke("use cache loader", () -> {
      assertThat(CCRegion.get(loadKey)).isEqualTo("loadedValue");
      assertThat((CCRegion.getRegionEntry(loadKey)).getVersionStamp().getEntryVersion())
          .isEqualTo(
              (long) 3);
    });
    if (!noAck) { // might be 3 or 4 with no-ack
      vm1.invoke("verify version number", () -> {
        assertThat(CCRegion.get(loadKey)).isEqualTo("loadedValue");
        assertThat((CCRegion.getRegionEntry(loadKey)).getVersionStamp().getEntryVersion())
            .isEqualTo(
                (long) 3);
      });
    }
  }

  protected void versionTestGetAllWithVersions() {
    // this test has timing issues in no-ack
    assumeThat(getRegionAttributes().getScope().isAck()).isTrue();
    // scopes

    final String regionName = getUniqueName() + "CCRegion";

    vm0.invoke("Create Region", () -> {
      final RegionFactory<?, ?> f = getCache()
          .createRegionFactory(getRegionAttributes(RegionShortcut.LOCAL.toString()));
      f.setScope(getRegionAttributes().getScope());

      CCRegion = (LocalRegion) f.create(regionName);
    });
    vm1.invoke("Create Region", () -> {

      final RegionFactory<?, ?> f = getCache().createRegionFactory(getRegionAttributes());
      CCRegion = (LocalRegion) f.create(regionName);
    });

    vm1.invoke("Populate region and perform some ops", () -> {
      for (int i = 0; i < 100; i++) {
        CCRegion.put("cckey" + i, i);
      }
      for (int i = 0; i < 100; i++) {
        CCRegion.put("cckey" + i, i + 1);
      }
    });

    vm0.invoke("Perform getAll", () -> {
      List<String> keys = new LinkedList<>();
      for (int i = 0; i < 100; i++) {
        keys.add("cckey" + i);
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> result = (Map<String, Object>) CCRegion.getAll(keys);
      assertThat(keys.size()).isEqualTo(result.size());
      LocalRegion r = CCRegion;
      for (int i = 0; i < 100; i++) {
        RegionEntry entry = r.getRegionEntry("cckey" + i);
        int stamp = entry.getVersionStamp().getEntryVersion();
        logger.info("checking key cckey" + i + " having version " + stamp + " entry=" + entry);
        assertThat(stamp).isEqualTo(2);
        assertThat(i + 1).isEqualTo(result.get("cckey" + i));
      }
    });
  }

  boolean waitForAsyncProcessing(AsyncInvocation async, String expectedError) {
    try {
      async.await();
    } catch (Throwable e) {
      assertThat(hasCauseMessage(e, expectedError)).isTrue();
    }
    return false;
  }

  /**
   * The number of milliseconds to try repeating validation code in the event that AssertionError is
   * thrown. For ACK scopes, no repeat should be necessary.
   */
  protected long getRepeatTimeoutMs() {
    return 0;
  }

  private static void assertNoClearTimeouts() {
    // if there are timeouts on waiting for version vector dominance then
    // some operation was not properly recorded in the VM throwing this
    // assertion error. All ops need to be recorded in the version vector
    assertThat(CCRegion.getCachePerfStats().getClearTimeouts()).describedAs(
        "expected there to be no timeouts - something is broken").isEqualTo(0);
  }

  private static void clearCCRegion() {
    CCRegion.clear();
  }

  @SuppressWarnings("unchecked")
  public static Map getCCRegionContents() {
    return new HashMap<>(CCRegion);
  }

  /**
   * Since version vectors aren't java.io.Serializable we use DataSerializer to return a serialized
   * form of the vector
   */
  private static byte[] getCCRegionVersionVector() throws Exception {
    Object id = getMemberId();
    int vm = VM.getVMId();
    logger.info(
        "vm" + vm + " with id " + id + " copying " + CCRegion.getVersionVector().fullToString());
    RegionVersionVector vector = CCRegion.getVersionVector().getCloneForTransmission();
    logger.info("clone is " + vector);
    HeapDataOutputStream dos = new HeapDataOutputStream(3000, KnownVersion.CURRENT);
    DataSerializer.writeObject(vector, dos);
    byte[] bytes = dos.toByteArray();
    logger.info("serialized size is " + bytes.length);
    return bytes;
  }

  /**
   * returns a map of keys->versiontags for the test region
   */
  private static Map<String, VersionTag<VersionSource<?>>> getCCRegionVersions() {
    Map<String, VersionTag<VersionSource<?>>> result = new HashMap<>();
    Map<String, Object> regionAsMap = (Map<String, Object>) CCRegion;
    for (String key : regionAsMap.keySet()) {
      result.put(key, CCRegion.getRegionEntry(key).getVersionStamp().asVersionTag());
    }
    return result;
  }

  private static InternalDistributedMember getMemberId() {
    return CCRegion.getDistributionManager().getDistributionManagerId();
  }

  private void sendSerialMessageToAll() {
    if (getCache() instanceof GemFireCacheImpl) {
      try {
        org.apache.geode.distributed.internal.SerialAckedMessage msg =
            new org.apache.geode.distributed.internal.SerialAckedMessage();
        msg.send(getCache().getDistributionManager().getNormalDistributionManagerIds(), false);
      } catch (Exception e) {
        throw new RuntimeException("Unable to send serial message due to exception", e);
      }
    }
  }

  /**
   * a class for testing handling of concurrent delta operations
   */
  static class DeltaValue implements org.apache.geode.Delta, Serializable {

    private String value;

    @SuppressWarnings("unused") // Required for deserialization
    public DeltaValue() {}

    DeltaValue(String value) {
      this.value = value;
    }

    @Override
    public boolean hasDelta() {
      return true;
    }

    @Override
    public void toDelta(DataOutput out) throws IOException {
      out.writeUTF(this.value);
    }

    @Override
    public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
      this.value = in.readUTF();
    }

    @Override
    public int hashCode() {
      return this.value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof DeltaValue)) {
        return false;
      }
      return this.value.equals(((DeltaValue) obj).value);
    }

    @Override
    public String toString() {
      return this.value;
    }

  }

  @SuppressWarnings("unchecked")
  private static <L extends TransactionListener> L firstTransactionListenerFrom(
      CacheTransactionManager transactionManager) {
    TransactionListener[] listeners = transactionManager.getListeners();
    assertThat(listeners).describedAs("Listeners on transactionManager for transaction with id "
        + transactionManager.getTransactionId()).hasSize(1);
    return (L) listeners[0];
  }

  @SuppressWarnings("unchecked")
  private static <A extends AttributesMutator> A attributesMutatorFor(LocalRegion region) {
    return (A) region.getAttributesMutator();
  }

  /**
   * Decorates the given runnable to make it repeat every {@link #POLL_INTERVAL_MILLIS} until either
   * it terminates without throwing or the test's repeat timeout expires. If the test's repeat
   * timeout (obtained by calling {@link #getRepeatTimeoutMs()}) is less than or equal to {@code
   * POLL_INTERVAL_MILLIS}, the given runnable is returned undecorated.
   *
   * @param runnable the runnable to run
   */
  private SerializableRunnableIF repeatingIfNecessary(SerializableRunnableIF runnable) {
    return repeatingIfNecessary(getRepeatTimeoutMs(), runnable);
  }

  /**
   * Decorates the given runnable to make it repeat every {@link #POLL_INTERVAL_MILLIS} until either
   * it terminates without throwing or the given timeout expires. If the timeout is less than or
   * equal to {@code POLL_INTERVAL_MILLIS}, the given runnable is returned undecorated.
   *
   * @param timeoutMillis the maximum length of time (in milliseconds) to repeat the runnable
   * @param runnable the runnable to run
   */
  private static SerializableRunnableIF repeatingIfNecessary(long timeoutMillis,
      SerializableRunnableIF runnable) {
    if (timeoutMillis > POLL_INTERVAL_MILLIS) {
      return () -> await()
          .untilAsserted(runnable::run);
    }
    return runnable;
  }
}
