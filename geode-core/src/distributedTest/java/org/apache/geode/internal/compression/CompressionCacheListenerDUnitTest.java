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
package org.apache.geode.internal.compression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;

/**
 * Asserts that values received in EntryEvents for CacheWriters and CacheListeners are not
 * compressed.
 *
 */

public class CompressionCacheListenerDUnitTest extends JUnit4CacheTestCase {
  /**
   * The name of our test region.
   */
  public static final String REGION_NAME = "compressedRegion";

  /**
   * Test virtual machine number.
   */
  public static final int TEST_VM = 0;

  /**
   * A key.
   */
  public static final String KEY_1 = "key1";

  /**
   * Another key.
   */
  public static final String KEY_2 = "key2";

  /**
   * Yet another key.
   */
  public static final String KEY_3 = "key3";

  /**
   * A value.
   */
  public static final String VALUE_1 =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam auctor bibendum tempus. Suspendisse potenti. Ut enim neque, mattis et mattis ac, vulputate quis leo. Cras a metus metus, eget cursus ipsum. Aliquam sagittis condimentum massa aliquet rhoncus. Aliquam sed luctus neque. In hac habitasse platea dictumst.";

  /**
   * Another value.
   */
  private static final String VALUE_2 =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent sit amet lorem consequat est commodo lacinia. Duis tortor sem, facilisis quis tempus in, luctus lacinia metus. Vivamus augue justo, porttitor in vulputate accumsan, adipiscing sit amet sem. Quisque faucibus porta ipsum in pellentesque. Donec malesuada ultrices sapien sit amet tempus. Sed fringilla ipsum at tellus condimentum et hendrerit arcu pretium. Nulla non leo ligula. Etiam commodo tempor ligula non placerat. Vivamus vestibulum varius arcu a varius. Duis sit amet erat imperdiet dui mattis auctor et id orci. Suspendisse non elit augue. Quisque ac orci turpis, nec sollicitudin justo. Sed bibendum justo ut lacus aliquet lacinia et et neque. Proin hendrerit varius mauris vel lacinia. Proin pellentesque lacus vitae nisl euismod bibendum.";

  /**
   * Yet another value.
   */
  private static final String VALUE_3 =
      "In ut nisi nisi, eu malesuada mauris. Vestibulum nec tellus felis. Pellentesque mauris ligula, pretium nec consequat ut, adipiscing non lorem. Vivamus pulvinar viverra nisl, sit amet vestibulum tellus lobortis in. Pellentesque blandit ipsum sed neque rhoncus eu tristique risus porttitor. Vivamus molestie dapibus mi in lacinia. Suspendisse bibendum, purus at gravida accumsan, libero turpis elementum leo, eget posuere purus nibh ac dolor.";

  /**
   * Queues events received by the CacheListener.
   */
  public static final BlockingQueue<EntryEvent> LISTENER_QUEUE =
      new LinkedBlockingQueue<>(1);

  /**
   * A CacheListener that simply stores received events in a queue for evaluating.
   */
  private static final CacheListener<String, String> CACHE_LISTENER =
      new CacheListenerAdapter<String, String>() {
        @Override
        public void afterCreate(EntryEvent<String, String> event) {
          EntryEventImpl copy = new EntryEventImpl((EntryEventImpl) event);
          copy.copyOffHeapToHeap();
          LISTENER_QUEUE.add(copy);
        }

        @Override
        public void afterDestroy(EntryEvent<String, String> event) {
          EntryEventImpl copy = new EntryEventImpl((EntryEventImpl) event);
          copy.copyOffHeapToHeap();
          LISTENER_QUEUE.add(copy);
        }

        @Override
        public void afterInvalidate(EntryEvent<String, String> event) {
          EntryEventImpl copy = new EntryEventImpl((EntryEventImpl) event);
          copy.copyOffHeapToHeap();
          LISTENER_QUEUE.add(copy);
        }

        @Override
        public void afterUpdate(EntryEvent<String, String> event) {
          EntryEventImpl copy = new EntryEventImpl((EntryEventImpl) event);
          copy.copyOffHeapToHeap();
          LISTENER_QUEUE.add(copy);
        }
      };

  /**
   * A queue for storing events received by a CacheWriter.
   */
  public static final BlockingQueue<EntryEvent> WRITER_QUEUE =
      new LinkedBlockingQueue<>(1);

  /**
   * A CacheWriter that simply stores received events in a queue for evaluation.
   */
  private static final CacheWriter<String, String> CACHE_WRITER =
      new CacheWriterAdapter<String, String>() {
        @Override
        public void beforeCreate(EntryEvent<String, String> event) {
          EntryEventImpl copy = new EntryEventImpl((EntryEventImpl) event);
          copy.copyOffHeapToHeap();
          WRITER_QUEUE.add(copy);
        }

        @Override
        public void beforeDestroy(EntryEvent<String, String> event) {
          EntryEventImpl copy = new EntryEventImpl((EntryEventImpl) event);
          copy.copyOffHeapToHeap();
          WRITER_QUEUE.add(copy);
        }

        @Override
        public void beforeUpdate(EntryEvent<String, String> event) {
          EntryEventImpl copy = new EntryEventImpl((EntryEventImpl) event);
          copy.copyOffHeapToHeap();
          WRITER_QUEUE.add(copy);
        }
      };

  /**
   * Creates a new CompressionCacheListenerDUnitTest.
   *
   * @param name a test name.
   */
  public CompressionCacheListenerDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
    createRegion();
  }

  protected void createRegion() {
    createCompressedRegionOnVm(getVM(TEST_VM), REGION_NAME, new SnappyCompressor());
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    preTearDownCompressionCacheListenerDUnitTest();
    cleanup(getVM(TEST_VM));
  }

  protected void preTearDownCompressionCacheListenerDUnitTest() throws Exception {}

  /**
   * Returns the VM for a given identifier.
   *
   * @param vm a virtual machine identifier.
   */
  protected VM getVM(int vm) {
    return Host.getHost(0).getVM(vm);
  }

  /**
   * Removes created regions from a VM.
   *
   * @param vm the virtual machine to cleanup.
   */
  private void cleanup(final VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        getCache().getRegion(REGION_NAME).destroyRegion();
      }
    });
  }

  /**
   * Tests CacheWriter and CacheListener events on the test vm.
   */
  @Test
  public void testCacheListenerAndWriter() {
    testCacheListenerAndWriterWithVM(getVM(TEST_VM));
  }

  /**
   * Tests that received values in EntryEvents are not compressed for the following methods:
   *
   * <ul>
   * <li>{@link CacheWriter#beforeCreate(EntryEvent)}</li>
   * <li>{@link CacheListener#afterCreate(EntryEvent)}</li>
   * <li>{@link CacheWriter#beforeUpdate(EntryEvent)}</li>
   * <li>{@link CacheListener#afterUpdate(EntryEvent)}</li>
   * <li>{@link CacheListener#afterInvalidate(EntryEvent)}</li>
   * <li>{@link CacheWriter#beforeDestroy(EntryEvent)}</li>
   * <li>{@link CacheListener#afterDestroy(EntryEvent)}</li>
   * </ul>
   *
   * @param vm a virtual machine to perform the test on.
   */
  private void testCacheListenerAndWriterWithVM(final VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Region<String, String> region = getCache().getRegion(REGION_NAME);
          assertNotNull(region);
          assertNull(region.get(KEY_1));

          // beforeCreate
          String oldValue = region.put(KEY_1, VALUE_1);
          EntryEvent<String, String> event = WRITER_QUEUE.poll(5, TimeUnit.SECONDS);
          assertNotNull(event);
          assertNull(oldValue);
          assertNull(event.getOldValue());
          assertEquals(VALUE_1, event.getNewValue());
          assertEquals(KEY_1, event.getKey());

          // afterCreate
          event = LISTENER_QUEUE.poll(5, TimeUnit.SECONDS);
          assertNotNull(event);
          assertNull(event.getOldValue());
          assertEquals(VALUE_1, event.getNewValue());
          assertEquals(KEY_1, event.getKey());

          // beforeUpdate
          oldValue = region.put(KEY_1, VALUE_2);
          event = WRITER_QUEUE.poll(5, TimeUnit.SECONDS);
          assertNotNull(event);
          assertNotNull(oldValue);
          assertEquals(VALUE_1, oldValue);
          assertEquals(VALUE_1, event.getOldValue());
          assertEquals(VALUE_2, event.getNewValue());
          assertEquals(KEY_1, event.getKey());

          // afterUpdate
          event = LISTENER_QUEUE.poll(5, TimeUnit.SECONDS);
          assertNotNull(event);
          assertEquals(VALUE_1, event.getOldValue());
          assertEquals(VALUE_2, event.getNewValue());
          assertEquals(KEY_1, event.getKey());

          // afterInvalidate
          region.invalidate(KEY_1);
          event = LISTENER_QUEUE.poll(5, TimeUnit.SECONDS);
          assertNotNull(event);
          assertEquals(VALUE_2, event.getOldValue());
          assertNull(event.getNewValue());
          assertEquals(KEY_1, event.getKey());

          // beforeDestroy
          oldValue = region.destroy(KEY_1);
          event = WRITER_QUEUE.poll(5, TimeUnit.SECONDS);
          assertNull(oldValue);
          assertNotNull(event);
          assertNull(event.getOldValue());
          assertNull(event.getNewValue());
          assertEquals(KEY_1, event.getKey());

          // afterDestroy
          event = LISTENER_QUEUE.poll(5, TimeUnit.SECONDS);
          assertNotNull(event);
          assertNull(event.getOldValue());
          assertNull(event.getNewValue());
          assertEquals(KEY_1, event.getKey());
        } catch (InterruptedException e) {
          fail();
        }
      }
    });
  }

  /**
   * Creates a region and assigns a compressor.
   *
   * @param vm a virtual machine to create the region on.
   * @param name a region name.
   * @param compressor a compressor.
   */
  private void createCompressedRegionOnVm(final VM vm, final String name,
      final Compressor compressor) {
    createCompressedRegionOnVm(vm, name, compressor, false);
  }

  protected void createCompressedRegionOnVm(final VM vm, final String name,
      final Compressor compressor, final boolean offHeap) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        createRegion(name, compressor, offHeap);
        return Boolean.TRUE;
      }
    });
  }

  /**
   * Creates a region and assigns a compressor.
   *
   * @param name a region name.
   * @param compressor a compressor.
   */
  private Region createRegion(String name, Compressor compressor, boolean offHeap) {
    return getCache().<String, String>createRegionFactory().addCacheListener(CACHE_LISTENER)
        .setCacheWriter(CACHE_WRITER).setDataPolicy(DataPolicy.REPLICATE).setCloningEnabled(true)
        .setCompressor(compressor).setOffHeap(offHeap).create(name);
  }
}
