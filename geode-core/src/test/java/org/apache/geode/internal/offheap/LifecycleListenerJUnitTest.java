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
package org.apache.geode.internal.offheap;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;


/**
 * Tests LifecycleListener
 */
public class LifecycleListenerJUnitTest {

  private final List<LifecycleListenerCallback> afterCreateCallbacks =
      new ArrayList<LifecycleListenerCallback>();
  private final List<LifecycleListenerCallback> afterReuseCallbacks =
      new ArrayList<LifecycleListenerCallback>();
  private final List<LifecycleListenerCallback> beforeCloseCallbacks =
      new ArrayList<LifecycleListenerCallback>();
  private final TestLifecycleListener listener = new TestLifecycleListener(
      afterCreateCallbacks, afterReuseCallbacks, beforeCloseCallbacks);

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @After
  public void tearDown() throws Exception {
    LifecycleListener.removeLifecycleListener(listener);
    afterCreateCallbacks.clear();
    afterReuseCallbacks.clear();
    beforeCloseCallbacks.clear();
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  @Test
  public void testAddRemoveListener() {
    LifecycleListener.addLifecycleListener(listener);
    LifecycleListener.removeLifecycleListener(listener);

    SlabImpl slab = new SlabImpl(1024); // 1k
    MemoryAllocatorImpl ma = MemoryAllocatorImpl.createForUnitTest(
        new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new SlabImpl[] {slab});

    assertEquals(0, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(0, beforeCloseCallbacks.size());

    ma.close();

    assertEquals(0, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(0, beforeCloseCallbacks.size());

    LifecycleListener.removeLifecycleListener(listener);
  }

  @Test
  public void testCallbacksAreCalledAfterCreate() {
    LifecycleListener.addLifecycleListener(listener);
    SlabImpl slab = new SlabImpl(1024); // 1k
    MemoryAllocatorImpl ma = MemoryAllocatorImpl.createForUnitTest(
        new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new SlabImpl[] {slab});

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(0, beforeCloseCallbacks.size());

    closeAndFree(ma);

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(1, beforeCloseCallbacks.size());

    LifecycleListener.removeLifecycleListener(listener);
  }

  @Test
  public void testCallbacksAreCalledAfterReuse() {
    LifecycleListener.addLifecycleListener(listener);

    System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "false");

    SlabImpl slab = new SlabImpl(1024); // 1k
    MemoryAllocatorImpl ma = createAllocator(new NullOutOfOffHeapMemoryListener(),
        new NullOffHeapMemoryStats(), new SlabImpl[] {slab});

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(0, beforeCloseCallbacks.size());

    ma.close();

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(1, beforeCloseCallbacks.size());

    ma = createAllocator(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), null);

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(1, afterReuseCallbacks.size());
    assertEquals(1, beforeCloseCallbacks.size());

    MemoryAllocatorImpl ma2 = createAllocator(new NullOutOfOffHeapMemoryListener(),
        new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
    assertEquals(null, ma2);

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(1, afterReuseCallbacks.size());
    assertEquals(1, beforeCloseCallbacks.size());

    ma.close();

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(1, afterReuseCallbacks.size());
    assertEquals(2, beforeCloseCallbacks.size());
  }

  private MemoryAllocatorImpl createAllocator(OutOfOffHeapMemoryListener ooohml,
      OffHeapMemoryStats ohms, SlabImpl[] slab) {
    try {
      return MemoryAllocatorImpl.createForUnitTest(ooohml, ohms, slab);
    } catch (IllegalStateException e) {
      return null;
    }
  }

  private void closeAndFree(MemoryAllocatorImpl ma) {
    System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "true");
    try {
      ma.close();
    } finally {
      System.clearProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY);
    }
  }

  @Test
  public void testCallbacksAreCalledAfterReuseWithFreeTrue() {
    LifecycleListener.addLifecycleListener(listener);

    SlabImpl slab = new SlabImpl(1024); // 1k
    MemoryAllocatorImpl ma = MemoryAllocatorImpl.createForUnitTest(
        new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new SlabImpl[] {slab});

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(0, beforeCloseCallbacks.size());

    closeAndFree(ma);

    assertEquals(1, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(1, beforeCloseCallbacks.size());

    slab = new SlabImpl(1024); // 1k
    MemoryAllocatorImpl ma2 = MemoryAllocatorImpl.createForUnitTest(
        new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new SlabImpl[] {slab});

    assertEquals(2, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(1, beforeCloseCallbacks.size());

    closeAndFree(ma);

    assertEquals(2, afterCreateCallbacks.size());
    assertEquals(0, afterReuseCallbacks.size());
    assertEquals(2, beforeCloseCallbacks.size());
  }

  private static class LifecycleListenerCallback {
    private final MemoryAllocatorImpl allocator;
    private final long timeStamp;
    private final Throwable creationTime;

    LifecycleListenerCallback(MemoryAllocatorImpl allocator) {
      this.allocator = allocator;
      timeStamp = System.currentTimeMillis();
      creationTime = new Exception();
    }
  }

  private static class TestLifecycleListener implements LifecycleListener {
    private final List<LifecycleListenerCallback> afterCreateCallbacks;
    private final List<LifecycleListenerCallback> afterReuseCallbacks;
    private final List<LifecycleListenerCallback> beforeCloseCallbacks;

    TestLifecycleListener(List<LifecycleListenerCallback> afterCreateCallbacks,
        List<LifecycleListenerCallback> afterReuseCallbacks,
        List<LifecycleListenerCallback> beforeCloseCallbacks) {
      this.afterCreateCallbacks = afterCreateCallbacks;
      this.afterReuseCallbacks = afterReuseCallbacks;
      this.beforeCloseCallbacks = beforeCloseCallbacks;
    }

    @Override
    public void afterCreate(MemoryAllocatorImpl allocator) {
      afterCreateCallbacks.add(new LifecycleListenerCallback(allocator));
    }

    @Override
    public void afterReuse(MemoryAllocatorImpl allocator) {
      afterReuseCallbacks.add(new LifecycleListenerCallback(allocator));
    }

    @Override
    public void beforeClose(MemoryAllocatorImpl allocator) {
      beforeCloseCallbacks.add(new LifecycleListenerCallback(allocator));
    }
  }
}
