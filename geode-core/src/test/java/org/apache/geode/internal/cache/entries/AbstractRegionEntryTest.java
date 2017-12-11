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
package org.apache.geode.internal.cache.entries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.offheap.OffHeapMemoryStats;
import org.apache.geode.internal.offheap.OutOfOffHeapMemoryListener;
import org.apache.geode.internal.offheap.SlabImpl;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.HashEntry;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractRegionEntryTest {

  @Test
  public void whenMakeTombstoneHasSetValueThatThrowsExceptionDoesNotChangeValueToTombstone()
      throws RegionClearedException {
    LocalRegion lr = mock(LocalRegion.class);
    RegionVersionVector<?> rvv = mock(RegionVersionVector.class);
    when(lr.getVersionVector()).thenReturn(rvv);
    VersionTag<?> vt = mock(VersionTag.class);
    Object value = "value";
    AbstractRegionEntry re = new TestableRegionEntry(lr, value);
    assertEquals(value, re.getValueField());
    Assertions.assertThatThrownBy(() -> re.makeTombstone(lr, vt))
        .isInstanceOf(RuntimeException.class).hasMessage("throw exception on setValue(TOMBSTONE)");
    Assert.assertEquals(Token.REMOVED_PHASE2, re.getValueField());
  }



  @Test
  public void whenPrepareValueForCacheCalledWithOffHeapEntryHasNewCachedSerializedValue()
      throws RegionClearedException, IOException, ClassNotFoundException {
    LocalRegion lr = mock(LocalRegion.class);
    RegionEntryContext regionEntryContext = mock(RegionEntryContext.class);
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);
    SlabImpl slab = new SlabImpl(1024); // 1k
    MemoryAllocatorImpl ma =
        MemoryAllocatorImpl.createForUnitTest(ooohml, stats, new SlabImpl[] {slab});
    try {
      when(regionEntryContext.getOffHeap()).thenReturn(true);
      String value = "value";
      AbstractRegionEntry re = new TestableRegionEntry(lr, value);
      assertEquals(value, re.getValueField());
      EntryEventImpl entryEvent = new EntryEventImpl();
      StoredObject valueForCache =
          (StoredObject) re.prepareValueForCache(regionEntryContext, value, entryEvent, true);
      final byte[] cachedSerializedNewValue = entryEvent.getCachedSerializedNewValue();
      assertNotNull(cachedSerializedNewValue);
      valueForCache.checkDataEquals(cachedSerializedNewValue);
      DataInputStream dataInputStream =
          new DataInputStream(new ByteArrayInputStream(cachedSerializedNewValue));
      Object o = DataSerializer.readObject(dataInputStream);
      assertEquals(o, value);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  public static class TestableRegionEntry extends AbstractRegionEntry
      implements OffHeapRegionEntry {

    private Object value;

    protected TestableRegionEntry(RegionEntryContext context, Object value) {
      super(context, value);
    }

    @Override
    protected Object getValueField() {
      return this.value;
    }

    @Override
    protected void setValueField(Object v) {
      this.value = v;
    }

    @Override
    public void setValue(RegionEntryContext context, @Unretained Object value)
        throws RegionClearedException {
      super.setValue(context, value);
      if (value == Token.TOMBSTONE) {
        throw new RuntimeException("throw exception on setValue(TOMBSTONE)");
      }
    }

    @Override
    public int getEntryHash() {
      return 0;
    }

    @Override
    public HashEntry<Object, Object> getNextEntry() {
      return null;
    }

    @Override
    public void setNextEntry(HashEntry<Object, Object> n) {}

    @Override
    public Object getKey() {
      return null;
    }

    @Override
    protected long getLastModifiedField() {
      return 0;
    }

    @Override
    protected boolean compareAndSetLastModifiedField(long expectedValue, long newValue) {
      return false;
    }

    @Override
    protected void setEntryHash(int v) {}

    @Override
    public void release() {

    }

    @Override
    public long getAddress() {
      return 0;
    }

    @Override
    public boolean setAddress(long expectedAddr, long newAddr) {
      return false;
    }
  }
}
