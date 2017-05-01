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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionTag;
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
    assertEquals(Token.REMOVED_PHASE2, re.getValueField());
  }


  public static class TestableRegionEntry extends AbstractRegionEntry {

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

  }
}
