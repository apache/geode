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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class AbstractRegionMapTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void invalidateOfNonExistentRegionThrowsEntryNotFound() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm.owner);
    when(arm.owner.isInitialized()).thenReturn(true);

    try {
      arm.invalidate(event, true, false, false);
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    verify(arm.owner, never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm.owner, never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }
  
  @Test
  public void invalidateOfNonExistentRegionThrowsEntryNotFoundWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
      EntryEventImpl event = createEventForInvalidate(arm.owner);
      when(arm.owner.isInitialized()).thenReturn(true);

      try {
        arm.invalidate(event, true, false, false);
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }
      verify(arm.owner, never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
      verify(arm.owner, times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }
  
  @Test
  public void invalidateOfAlreadyInvalidEntryReturnsFalse() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm.owner);
    
    // invalidate on region that is not initialized should create
    // entry in map as invalid.
    try {
      arm.invalidate(event, true, false, false);
      fail("expected EntryNotFoundException");
    } catch (EntryNotFoundException expected) {
    }
    
    when(arm.owner.isInitialized()).thenReturn(true);
    assertFalse(arm.invalidate(event, true, false, false));
    verify(arm.owner, never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm.owner, never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateOfAlreadyInvalidEntryReturnsFalseWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
      EntryEventImpl event = createEventForInvalidate(arm.owner);

      // invalidate on region that is not initialized should create
      // entry in map as invalid.
      try {
        arm.invalidate(event, true, false, false);
        fail("expected EntryNotFoundException");
      } catch (EntryNotFoundException expected) {
      }

      when(arm.owner.isInitialized()).thenReturn(true);
      assertFalse(arm.invalidate(event, true, false, false));
      verify(arm.owner, never()).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
      verify(arm.owner, times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }

  private EntryEventImpl createEventForInvalidate(LocalRegion lr) {
    Object key = "key";
    when(lr.getKeyInfo(key)).thenReturn(new KeyInfo(key, null, null));
    return EntryEventImpl.create(lr, Operation.INVALIDATE, key, false, null, true, false);
  }
  
  @Test
  public void invalidateForceNewEntryOfAlreadyInvalidEntryReturnsFalse() {
    TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
    EntryEventImpl event = createEventForInvalidate(arm.owner);

    // invalidate on region that is not initialized should create
    // entry in map as invalid.
    assertTrue(arm.invalidate(event, true, true, false));
    verify(arm.owner, times(1)).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    
    when(arm.owner.isInitialized()).thenReturn(true);
    assertFalse(arm.invalidate(event, true, true, false));
    verify(arm.owner, times(1)).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
    verify(arm.owner, never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());
  }

  @Test
  public void invalidateForceNewEntryOfAlreadyInvalidEntryReturnsFalseWithForce() {
    AbstractRegionMap.FORCE_INVALIDATE_EVENT = true;
    try {
      TestableAbstractRegionMap arm = new TestableAbstractRegionMap();
      EntryEventImpl event = createEventForInvalidate(arm.owner);

      // invalidate on region that is not initialized should create
      // entry in map as invalid.
      assertTrue(arm.invalidate(event, true, true, false));
      verify(arm.owner, times(1)).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
      verify(arm.owner, never()).invokeInvalidateCallbacks(any(), any(), anyBoolean());

      when(arm.owner.isInitialized()).thenReturn(true);
      assertFalse(arm.invalidate(event, true, true, false));
      verify(arm.owner, times(1)).basicInvalidatePart2(any(), any(), anyBoolean(), anyBoolean());
      verify(arm.owner, times(1)).invokeInvalidateCallbacks(any(), any(), anyBoolean());
    } finally {
      AbstractRegionMap.FORCE_INVALIDATE_EVENT = false;
    }
  }

  public static class TestableAbstractRegionMap extends AbstractRegionMap {
    public LocalRegion owner;

    protected TestableAbstractRegionMap() {
      super(null);
      this.owner = mock(LocalRegion.class);
      when(this.owner.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
      doThrow(EntryNotFoundException.class).when(this.owner).checkEntryNotFound(any());
      initialize(owner, new Attributes(), null, false);
    }
  }
}
