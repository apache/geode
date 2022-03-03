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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.VMCachedDeserializable;

public class OffHeapHelperJUnitTest extends AbstractStoredObjectTestBase {

  private StoredObject storedObject = null;
  private Object deserializedRegionEntryValue = null;
  private byte[] serializedRegionEntryValue = null;
  private MemoryAllocator ma;

  @Before
  public void setUp() {
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);

    ma = MemoryAllocatorImpl.create(ooohml, stats, 3, OffHeapStorage.MIN_SLAB_SIZE * 3,
        OffHeapStorage.MIN_SLAB_SIZE);
  }

  /**
   * Extracted from JUnit setUp() to reduce test overhead for cases where offheap memory isn't
   * needed.
   */
  private void allocateOffHeapSerialized() {
    Object regionEntryValue = getValue();
    storedObject = createValueAsSerializedStoredObject(regionEntryValue);
    deserializedRegionEntryValue = storedObject.getValueAsDeserializedHeapObject();
    serializedRegionEntryValue = storedObject.getSerializedValue();
  }

  private void allocateOffHeapDeserialized() {
    Object regionEntryValue = getValue();
    storedObject = createValueAsUnserializedStoredObject(regionEntryValue);
    deserializedRegionEntryValue = storedObject.getValueAsDeserializedHeapObject();
    serializedRegionEntryValue = storedObject.getSerializedValue();
  }

  @After
  public void tearDown() {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  @Override
  public Object getValue() {
    return Long.MAX_VALUE;
  }

  @Override
  protected byte[] getValueAsByteArray() {
    return convertValueToByteArray(getValue());
  }

  private byte[] convertValueToByteArray(Object value) {
    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) value).array();
  }

  @Override
  protected Object convertByteArrayToObject(byte[] valueInByteArray) {
    return ByteBuffer.wrap(valueInByteArray).getLong();
  }

  @Override
  protected Object convertSerializedByteArrayToObject(byte[] valueInSerializedByteArray) {
    return EntryEventImpl.deserialize(valueInSerializedByteArray);
  }

  @Override
  protected StoredObject createValueAsUnserializedStoredObject(Object value) {
    byte[] valueInByteArray;
    if (value instanceof Long) {
      valueInByteArray = convertValueToByteArray(value);
    } else {
      valueInByteArray = (byte[]) value;
    }

    boolean isSerialized = false;
    boolean isCompressed = false;

    StoredObject createdObject = createChunk(valueInByteArray, isSerialized, isCompressed);
    return createdObject;
  }

  @Override
  protected StoredObject createValueAsSerializedStoredObject(Object value) {
    byte[] valueInSerializedByteArray = EntryEventImpl.serialize(value);

    boolean isSerialized = true;
    boolean isCompressed = false;

    StoredObject createdObject =
        createChunk(valueInSerializedByteArray, isSerialized, isCompressed);
    return createdObject;
  }

  private OffHeapStoredObject createChunk(byte[] v, boolean isSerialized, boolean isCompressed) {
    OffHeapStoredObject chunk =
        (OffHeapStoredObject) ma.allocateAndInitialize(v, isSerialized, isCompressed);
    return chunk;
  }

  @Test
  public void getHeapFormOfSerializedStoredObjectReturnsDeserializedObject() {
    allocateOffHeapSerialized();
    Object heapObject = OffHeapHelper.getHeapForm(storedObject);
    assertThat("getHeapForm returns non-null object", heapObject, notNullValue());
    assertThat("Heap and off heap objects are different objects", heapObject,
        is(not(storedObject)));
    assertThat("Deserialzed values of offHeap object and returned object are equal", heapObject,
        is(equalTo(deserializedRegionEntryValue)));
  }

  @Test
  public void getHeapFormOfNonOffHeapObjectReturnsOriginal() {
    Object testObject = getValue();
    Object heapObject = OffHeapHelper.getHeapForm(testObject);
    assertNotNull(heapObject);
    assertSame(testObject, heapObject);
  }

  @Test
  public void getHeapFormWithNullReturnsNull() {
    Object testObject = null;
    Object returnObject = OffHeapHelper.getHeapForm(testObject);
    assertThat(returnObject, is(equalTo(null)));
  }

  @Test
  public void copyAndReleaseWithNullReturnsNull() {
    Object testObject = null;
    Object returnObject = OffHeapHelper.copyAndReleaseIfNeeded(testObject, null);
    assertThat(returnObject, is(equalTo(null)));
  }

  @Test
  public void copyAndReleaseWithRetainedDeserializedObjectDecreasesRefCnt() {
    allocateOffHeapDeserialized();
    assertTrue(storedObject.retain());
    assertThat("Retained chunk ref count", storedObject.getRefCount(), is(2));
    OffHeapHelper.copyAndReleaseIfNeeded(storedObject, null);
    assertThat("Chunk ref count decreases", storedObject.getRefCount(), is(1));
  }

  @Test
  public void copyAndReleaseWithNonRetainedObjectDecreasesRefCnt() {
    allocateOffHeapDeserialized();
    // assertTrue(storedObject.retain());
    assertThat("Retained chunk ref count", storedObject.getRefCount(), is(1));
    OffHeapHelper.copyAndReleaseIfNeeded(storedObject, null);
    assertThat("Chunk ref count decreases", storedObject.getRefCount(), is(0));
  }

  @Test
  public void copyAndReleaseWithDeserializedReturnsValueOfOriginal() {
    allocateOffHeapDeserialized();
    assertTrue(storedObject.retain());
    Object returnObject = OffHeapHelper.copyAndReleaseIfNeeded(storedObject, null);
    assertThat(returnObject, is(equalTo(deserializedRegionEntryValue)));
  }

  @Test
  public void copyAndReleaseWithSerializedReturnsValueOfOriginal() {
    allocateOffHeapSerialized();
    assertTrue(storedObject.retain());
    Object returnObject =
        ((VMCachedDeserializable) OffHeapHelper.copyAndReleaseIfNeeded(storedObject, null))
            .getSerializedValue();
    assertThat(returnObject, is(equalTo(serializedRegionEntryValue)));
  }

  @Test
  public void copyAndReleaseNonStoredObjectReturnsOriginal() {
    Object testObject = getValue();
    Object returnObject = OffHeapHelper.copyAndReleaseIfNeeded(testObject, null);
    assertThat(returnObject, is(testObject));
  }

  @Test
  public void copyIfNeededWithNullReturnsNull() {
    Object testObject = null;
    Object returnObject = OffHeapHelper.copyAndReleaseIfNeeded(testObject, null);
    assertThat(returnObject, is(equalTo(null)));
  }

  @Test
  public void copyIfNeededNonOffHeapReturnsOriginal() {
    Object testObject = getValue();
    Object returnObject = OffHeapHelper.copyIfNeeded(testObject, null);
    assertThat(returnObject, is(testObject));
  }

  @Test
  public void copyIfNeededOffHeapSerializedReturnsValueOfOriginal() {
    allocateOffHeapSerialized();
    Object returnObject = ((VMCachedDeserializable) OffHeapHelper.copyIfNeeded(storedObject, null))
        .getSerializedValue();
    assertThat(returnObject, is(equalTo(serializedRegionEntryValue)));
  }

  @Test
  public void copyIfNeededOffHeapDeserializedReturnsOriginal() {
    allocateOffHeapDeserialized();
    Object returnObject = OffHeapHelper.copyIfNeeded(storedObject, null);
    assertThat(returnObject, is(equalTo(deserializedRegionEntryValue)));
  }

  @Test
  public void copyIfNeededWithOffHeapDeserializedObjDoesNotRelease() {
    allocateOffHeapDeserialized();
    int initialRefCountOfObject = storedObject.getRefCount();
    OffHeapHelper.copyIfNeeded(storedObject, null);
    assertThat("Ref count after copy", storedObject.getRefCount(), is(initialRefCountOfObject));
  }

  @Test
  public void copyIfNeededWithOffHeapSerializedObjDoesNotRelease() {
    allocateOffHeapSerialized();
    int initialRefCountOfObject = storedObject.getRefCount();
    OffHeapHelper.copyIfNeeded(storedObject, null);
    assertThat("Ref count after copy", storedObject.getRefCount(), is(initialRefCountOfObject));
  }

  @Test
  public void releaseOfOffHeapDecrementsRefCount() {
    allocateOffHeapSerialized();
    assertThat("Initial Ref Count", storedObject.getRefCount(), is(1));
    OffHeapHelper.release(storedObject);
    assertThat("Ref Count Decremented", storedObject.getRefCount(), is(0));
  }

  @Test
  public void releaseOfOffHeapReturnsTrue() {
    allocateOffHeapSerialized();
    assertThat("Releasing OFfHeap object is true", OffHeapHelper.release(storedObject), is(true));
  }

  @Test
  public void releaseOfNonOffHeapReturnsFalse() {
    Object testObject = getValue();
    assertThat("Releasing OFfHeap object is true", OffHeapHelper.release(testObject), is(false));
  }

  @Test
  public void releaseWithOutTrackingOfOffHeapDecrementsRefCount() {
    allocateOffHeapSerialized();
    assertThat("Initial Ref Count", storedObject.getRefCount(), is(1));
    OffHeapHelper.releaseWithNoTracking(storedObject);
    assertThat("Ref Count Decremented", storedObject.getRefCount(), is(0));
  }

  @Test
  public void releaseWithoutTrackingOfOffHeapReturnsTrue() {
    allocateOffHeapSerialized();
    assertThat("Releasing OFfHeap object is true",
        OffHeapHelper.releaseWithNoTracking(storedObject), is(true));
  }

  @Test
  public void releaseWithoutTrackingOfNonOffHeapReturnsFalse() {
    Object testObject = getValue();
    assertThat("Releasing OFfHeap object is true", OffHeapHelper.releaseWithNoTracking(testObject),
        is(false));
  }

  @Test
  public void releaseAndTrackOwnerOfOffHeapDecrementsRefCount() {
    allocateOffHeapSerialized();
    assertThat("Initial Ref Count", storedObject.getRefCount(), is(1));
    OffHeapHelper.releaseAndTrackOwner(storedObject, "owner");
    assertThat("Ref Count Decremented", storedObject.getRefCount(), is(0));
  }

  @Test
  public void releaseAndTrackOwnerOfOffHeapReturnsTrue() {
    allocateOffHeapSerialized();
    assertThat("Releasing OFfHeap object is true",
        OffHeapHelper.releaseAndTrackOwner(storedObject, "owner"), is(true));
  }

  @Test
  public void releaseAndTrackOwnerOfNonOffHeapReturnsFalse() {
    Object testObject = getValue();
    assertThat("Releasing OFfHeap object is true",
        OffHeapHelper.releaseAndTrackOwner(testObject, "owner"), is(false));
  }

}
