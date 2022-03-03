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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.pdx.PdxInstance;

public class ValueComparisonHelperTest {
  private static final String STRING = "ABCD";
  private static final byte[] BYTE_ARRAY = {65, 66, 67, 68};
  private static final String STRING2 = "EFGH";
  private static final int[] INT_ARRAY = {65, 66, 67, 68};
  private static final int[] INT_ARRAY2 = new int[] {65, 66, 67, 68};
  private static final int[] INT_ARRAY3 = new int[] {65, 66, 67, 69};
  private static final boolean[] BOOLEAN_ARRAY = {true, false, false, true};
  private static final boolean[] BOOLEAN_ARRAY2 = new boolean[] {true, false, false, true};
  private static final boolean[] BOOLEAN_ARRAY3 = new boolean[] {true, false, true, true};
  private static final short[] SHORT_ARRAY = {65, 66, 67, 68};
  private static final short[] SHORT_ARRAY2 = new short[] {65, 66, 67, 68};
  private static final short[] SHORT_ARRAY3 = new short[] {65, 66, 67, 69};
  private static final float[] FLOAT_ARRAY = {65.1f, 66.0f, 67.3f, 68.9f};
  private static final float[] FLOAT_ARRAY2 = new float[] {65.1f, 66.0f, 67.3f, 68.9f};
  private static final float[] FLOAT_ARRAY3 = {65.1f, 66.0f, 67.3f, 69.9f};
  private static final String[] STRING_ARRAY = {"ABCD", "EFGH"};
  private static final String[] STRING_ARRAY2 = {STRING, STRING2};
  private static final String[] STRING_ARRAY3 = {STRING, STRING2, ""};

  @Test
  public void basicEqualsCanCompareTwoNullObjects() {
    assertThat(ValueComparisonHelper.basicEquals(null, null)).isTrue();
  }

  @Test
  public void basicEqualsCanCompareWhenOnlyFirstObjectIsNull() {
    assertThat(ValueComparisonHelper.basicEquals(null, new Object())).isFalse();
  }

  @Test
  public void basicEqualsCanCompareWhenOnlySecondObjectIsNull() {
    assertThat(ValueComparisonHelper.basicEquals(new Object(), null)).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualByteArrays() {
    assertThat(ValueComparisonHelper.basicEquals(STRING.getBytes(), BYTE_ARRAY)).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualByteArrays() {
    assertThat(ValueComparisonHelper.basicEquals(STRING.getBytes(), STRING2.getBytes())).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlyFirstObjectIsAByteArray() {
    assertThat(ValueComparisonHelper.basicEquals(BYTE_ARRAY, INT_ARRAY)).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlySecondObjectIsAByteArray() {
    assertThat(ValueComparisonHelper.basicEquals(INT_ARRAY, BYTE_ARRAY)).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualIntArrays() {
    assertThat(ValueComparisonHelper.basicEquals(INT_ARRAY, INT_ARRAY2)).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualIntArrays() {
    assertThat(ValueComparisonHelper.basicEquals(INT_ARRAY, INT_ARRAY3)).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualLongArrays() {
    assertThat(ValueComparisonHelper.basicEquals(Arrays.stream(INT_ARRAY).asLongStream().toArray(),
        Arrays.stream(INT_ARRAY2).asLongStream().toArray())).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualLongArrays() {
    assertThat(ValueComparisonHelper.basicEquals(Arrays.stream(INT_ARRAY3).asLongStream().toArray(),
        Arrays.stream(INT_ARRAY2).asLongStream().toArray())).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualBooleanArrays() {
    assertThat(ValueComparisonHelper.basicEquals(BOOLEAN_ARRAY, BOOLEAN_ARRAY2)).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualBooleanArrays() {
    assertThat(ValueComparisonHelper.basicEquals(BOOLEAN_ARRAY3, BOOLEAN_ARRAY)).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlyFirstObjectIsABooleanArray() {
    assertThat(ValueComparisonHelper.basicEquals(BOOLEAN_ARRAY, SHORT_ARRAY)).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlySecondObjectIsABooleanArray() {
    assertThat(ValueComparisonHelper.basicEquals(SHORT_ARRAY, BOOLEAN_ARRAY)).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualShortArrays() {
    assertThat(ValueComparisonHelper.basicEquals(SHORT_ARRAY, SHORT_ARRAY2)).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualShortArrays() {
    assertThat(ValueComparisonHelper.basicEquals(SHORT_ARRAY2, SHORT_ARRAY3)).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualDoubleArrays() {
    assertThat(
        ValueComparisonHelper.basicEquals(Arrays.stream(INT_ARRAY).asDoubleStream().toArray(),
            Arrays.stream(INT_ARRAY2).asDoubleStream().toArray())).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualDoubleArrays() {
    assertThat(
        ValueComparisonHelper.basicEquals(Arrays.stream(INT_ARRAY3).asDoubleStream().toArray(),
            Arrays.stream(INT_ARRAY).asDoubleStream().toArray())).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualCharArrays() throws Exception {
    assertThat(ValueComparisonHelper.basicEquals(STRING.toCharArray(),
        (new String(BYTE_ARRAY, StandardCharsets.UTF_8)).toCharArray())).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualCharArrays() throws Exception {
    assertThat(ValueComparisonHelper.basicEquals(STRING2.toCharArray(),
        (new String(BYTE_ARRAY, StandardCharsets.UTF_8)).toCharArray())).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlyFirstObjectIsACharArray() {
    assertThat(ValueComparisonHelper.basicEquals(STRING.toCharArray(),
        Arrays.stream(INT_ARRAY).asDoubleStream().toArray())).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlySecondObjectIsACharArray() {
    assertThat(ValueComparisonHelper
        .basicEquals(Arrays.stream(INT_ARRAY).asDoubleStream().toArray(), STRING.toCharArray()))
            .isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualFloatArrays() throws Exception {
    assertThat(ValueComparisonHelper.basicEquals(FLOAT_ARRAY, FLOAT_ARRAY2)).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualFloatArrays() throws Exception {
    assertThat(ValueComparisonHelper.basicEquals(FLOAT_ARRAY2, FLOAT_ARRAY3)).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlySecondObjectIsAFloatArray() throws Exception {
    assertThat(ValueComparisonHelper.basicEquals(STRING, FLOAT_ARRAY3)).isFalse();
  }

  @Test
  public void basicEqualsReturnsTrueWhenCompareTwoEqualStringArrays() throws Exception {
    assertThat(ValueComparisonHelper.basicEquals(STRING_ARRAY, STRING_ARRAY2)).isTrue();
  }

  @Test
  public void basicEqualsReturnsFalseWhenCompareTwoNotEqualStringArrays() throws Exception {
    assertThat(ValueComparisonHelper.basicEquals(STRING_ARRAY, STRING_ARRAY3)).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlyFirstObjectIsAStringArray() {
    assertThat(ValueComparisonHelper.basicEquals(STRING_ARRAY,
        Arrays.stream(INT_ARRAY).asLongStream().toArray())).isFalse();
  }

  @Test
  public void basicEqualsReturnsFalseWhenOnlySecondObjectIsAStringArray() {
    assertThat(ValueComparisonHelper
        .basicEquals(Arrays.stream(INT_ARRAY).asDoubleStream().toArray(), STRING_ARRAY2)).isFalse();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingNotSerializedCacheDeserializableWithPdxInstance() {
    PdxInstance pdxInstance1 = mock(PdxInstance.class);
    CachedDeserializable object = mock(CachedDeserializable.class);
    when(object.isSerialized()).thenReturn(false);

    assertThat(
        ValueComparisonHelper.checkEquals(object, pdxInstance1, false, mock(InternalCache.class)))
            .isFalse();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingPdxInstanceWithNonPdxCacheDeserializable() {
    int[] value = {0, 1, 2, 3};
    Object object = new VMCachedDeserializable(EntryEventImpl.serialize(value));
    PdxInstance pdxInstance1 = mock(PdxInstance.class);

    assertThat(
        ValueComparisonHelper.checkEquals(pdxInstance1, object, false, mock(InternalCache.class)))
            .isFalse();
  }

  @Test
  public void checkEqualsReturnsTrueWhenComparingPdxInstanceWithItsCacheDeserializable() {
    PdxInstance pdxInstance1 = mock(PdxInstance.class);
    Object object = new VMCachedDeserializable(pdxInstance1, 1);

    assertThat(
        ValueComparisonHelper.checkEquals(pdxInstance1, object, false, mock(InternalCache.class)))
            .isTrue();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingTwoNonEqualStoredObject() {
    StoredObject object1 = mock(StoredObject.class);
    StoredObject object2 = mock(StoredObject.class);

    when(object1.checkDataEquals(object2)).thenReturn(false);

    assertThat(
        ValueComparisonHelper.checkEquals(object1, object2, false, mock(InternalCache.class)))
            .isFalse();
  }

  @Test
  public void checkEqualsReturnsTrueWhenComparingTwoEqualStoredObject() {
    StoredObject object1 = mock(StoredObject.class);
    StoredObject object2 = mock(StoredObject.class);

    when(object1.checkDataEquals(object2)).thenReturn(true);

    assertThat(
        ValueComparisonHelper.checkEquals(object1, object2, false, mock(InternalCache.class)))
            .isTrue();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingStoredObjectWithByteArrayAsCacheDeserializable() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(false);
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, cachedDeserializable, false,
            mock(InternalCache.class)))
                .isFalse();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingNonSerializedStoredObjectWithCacheDeserializable() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(false);
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, cachedDeserializable, false,
            mock(InternalCache.class)))
                .isFalse();
  }

  @Test
  public void checkEqualsCanCompareStoredObjectWithCacheDeserializable() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(true);
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    byte[] serializedObj = new byte[1];
    when(cachedDeserializable.getSerializedValue()).thenReturn(serializedObj);

    ValueComparisonHelper.checkEquals(storedObject, cachedDeserializable, false,
        mock(InternalCache.class));

    verify(storedObject).checkDataEquals(serializedObj);
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingSerializedStoredObjectWithByteArray() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(true);
    byte[] object = new byte[1];

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, object, false, mock(InternalCache.class)))
            .isFalse();
  }

  @Test
  public void checkEqualsCanCompareNonSerializedStoredObjectWithByteArray() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(false);
    byte[] object = new byte[1];

    ValueComparisonHelper.checkEquals(storedObject, object, false, mock(InternalCache.class));

    verify(storedObject).checkDataEquals(object);
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingNonSerializedStoredObjectWithAnObject() {
    StoredObject storedObject = mock(StoredObject.class);
    // storeObject is a byte[]
    when(storedObject.isSerialized()).thenReturn(false);
    Object object = new Object();

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, object, false, mock(InternalCache.class)))
            .isFalse();
    verify(storedObject, never()).checkDataEquals(any(byte[].class));
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingSerializedStoredObjectWithANullObject() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(true);

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, null, false, mock(InternalCache.class)))
            .isFalse();
    verify(storedObject, never()).checkDataEquals(any(byte[].class));
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingSerializedStoredObjectWithNotAvailableToken() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(true);

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, Token.NOT_AVAILABLE, false,
            mock(InternalCache.class)))
                .isFalse();
    verify(storedObject, never()).checkDataEquals(any(byte[].class));
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingSerializedStoredObjectWithInvalidToken() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(true);

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, Token.INVALID, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, Token.LOCAL_INVALID, false,
            mock(InternalCache.class)))
                .isFalse();
    verify(storedObject, never()).checkDataEquals(any(byte[].class));
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingSerializedStoredObjectWithRemovedToken() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(true);

    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, Token.DESTROYED, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, Token.REMOVED_PHASE1, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, Token.REMOVED_PHASE2, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(storedObject, Token.TOMBSTONE, false,
            mock(InternalCache.class)))
                .isFalse();
    verify(storedObject, never()).checkDataEquals(any(byte[].class));
  }

  @Test
  public void checkEqualsCanCompareSerializedStoredObjectWithAnObject() {
    StoredObject storedObject = mock(StoredObject.class);
    when(storedObject.isSerialized()).thenReturn(true);
    Object object = mock(Serializable.class);

    ValueComparisonHelper.checkEquals(object, storedObject, false, mock(InternalCache.class));

    verify(storedObject).checkDataEquals(any(byte[].class));
  }

  @Test
  public void checkEqualsReturnsTrueWhenComparingTwoEqualNotSerializedCacheDeserializable() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(false);
    CachedDeserializable object = mock(CachedDeserializable.class);
    when(object.isSerialized()).thenReturn(false);
    when(cachedDeserializable.getDeserializedForReading()).thenReturn(BYTE_ARRAY);
    when(object.getDeserializedForReading()).thenReturn(STRING.getBytes());

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, object, false,
        mock(InternalCache.class))).isTrue();
  }

  @Test
  public void checkEqualsReturnsTrueWhenComparingNotSerializedDeserializableWithAByteArray() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(false);
    byte[] object = STRING.getBytes();
    when(cachedDeserializable.getDeserializedForReading()).thenReturn(BYTE_ARRAY);

    assertThat(ValueComparisonHelper.checkEquals(object, cachedDeserializable, false,
        mock(InternalCache.class))).isTrue();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingNotSerializedCacheDeserializableWithAnObject() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(false);
    Object object = new Object();

    assertThat(ValueComparisonHelper.checkEquals(object, cachedDeserializable, false,
        mock(InternalCache.class))).isFalse();
  }

  @Test
  public void checkEqualsCanCompareNotSerializedCacheDeserializableWithSerializedOne() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(false);
    CachedDeserializable object = mock(CachedDeserializable.class);
    when(object.isSerialized()).thenReturn(true);

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, object, false,
        mock(InternalCache.class))).isFalse();
  }

  @Test
  public void checkEqualsCanCompareCacheDeserializableIfIsCompressedOffheap() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(BYTE_ARRAY);
    CachedDeserializable object = mock(CachedDeserializable.class);
    when(object.getSerializedValue()).thenReturn(STRING.getBytes());

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, object, true,
        mock(InternalCache.class))).isTrue();
  }

  @Test
  public void checkEqualsCanCompareCacheDeserializableWithAnObjectIfIsCompressedOffheap() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(EntryEventImpl.serialize(STRING));

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, STRING, true,
        mock(InternalCache.class))).isTrue();
  }

  @Test
  public void checkEqualsCanCompareCacheDeserializable() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(BYTE_ARRAY);
    when(cachedDeserializable.getDeserializedForReading()).thenReturn(STRING);
    CachedDeserializable object = mock(CachedDeserializable.class);
    when(object.getDeserializedForReading()).thenReturn(STRING);

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, object, false,
        mock(InternalCache.class))).isTrue();
    verify(cachedDeserializable).getDeserializedForReading();
    verify(object).getDeserializedForReading();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingCacheDeserializableWithANullObject() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(BYTE_ARRAY);

    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, null, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, null, true,
            mock(InternalCache.class)))
                .isFalse();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingCacheDeserializableWithNotAvailableToken() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(BYTE_ARRAY);

    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.NOT_AVAILABLE, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.NOT_AVAILABLE, true,
            mock(InternalCache.class)))
                .isFalse();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingCacheDeserializableWithInvalidToken() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(BYTE_ARRAY);

    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.INVALID, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.LOCAL_INVALID, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.INVALID, true,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.LOCAL_INVALID, true,
            mock(InternalCache.class)))
                .isFalse();
  }

  @Test
  public void checkEqualsReturnsFalseWhenComparingCacheDeserializableWithRemovedToken() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(BYTE_ARRAY);

    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.DESTROYED, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.REMOVED_PHASE1, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.REMOVED_PHASE2, false,
            mock(InternalCache.class)))
                .isFalse();
    assertThat(
        ValueComparisonHelper.checkEquals(cachedDeserializable, Token.TOMBSTONE, false,
            mock(InternalCache.class)))
                .isFalse();
  }

  @Test
  public void checkEqualsCanCompareCacheDeserializableWithAnObject() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(BYTE_ARRAY);
    when(cachedDeserializable.getDeserializedForReading()).thenReturn(STRING);

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, STRING, false,
        mock(InternalCache.class))).isTrue();
  }

  @Test
  public void checkEqualsCanCompareObjectAsValueCacheDeserializableWithCacheDeserializable() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(STRING);
    CachedDeserializable object = mock(CachedDeserializable.class);
    when(object.getDeserializedForReading()).thenReturn(STRING);

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, object, false,
        mock(InternalCache.class))).isTrue();
  }

  @Test
  public void checkEqualsCanCompareObjectAsValueCacheDeserializableWithAnObject() {
    CachedDeserializable cachedDeserializable = mock(CachedDeserializable.class);
    when(cachedDeserializable.isSerialized()).thenReturn(true);
    when(cachedDeserializable.getValue()).thenReturn(STRING);

    assertThat(ValueComparisonHelper.checkEquals(cachedDeserializable, STRING, false,
        mock(InternalCache.class))).isTrue();
  }

  @Test
  public void checkEqualsCanCompareTwoObjects() {
    assertThat(ValueComparisonHelper.checkEquals(BOOLEAN_ARRAY, BOOLEAN_ARRAY2, false,
        mock(InternalCache.class))).isTrue();
  }

}
