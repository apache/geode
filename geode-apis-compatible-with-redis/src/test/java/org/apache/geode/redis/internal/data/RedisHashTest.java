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
 *
 */

package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.data.RedisHash.BASE_REDIS_HASH_OVERHEAD;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.internal.size.ReflectionSingleObjectSizer;
import org.apache.geode.redis.internal.collections.SizeableObject2ObjectOpenCustomHashMapWithCursor;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisHashTest {
  private final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();
  private final ReflectionSingleObjectSizer elementSizer =
      new ReflectionSingleObjectSizer();

  @BeforeClass
  public static void beforeClass() {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_HASH_ID,
        RedisHash.class);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier.isSynchronized(RedisHash.class
        .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
            .isTrue();
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(o1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisHash o2 = DataSerializer.readObject(in);
    assertThat(o2).isEqualTo(o1);
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    RedisHash o2 = createRedisHash("k1", "v1", "k2", "v2");
    o2.setExpirationTimestampNoDelta(999);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    RedisHash o2 = createRedisHash("k1", "v1", "k2", "v3");
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    o1.setExpirationTimestampNoDelta(1000);
    RedisHash o2 = createRedisHash("k1", "v1", "k2", "v2");
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isEqualTo(o2);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptyHashes() {
    RedisHash o1 = new RedisHash(Collections.emptyList());
    RedisHash o2 = NullRedisDataStructures.NULL_REDIS_HASH;
    assertThat(o1).isEqualTo(o2);
    assertThat(o2).isEqualTo(o1);
  }

  /************* HSET *************/
  @SuppressWarnings("unchecked")
  @Test
  public void hset_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = Mockito.mock(Region.class);
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    byte[] k3 = stringToBytes("k3");
    byte[] v3 = stringToBytes("v3");
    ArrayList<byte[]> adds = new ArrayList<>();
    adds.add(k3);
    adds.add(v3);
    o1.hset(region, null, adds, false);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisHash o2 = createRedisHash("k1", "v1", "k2", "v2");
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void hdel_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = mock(Region.class);
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    byte[] k1 = stringToBytes("k1");
    ArrayList<byte[]> removes = new ArrayList<>();
    removes.add(k1);
    o1.hdel(region, null, removes);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisHash o2 = createRedisHash("k1", "v1", "k2", "v2");
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }

  /************* Expiration *************/
  @SuppressWarnings("unchecked")
  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = mock(Region.class);
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    o1.setExpirationTimestamp(region, null, 999);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisHash o2 = createRedisHash("k1", "v1", "k2", "v2");
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }

  /************* HSCAN *************/
  @Test
  public void hscanReturnsCorrectNumberOfElements() {
    RedisHash hash = createRedisHash("k1", "v1", "k2", "v2", "k3", "v3", "k4", "v4");
    ImmutablePair<Integer, List<byte[]>> result = hash.hscan(null, 2, 0);

    assertThat(result.left).isNotEqualTo(0);
    assertThat(result.right).hasSize(4);
    result = hash.hscan(null, 3, result.left);
    assertThat(result.left).isEqualTo(0);
    assertThat(result.right).hasSize(4);
  }

  @Test
  public void hscanOnlyReturnsElementsMatchingPattern() {
    RedisHash hash = createRedisHash("ak1", "v1", "k2", "v2", "ak3", "v3", "k4", "v4");
    ImmutablePair<Integer, List<byte[]>> result = hash.hscan(Pattern.compile("a.*"), 3, 0);

    assertThat(result.left).isEqualTo(0);
    List<String> fieldsAndValues =
        result.right.stream().map(Coder::bytesToString).collect(Collectors.toList());

    assertThat(fieldsAndValues).containsExactly("ak1", "v1", "ak3", "v3");
  }

  @Test
  public void hscanThrowsWhenReturnedArrayListLengthWouldExceedVMLimit() {
    RedisHash hash = spy(new RedisHash());
    doReturn(Integer.MAX_VALUE - 10).when(hash).hlen();

    assertThatThrownBy(() -> hash.hscan(null, Integer.MAX_VALUE - 10, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /************* Hash Size *************/
  /******* constants *******/
  // this test contains the math that was used to derive the constant in RedisHash. If this test
  // starts failing, it is because the overhead of RedisHash has changed. If it has decreased, good
  // job! You can change the constant in RedisHash to reflect that. If it has increased, carefully
  // consider that increase before changing the constant.
  @Test
  public void constantBaseRedisHashOverhead_shouldEqualCalculatedOverhead() {
    RedisHash hash = new RedisHash(Collections.emptyList());
    SizeableObject2ObjectOpenCustomHashMapWithCursor<byte[], byte[]> backingHash =
        new SizeableObject2ObjectOpenCustomHashMapWithCursor<>(0, ByteArrays.HASH_STRATEGY);

    assertThat(sizer.sizeof(hash) - sizer.sizeof(backingHash)).isEqualTo(BASE_REDIS_HASH_OVERHEAD);
  }

  /******* constructor *******/

  @Test
  public void should_calculateSize_equalToROSSize_ofIndividualInstanceWithSingleValue() {
    ArrayList<byte[]> data = new ArrayList<>();
    data.add(stringToBytes("field"));
    data.add(stringToBytes("valuethatisverylonggggggggg"));

    RedisHash redisHash = new RedisHash(data);

    final int expected = sizer.sizeof(redisHash);
    final int actual = redisHash.getSizeInBytes();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROSSize_ofIndividualInstanceWithMultipleValues() {
    RedisHash redisHash =
        createRedisHash("aSuperLongField", "value", "field", "aSuperLongValue");

    final int expected = sizer.sizeof(redisHash);
    final int actual = redisHash.getSizeInBytes();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROSSize_withManyEntries() {
    final String baseField = "longerbase";
    final String baseValue = "base";

    ArrayList<byte[]> elements = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      elements.add(stringToBytes(baseField + i));
      elements.add(stringToBytes(baseValue + i));
    }
    RedisHash hash = new RedisHash(elements);

    Integer actual = hash.getSizeInBytes();
    int expected = sizer.sizeof(hash);

    assertThat(actual).isEqualTo(expected);
  }

  /******* put *******/
  @Test
  public void hsetShould_calculateSize_equalToSizeCalculatedInConstructor_forMultipleEntries() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String baseField = "field";
    final String baseValue = "value";

    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(), any())).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      data.add(stringToBytes((baseField + i)));
      data.add(stringToBytes((baseValue + i)));
    }

    hash.hset(region, key, data, false);
    RedisHash expectedRedisHash = new RedisHash(new ArrayList<>(data));

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedRedisHash.getSizeInBytes());
  }

  @Test
  public void hsetShould_calculateSizeDifference_whenUpdatingExistingEntry_newIsShorterThanOld() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String field = "field";
    final String initialValue = "initialValueThatIsMuchLongerThanFinalValue";
    final String finalValue = "finalValue";

    testThatSizeIsUpdatedWhenUpdatingValue(key, field, initialValue, finalValue);
  }

  @Test
  public void hsetShould_calculateSizeDifference_whenUpdatingExistingEntry_oldIsShorterThanNew() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String field = "field";
    final String initialValue = "initialValue";
    final String finalValue = "finalValueThatIsMuchLongerThanInitialValue";

    testThatSizeIsUpdatedWhenUpdatingValue(key, field, initialValue, finalValue);
  }

  @Test
  public void hsetShould_calculateSizeDifference_whenUpdatingExistingEntry_valuesAreSameLength() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String field = "field";
    final String initialValue = "initialValue";
    final String finalValue = "finalValueee";

    testThatSizeIsUpdatedWhenUpdatingValue(key, field, initialValue, finalValue);
  }

  public void testThatSizeIsUpdatedWhenUpdatingValue(final RedisKey key, final String field,
      final String initialValue, final String finalValue) {
    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> initialData = new ArrayList<>();
    initialData.add(stringToBytes(field));
    initialData.add(stringToBytes(initialValue));

    hash.hset(region, key, initialData, false);
    hash.clearDelta();

    long initialSize = sizer.sizeof(hash);

    List<byte[]> finalData = new ArrayList<>();
    finalData.add(stringToBytes(field));
    finalData.add(stringToBytes(finalValue));

    hash.hset(region, key, finalData, false);
    hash.clearDelta();

    assertThat(hash.getSizeInBytes()).isEqualTo(sizer.sizeof(hash));

    long expectedSizeChange = elementSizer.sizeof(Coder.stringToBytes(finalValue))
        - elementSizer.sizeof(Coder.stringToBytes(initialValue));
    long actualSizeChange = hash.getSizeInBytes() - initialSize;

    assertThat(actualSizeChange).isEqualTo(expectedSizeChange);
  }

  /******* put if absent *******/
  @Test
  public void putIfAbsentShould_calculateSizeEqualToSizeCalculatedInConstructor_forMultipleUniqueEntries() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String baseField = "field";
    final String baseValue = "value";

    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      data.add(stringToBytes((baseField + i)));
      data.add(stringToBytes((baseValue + i)));
    }

    hash.hset(region, key, data, true);
    RedisHash expectedRedisHash = new RedisHash(new ArrayList<>(data));

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedRedisHash.getSizeInBytes());
  }

  @Test
  public void putIfAbsentShould_notChangeSize_whenSameDataIsSetTwice() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String baseField = "field";
    final String baseValue = "value";

    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      data.add(stringToBytes((baseField + i)));
      data.add(stringToBytes((baseValue + i)));
    }

    hash.hset(region, key, data, true);

    int expectedSize = hash.getSizeInBytes();

    hash.hset(region, key, data, true);

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize);
  }

  @Test
  public void putIfAbsent_shouldNotChangeSize_whenPuttingToExistingFields() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String baseField = "field";
    final String initialBaseValue = "value";
    final String finalBaseValue = "longerValue";

    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> initialData = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      initialData.add(stringToBytes((baseField + i)));
      initialData.add(stringToBytes((initialBaseValue + i)));
    }

    hash.hset(region, key, initialData, true);

    int expectedSize = hash.getSizeInBytes();

    List<byte[]> finalData = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      finalData.add(stringToBytes((baseField + i)));
      finalData.add(stringToBytes((finalBaseValue + i)));
    }

    hash.hset(region, key, finalData, true);

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize);
  }

  /******* remove *******/
  @Test
  public void sizeShouldDecrease_whenValueIsRemoved() {
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final String baseField = "field";
    final String baseValue = "value";
    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    List<byte[]> data = new ArrayList<>();
    List<byte[]> dataToRemove = new ArrayList<>();
    byte[] field1 = stringToBytes((baseField + 1));
    byte[] value1 = stringToBytes((baseValue + 1));
    byte[] field2 = stringToBytes((baseField + 2));
    byte[] value2 = stringToBytes((baseValue + 2));
    data.add(field1);
    data.add(value1);
    data.add(field2);
    data.add(value2);
    dataToRemove.add(field1);

    RedisHash redisHash = new RedisHash(data);
    redisHash.clearDelta();

    int initialSize = redisHash.getSizeInBytes();

    redisHash.hdel(region, key, dataToRemove);
    redisHash.clearDelta();

    int finalSize = redisHash.getSizeInBytes();
    assertThat(finalSize).isLessThan(initialSize);

    assertThat(finalSize).isEqualTo(sizer.sizeof(redisHash));
  }

  private RedisHash createRedisHash(String... keysAndValues) {
    final List<byte[]> keysAndValuesList = Arrays
        .stream(keysAndValues)
        .map(Coder::stringToBytes)
        .collect(Collectors.toList());
    return new RedisHash(keysAndValuesList);
  }
}
