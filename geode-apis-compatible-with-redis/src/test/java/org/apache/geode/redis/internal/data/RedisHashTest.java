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

import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.redis.internal.data.RedisHash.BASE_REDIS_HASH_OVERHEAD;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.assertj.core.data.Offset;
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
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class RedisHashTest {
  private final ReflectionObjectSizer reflectionObjectSizer = ReflectionObjectSizer.getInstance();

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
    byte[] k3 = Coder.stringToBytes("k3");
    byte[] v3 = Coder.stringToBytes("v3");
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
    byte[] k1 = Coder.stringToBytes("k1");
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
  public void hscanSnaphots_shouldBeEmpty_givenHscanHasNotBeenCalled() {
    RedisHash subject = createRedisHash(100);
    assertThat(subject.getHscanSnapShots()).isEmpty();
  }

  @Test
  public void hscanSnaphots_shouldContainSnapshot_givenHscanHasBeenCalled() {

    final List<byte[]> FIELDS_AND_VALUES_FOR_HASH = createListOfDataElements(100);
    RedisHash subject = new RedisHash(FIELDS_AND_VALUES_FOR_HASH);
    UUID clientID = UUID.randomUUID();

    subject.hscan(clientID, null, 10, 0);

    ConcurrentHashMap<UUID, List<byte[]>> hscanSnapShotMap = subject.getHscanSnapShots();

    assertThat(hscanSnapShotMap.containsKey(clientID)).isTrue();

    List<byte[]> keyList = hscanSnapShotMap.get(clientID);
    assertThat(keyList).isNotEmpty();

    FIELDS_AND_VALUES_FOR_HASH.forEach((entry) -> {
      if (Coder.bytesToString(entry).contains("field")) {
        assertThat(keyList).contains(entry);
      } else if (Coder.bytesToString(entry).contains("value")) {
        assertThat(keyList).doesNotContain(entry);
      }
    });

  }

  @Test
  public void hscanSnaphots_shouldContainSnapshot_givenHscanHasBeenCalled_WithNonZeroCursor() {

    final List<byte[]> FIELDS_AND_VALUES_FOR_HASH = createListOfDataElements(100);
    RedisHash subject = new RedisHash(FIELDS_AND_VALUES_FOR_HASH);
    UUID clientID = UUID.randomUUID();

    subject.hscan(clientID, null, 10, 10);

    ConcurrentHashMap<UUID, List<byte[]>> hscanSnapShotMap = subject.getHscanSnapShots();

    assertThat(hscanSnapShotMap.containsKey(clientID)).isTrue();

    List<byte[]> keyList = hscanSnapShotMap.get(clientID);
    assertThat(keyList).isNotEmpty();

    FIELDS_AND_VALUES_FOR_HASH.forEach((entry) -> {
      if (Coder.bytesToString(entry).contains("field")) {
        assertThat(keyList).contains(entry);
      } else if (Coder.bytesToString(entry).contains("value")) {
        assertThat(keyList).doesNotContain(entry);
      }
    });
  }

  @Test
  public void hscanSnaphots_shouldBeRemoved_givenCompleteIteration() {
    RedisHash subject = createRedisHashWithExpiration(1, 100000);
    UUID client_ID = UUID.randomUUID();

    subject.hscan(client_ID, null, 10, 0);

    ConcurrentHashMap<UUID, List<byte[]>> hscanSnapShotMap = subject.getHscanSnapShots();
    assertThat(hscanSnapShotMap).isEmpty();
  }

  @Test
  public void hscanSnaphots_shouldExpireAfterExpiryPeriod() {
    RedisHash subject = createRedisHashWithExpiration(1000, 1);
    UUID client_ID = UUID.randomUUID();

    subject.hscan(client_ID, null, 1, 0);

    GeodeAwaitility.await().atMost(4, SECONDS).untilAsserted(() -> {
      ConcurrentHashMap<UUID, List<byte[]>> hscanSnapShotMap =
          subject.getHscanSnapShots();
      assertThat(hscanSnapShotMap).isEmpty();
    });
  }

  /************* Hash Size *************/
  /******* constants *******/
  // these tests contain the math that was used to derive the constants in RedisHash. If these tests
  // start failing, it is because the overhead of RedisHash has changed. If it has decreased, good
  // job! You can change the constants in RedisHash to reflect that. If it has increased, carefully
  // consider that increase before changing the constants.
  @Test
  public void constantBaseRedisHashOverhead_shouldEqualCalculatedOverhead() {
    RedisHash hash = new RedisHash(Collections.emptyList());
    int baseRedisHashOverhead = reflectionObjectSizer.sizeof(hash);

    assertThat(baseRedisHashOverhead).isEqualTo(BASE_REDIS_HASH_OVERHEAD);
    assertThat(hash.getSizeInBytes()).isEqualTo(baseRedisHashOverhead);
  }

  @Test
  public void constantValuePairOverhead_shouldEqualCalculatedOverhead() {
    RedisHash hash = new RedisHash(Collections.emptyList());
    long totalOverhead = 0;
    final int totalFields = 1000;
    Random random = new Random(0);
    for (int fieldCount = 1; fieldCount < totalFields; fieldCount++) {
      byte[] data = new byte[random.nextInt(30)];
      random.nextBytes(data);
      hash.hashPut(data, data);
      int size = reflectionObjectSizer.sizeof(hash);
      final int dataSize = 2 * data.length;
      int overHeadPerField = (size - BASE_REDIS_HASH_OVERHEAD - dataSize) / fieldCount;
      totalOverhead += overHeadPerField;
    }

    long averageOverhead = totalOverhead / totalFields;

    assertThat(RedisHash.HASH_MAP_VALUE_PAIR_OVERHEAD).isEqualTo(averageOverhead);
  }

  /******* constructor *******/

  @Test
  public void should_calculateSize_closeToROSSize_ofIndividualInstanceWithSingleValue() {
    ArrayList<byte[]> data = new ArrayList<>();
    data.add("field".getBytes());
    data.add("valuethatisverylonggggggggg".getBytes());

    RedisHash redisHash = new RedisHash(data);

    final int expected = reflectionObjectSizer.sizeof(redisHash);
    final int actual = redisHash.getSizeInBytes();

    final Offset<Integer> offset = Offset.offset((int) round(expected * 0.03));

    assertThat(actual).isCloseTo(expected, offset);
  }

  @Test
  public void should_calculateSize_closeToROSSize_ofIndividualInstanceWithMultipleValues() {
    RedisHash redisHash =
        createRedisHash("aSuperLongField", "value", "field", "aSuperLongValue");

    final int expected = reflectionObjectSizer.sizeof(redisHash);
    final int actual = redisHash.getSizeInBytes();

    final Offset<Integer> offset = Offset.offset((int) round(expected * 0.03));

    assertThat(actual).isCloseTo(expected, offset);
  }

  @Test
  public void should_calculateSize_closeToROSSize_withManyEntries() {
    final String baseField = "longerbase";
    final String baseValue = "base";

    ArrayList<byte[]> elements = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      elements.add(Coder.stringToBytes(baseField + i));
      elements.add(Coder.stringToBytes(baseValue + i));
    }
    RedisHash hash = new RedisHash(elements);

    Integer actual = hash.getSizeInBytes();
    int expected = reflectionObjectSizer.sizeof(hash);
    Offset<Integer> offset = offset((int) round(expected * 0.03));

    assertThat(actual).isCloseTo(expected, offset);
  }

  /******* put *******/
  @SuppressWarnings("unchecked")
  @Test
  public void hsetShould_calculateSize_equalToSizeCalculatedInConstructor_forMultipleEntries() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String baseField = "field";
    final String baseValue = "value";

    final Region region = mock(Region.class);
    final RedisData returnData = mock(RedisData.class);
    when(region.put(Object.class, Object.class)).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      data.add(Coder.stringToBytes((baseField + i)));
      data.add(Coder.stringToBytes((baseValue + i)));
    }

    hash.hset(region, key, data, false);
    RedisHash expectedRedisHash = new RedisHash(new ArrayList<>(data));

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedRedisHash.getSizeInBytes());
  }

  @Test
  public void hsetShould_calculateSizeDifference_whenUpdatingExistingEntry_newIsShorterThanOld() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String field = "field";
    final String initialValue = "initialValue";
    final String finalValue = "finalValue";

    testThatSizeIsUpdatedWhenUpdatingValue(key, field, initialValue, finalValue);
  }

  @Test
  public void hsetShould_calculateSizeDifference_whenUpdatingExistingEntry_oldIsShorterThanNew() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String field = "field";
    final String initialValue = "initialValue";
    final String finalValue = "longerfinalValue";

    testThatSizeIsUpdatedWhenUpdatingValue(key, field, initialValue, finalValue);
  }

  @Test
  public void hsetShould_calculateSizeDifference_whenUpdatingExistingEntry_valuesAreSameLength() {
    final RedisKey key = new RedisKey("key".getBytes());
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
    initialData.add(Coder.stringToBytes(field));
    initialData.add(Coder.stringToBytes(initialValue));

    hash.hset(region, key, initialData, false);
    RedisHash expectedRedisHash = new RedisHash(new ArrayList<>(initialData));

    List<byte[]> finalData = new ArrayList<>();
    finalData.add(Coder.stringToBytes(field));
    finalData.add(Coder.stringToBytes(finalValue));

    hash.hset(region, key, finalData, false);

    int expectedUpdatedRedisHashSize = expectedRedisHash.getSizeInBytes()
        + (Coder.stringToBytes(finalValue).length - Coder.stringToBytes(initialValue).length);

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedUpdatedRedisHashSize);
  }

  /******* put if absent *******/
  @Test
  public void putIfAbsentShould_calculateSizeEqualToSizeCalculatedInConstructor_forMultipleUniqueEntries() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String baseField = "field";
    final String baseValue = "value";

    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      data.add(Coder.stringToBytes((baseField + i)));
      data.add(Coder.stringToBytes((baseValue + i)));
    }

    hash.hset(region, key, data, true);
    RedisHash expectedRedisHash = new RedisHash(new ArrayList<>(data));
    Offset<Integer> offset = offset((int) round(expectedRedisHash.getSizeInBytes() * 0.05));

    assertThat(hash.getSizeInBytes()).isCloseTo(expectedRedisHash.getSizeInBytes(), offset);
  }

  @Test
  public void putIfAbsentShould_notChangeSize_whenSameDataIsSetTwice() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String baseField = "field";
    final String baseValue = "value";

    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      data.add(Coder.stringToBytes((baseField + i)));
      data.add(Coder.stringToBytes((baseValue + i)));
    }

    hash.hset(region, key, data, true);

    int expectedSize = hash.getSizeInBytes();

    hash.hset(region, key, data, true);

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize);
  }

  @Test
  public void putIfAbsent_shouldNotChangeSize_whenPuttingToExistingFields() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String baseField = "field";
    final String initialBaseValue = "value";
    final String finalBaseValue = "longerValue";

    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    List<byte[]> initialData = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      initialData.add(Coder.stringToBytes((baseField + i)));
      initialData.add(Coder.stringToBytes((initialBaseValue + i)));
    }

    hash.hset(region, key, initialData, true);

    int expectedSize = hash.getSizeInBytes();

    List<byte[]> finalData = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      finalData.add(Coder.stringToBytes((baseField + i)));
      finalData.add(Coder.stringToBytes((finalBaseValue + i)));
    }

    hash.hset(region, key, finalData, true);

    assertThat(hash.getSizeInBytes()).isEqualTo(expectedSize);
  }

  /******* remove *******/
  @Test
  public void sizeShouldDecrease_whenValueIsRemoved() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String baseField = "field";
    final String baseValue = "value";
    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    List<byte[]> data = new ArrayList<>();
    List<byte[]> dataToRemove = new ArrayList<>();
    byte[] field1 = Coder.stringToBytes((baseField + 1));
    byte[] value1 = Coder.stringToBytes((baseValue + 1));
    byte[] field2 = Coder.stringToBytes((baseField + 2));
    byte[] value2 = Coder.stringToBytes((baseValue + 2));
    data.add(field1);
    data.add(value1);
    data.add(field2);
    data.add(value2);
    dataToRemove.add(field1);

    RedisHash redisHash = new RedisHash(data);
    int initialSize = redisHash.getSizeInBytes();

    redisHash.hdel(region, key, dataToRemove);

    int expectedSize = initialSize - RedisHash.HASH_MAP_VALUE_PAIR_OVERHEAD - field1.length;
    Offset<Integer> offset = Offset.offset((int) round(expectedSize * 0.05));

    assertThat(redisHash.getSizeInBytes()).isCloseTo(expectedSize, offset);
  }

  @Test
  public void dataStoreBytesInUse_shouldReturnToHashOverhead_whenAllFieldsAreRemoved() {
    final RedisKey key = new RedisKey("key".getBytes());
    final String baseField = "field";
    final String baseValue = "value";
    final Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);

    RedisHash hash = new RedisHash(Collections.emptyList());
    final int baseRedisHashOverhead = hash.getSizeInBytes();

    List<byte[]> data = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      data.add(Coder.stringToBytes((baseField + i)));
      data.add(Coder.stringToBytes((baseValue + i)));
    }

    hash.hset(region, key, data, false);

    assertThat(hash.getSizeInBytes()).isGreaterThan(0);

    for (int i = 0; i < 100; i++) {
      List<byte[]> toRm = new ArrayList<>();
      toRm.add(Coder.stringToBytes((baseField + i)));
      hash.hdel(region, key, toRm);
    }

    assertThat(hash.getSizeInBytes()).isEqualTo(baseRedisHashOverhead);
    assertThat(hash.hgetall()).isEmpty();
  }

  /************* Helper Methods *************/
  private RedisHash createRedisHash(int NumberOfFields) {
    ArrayList<byte[]> elements = createListOfDataElements(NumberOfFields);
    return new RedisHash(elements);
  }

  private RedisHash createRedisHash(String... keysAndValues) {
    final List<byte[]> keysAndValuesList = Arrays
        .stream(keysAndValues)
        .map(Coder::stringToBytes)
        .collect(Collectors.toList());
    return new RedisHash(keysAndValuesList);
  }

  private ArrayList<byte[]> createListOfDataElements(int NumberOfFields) {
    ArrayList<byte[]> elements = new ArrayList<>();
    for (int i = 0; i < NumberOfFields; i++) {
      elements.add(toBytes("field_" + i));
      elements.add(toBytes("value_" + i));
    }
    return elements;
  }

  private RedisHash createRedisHashWithExpiration(int NumberOfFields, int hcanSnapshotExpiry) {
    ArrayList<byte[]> elements = createListOfDataElements(NumberOfFields);
    return new RedisHash(elements, hcanSnapshotExpiry, hcanSnapshotExpiry);
  }

  private byte[] toBytes(String str) {
    return Coder.stringToBytes(str);
  }
}
