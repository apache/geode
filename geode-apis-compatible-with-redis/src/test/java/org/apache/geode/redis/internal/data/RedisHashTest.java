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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
    InternalDataSerializer
        .getDSFIDSerializer()
        .registerDSFID(DataSerializableFixedID.REDIS_BYTE_ARRAY_WRAPPER, ByteArrayWrapper.class);
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

  private RedisHash createRedisHash(String k1, String v1, String k2, String v2) {
    ArrayList<ByteArrayWrapper> elements = new ArrayList<>();
    elements.add(createByteArrayWrapper(k1));
    elements.add(createByteArrayWrapper(v1));
    elements.add(createByteArrayWrapper(k2));
    elements.add(createByteArrayWrapper(v2));
    return new RedisHash(elements);
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

  @SuppressWarnings("unchecked")
  @Test
  public void hset_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = Mockito.mock(Region.class);
    RedisHash o1 = createRedisHash("k1", "v1", "k2", "v2");
    ByteArrayWrapper k3 = createByteArrayWrapper("k3");
    ByteArrayWrapper v3 = createByteArrayWrapper("v3");
    ArrayList<ByteArrayWrapper> adds = new ArrayList<>();
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
    ByteArrayWrapper k1 = createByteArrayWrapper("k1");
    ArrayList<ByteArrayWrapper> removes = new ArrayList<>();
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

  @Test
  public void hscanSnaphots_shouldBeEmpty_givenHscanHasNotBeenCalled() {
    RedisHash subject = createRedisHash(100);
    assertThat(subject.getHscanSnapShots()).isEmpty();
  }

  @Test
  public void hscanSnaphots_shouldContainSnapshot_givenHscanHasBeenCalled() {

    final List<ByteArrayWrapper> FIELDS_AND_VALUES_FOR_HASH = createListOfDataElements(100);
    RedisHash subject = new RedisHash(FIELDS_AND_VALUES_FOR_HASH);
    UUID clientID = UUID.randomUUID();

    subject.hscan(clientID, null, 10, 0);

    ConcurrentHashMap<UUID, List<ByteArrayWrapper>> hscanSnapShotMap = subject.getHscanSnapShots();

    assertThat(hscanSnapShotMap.containsKey(clientID)).isTrue();

    List<ByteArrayWrapper> keyList = hscanSnapShotMap.get(clientID);
    assertThat(keyList).isNotEmpty();

    FIELDS_AND_VALUES_FOR_HASH.forEach((entry) -> {
      if (entry.toString().contains("field")) {
        assertThat(keyList).contains(entry);
      } else if (entry.toString().contains("value")) {
        assertThat(keyList).doesNotContain(entry);
      }
    });

  }

  @Test
  public void hscanSnaphots_shouldContainSnapshot_givenHscanHasBeenCalled_WithNonZeroCursor() {

    final List<ByteArrayWrapper> FIELDS_AND_VALUES_FOR_HASH = createListOfDataElements(100);
    RedisHash subject = new RedisHash(FIELDS_AND_VALUES_FOR_HASH);
    UUID clientID = UUID.randomUUID();

    subject.hscan(clientID, null, 10, 10);

    ConcurrentHashMap<UUID, List<ByteArrayWrapper>> hscanSnapShotMap = subject.getHscanSnapShots();

    assertThat(hscanSnapShotMap.containsKey(clientID)).isTrue();

    List<ByteArrayWrapper> keyList = hscanSnapShotMap.get(clientID);
    assertThat(keyList).isNotEmpty();

    FIELDS_AND_VALUES_FOR_HASH.forEach((entry) -> {
      if (entry.toString().contains("field")) {
        assertThat(keyList).contains(entry);
      } else if (entry.toString().contains("value")) {
        assertThat(keyList).doesNotContain(entry);
      }
    });
  }

  @Test
  public void hscanSnaphots_shouldBeRemoved_givenCompleteIteration() {
    RedisHash subject = createRedisHashWithExpiration(1, 100000);
    UUID client_ID = UUID.randomUUID();

    subject.hscan(client_ID, null, 10, 0);

    ConcurrentHashMap<UUID, List<ByteArrayWrapper>> hscanSnapShotMap = subject.getHscanSnapShots();
    assertThat(hscanSnapShotMap).isEmpty();
  }

  @Test
  public void hscanSnaphots_shouldExpireAfterExpiryPeriod() {
    RedisHash subject = createRedisHashWithExpiration(1000, 1);
    UUID client_ID = UUID.randomUUID();

    subject.hscan(client_ID, null, 1, 0);

    GeodeAwaitility.await().atMost(4, SECONDS).untilAsserted(() -> {
      ConcurrentHashMap<UUID, List<ByteArrayWrapper>> hscanSnapShotMap =
          subject.getHscanSnapShots();
      assertThat(hscanSnapShotMap).isEmpty();
    });
  }

  @Test
  public void hashSizeOverhead_shouldNotBeChanged_withoutForethoughtAndTesting() {
    assertThat(RedisHash.PER_OBJECT_OVERHEAD).isEqualTo(8);
    assertThat(RedisHash.getPerByteArrayWrapperOverhead())
        .isEqualTo(RedisHash.PER_OBJECT_OVERHEAD + 46);
    assertThat(RedisHash.getPerHashOverhead()).isEqualTo(RedisHash.PER_OBJECT_OVERHEAD + 116);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void should_calculateSize_closeToROSSize_ofIndividualInstance() {
    RedisHash current = createRedisHash("k1", "v1", "k2", "v2");

    int expected = reflectionObjectSizer.sizeof(current);
    Long actual = Long.valueOf(current.getSizeInBytes());
    Offset<Long> offset = Offset.offset(Math.round(expected * 0.05));

    assertThat(actual).isCloseTo(expected, offset);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void should_calculateSize_closeToROSSize_withManyEntries() {
    final String baseString = "b";

    ArrayList<ByteArrayWrapper> elements = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      elements.add(createByteArrayWrapper(baseString + i));
      elements.add(createByteArrayWrapper(baseString + i));
    }
    RedisHash o1 = new RedisHash(elements);

    Long actual = Long.valueOf(o1.getSizeInBytes());
    int expected = reflectionObjectSizer.sizeof(o1);
    Offset<Long> offset = Offset.offset(Math.round(expected * 0.05));

    assertThat(actual).isCloseTo(expected, offset);
  }

  private RedisHash createRedisHash(int NumberOfFields) {
    ArrayList<ByteArrayWrapper> elements = createListOfDataElements(NumberOfFields);
    return new RedisHash(elements);
  }

  private ArrayList<ByteArrayWrapper> createListOfDataElements(int NumberOfFields) {
    ArrayList<ByteArrayWrapper> elements = new ArrayList<>();
    for (int i = 0; i < NumberOfFields; i++) {
      elements.add(createByteArrayWrapper("field_" + i));
      elements.add(createByteArrayWrapper("value_" + i));
    }
    return elements;
  }

  private RedisHash createRedisHashWithExpiration(int NumberOfFields, int hcanSnapshotExpiry) {
    ArrayList<ByteArrayWrapper> elements = createListOfDataElements(NumberOfFields);
    return new RedisHash(elements, hcanSnapshotExpiry, hcanSnapshotExpiry);
  }

  private ByteArrayWrapper createByteArrayWrapper(String str) {
    return new ByteArrayWrapper(Coder.stringToBytes(str));
  }
}
