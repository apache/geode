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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.assertj.core.data.Offset;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.ReflectionObjectSizer;

public class RedisSetTest {
  private final ReflectionObjectSizer reflectionObjectSizer = ReflectionObjectSizer.getInstance();

  @BeforeClass
  public static void beforeClass() {
    InternalDataSerializer
        .getDSFIDSerializer()
        .registerDSFID(DataSerializableFixedID.REDIS_BYTE_ARRAY_WRAPPER, ByteArrayWrapper.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_SET_ID,
        RedisSet.class);
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisSet o1 = createRedisSet(1, 2);
    o1.setExpirationTimestampNoDelta(1000);
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(o1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet o2 = DataSerializer.readObject(in);
    assertThat(o2).isEqualTo(o1);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier
        .isSynchronized(RedisSet.class
            .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
                .isTrue();
  }

  private RedisSet createRedisSet(int m1, int m2) {
    return new RedisSet(Arrays.asList(
        new ByteArrayWrapper(new byte[] {(byte) m1}),
        new ByteArrayWrapper(new byte[] {(byte) m2})));
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisSet o1 = createRedisSet(1, 2);
    o1.setExpirationTimestampNoDelta(1000);
    RedisSet o2 = createRedisSet(1, 2);
    o2.setExpirationTimestampNoDelta(999);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisSet o1 = createRedisSet(1, 2);
    o1.setExpirationTimestampNoDelta(1000);
    RedisSet o2 = createRedisSet(1, 3);
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isNotEqualTo(o2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisSet o1 = createRedisSet(1, 2);
    o1.setExpirationTimestampNoDelta(1000);
    RedisSet o2 = createRedisSet(1, 2);
    o2.setExpirationTimestampNoDelta(1000);
    assertThat(o1).isEqualTo(o2);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptySets() {
    RedisSet o1 = new RedisSet(Collections.emptyList());
    RedisSet o2 = NullRedisDataStructures.NULL_REDIS_SET;
    assertThat(o1).isEqualTo(o2);
    assertThat(o2).isEqualTo(o1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void sadd_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = mock(Region.class);
    RedisSet o1 = createRedisSet(1, 2);
    ByteArrayWrapper member3 = new ByteArrayWrapper(new byte[] {3});
    ArrayList<ByteArrayWrapper> adds = new ArrayList<>();
    adds.add(member3);
    o1.sadd(adds, region, null);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet o2 = createRedisSet(1, 2);
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void srem_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = mock(Region.class);
    RedisSet o1 = createRedisSet(1, 2);
    ByteArrayWrapper member1 = new ByteArrayWrapper(new byte[] {1});
    ArrayList<ByteArrayWrapper> removes = new ArrayList<>();
    removes.add(member1);
    o1.srem(removes, region, null);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet o2 = createRedisSet(1, 2);
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = mock(Region.class);
    RedisSet o1 = createRedisSet(1, 2);
    o1.setExpirationTimestamp(region, null, 999);
    assertThat(o1.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    o1.toDelta(out);
    assertThat(o1.hasDelta()).isFalse();
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet o2 = createRedisSet(1, 2);
    assertThat(o2).isNotEqualTo(o1);
    o2.fromDelta(in);
    assertThat(o2).isEqualTo(o1);
  }

  @Test
  public void overheadConstants_shouldNotChange_withoutForethoughtAndTesting() {
    assertThat(RedisSet.PER_OBJECT_OVERHEAD).isEqualTo(8);
    assertThat(RedisSet.getPerSetOverhead()).isEqualTo(RedisSet.PER_OBJECT_OVERHEAD + 104);
    assertThat(RedisSet.getPerMemberOverhead()).isEqualTo(RedisSet.PER_OBJECT_OVERHEAD + 70);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void should_calculateSize_closeToROS_withVaryingMemberCounts() {
    for (int i = 0; i < 1024; i += 16) {
      RedisSet set = createRedisSetOfSpecifiedSize(i);

      int expected = reflectionObjectSizer.sizeof(set);
      Long actual = Long.valueOf(set.getSizeInBytes());
      Offset<Long> offset = Offset.offset(Math.round(expected * 0.06));

      assertThat(actual).isCloseTo(expected, offset);
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void should_calculateSize_closeToROS_withVaryingMemberSize() {
    for (int i = 0; i < 16; i++) {
      RedisSet set = createRedisSetWithMemberOfSpecifiedSize(i * 64);
      int expected = reflectionObjectSizer.sizeof(set);
      Long actual = Long.valueOf(set.getSizeInBytes());
      Offset<Long> offset = Offset.offset(Math.round(expected * 0.06));

      assertThat(actual).isCloseTo(expected, offset);
    }
  }

  private RedisSet createRedisSetOfSpecifiedSize(int setSize) {
    ArrayList<ByteArrayWrapper> arrayList = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      arrayList.add(new ByteArrayWrapper(("abcdefgh" + i).getBytes()));
    }
    return new RedisSet(arrayList);
  }

  private RedisSet createRedisSetWithMemberOfSpecifiedSize(int memberSize) {
    ArrayList<ByteArrayWrapper> arrayList = new ArrayList<>();
    ByteArrayWrapper member =
        new ByteArrayWrapper(createMemberOfSpecifiedSize("a", memberSize).getBytes());
    if (member.length() > 0) {
      arrayList.add(member);
    }
    return new RedisSet(arrayList);
  }

  private String createMemberOfSpecifiedSize(final String base, final int stringSize) {
    Random random = new Random();
    if (base.length() > stringSize) {
      return "";
    }
    StringBuffer sb = new StringBuffer(stringSize);
    sb.append(base);
    for (int i = base.length(); i < stringSize; i++) {
      int randy = random.nextInt(10);
      sb.append(randy);
    }
    String javaString = sb.toString();
    return javaString;
  }

}
