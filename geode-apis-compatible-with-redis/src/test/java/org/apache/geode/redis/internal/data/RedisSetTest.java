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

import static org.apache.geode.redis.internal.data.RedisSet.BASE_REDIS_SET_OVERHEAD;
import static org.apache.geode.redis.internal.data.RedisSet.INTERNAL_HASH_SET_STORAGE_OVERHEAD;
import static org.apache.geode.redis.internal.data.RedisSet.PER_MEMBER_OVERHEAD;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
  private final double percentTolerance = 0.05;

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

  /************* test size of bytes in use *************/

  /******* constructor *******/
  @Test
  public void should_calculateSize_equalToROS_withNoMembers() {
    HashSet<ByteArrayWrapper> members = new HashSet<>();
    RedisSet set = new RedisSet(members);

    int expected = reflectionObjectSizer.sizeof(set);
    int actual = set.getSizeInBytes();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROS_withSingleMember() {
    HashSet<ByteArrayWrapper> members = new HashSet<>();
    members.add(new ByteArrayWrapper("value".getBytes()));
    RedisSet set = new RedisSet(members);

    int expected = reflectionObjectSizer.sizeof(set);
    int actual = set.getSizeInBytes();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_closeToROS_withVaryingMemberCounts() {
    for (int i = 0; i < 1024; i += 16) {
      RedisSet set = createRedisSetOfSpecifiedSize(i);

      int expected = reflectionObjectSizer.sizeof(set);
      Long actual = (long) set.getSizeInBytes();
      Offset<Long> offset = Offset.offset(Math.round(expected * percentTolerance));

      assertThat(actual).isCloseTo(expected, offset);
    }
  }

  @Test
  public void should_calculateSize_closeToROS_withVaryingMemberSize() {
    for (int i = 0; i < 1_600; i++) {
      RedisSet set = createRedisSetWithMemberOfSpecifiedSize(i * 64);
      int expected = reflectionObjectSizer.sizeof(set);
      Long actual = (long) set.getSizeInBytes();
      Offset<Long> offset = Offset.offset(Math.round(expected * percentTolerance));

      assertThat(actual).isCloseTo(expected, offset);
    }
  }

  /******* sadd *******/
  @Test
  public void bytesInUse_sadd_withOneMember() {
    RedisSet set = new RedisSet(new ArrayList<>());
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    String valueString = "value";

    final ByteArrayWrapper value = new ByteArrayWrapper(valueString.getBytes());
    ArrayList<ByteArrayWrapper> members = new ArrayList<>();
    members.add(value);

    set.sadd(members, region, key);

    int actual = set.getSizeInBytes();
    int expected = BASE_REDIS_SET_OVERHEAD + INTERNAL_HASH_SET_STORAGE_OVERHEAD
        + PER_MEMBER_OVERHEAD + valueString.length();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void bytesInUse_sadd_withMultipleMembers() {
    RedisSet set = new RedisSet(new ArrayList<>());
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    String baseString = "value";
    int currentDataSize = 0;

    for (int i = 0; i < 1_000; i++) {
      ArrayList<ByteArrayWrapper> members = new ArrayList<>();
      String valueString = baseString + i;
      currentDataSize += valueString.length();
      final ByteArrayWrapper value = new ByteArrayWrapper((valueString).getBytes());
      members.add(value);
      set.sadd(members, region, key);

      long actual = set.getSizeInBytes();
      long expected = BASE_REDIS_SET_OVERHEAD + INTERNAL_HASH_SET_STORAGE_OVERHEAD
          + (PER_MEMBER_OVERHEAD * (i + 1)) + currentDataSize;
      Offset<Long> offset = Offset.offset(Math.round(expected * percentTolerance));

      assertThat(actual).as("i=" + i).isCloseTo(expected, offset);
    }
  }

  /******* remove *******/
  @Test
  public void size_shouldDecrease_WhenValueIsRemoved() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    final ByteArrayWrapper value1 = new ByteArrayWrapper("value1".getBytes());
    final ByteArrayWrapper value2 = new ByteArrayWrapper("value2".getBytes());

    ArrayList<ByteArrayWrapper> members = new ArrayList<>();
    members.add(value1);
    members.add(value2);
    RedisSet set = new RedisSet(members);

    int initialSize = set.getSizeInBytes();

    ArrayList<ByteArrayWrapper> membersToRemove = new ArrayList<>();
    membersToRemove.add(value1);
    set.srem(membersToRemove, region, key);

    long finalSize = set.getSizeInBytes();
    long expectedSize = initialSize - value1.length() - PER_MEMBER_OVERHEAD;
    Offset<Long> offset = Offset.offset(Math.round(expectedSize * percentTolerance));

    assertThat(finalSize).isCloseTo(expectedSize, offset);
  }

  @Test
  public void remove_sizeShouldReturnToBaseOverhead_whenLastMemberIsRemoved() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey("key".getBytes());
    final ByteArrayWrapper value = new ByteArrayWrapper("value".getBytes());

    ArrayList<ByteArrayWrapper> members = new ArrayList<>();
    members.add(value);
    RedisSet set = new RedisSet(members);

    assertThat(set.getSizeInBytes()).isGreaterThan(BASE_REDIS_SET_OVERHEAD);

    set.srem(members, region, key);

    int finalSize = set.getSizeInBytes();

    assertThat(finalSize).isEqualTo(BASE_REDIS_SET_OVERHEAD);
  }

  /******** constants *******/
  // these tests contain the math that was used to derive the constants in RedisSet. If these tests
  // start failing, it is because the overhead of RedisSet has changed. If it has decreased, good
  // job! You can change the constants in RedisSet to reflect that. If it has increased, carefully
  // consider that increase before changing the constants.
  // Note: the per member overhead is known to not be constant. it changes as more members are
  // added, and/or as the members get longer
  @Test
  public void baseOverheadConstant_shouldMatchCalculatedValue() {
    HashSet<ByteArrayWrapper> members = new HashSet<>();
    int baseRedisSetOverhead = reflectionObjectSizer.sizeof(new RedisSet(members));

    assertThat(baseRedisSetOverhead).isEqualTo(BASE_REDIS_SET_OVERHEAD);
  }

  @Test
  public void perMemberOverheadConstant_shouldMatchCalculatedValue() {
    HashSet<ByteArrayWrapper> tempHashSet = new HashSet<>();
    ByteArrayWrapper member1 = new ByteArrayWrapper("ab".getBytes());
    ByteArrayWrapper member2 = new ByteArrayWrapper("bc".getBytes());
    tempHashSet.add(member1);
    int oneEntryHashSetSize = reflectionObjectSizer.sizeof(tempHashSet);

    tempHashSet.add(member2);
    int twoEntriesHashSetSize = reflectionObjectSizer.sizeof(tempHashSet);

    int perMemberOverhead = twoEntriesHashSetSize - oneEntryHashSetSize + 5;

    assertThat(perMemberOverhead).isEqualTo(PER_MEMBER_OVERHEAD);
  }

  @Test
  public void internalHashsetStorageOverheadConstant_shouldMatchCalculatedValue() {
    HashSet<ByteArrayWrapper> tempHashSet = new HashSet<>();
    int baseHashSetSize = reflectionObjectSizer.sizeof(tempHashSet);

    ByteArrayWrapper baw1 = new ByteArrayWrapper("a".getBytes());
    ByteArrayWrapper baw2 = new ByteArrayWrapper("b".getBytes());

    tempHashSet.add(baw1);
    tempHashSet.add(baw2);

    int twoEntryHashSetSize = reflectionObjectSizer.sizeof(tempHashSet);

    int internalHashsetStorageOverhead =
        twoEntryHashSetSize - (2 * PER_MEMBER_OVERHEAD) - baseHashSetSize;

    assertThat(internalHashsetStorageOverhead).isEqualTo(INTERNAL_HASH_SET_STORAGE_OVERHEAD);
  }

  /******* helper methods *******/
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
    StringBuilder sb = new StringBuilder(stringSize);
    sb.append(base);
    for (int i = base.length(); i < stringSize; i++) {
      int randy = random.nextInt(10);
      sb.append(randy);
    }
    return sb.toString();
  }

}
