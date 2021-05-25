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
import static org.apache.geode.redis.internal.data.RedisSortedSet.BASE_REDIS_SORTED_SET_OVERHEAD;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.assertj.core.data.Offset;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisSortedSetTest {
  private final ReflectionObjectSizer reflectionObjectSizer = ReflectionObjectSizer.getInstance();

  @BeforeClass
  public static void beforeClass() {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_SORTED_SET_ID,
        RedisSortedSet.class);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier.isSynchronized(RedisSortedSet.class
        .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
            .isTrue();
  }

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisSortedSet sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);

    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(sortedSet1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());

    RedisSortedSet sortedSet2 = DataSerializer.readObject(in);
    assertThat(sortedSet2.equals(sortedSet1)).isTrue();
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisSortedSet sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);

    RedisSortedSet sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet2.setExpirationTimestampNoDelta(999);
    assertThat(sortedSet1).isNotEqualTo(sortedSet2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisSortedSet sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);
    RedisSortedSet sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v3");
    sortedSet2.setExpirationTimestampNoDelta(1000);
    assertThat(sortedSet1).isNotEqualTo(sortedSet2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisSortedSet sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestampNoDelta(1000);
    RedisSortedSet sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet2.setExpirationTimestampNoDelta(1000);
    assertThat(sortedSet1).isEqualTo(sortedSet2);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptySortedSets() {
    RedisSortedSet sortedSet1 = new RedisSortedSet(Collections.emptyList());
    RedisSortedSet sortedSet2 = NullRedisDataStructures.NULL_REDIS_SORTED_SET;
    assertThat(sortedSet1).isEqualTo(sortedSet2);
    assertThat(sortedSet2).isEqualTo(sortedSet1);
  }

  @Test
  public void zadd_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = uncheckedCast(Mockito.mock(Region.class));
    RedisSortedSet sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");

    List<byte[]> adds = new ArrayList<>();
    adds.add("1.61803".getBytes());
    adds.add("v3".getBytes());

    sortedSet1.zadd(region, null, adds, new ZAddOptions(ZAddOptions.Exists.NONE));
    assertThat(sortedSet1.hasDelta()).isTrue();

    HeapDataOutputStream out = new HeapDataOutputStream(100);
    sortedSet1.toDelta(out);
    assertThat(sortedSet1.hasDelta()).isFalse();

    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSortedSet sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    assertThat(sortedSet2).isNotEqualTo(sortedSet1);

    sortedSet2.fromDelta(in);
    assertThat(sortedSet2).isEqualTo(sortedSet1);
  }

  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() throws IOException {
    Region<RedisKey, RedisData> region = uncheckedCast(Mockito.mock(Region.class));
    RedisSortedSet sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    sortedSet1.setExpirationTimestamp(region, null, 999);
    assertThat(sortedSet1.hasDelta()).isTrue();

    HeapDataOutputStream out = new HeapDataOutputStream(100);
    sortedSet1.toDelta(out);
    assertThat(sortedSet1.hasDelta()).isFalse();

    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSortedSet sortedSet2 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");
    assertThat(sortedSet2).isNotEqualTo(sortedSet1);

    sortedSet2.fromDelta(in);
    assertThat(sortedSet2).isEqualTo(sortedSet1);
  }

  /****************** Size ******************/
  @Test
  public void constantBaseRedisSortedSetOverhead_shouldEqualCalculatedOverhead() {
    RedisSortedSet sortedSet = new RedisSortedSet(Collections.emptyList());
    int baseRedisSortedSetOverhead = reflectionObjectSizer.sizeof(sortedSet);

    assertThat(baseRedisSortedSetOverhead).isEqualTo(BASE_REDIS_SORTED_SET_OVERHEAD);
    assertThat(sortedSet.getSizeInBytes()).isEqualTo(baseRedisSortedSetOverhead);
  }

  @Test
  public void constantValuePairOverhead_shouldEqualCalculatedOverhead() {
    RedisSortedSet sortedSet = new RedisSortedSet(Collections.emptyList());

    // Used to compute the average per field overhead
    double totalOverhead = 0;
    final int totalFields = 1000;

    // Generate pseudo-random data, but use fixed seed so the test is deterministic
    Random random = new Random(0);

    // Add 1000 fields and compute the per field overhead after each add
    for (int fieldCount = 1; fieldCount < totalFields; fieldCount++) {
      // Add a random field
      byte[] data = new byte[random.nextInt(30)];
      random.nextBytes(data);
      sortedSet.memberAdd(data, data);

      // Compute the measured size
      int size = reflectionObjectSizer.sizeof(sortedSet);
      final int dataSize = 2 * data.length;

      // Compute per field overhead with this number of fields
      int overHeadPerField = (size - BASE_REDIS_SORTED_SET_OVERHEAD - dataSize) / fieldCount;
      totalOverhead += overHeadPerField;
    }

    // Assert that the average overhead matches the constant
    long averageOverhead = Math.round(totalOverhead / totalFields);
    assertThat(RedisSortedSet.PER_PAIR_OVERHEAD).isEqualTo(averageOverhead);
  }

  @Test
  @Ignore("Redo when we have a defined Sizable strategy")
  public void should_calculateSize_closeToROSSize_ofIndividualInstanceWithSingleValue() {
    List<byte[]> data = new ArrayList<>();
    data.add("1.0".getBytes());
    data.add("membernamethatisverylonggggggggg".getBytes());

    RedisSortedSet sortedSet = new RedisSortedSet(data);

    final int expected = reflectionObjectSizer.sizeof(sortedSet);
    final int actual = sortedSet.getSizeInBytes();

    final Offset<Integer> offset = Offset.offset((int) round(expected * 0.03));

    assertThat(actual).isCloseTo(expected, offset);
  }

  @Test
  @Ignore("Redo when we have a defined Sizable strategy")
  public void should_calculateSize_closeToROSSize_ofIndividualInstanceWithMultipleValues() {
    RedisSortedSet sortedSet =
        createRedisSortedSet("1.0", "memberUnoIsTheFirstMember",
            "2.0", "memberDueIsTheSecondMember", "3.0", "memberTreIsTheThirdMember",
            "4.0", "memberQuatroIsTheThirdMember");

    final int expected = reflectionObjectSizer.sizeof(sortedSet);
    final int actual = sortedSet.getSizeInBytes();

    final Offset<Integer> offset = Offset.offset((int) round(expected * 0.03));

    assertThat(actual).isCloseTo(expected, offset);
  }

  private RedisSortedSet createRedisSortedSet(String... membersAndScores) {
    final List<byte[]> membersAndScoresList = Arrays
        .stream(membersAndScores)
        .map(Coder::stringToBytes)
        .collect(Collectors.toList());
    return new RedisSortedSet(membersAndScoresList);
  }
}
