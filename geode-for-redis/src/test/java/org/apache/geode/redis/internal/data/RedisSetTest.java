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

import static org.apache.geode.redis.internal.data.AbstractRedisData.NO_EXPIRATION;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisSet.setOpStoreResult;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.ReflectionObjectSizer;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.services.RegionProvider;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RedisSetTest {

  @ClassRule
  public static final ExecutorServiceRule executor = new ExecutorServiceRule();

  private final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();

  @Test
  public void confirmSerializationIsStable() throws IOException, ClassNotFoundException {
    RedisSet set1 = createRedisSet(1, 2);
    int expirationTimestamp = 1000;
    set1.setExpirationTimestampNoDelta(expirationTimestamp);
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    DataSerializer.writeObject(set1, out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet set2 = DataSerializer.readObject(in);
    assertThat(set2).isEqualTo(set1);
    assertThat(set2.getExpirationTimestamp())
        .isEqualTo(set1.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
  }

  @Test
  public void confirmToDataIsSynchronized() throws NoSuchMethodException {
    assertThat(Modifier
        .isSynchronized(RedisSet.class
            .getMethod("toData", DataOutput.class, SerializationContext.class).getModifiers()))
                .isTrue();
  }

  private RedisSet createRedisSet(int m1, int m2) {
    return new RedisSet(Arrays.asList(new byte[] {(byte) m1}, new byte[] {(byte) m2}));
  }

  @Test
  public void equals_returnsFalse_givenDifferentExpirationTimes() {
    RedisSet set1 = createRedisSet(1, 2);
    set1.setExpirationTimestampNoDelta(1000);
    RedisSet set2 = createRedisSet(1, 2);
    set2.setExpirationTimestampNoDelta(999);
    assertThat(set1).isNotEqualTo(set2);
  }

  @Test
  public void equals_returnsFalse_givenDifferentValueBytes() {
    RedisSet set1 = createRedisSet(1, 2);
    set1.setExpirationTimestampNoDelta(1000);
    RedisSet set2 = createRedisSet(1, 3);
    set2.setExpirationTimestampNoDelta(1000);
    assertThat(set1).isNotEqualTo(set2);
  }

  @Test
  public void equals_returnsTrue_givenEqualValueBytesAndExpiration() {
    RedisSet set1 = createRedisSet(1, 2);
    int expirationTimestamp = 1000;
    set1.setExpirationTimestampNoDelta(expirationTimestamp);
    RedisSet set2 = createRedisSet(1, 2);
    set2.setExpirationTimestampNoDelta(expirationTimestamp);
    assertThat(set1).isEqualTo(set2);
    assertThat(set2.getExpirationTimestamp())
        .isEqualTo(set1.getExpirationTimestamp())
        .isEqualTo(expirationTimestamp);
  }

  @Test
  public void equals_returnsTrue_givenDifferentEmptySets() {
    RedisSet set1 = new RedisSet(Collections.emptyList());
    RedisSet set2 = NULL_REDIS_SET;
    assertThat(set1).isEqualTo(set2);
    assertThat(set2).isEqualTo(set1);
  }

  @Test
  public void sadd_stores_delta_that_is_stable() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    when(region.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    RedisSet set1 = createRedisSet(1, 2);
    byte[] member3 = new byte[] {3};
    ArrayList<byte[]> adds = new ArrayList<>();
    adds.add(member3);

    set1.sadd(adds, region, null);

    verify(region).put(any(), any());
    assertThat(set1.hasDelta()).isFalse();
  }

  @Test
  public void srem_stores_delta_that_is_stable() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    when(region.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    RedisSet set1 = createRedisSet(1, 2);
    byte[] member1 = new byte[] {1};
    ArrayList<byte[]> removes = new ArrayList<>();
    removes.add(member1);

    set1.srem(removes, region, null);

    verify(region).put(any(), any());
    assertThat(set1.hasDelta()).isFalse();
  }

  @Test
  public void sdiffstore_stores_delta_that_is_stable() {
    RegionProvider regionProvider = uncheckedCast(mock(RegionProvider.class));
    Region<RedisKey, RedisData> dataRegion = uncheckedCast(mock(Region.class));

    RedisSet setDest = createRedisSet(1, 2);

    when(regionProvider.getTypedRedisDataElseRemove(REDIS_SET, null, false)).thenReturn(setDest);
    when(regionProvider.getDataRegion()).thenReturn(dataRegion);
    when(dataRegion.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    RedisSet.MemberSet set = new RedisSet.MemberSet();
    set.add(new byte[] {3});
    setOpStoreResult(regionProvider, null, set);

    verify(dataRegion).put(any(), any());
    assertThat(setDest.hasDelta()).isFalse();
  }

  @Test
  public void sdiffstore_sets_expiration_time_to_zero() {
    RegionProvider regionProvider = uncheckedCast(mock(RegionProvider.class));
    Region<RedisKey, RedisData> dataRegion = uncheckedCast(mock(Region.class));

    RedisSet setDest = createRedisSet(1, 2);
    setDest.setExpirationTimestamp(dataRegion, null, 100);

    when(regionProvider.getTypedRedisDataElseRemove(REDIS_SET, null, false)).thenReturn(setDest);
    when(regionProvider.getDataRegion()).thenReturn(dataRegion);
    when(dataRegion.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    RedisSet.MemberSet set = new RedisSet.MemberSet();
    set.add(new byte[] {3});
    setOpStoreResult(regionProvider, null, set);

    assertThat(setDest.getExpirationTimestamp()).isEqualTo(NO_EXPIRATION);
    assertThat(setDest.hasDelta()).isFalse();
  }

  @Test
  public void setExpirationTimestamp_stores_delta_that_is_stable() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    when(region.put(any(), any())).thenAnswer(this::validateDeltaSerialization);

    RedisSet set1 = createRedisSet(1, 2);
    set1.setExpirationTimestamp(region, null, 999);

    verify(region).put(any(), any());
    assertThat(set1.hasDelta()).isFalse();
  }

  /************* test size of bytes in use *************/

  /******* constructor *******/
  @Test
  public void should_calculateSize_equalToROS_withNoMembers() {
    Set<byte[]> members = new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);
    RedisSet set = new RedisSet(members);

    int expected = expectedSize(set);
    int actual = set.getSizeInBytes();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROS_withSingleMember() {
    Set<byte[]> members = new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);
    members.add(stringToBytes("value"));
    RedisSet set = new RedisSet(members);

    int actual = set.getSizeInBytes();
    int expected = expectedSize(set);

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_calculateSize_equalToROS_withVaryingMemberCounts() {
    for (int i = 0; i < 1024; i += 16) {
      RedisSet set = createRedisSetOfSpecifiedSize(i);

      int expected = expectedSize(set);
      int actual = set.getSizeInBytes();

      assertThat(actual).isEqualTo(expected);
    }
  }

  private int expectedSize(RedisSet set) {
    return sizer.sizeof(set) - sizer.sizeof(ByteArrays.HASH_STRATEGY);
  }

  @Test
  public void should_calculateSize_equalToROS_withVaryingMemberSize() {
    for (int i = 0; i < 1024; i += 16) {
      RedisSet set = createRedisSetWithMemberOfSpecifiedSize(i * 64);
      int expected = expectedSize(set);
      int actual = set.getSizeInBytes();

      assertThat(actual).isEqualTo(expected);
    }
  }

  /******* sadd *******/
  @Test
  public void bytesInUse_sadd_withOneMember() {
    RedisSet set = new RedisSet(new ArrayList<>());
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey(stringToBytes("key"));
    String valueString = "value";

    final byte[] value = stringToBytes(valueString);
    List<byte[]> members = new ArrayList<>();
    members.add(value);

    set.sadd(members, region, key);

    assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
  }

  @Test
  public void bytesInUse_sadd_withMultipleMembers() {
    RedisSet set = new RedisSet(new ArrayList<>());
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey(stringToBytes("key"));
    String baseString = "value";

    for (int i = 0; i < 1_000; i++) {
      List<byte[]> members = new ArrayList<>();
      String valueString = baseString + i;
      final byte[] value = stringToBytes(valueString);
      members.add(value);
      set.sadd(members, region, key);

      long actual = set.getSizeInBytes();
      long expected = expectedSize(set);

      assertThat(actual).isEqualTo(expected);
    }
  }

  /******* remove *******/
  @Test
  public void size_shouldDecrease_WhenValueIsRemoved() {
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    final RedisData returnData = mock(RedisData.class);
    when(region.put(any(RedisKey.class), any(RedisData.class))).thenReturn(returnData);
    final RedisKey key = new RedisKey(stringToBytes("key"));
    final byte[] value1 = stringToBytes("value1");
    final byte[] value2 = stringToBytes("value2");

    List<byte[]> members = new ArrayList<>();
    members.add(value1);
    members.add(value2);
    RedisSet set = new RedisSet(members);

    List<byte[]> membersToRemove = new ArrayList<>();
    membersToRemove.add(value1);
    set.srem(membersToRemove, region, key);

    long finalSize = set.getSizeInBytes();
    long expectedSize = expectedSize(set);

    assertThat(finalSize).isEqualTo(expectedSize);
  }

  /******** add and remove *******/
  @Test
  public void testSAddsAndSRems_changeSizeToMatchROSSize() {
    // Start with a non-empty set, add enough members to force a resize of the backing set, remove
    // all but one member, then add members back and assert that the calculated size is correct
    // after every operation
    List<byte[]> initialMembers = new ArrayList<>();
    int numOfInitialMembers = 128;
    for (int i = 0; i < numOfInitialMembers; ++i) {
      byte[] data = Coder.intToBytes(i);
      initialMembers.add(data);
    }

    RedisSet set = new RedisSet(initialMembers);

    assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));

    int membersToAdd = numOfInitialMembers * 3;
    doAddsAndAssertSize(set, membersToAdd);

    doRemovesAndAssertSize(set, set.scard() - 1);

    doAddsAndAssertSize(set, membersToAdd);
  }

  @Test
  public void testConcurrencyWhenAddingMembers() throws Exception {
    int expectedSize = 1000;
    // Make sure the initial size is smaller than the expected size.
    RedisSet set = new RedisSet(1);

    AtomicBoolean running = new AtomicBoolean(true);
    Future<Void> future1 = executor.submit(() -> iterateOverSet(set, running));
    Future<Void> future2 = executor.submit(() -> addToSet(set, expectedSize));

    future2.get();
    running.set(false);
    future1.get();

    assertThat(set.scard()).isEqualTo(expectedSize);
  }

  @Test
  public void testConcurrencyWhenRemovingMembers() throws Exception {
    int numOfInitialMembers = 1000;
    RedisSet set = new RedisSet(numOfInitialMembers);
    for (int i = 0; i < numOfInitialMembers; ++i) {
      set.membersAdd(Coder.intToBytes(i));
    }

    AtomicBoolean running = new AtomicBoolean(true);
    Future<Void> future1 = executor.submit(() -> iterateOverSet(set, running));
    Future<Void> future2 = executor.submit(() -> deleteFromSet(set));

    future2.get();
    running.set(false);
    future1.get();

    assertThat(set.scard()).isEqualTo(0);
  }


  /******* helper methods *******/

  private void addToSet(RedisSet set, int count) {
    for (int i = 0; i < count; i++) {
      assertThat(set.membersAdd(Coder.intToBytes(i))).isTrue();
    }
  }

  private void deleteFromSet(RedisSet set) {
    int size = set.scard();
    for (int i = 0; i < size; i++) {
      Collection<byte[]> removals = set.srandmember(1);
      byte[] candidate = removals.iterator().next();
      assertThat(set.membersRemove(candidate))
          .as("Did not find " + Coder.bytesToLong(candidate)).isTrue();
    }
  }

  private void iterateOverSet(RedisSet set, AtomicBoolean running) throws Exception {
    while (running.get()) {
      HeapDataOutputStream out = new HeapDataOutputStream(100);
      set.toData(out, null);
    }
  }

  private RedisSet createRedisSetOfSpecifiedSize(int setSize) {
    List<byte[]> arrayList = new ArrayList<>();
    for (int i = 0; i < setSize; i++) {
      arrayList.add(stringToBytes(("abcdefgh" + i)));
    }
    return new RedisSet(arrayList);
  }

  private RedisSet createRedisSetWithMemberOfSpecifiedSize(int memberSize) {
    List<byte[]> arrayList = new ArrayList<>();
    byte[] member = stringToBytes(createMemberOfSpecifiedSize("a", memberSize));
    if (member.length > 0) {
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

  void doAddsAndAssertSize(RedisSet set, int membersToAdd) {
    for (int i = 0; i < membersToAdd; ++i) {
      int initialSize = sizer.sizeof(set);
      int initialCalculatedSize = set.getSizeInBytes();

      byte[] data = Coder.intToBytes(set.scard());
      assertThat(set.membersAdd(data)).isTrue();

      int actualOverhead = sizer.sizeof(set) - initialSize;
      int calculatedOH = set.getSizeInBytes() - initialCalculatedSize;

      assertThat(calculatedOH).isEqualTo(actualOverhead);
    }
    assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
  }

  void doRemovesAndAssertSize(RedisSet set, int membersToRemove) {
    int initialCapacity = set.scard();
    for (int i = 1; i < membersToRemove; ++i) {
      int initialSize = sizer.sizeof(set);
      int initialCalculatedSize = set.getSizeInBytes();

      byte[] data = Coder.intToBytes(initialCapacity - i);
      assertThat(set.membersRemove(data)).isTrue();

      int actualOverhead = sizer.sizeof(set) - initialSize;
      int calculatedOH = set.getSizeInBytes() - initialCalculatedSize;

      assertThat(calculatedOH).isEqualTo(actualOverhead);
    }
    assertThat(set.getSizeInBytes()).isEqualTo(expectedSize(set));
  }

  private Object validateDeltaSerialization(InvocationOnMock invocation) throws IOException {
    RedisSet value = invocation.getArgument(1, RedisSet.class);
    assertThat(value.hasDelta()).isTrue();
    HeapDataOutputStream out = new HeapDataOutputStream(100);
    value.toDelta(out);
    ByteArrayDataInput in = new ByteArrayDataInput(out.toByteArray());
    RedisSet set2 = createRedisSet(1, 2);
    assertThat(set2).isNotEqualTo(value);
    set2.fromDelta(in);
    assertThat(set2).isEqualTo(value);
    return null;
  }
}
