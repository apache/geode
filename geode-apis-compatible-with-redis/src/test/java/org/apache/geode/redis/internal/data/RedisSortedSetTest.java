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

import static org.apache.geode.redis.internal.data.RedisSortedSet.BASE_REDIS_SORTED_SET_OVERHEAD;
import static org.apache.geode.redis.internal.data.RedisSortedSet.OrderedSetEntry.BASE_ORDERED_SET_ENTRY_SIZE;
import static org.apache.geode.redis.internal.netty.Coder.doubleToBytes;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bGREATEST_MEMBER_NAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEAST_MEMBER_NAME;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
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
import org.apache.geode.redis.internal.collections.OrderStatisticsSet;
import org.apache.geode.redis.internal.collections.OrderStatisticsTree;
import org.apache.geode.redis.internal.collections.SizeableObject2ObjectOpenCustomHashMapWithCursor;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisSortedSetTest {
  private final ReflectionObjectSizer sizer = ReflectionObjectSizer.getInstance();

  private final String member1 = "member1";
  private final String member2 = "member2";
  private final String score1 = "5.55555";
  private final String score2 = "209030.31";
  private final RedisSortedSet rangeSortedSet =
      createRedisSortedSet(
          "1.0", member1, "1.1", member2, "1.2", "member3", "1.3", "member4",
          "1.4", "member5", "1.5", "member6", "1.6", "member7", "1.7", "member8",
          "1.8", "member9", "1.9", "member10", "2.0", "member11", "2.1", "member12");

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
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    RedisSortedSet sortedSet1 = createRedisSortedSet("3.14159", "v1", "2.71828", "v2");

    List<byte[]> adds = new ArrayList<>();
    adds.add(stringToBytes("1.61803"));
    adds.add(stringToBytes("v3"));

    sortedSet1.zadd(region, null, adds, new ZAddOptions(ZAddOptions.Exists.NONE, false, false));
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
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
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

  @Test
  public void zremCanRemoveMembersToBeRemoved() {
    String member3 = "member3";
    String score3 = "998955255.66361191";
    RedisSortedSet sortedSet =
        spy(createRedisSortedSet(score1, member1, score2, member2, score3, member3));
    Region<RedisKey, RedisData> region = uncheckedCast(mock(Region.class));
    RedisKey key = new RedisKey();
    ArrayList<byte[]> membersToRemove = new ArrayList<>();
    membersToRemove.add(stringToBytes("nonExisting"));
    membersToRemove.add(stringToBytes(member1));
    membersToRemove.add(stringToBytes(member3));

    long removed = sortedSet.zrem(region, key, membersToRemove);

    assertThat(removed).isEqualTo(2);
    verify(sortedSet).storeChanges(eq(region), eq(key), any(RemsDeltaInfo.class));
  }

  @Test
  public void memberRemoveCanRemoveMemberInSortedSet() {
    RedisSortedSet sortedSet = createRedisSortedSet(score1, member1, score2, member2);
    RedisSortedSet sortedSet2 = createRedisSortedSet(score2, member2);

    byte[] returnValue = sortedSet.memberRemove(stringToBytes(member1));

    assertThat(sortedSet).isEqualTo(sortedSet2);
    assertThat(returnValue).isEqualTo(stringToBytes(score1));
  }

  @Test
  public void zrange_ShouldReturnEmptyList_GivenInvalidRanges() {
    Collection<byte[]> rangeList = rangeSortedSet.zrange(5, 0, false);
    assertThat(rangeList).isEmpty();
    rangeList = rangeSortedSet.zrange(13, 15, false);
    assertThat(rangeList).isEmpty();
    rangeList = rangeSortedSet.zrange(17, -2, false);
    assertThat(rangeList).isEmpty();
    rangeList = rangeSortedSet.zrange(12, 12, false);
    assertThat(rangeList).isEmpty();
  }

  @Test
  public void zrange_ShouldReturnSimpleRanges() {
    Collection<byte[]> rangeList = rangeSortedSet.zrange(0, 5, false);
    assertThat(rangeList).hasSize(6);
    assertThat(rangeList)
        .containsExactly("member1".getBytes(), "member2".getBytes(), "member3".getBytes(),
            "member4".getBytes(), "member5".getBytes(), "member6".getBytes());

    rangeList = rangeSortedSet.zrange(5, 10, false);
    assertThat(rangeList).hasSize(6);
    assertThat(rangeList)
        .containsExactly("member6".getBytes(), "member7".getBytes(), "member8".getBytes(),
            "member9".getBytes(), "member10".getBytes(), "member11".getBytes());

    rangeList = rangeSortedSet.zrange(10, 13, false);
    assertThat(rangeList).hasSize(2);
    assertThat(rangeList).containsExactly("member11".getBytes(), "member12".getBytes());
  }

  @Test
  public void zrange_ShouldReturnRanges_SpecifiedWithNegativeOffsets() {
    Collection<byte[]> rangeList = rangeSortedSet.zrange(-2, 12, false);
    assertThat(rangeList).hasSize(2);
    assertThat(rangeList).containsExactly("member11".getBytes(), "member12".getBytes());

    rangeList = rangeSortedSet.zrange(-6, -1, false);
    assertThat(rangeList).hasSize(6);
    assertThat(rangeList)
        .containsExactly("member7".getBytes(), "member8".getBytes(),
            "member9".getBytes(), "member10".getBytes(), "member11".getBytes(),
            "member12".getBytes());

    rangeList = rangeSortedSet.zrange(-11, -5, false);
    assertThat(rangeList).hasSize(7);
    assertThat(rangeList)
        .containsExactly("member2".getBytes(), "member3".getBytes(),
            "member4".getBytes(), "member5".getBytes(), "member6".getBytes(),
            "member7".getBytes(), "member8".getBytes());

    rangeList = rangeSortedSet.zrange(-12, -11, false);
    assertThat(rangeList).hasSize(2);
    assertThat(rangeList)
        .containsExactly("member1".getBytes(), "member2".getBytes());
  }

  @Test
  public void zrange_shouldAlsoReturnScores_whenWithScoresSpecified() {
    Collection<byte[]> rangeList = rangeSortedSet.zrange(0, 5, true);
    assertThat(rangeList).hasSize(12);
    assertThat(rangeList).containsExactly("member1".getBytes(), "1".getBytes(),
        "member2".getBytes(), "1.1".getBytes(), "member3".getBytes(), "1.2".getBytes(),
        "member4".getBytes(), "1.3".getBytes(), "member5".getBytes(), "1.4".getBytes(),
        "member6".getBytes(), "1.5".getBytes());
  }

  @Test
  public void scoreSet_shouldNotRetainOldEntries_whenEntriesUpdated() {
    Collection<byte[]> rangeList = rangeSortedSet.zrange(0, 100, false);
    assertThat(rangeList).hasSize(12);
    assertThat(rangeList).containsExactly("member1".getBytes(), "member2".getBytes(),
        "member3".getBytes(), "member4".getBytes(), "member5".getBytes(),
        "member6".getBytes(), "member7".getBytes(), "member8".getBytes(),
        "member9".getBytes(), "member10".getBytes(), "member11".getBytes(), "member12".getBytes());
  }

  @Test
  public void orderedSetEntryCompareTo_returnsCorrectly_givenDifferentScores() {
    byte[] memberName = stringToBytes("member");

    RedisSortedSet.AbstractOrderedSetEntry negativeInf =
        new RedisSortedSet.OrderedSetEntry(memberName, doubleToBytes(Double.NEGATIVE_INFINITY));
    RedisSortedSet.AbstractOrderedSetEntry negativeOne =
        new RedisSortedSet.OrderedSetEntry(memberName, doubleToBytes(-1.0));
    RedisSortedSet.AbstractOrderedSetEntry zero =
        new RedisSortedSet.OrderedSetEntry(memberName, doubleToBytes(0.0));
    RedisSortedSet.AbstractOrderedSetEntry one =
        new RedisSortedSet.OrderedSetEntry(memberName, doubleToBytes(1.0));
    RedisSortedSet.AbstractOrderedSetEntry positiveInf =
        new RedisSortedSet.OrderedSetEntry(memberName, doubleToBytes(Double.POSITIVE_INFINITY));

    assertThat(negativeInf.compareTo(negativeOne)).isEqualTo(-1);
    assertThat(negativeOne.compareTo(zero)).isEqualTo(-1);
    assertThat(zero.compareTo(one)).isEqualTo(-1);
    assertThat(one.compareTo(positiveInf)).isEqualTo(-1);

    assertThat(positiveInf.compareTo(one)).isEqualTo(1);
    assertThat(one.compareTo(zero)).isEqualTo(1);
    assertThat(zero.compareTo(negativeOne)).isEqualTo(1);
    assertThat(negativeOne.compareTo(negativeInf)).isEqualTo(1);
  }

  @Test
  public void dummyOrderedSetEntryCompareTo_throws_givenBothArraysAreGreatestOrLeastMemberNameAndScoresAreEqual() {
    double score = 1.0;

    RedisSortedSet.AbstractOrderedSetEntry greatest1 =
        new RedisSortedSet.DummyOrderedSetEntry(score, true, true);
    RedisSortedSet.AbstractOrderedSetEntry greatest2 =
        new RedisSortedSet.DummyOrderedSetEntry(score, false, false);

    // noinspection ResultOfMethodCallIgnored
    assertThatThrownBy(() -> greatest1.compareTo(greatest2))
        .isInstanceOf(IllegalStateException.class);

    RedisSortedSet.AbstractOrderedSetEntry least1 =
        new RedisSortedSet.DummyOrderedSetEntry(score, false, true);
    RedisSortedSet.AbstractOrderedSetEntry least2 =
        new RedisSortedSet.DummyOrderedSetEntry(score, true, false);

    // noinspection ResultOfMethodCallIgnored
    assertThatThrownBy(() -> least1.compareTo(least2)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void dummyOrderedSetEntryCompareTo_handlesDummyMemberNames_givenScoresAreEqual() {
    double score = 1.0;

    RedisSortedSet.AbstractOrderedSetEntry greatest =
        new RedisSortedSet.DummyOrderedSetEntry(score, true, true);
    RedisSortedSet.AbstractOrderedSetEntry least =
        new RedisSortedSet.DummyOrderedSetEntry(score, true, false);
    RedisSortedSet.AbstractOrderedSetEntry middle =
        new RedisSortedSet.OrderedSetEntry(stringToBytes("middle"), doubleToBytes(score));

    // greatest > least
    assertThat(greatest.compareTo(least)).isEqualTo(1);
    // least < greatest
    assertThat(least.compareTo(greatest)).isEqualTo(-1);
    // greatest > middle
    assertThat(greatest.compareTo(middle)).isEqualTo(1);
    // least < middle
    assertThat(least.compareTo(middle)).isEqualTo(-1);
  }

  @Test
  public void dummyOrderedSetEntryCompareTo_handlesDummyMemberNameEquivalents_givenScoresAreEqual() {
    double score = 1.0;
    byte[] scoreBytes = doubleToBytes(score);

    RedisSortedSet.AbstractOrderedSetEntry greatest =
        new RedisSortedSet.DummyOrderedSetEntry(score, true, true);
    RedisSortedSet.AbstractOrderedSetEntry greatestEquivalent =
        new RedisSortedSet.OrderedSetEntry(bGREATEST_MEMBER_NAME.clone(), scoreBytes);

    RedisSortedSet.AbstractOrderedSetEntry least =
        new RedisSortedSet.DummyOrderedSetEntry(score, true, false);
    RedisSortedSet.AbstractOrderedSetEntry leastEquivalent =
        new RedisSortedSet.OrderedSetEntry(bLEAST_MEMBER_NAME.clone(), scoreBytes);

    // bGREATEST_MEMBER_NAME > an array with contents equal to bGREATEST_MEMBER_NAME
    assertThat(greatest.compareTo(greatestEquivalent)).isEqualTo(1);

    // bLEAST_MEMBER_NAME < an array with contents equal to bLEAST_MEMBER_NAME
    assertThat(least.compareTo(leastEquivalent)).isEqualTo(-1);
  }

  @Test
  public void dummyOrderedSetEntryConstructor_setsAppropriateMemberName() {
    RedisSortedSet.AbstractOrderedSetEntry entry =
        new RedisSortedSet.DummyOrderedSetEntry(1, false, false);
    assertThat(entry.getMember()).isSameAs(bGREATEST_MEMBER_NAME);

    entry = new RedisSortedSet.DummyOrderedSetEntry(1, true, false);
    assertThat(entry.getMember()).isSameAs(bLEAST_MEMBER_NAME);

    entry = new RedisSortedSet.DummyOrderedSetEntry(1, false, true);
    assertThat(entry.getMember()).isSameAs(bLEAST_MEMBER_NAME);

    entry = new RedisSortedSet.DummyOrderedSetEntry(1, true, true);
    assertThat(entry.getMember()).isSameAs(bGREATEST_MEMBER_NAME);
  }


  /******** constants *******/
  // These tests contain the math that is used to derive the constants in RedisSortedSet and
  // OrderedSetEntry. If these tests start failing, it is because the overheads of RedisSortedSet or
  // OrderedSetEntry have changed. If they have decreased, good job! You can change the constant in
  // RedisSortedSet or OrderedSetEntry to reflect that. If they have increased, carefully consider
  // that increase before changing the constant.
  @Test
  public void baseRedisSortedSetOverheadConstant_shouldMatchReflectedSize() {
    RedisSortedSet set = new RedisSortedSet(Collections.emptyList());
    SizeableObject2ObjectOpenCustomHashMapWithCursor<byte[], RedisSortedSet.OrderedSetEntry> backingMap =
        new SizeableObject2ObjectOpenCustomHashMapWithCursor<>(0, ByteArrays.HASH_STRATEGY);
    OrderStatisticsSet<RedisSortedSet.OrderedSetEntry> backingTree = new OrderStatisticsTree<>();
    int baseRedisSetOverhead =
        sizer.sizeof(set) - sizer.sizeof(backingMap) - sizer.sizeof(backingTree);

    assertThat(BASE_REDIS_SORTED_SET_OVERHEAD).isEqualTo(baseRedisSetOverhead);
  }

  @Test
  public void baseOrderedSetEntrySize_shouldMatchReflectedSize() {
    byte[] scoreBytes = doubleToBytes(1.0);
    byte[] memberBytes = stringToBytes("member");
    RedisSortedSet.OrderedSetEntry entry =
        new RedisSortedSet.OrderedSetEntry(memberBytes, scoreBytes);
    int expectedSize = sizer.sizeof(entry) - sizer.sizeof(scoreBytes) - sizer.sizeof(memberBytes);

    assertThat(BASE_ORDERED_SET_ENTRY_SIZE).isEqualTo(expectedSize);
  }

  /****************** Size ******************/

  @Test
  public void redisSortedSetGetSizeInBytes_isAccurate() {
    Region<RedisKey, RedisData> mockRegion = uncheckedCast(mock(Region.class));
    RedisKey mockKey = mock(RedisKey.class);
    ZAddOptions options = new ZAddOptions(ZAddOptions.Exists.NONE, false, false);
    RedisSortedSet sortedSet = new RedisSortedSet(Collections.emptyList());

    int actualSize = sizer.sizeof(sortedSet);
    int calculatedSize = sortedSet.getSizeInBytes();
    assertThat(calculatedSize).isEqualTo(actualSize);

    // Add members and scores and confirm that the calculated size is accurate after each operation
    int numberOfEntries = 100;
    for (int i = 0; i < numberOfEntries; ++i) {
      List<byte[]> scoreAndMember = Arrays.asList(doubleToBytes(i), new byte[i]);
      sortedSet.zadd(mockRegion, mockKey, scoreAndMember, options);
      sortedSet.clearDelta();
      actualSize = sizer.sizeof(sortedSet);
      calculatedSize = sortedSet.getSizeInBytes();
      assertThat(calculatedSize).isEqualTo(actualSize);
    }

    // Update half the scores and confirm that the calculated size is accurate after each operation
    for (int i = 0; i < numberOfEntries / 2; ++i) {
      List<byte[]> scoreAndMember = Arrays.asList(doubleToBytes(i * 2), new byte[i]);
      sortedSet.zadd(mockRegion, mockKey, scoreAndMember, options);
      sortedSet.clearDelta();
      actualSize = sizer.sizeof(sortedSet);
      calculatedSize = sortedSet.getSizeInBytes();
      assertThat(calculatedSize).isEqualTo(actualSize);
    }

    // Remove all members and confirm that the calculated size is accurate after each operation
    for (int i = 0; i < numberOfEntries; ++i) {
      List<byte[]> memberToRemove = Collections.singletonList(new byte[i]);
      sortedSet.zrem(mockRegion, mockKey, memberToRemove);
      sortedSet.clearDelta();
      actualSize = sizer.sizeof(sortedSet);
      calculatedSize = sortedSet.getSizeInBytes();
      assertThat(calculatedSize).isEqualTo(actualSize);
    }
  }

  @Test
  public void orderedSetEntryGetSizeInBytes_isAccurate() {
    Random random = new Random();
    byte[] member;
    byte[] scoreBytes;
    for (int i = 0; i < 100; ++i) {
      member = new byte[random.nextInt(50_000)];
      scoreBytes = String.valueOf(random.nextDouble()).getBytes();
      RedisSortedSet.OrderedSetEntry entry = new RedisSortedSet.OrderedSetEntry(member, scoreBytes);
      assertThat(entry.getSizeInBytes()).isEqualTo(sizer.sizeof(entry));
    }
  }

  private RedisSortedSet createRedisSortedSet(String... membersAndScores) {
    final List<byte[]> membersAndScoresList = Arrays
        .stream(membersAndScores)
        .map(Coder::stringToBytes)
        .collect(Collectors.toList());
    return new RedisSortedSet(membersAndScoresList);
  }
}
