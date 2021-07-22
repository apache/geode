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

import static org.apache.geode.internal.size.ReflectionSingleObjectSizer.roundUpSize;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_A_VALID_FLOAT;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.netty.Coder.bytesToDouble;
import static org.apache.geode.redis.internal.netty.Coder.doubleToBytes;
import static org.apache.geode.redis.internal.netty.Coder.narrowLongToInt;
import static org.apache.geode.redis.internal.netty.Coder.stripTrailingZeroFromDouble;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bGREATEST_MEMBER_NAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEAST_MEMBER_NAME;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import it.unimi.dsi.fastutil.bytes.ByteArrays;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.collections.OrderStatisticsSet;
import org.apache.geode.redis.internal.collections.OrderStatisticsTree;
import org.apache.geode.redis.internal.collections.SizeableObject2ObjectOpenCustomHashMapWithCursor;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.executor.sortedset.AbstractSortedSetRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetLexRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetScoreRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;

public class RedisSortedSet extends AbstractRedisData {
  private SizeableObject2ObjectOpenCustomHashMapWithCursor<byte[], OrderedSetEntry> members;
  private OrderStatisticsSet<AbstractOrderedSetEntry> scoreSet;
  // This field is used to keep track of the size associated with objects that are referenced by
  // both backing collections, since they will be counted twice otherwise
  private int sizeInBytesAdjustment = 0;

  // The following constant was calculated using reflection. You can find the test for this value in
  // RedisSortedSetTest, which shows the way this number was calculated. If our internal
  // implementation changes, this value may be incorrect. An increase in overhead should be
  // carefully considered.
  protected static final int BASE_REDIS_SORTED_SET_OVERHEAD = 40;

  @Override
  public int getSizeInBytes() {
    return BASE_REDIS_SORTED_SET_OVERHEAD + members.getSizeInBytes() + scoreSet.getSizeInBytes()
        - sizeInBytesAdjustment;
  }

  RedisSortedSet(List<byte[]> members) {
    this.members =
        new SizeableObject2ObjectOpenCustomHashMapWithCursor<>(members.size() / 2,
            ByteArrays.HASH_STRATEGY);
    scoreSet = new OrderStatisticsTree<>();

    Iterator<byte[]> iterator = members.iterator();

    while (iterator.hasNext()) {
      byte[] score = iterator.next();
      byte[] member = iterator.next();
      memberAdd(member, score);
    }
  }

  protected int getSortedSetSize() {
    return scoreSet.size();
  }

  // for serialization
  public RedisSortedSet() {}

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    if (deltaInfo instanceof AddsDeltaInfo) {
      AddsDeltaInfo addsDeltaInfo = (AddsDeltaInfo) deltaInfo;
      membersAddAll(addsDeltaInfo);
    } else {
      RemsDeltaInfo remsDeltaInfo = (RemsDeltaInfo) deltaInfo;
      membersRemoveAll(remsDeltaInfo);
    }
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads are modifying this
   * object, the striped executor will not protect toData. So any methods that modify "members"
   * needs to be thread safe with toData.
   */

  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    InternalDataSerializer.writePrimitiveInt(members.size(), out);
    for (Map.Entry<byte[], OrderedSetEntry> entry : members.entrySet()) {
      byte[] member = entry.getKey();
      byte[] score = entry.getValue().getScoreBytes();
      InternalDataSerializer.writeByteArray(member, out);
      InternalDataSerializer.writeByteArray(score, out);
    }
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = InternalDataSerializer.readPrimitiveInt(in);
    members =
        new SizeableObject2ObjectOpenCustomHashMapWithCursor<>(size, ByteArrays.HASH_STRATEGY);
    scoreSet = new OrderStatisticsTree<>();
    for (int i = 0; i < size; i++) {
      byte[] member = InternalDataSerializer.readByteArray(in);
      byte[] score = InternalDataSerializer.readByteArray(in);
      OrderedSetEntry newEntry = new OrderedSetEntry(member, score);
      members.put(member, newEntry);
      scoreSet.add(newEntry);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisSortedSet)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisSortedSet other = (RedisSortedSet) o;
    if (members.size() != other.members.size()) {
      return false;
    }

    for (Map.Entry<byte[], OrderedSetEntry> entry : members.entrySet()) {
      OrderedSetEntry otherEntry = other.members.get(entry.getKey());
      if (otherEntry == null) {
        return false;
      }
      if (Double.compare(otherEntry.getScore(), entry.getValue().getScore()) != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int getDSFID() {
    return REDIS_SORTED_SET_ID;
  }

  protected synchronized byte[] memberAdd(byte[] memberToAdd, byte[] scoreToAdd) {
    OrderedSetEntry existingEntry = members.get(memberToAdd);
    if (existingEntry == null) {
      OrderedSetEntry newEntry = new OrderedSetEntry(memberToAdd, scoreToAdd);
      members.put(memberToAdd, newEntry);
      scoreSet.add(newEntry);
      // Without this adjustment, we count the entry and member name array twice, since references
      // to them appear in both backing collections.
      sizeInBytesAdjustment += newEntry.getSizeInBytes() + calculateByteArraySize(memberToAdd);
      return null;
    } else {
      scoreSet.remove(existingEntry);
      byte[] oldScore = existingEntry.scoreBytes;
      existingEntry.updateScore(stripTrailingZeroFromDouble(scoreToAdd));
      members.put(memberToAdd, existingEntry);
      scoreSet.add(existingEntry);
      return oldScore;
    }
  }

  synchronized byte[] memberRemove(byte[] member) {
    byte[] oldValue = null;
    OrderedSetEntry orderedSetEntry = members.remove(member);
    if (orderedSetEntry != null) {
      scoreSet.remove(orderedSetEntry);
      oldValue = orderedSetEntry.getScoreBytes();
      // Adjust for removal.
      sizeInBytesAdjustment -= orderedSetEntry.getSizeInBytes() + calculateByteArraySize(member);
    }

    return oldValue;
  }

  private synchronized void membersAddAll(AddsDeltaInfo addsDeltaInfo) {
    Iterator<byte[]> iterator = addsDeltaInfo.getAdds().iterator();
    while (iterator.hasNext()) {
      byte[] member = iterator.next();
      byte[] score = iterator.next();
      memberAdd(member, score);
    }
  }

  private synchronized void membersRemoveAll(RemsDeltaInfo remsDeltaInfo) {
    for (byte[] member : remsDeltaInfo.getRemoves()) {
      memberRemove(member);
    }
  }

  /**
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @param membersToAdd members to add to this set; NOTE this list may by modified by this call
   * @return the number of members actually added OR incremented value if INCR option specified
   */
  Object zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToAdd,
      ZAddOptions options) {
    if (options.isINCR()) {
      return zaddIncr(region, key, membersToAdd, options);
    }
    AddsDeltaInfo deltaInfo = null;
    Iterator<byte[]> iterator = membersToAdd.iterator();
    int initialSize = scoreSet.size();
    int changesCount = 0;
    while (iterator.hasNext()) {
      byte[] score = iterator.next();
      byte[] member = iterator.next();
      if (options.isNX() && members.containsKey(member)) {
        continue;
      }
      if (options.isXX() && !members.containsKey(member)) {
        continue;
      }
      byte[] oldScore = memberAdd(member, score);
      if (options.isCH() && oldScore != null
          && !Arrays.equals(oldScore, stripTrailingZeroFromDouble(score))) {
        changesCount++;
      }

      if (deltaInfo == null) {
        deltaInfo = new AddsDeltaInfo(new ArrayList<>());
      }
      deltaInfo.add(member);
      deltaInfo.add(score);
    }

    storeChanges(region, key, deltaInfo);
    return scoreSet.size() - initialSize + changesCount;
  }

  long zcard() {
    return members.size();
  }

  long zcount(SortedSetScoreRangeOptions rangeOptions) {
    AbstractOrderedSetEntry minEntry = new ScoreDummyOrderedSetEntry(rangeOptions.getMinimum(),
        rangeOptions.isMinExclusive(), true);
    long minIndex = scoreSet.indexOf(minEntry);

    AbstractOrderedSetEntry maxEntry = new ScoreDummyOrderedSetEntry(rangeOptions.getMaximum(),
        rangeOptions.isMaxExclusive(), false);
    long maxIndex = scoreSet.indexOf(maxEntry);

    return maxIndex - minIndex;
  }

  byte[] zincrby(Region<RedisKey, RedisData> region, RedisKey key, byte[] increment,
      byte[] member) {
    OrderedSetEntry orderedSetEntry = members.get(member);
    double incr = processByteArrayAsDouble(increment);

    if (orderedSetEntry != null) {
      double byteScore = orderedSetEntry.getScore();
      incr += byteScore;

      if (Double.isNaN(incr)) {
        throw new NumberFormatException(RedisConstants.ERROR_OPERATION_PRODUCED_NAN);
      }
    }

    byte[] byteIncr = doubleToBytes(incr);
    memberAdd(member, byteIncr);

    AddsDeltaInfo deltaInfo = new AddsDeltaInfo(new ArrayList<>());
    deltaInfo.add(member);
    deltaInfo.add(byteIncr);

    storeChanges(region, key, deltaInfo);

    return byteIncr;
  }

  List<byte[]> zrange(int min, int max, boolean withScores) {
    return getRange(min, max, withScores, false);
  }

  List<byte[]> zrangebylex(SortedSetLexRangeOptions rangeOptions) {
    // Assume that all members have the same score. Behaviour is unspecified otherwise.
    double score = scoreSet.get(0).score;

    AbstractOrderedSetEntry minEntry = new MemberDummyOrderedSetEntry(rangeOptions.getMinimum(),
        score, rangeOptions.isMinExclusive(), true);
    int minIndex = scoreSet.indexOf(minEntry);
    if (minIndex >= scoreSet.size()) {
      return Collections.emptyList();
    }

    AbstractOrderedSetEntry maxEntry = new MemberDummyOrderedSetEntry(rangeOptions.getMaximum(),
        score, rangeOptions.isMaxExclusive(), false);
    int maxIndex = scoreSet.indexOf(maxEntry);
    if (minIndex == maxIndex) {
      return Collections.emptyList();
    }

    return addLimitToRange(rangeOptions, false, minIndex, maxIndex);
  }

  List<byte[]> zrangebyscore(SortedSetScoreRangeOptions rangeOptions, boolean withScores) {
    AbstractOrderedSetEntry minEntry = new ScoreDummyOrderedSetEntry(rangeOptions.getMinimum(),
        rangeOptions.isMinExclusive(), true);
    int minIndex = scoreSet.indexOf(minEntry);
    if (minIndex >= scoreSet.size()) {
      return Collections.emptyList();
    }

    AbstractOrderedSetEntry maxEntry = new ScoreDummyOrderedSetEntry(rangeOptions.getMaximum(),
        rangeOptions.isMaxExclusive(), false);
    int maxIndex = scoreSet.indexOf(maxEntry);
    if (minIndex == maxIndex) {
      return Collections.emptyList();
    }

    // Okay, if we make it this far there's a potential range of things to return.
    return addLimitToRange(rangeOptions, withScores, minIndex, maxIndex);
  }

  long zrank(byte[] member) {
    OrderedSetEntry orderedSetEntry = members.get(member);
    if (orderedSetEntry == null) {
      return -1;
    }
    return scoreSet.indexOf(orderedSetEntry);
  }

  long zrem(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToRemove) {
    int membersRemoved = 0;
    RemsDeltaInfo deltaInfo = null;
    for (byte[] memberToRemove : membersToRemove) {
      if (memberRemove(memberToRemove) != null) {
        if (deltaInfo == null) {
          deltaInfo = new RemsDeltaInfo();
        }
        deltaInfo.add(memberToRemove);
        membersRemoved++;
      }
    }
    storeChanges(region, key, deltaInfo);
    return membersRemoved;
  }

  List<byte[]> zrevrange(int min, int max, boolean withScores) {
    return getRange(min, max, withScores, true);
  }

  long zrevrank(byte[] member) {
    OrderedSetEntry orderedSetEntry = members.get(member);
    if (orderedSetEntry == null) {
      return -1;
    }
    return scoreSet.size() - scoreSet.indexOf(orderedSetEntry) - 1;
  }

  byte[] zscore(byte[] member) {
    OrderedSetEntry orderedSetEntry = members.get(member);
    if (orderedSetEntry != null) {
      return orderedSetEntry.getScoreBytes();
    }
    return null;
  }

  private byte[] zaddIncr(Region<RedisKey, RedisData> region, RedisKey key,
      List<byte[]> membersToAdd, ZAddOptions options) {
    // for zadd incr option, only one incrementing element pair is allowed to get here.
    byte[] increment = membersToAdd.get(0);
    byte[] member = membersToAdd.get(1);
    if (options.isNX() && members.containsKey(member)) {
      return null;
    }
    if (options.isXX() && !members.containsKey(member)) {
      return null;
    }
    return zincrby(region, key, increment, member);
  }

  private List<byte[]> getRange(int min, int max, boolean withScores, boolean isReverse) {
    List<byte[]> result = new ArrayList<>();
    int start;
    int rangeSize;
    if (isReverse) {
      // scoreSet.size() - 1 is the maximum index of elements in the sorted set
      start = scoreSet.size() - 1 - getBoundedStartIndex(min, scoreSet.size());
      int end = scoreSet.size() - 1 - getBoundedEndIndex(max, scoreSet.size());
      // Add one to rangeSize because the range is inclusive, so even if start == end, we return one
      // element
      rangeSize = start - end + 1;
    } else {
      start = getBoundedStartIndex(min, scoreSet.size());
      int end = getBoundedEndIndex(max, scoreSet.size());
      // Add one to rangeSize because the range is inclusive, so even if start == end, we return one
      // element
      rangeSize = end - start + 1;
    }
    if (rangeSize <= 0 || start == scoreSet.size()) {
      return result;
    }

    Iterator<AbstractOrderedSetEntry> entryIterator =
        scoreSet.getIndexRange(start, rangeSize, isReverse);
    while (entryIterator.hasNext()) {
      AbstractOrderedSetEntry entry = entryIterator.next();
      result.add(entry.member);
      if (withScores) {
        result.add(entry.scoreBytes);
      }
    }
    return result;
  }

  private List<byte[]> addLimitToRange(AbstractSortedSetRangeOptions<?> rangeOptions,
      boolean withScores, int minIndex, int maxIndex) {
    int count = Integer.MAX_VALUE;
    if (rangeOptions.hasLimit()) {
      count = rangeOptions.getCount();
      minIndex += rangeOptions.getOffset();
      if (minIndex > getSortedSetSize()) {
        return Collections.emptyList();
      }
    }
    Iterator<AbstractOrderedSetEntry> entryIterator =
        scoreSet.getIndexRange(minIndex, Math.min(count, maxIndex - minIndex), false);

    List<byte[]> result = new ArrayList<>();
    while (entryIterator.hasNext()) {
      AbstractOrderedSetEntry entry = entryIterator.next();

      result.add(entry.member);
      if (withScores) {
        result.add(entry.scoreBytes);
      }
    }
    return result;
  }

  private int getBoundedStartIndex(int index, int size) {
    if (index >= 0) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, 0);
    }
  }

  private int getBoundedEndIndex(int index, int size) {
    if (index >= 0) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, -1);
    }
  }

  @Override
  public RedisDataType getType() {
    return REDIS_SORTED_SET;
  }

  @Override
  protected boolean removeFromRegion() {
    return members.isEmpty();
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), members);
  }

  @Override
  public String toString() {
    return "RedisSortedSet{" + super.toString() + ", " + "size=" + members.size() + '}';
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  public static double processByteArrayAsDouble(byte[] value) {
    try {
      double doubleValue = bytesToDouble(value);
      if (Double.isNaN(doubleValue)) {
        throw new NumberFormatException(ERROR_NOT_A_VALID_FLOAT);
      }
      return doubleValue;
    } catch (NumberFormatException nfe) {
      throw new NumberFormatException(ERROR_NOT_A_VALID_FLOAT);
    }
  }

  // Comparison to allow the use of bLEAST_MEMBER_NAME and bGREATEST_MEMBER_NAME to always be less
  // than or greater than respectively any other byte array representing a member name. The use of
  // "==" is important as it allows us to differentiate between the constant byte arrays defined
  // in StringBytesGlossary and user-supplied member names which may be equal in content but have
  // a different memory address.
  public static int checkDummyMemberNames(byte[] array1, byte[] array2) {
    if (array2 == bLEAST_MEMBER_NAME || array1 == bGREATEST_MEMBER_NAME) {
      if (array1 == bLEAST_MEMBER_NAME || array2 == bGREATEST_MEMBER_NAME) {
        throw new IllegalStateException(
            "Arrays cannot both be least member name or greatest member name");
      }
      return 1; // array2 < array1
    } else if (array1 == bLEAST_MEMBER_NAME || array2 == bGREATEST_MEMBER_NAME) {
      return -1; // array1 < array2
    } else {
      // Neither of the input arrays are using bLEAST_MEMBER_NAME or bGREATEST_MEMBER_NAME, so real
      // lexicographical comparison is needed
      return 0;
    }
  }

  @VisibleForTesting
  public static int javaImplementationOfAnsiCMemCmp(byte[] array1, byte[] array2) {
    int shortestLength = Math.min(array1.length, array2.length);
    for (int i = 0; i < shortestLength; i++) {
      int localComp = Byte.toUnsignedInt(array1[i]) - Byte.toUnsignedInt(array2[i]);
      if (localComp != 0) {
        return localComp;
      }
    }
    // shorter array whose items are all equal to the first items of a longer array is
    // considered 'less than'
    if (array1.length < array2.length) {
      return -1; // array1 < array2
    } else if (array1.length > array2.length) {
      return 1; // array2 < array1
    }
    return 0; // totally equal - should never happen, because you can't have two members
    // with same name...
  }

  private static int calculateByteArraySize(byte[] bytes) {
    return narrowLongToInt(roundUpSize(bytes.length) + 16);
  }

  abstract static class AbstractOrderedSetEntry implements Comparable<AbstractOrderedSetEntry>,
      Sizeable {
    byte[] member;
    byte[] scoreBytes;
    double score;

    private AbstractOrderedSetEntry() {}

    public int compareTo(AbstractOrderedSetEntry o) {
      int comparison = Double.compare(score, o.score);
      if (comparison == 0) {
        // Scores equal, try lexical ordering
        return compareMembers(member, o.member);
      }
      return comparison;
    }

    public byte[] getMember() {
      return member;
    }

    public byte[] getScoreBytes() {
      return scoreBytes;
    }

    public double getScore() {
      return score;
    }

    public abstract int compareMembers(byte[] array1, byte[] array2);

    public abstract int getSizeInBytes();
  }

  // Entry used to store data in the scoreSet
  public static class OrderedSetEntry extends AbstractOrderedSetEntry {

    // The following constant was calculated using reflection. You can find the test for this value
    // in RedisSortedSetTest, which shows the way this number was calculated. If our internal
    // implementation changes, this value may be incorrect. An increase in overhead should be
    // carefully considered.
    public static final int BASE_ORDERED_SET_ENTRY_SIZE = 32;

    public OrderedSetEntry(byte[] member, byte[] score) {
      this.member = member;
      this.scoreBytes = stripTrailingZeroFromDouble(score);
      this.score = processByteArrayAsDouble(score);
    }

    @Override
    public int compareMembers(byte[] array1, byte[] array2) {
      return javaImplementationOfAnsiCMemCmp(array1, array2);
    }

    @Override
    public int getSizeInBytes() {
      return BASE_ORDERED_SET_ENTRY_SIZE + calculateByteArraySize(member)
          + calculateByteArraySize(scoreBytes);
    }

    void updateScore(byte[] newScore) {
      if (!Arrays.equals(newScore, scoreBytes)) {
        scoreBytes = newScore;
        score = processByteArrayAsDouble(newScore);
      }
    }
  }

  // Dummy entry used to find the rank of an element with the given score for inclusive or
  // exclusive ranges
  static class ScoreDummyOrderedSetEntry extends AbstractOrderedSetEntry {

    ScoreDummyOrderedSetEntry(double score, boolean isExclusive, boolean isMinimum) {
      if (isExclusive && isMinimum || !isExclusive && !isMinimum) {
        // For use in (this < other) and (this >= other) comparisons
        this.member = bGREATEST_MEMBER_NAME;
      } else {
        // For use in (this <= other) and (this > other) comparisons
        this.member = bLEAST_MEMBER_NAME;
      }
      this.scoreBytes = null;
      this.score = score;
    }

    @Override
    public int compareMembers(byte[] array1, byte[] array2) {
      return checkDummyMemberNames(array1, array2);
    }

    @Override
    // This object should never be persisted, so its size never needs to be calculated
    public int getSizeInBytes() {
      return 0;
    }
  }

  // Dummy entry used to find the rank of an element with the given member name for lexically
  // ordered sets
  static class MemberDummyOrderedSetEntry extends AbstractOrderedSetEntry {
    boolean isExclusive;
    boolean isMinimum;

    MemberDummyOrderedSetEntry(byte[] member, double score, boolean isExclusive,
        boolean isMinimum) {
      this.member = member;
      this.scoreBytes = null;
      this.score = score;
      this.isExclusive = isExclusive;
      this.isMinimum = isMinimum;
    }

    @Override
    public int compareMembers(byte[] array1, byte[] array2) {
      int dummyNameComparison = checkDummyMemberNames(array1, array2);
      if (dummyNameComparison != 0) {
        return dummyNameComparison;
      } else {
        // If not using dummy member names, move on to actual lexical comparison
        int stringComparison = javaImplementationOfAnsiCMemCmp(array1, array2);
        if (stringComparison == 0) {
          if (isMinimum && isExclusive || !isMinimum && !isExclusive) {
            // If the member names are equal but we are using an exclusive minimum comparison, or an
            // inclusive maximum comparison then this entry should act as if it is greater than the
            // entry it's being compared to
            return 1;
          } else {
            return -1;
          }
        }
        return stringComparison;
      }
    }

    @Override
    // This object should never be persisted, so its size never needs to be calculated
    public int getSizeInBytes() {
      return 0;
    }
  }
}
