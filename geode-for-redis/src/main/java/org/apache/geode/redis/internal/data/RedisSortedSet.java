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

import static java.lang.Double.compare;
import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SORTED_SET;
import static org.apache.geode.redis.internal.netty.Coder.doubleToBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bGREATEST_MEMBER_NAME;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bLEAST_MEMBER_NAME;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.collections.OrderStatisticsTree;
import org.apache.geode.redis.internal.collections.SizeableBytes2ObjectOpenCustomHashMapWithCursor;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.delta.ZAddsDeltaInfo;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.executor.sortedset.AbstractSortedSetRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetLexRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetRankRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.SortedSetScoreRangeOptions;
import org.apache.geode.redis.internal.executor.sortedset.ZAddOptions;
import org.apache.geode.redis.internal.executor.sortedset.ZAggregator;
import org.apache.geode.redis.internal.executor.sortedset.ZKeyWeight;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisSortedSet extends AbstractRedisData {
  protected static final int REDIS_SORTED_SET_OVERHEAD = memoryOverhead(RedisSortedSet.class);
  private static final Logger logger = LogService.getLogger();

  private MemberMap members;
  private final ScoreSet scoreSet = new ScoreSet();

  @Override
  public int getSizeInBytes() {
    return REDIS_SORTED_SET_OVERHEAD + members.getSizeInBytes() + scoreSet.getSizeInBytes();
  }

  RedisSortedSet(List<byte[]> members, double[] scores) {
    this.members = new MemberMap(members.size());

    for (int i = 0; i < members.size(); i++) {
      double score = scores[i];
      byte[] member = members.get(i);
      memberAdd(member, score);
    }
  }

  public RedisSortedSet(int size) {
    this.members = new MemberMap(size);
  }

  protected int getSortedSetSize() {
    return scoreSet.size();
  }

  // for serialization
  public RedisSortedSet() {}

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    if (deltaInfo instanceof ZAddsDeltaInfo) {
      ZAddsDeltaInfo zaddsDeltaInfo = (ZAddsDeltaInfo) deltaInfo;
      membersAddAll(zaddsDeltaInfo);
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
    members.toData(out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = DataSerializer.readPrimitiveInt(in);
    members = new MemberMap(size);
    for (int i = 0; i < size; i++) {
      byte[] member = DataSerializer.readByteArray(in);
      double score = DataSerializer.readPrimitiveDouble(in);
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

    return members.fastWhileEachValue(entry -> {
      OrderedSetEntry otherEntry = other.members.get(entry.getMember());
      if (otherEntry == null) {
        return false;
      }
      return compare(otherEntry.getScore(), entry.getScore()) == 0;
    });
  }

  @Override
  public int getDSFID() {
    return REDIS_SORTED_SET_ID;
  }

  protected synchronized MemberAddResult memberAdd(byte[] memberToAdd, double scoreToAdd) {
    OrderedSetEntry entry = members.get(memberToAdd);
    if (entry == null) {
      OrderedSetEntry newEntry = new OrderedSetEntry(memberToAdd, scoreToAdd);
      members.put(memberToAdd, newEntry);
      scoreSet.add(newEntry);
      return MemberAddResult.CREATE;
    } else {
      if (entry.score == scoreToAdd) {
        return MemberAddResult.NO_OP;
      }
      scoreSet.remove(entry);
      entry.updateScore(scoreToAdd);
      scoreSet.add(entry);
      return MemberAddResult.UPDATE;
    }
  }

  synchronized boolean memberRemove(byte[] member) {
    OrderedSetEntry orderedSetEntry = members.remove(member);
    if (orderedSetEntry != null) {
      scoreSet.remove(orderedSetEntry);
      return true;
    }
    return false;
  }

  private synchronized void membersAddAll(ZAddsDeltaInfo zaddsDeltaInfo) {
    List<byte[]> members = zaddsDeltaInfo.getZAddMembers();
    double[] scores = zaddsDeltaInfo.getZAddScores();
    for (int i = 0; i < members.size(); i++) {
      memberAdd(members.get(i), scores[i]);
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
  public Object zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToAdd,
      double[] scoresToAdd, ZAddOptions options) {
    if (options.isINCR()) {
      // if INCR, only one score and member can be added
      return zaddIncr(region, key, membersToAdd.get(0), scoresToAdd[0], options);
    }

    ZAddsDeltaInfo deltaInfo = null;
    int initialSize = scoreSet.size();
    int changesCount = 0;

    for (int i = 0; i < membersToAdd.size(); i++) {
      double score = scoresToAdd[i];
      byte[] member = membersToAdd.get(i);
      if (options.isNX() && members.containsKey(member)) {
        continue;
      }
      if (options.isXX() && !members.containsKey(member)) {
        continue;
      }
      MemberAddResult addResult = memberAdd(member, score);

      if (options.isCH() && addResult.equals(MemberAddResult.UPDATE)) {
        changesCount++;
      }

      if (!addResult.equals(MemberAddResult.NO_OP)) {
        if (deltaInfo == null) {
          deltaInfo = new ZAddsDeltaInfo(membersToAdd.size());
          deltaInfo.add(member, score);
        } else {
          deltaInfo.add(member, score);
        }
      }
    }

    storeChanges(region, key, deltaInfo);
    return scoreSet.size() - initialSize + changesCount;
  }

  public long zcard() {
    return members.size();
  }

  public long zcount(SortedSetScoreRangeOptions rangeOptions) {
    long minIndex = rangeOptions.getRangeIndex(scoreSet, true);
    if (minIndex >= scoreSet.size()) {
      return 0;
    }

    long maxIndex = rangeOptions.getRangeIndex(scoreSet, false);
    if (minIndex >= maxIndex) {
      return 0;
    }

    return maxIndex - minIndex;
  }

  public byte[] zincrby(Region<RedisKey, RedisData> region, RedisKey key, double incr,
      byte[] member) {
    double score;
    OrderedSetEntry orderedSetEntry = members.get(member);

    if (orderedSetEntry != null) {
      score = orderedSetEntry.getScore() + incr;

      if (Double.isNaN(score)) {
        throw new NumberFormatException(RedisConstants.ERROR_OPERATION_PRODUCED_NAN);
      }
    } else {
      score = incr;
    }

    if (!(memberAdd(member, score) == MemberAddResult.NO_OP)) {
      ZAddsDeltaInfo deltaInfo = new ZAddsDeltaInfo(1);
      deltaInfo.add(member, score);
      storeChanges(region, key, deltaInfo);
    }

    return Coder.doubleToBytes(score);
  }

  public long zinterstore(RegionProvider regionProvider, RedisKey key, List<ZKeyWeight> keyWeights,
      ZAggregator aggregator) {
    List<RedisSortedSet> sets = new ArrayList<>(keyWeights.size());
    for (ZKeyWeight keyWeight : keyWeights) {
      RedisSortedSet set =
          regionProvider.getTypedRedisData(REDIS_SORTED_SET, keyWeight.getKey(), false);

      if (set == NULL_REDIS_SORTED_SET) {
        regionProvider.getLocalDataRegion().remove(key);
        return 0;
      } else {
        sets.add(set);
      }
    }

    RedisSortedSet smallestSet = sets.get(0);
    for (RedisSortedSet set : sets) {
      if (set.getSortedSetSize() < smallestSet.getSortedSetSize()) {
        smallestSet = set;
      }
    }

    for (byte[] member : smallestSet.members.keySet()) {
      boolean addToSet = true;
      double newScore;

      if (aggregator.equals(ZAggregator.SUM)) {
        newScore = 0;
      } else if (aggregator.equals(ZAggregator.MAX)) {
        newScore = Double.NEGATIVE_INFINITY;
      } else {
        newScore = Double.POSITIVE_INFINITY;
      }
      for (int i = 0; i < sets.size(); i++) {
        RedisSortedSet otherSet = sets.get(i);
        double weight = keyWeights.get(i).getWeight();
        OrderedSetEntry otherEntry = otherSet.members.get(member);
        if (otherEntry == null) {
          addToSet = false;
          break;
        } else {
          double otherScore = otherEntry.getScore();
          if (weight == 0 || otherScore == 0) {
            newScore = 0;
          } else {
            newScore = aggregator.getFunction().apply(newScore, otherScore * weight);
          }
        }
      }
      if (addToSet) {
        memberAdd(member, newScore);
      }
    }

    if (removeFromRegion()) {
      regionProvider.getDataRegion().remove(key);
    } else {
      regionProvider.getLocalDataRegion().put(key, this);
    }

    return getSortedSetSize();
  }

  public long zlexcount(SortedSetLexRangeOptions lexOptions) {
    int minIndex = lexOptions.getRangeIndex(scoreSet, true);
    if (minIndex >= scoreSet.size()) {
      return 0;
    }

    int maxIndex = lexOptions.getRangeIndex(scoreSet, false);
    if (minIndex >= maxIndex) {
      return 0;
    }

    return maxIndex - minIndex;
  }

  public List<byte[]> zpopmax(Region<RedisKey, RedisData> region, RedisKey key, int count) {
    Iterator<AbstractOrderedSetEntry> scoresIterator =
        scoreSet.getIndexRange(scoreSet.size() - 1, count, true);

    return zpop(scoresIterator, region, key);
  }

  public List<byte[]> zpopmin(Region<RedisKey, RedisData> region, RedisKey key, int count) {
    Iterator<AbstractOrderedSetEntry> scoresIterator =
        scoreSet.getIndexRange(0, count, false);

    return zpop(scoresIterator, region, key);
  }

  public List<byte[]> zrange(SortedSetRankRangeOptions rangeOptions) {
    return getRange(rangeOptions);
  }

  public List<byte[]> zrangebylex(SortedSetLexRangeOptions rangeOptions) {
    return getRange(rangeOptions);
  }

  public List<byte[]> zrangebyscore(SortedSetScoreRangeOptions rangeOptions) {
    return getRange(rangeOptions);
  }

  public long zrank(byte[] member) {
    OrderedSetEntry orderedSetEntry = members.get(member);
    if (orderedSetEntry == null) {
      return -1;
    }
    return scoreSet.indexOf(orderedSetEntry);
  }

  public long zrem(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToRemove) {
    int membersRemoved = 0;
    RemsDeltaInfo deltaInfo = null;
    for (byte[] memberToRemove : membersToRemove) {
      if (memberRemove(memberToRemove)) {
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

  public long zremrangebylex(Region<RedisKey, RedisData> region, RedisKey key,
      SortedSetLexRangeOptions rangeOptions) {
    return removeRange(region, key, rangeOptions);
  }

  public long zremrangebyrank(Region<RedisKey, RedisData> region, RedisKey key,
      SortedSetRankRangeOptions rangeOptions) {
    return removeRange(region, key, rangeOptions);
  }

  public long zremrangebyscore(Region<RedisKey, RedisData> region, RedisKey key,
      SortedSetScoreRangeOptions rangeOptions) {
    return removeRange(region, key, rangeOptions);
  }

  public List<byte[]> zrevrange(SortedSetRankRangeOptions rangeOptions) {
    return getRange(rangeOptions);
  }

  public List<byte[]> zrevrangebylex(SortedSetLexRangeOptions rangeOptions) {
    return getRange(rangeOptions);
  }

  public List<byte[]> zrevrangebyscore(SortedSetScoreRangeOptions rangeOptions) {
    return getRange(rangeOptions);
  }

  public long zrevrank(byte[] member) {
    OrderedSetEntry orderedSetEntry = members.get(member);
    if (orderedSetEntry == null) {
      return -1;
    }
    return scoreSet.size() - scoreSet.indexOf(orderedSetEntry) - 1;
  }

  public ImmutablePair<Integer, List<byte[]>> zscan(GlobPattern matchPattern, int count,
      int cursor) {
    // No need to allocate more space than it's possible to use given the size of the sorted set. We
    // need to add 1 to zcard() to ensure that if count > members.size(), we return a cursor of 0
    long maximumCapacity = 2L * Math.min(count, zcard() + 1);
    if (maximumCapacity > Integer.MAX_VALUE) {
      logger.info(
          "The size of the data to be returned by zscan, {}, exceeds the maximum capacity of an array. A value for the ZSCAN COUNT argument less than {} should be used",
          maximumCapacity, Integer.MAX_VALUE / 2);
      throw new IllegalArgumentException("Requested array size exceeds VM limit");
    }
    List<byte[]> resultList = new ArrayList<>((int) maximumCapacity);

    cursor = members.scan(cursor, count,
        (list, key, value) -> addIfMatching(matchPattern, list, key, value.score), resultList);

    return new ImmutablePair<>(cursor, resultList);
  }

  public byte[] zscore(byte[] member) {
    OrderedSetEntry orderedSetEntry = members.get(member);
    if (orderedSetEntry != null) {
      return Coder.doubleToBytes(orderedSetEntry.getScore());
    }
    return null;
  }

  public long zunionstore(RegionProvider regionProvider, RedisKey key, List<ZKeyWeight> keyWeights,
      ZAggregator aggregator) {
    for (ZKeyWeight keyWeight : keyWeights) {
      RedisSortedSet set =
          regionProvider.getTypedRedisData(REDIS_SORTED_SET, keyWeight.getKey(), false);
      if (set == NULL_REDIS_SORTED_SET) {
        continue;
      }
      double weight = keyWeight.getWeight();

      for (AbstractOrderedSetEntry entry : set.members.values()) {
        OrderedSetEntry existingValue = members.get(entry.getMember());
        if (existingValue == null) {
          double score;
          // Redis math and Java math are different when handling infinity. Specifically:
          // Java: INFINITY * 0 = NaN
          // Redis: INFINITY * 0 = 0
          if (weight == 0) {
            score = 0;
          } else if (weight == 1) {
            score = entry.getScore();
          } else {
            double newScore = entry.getScore() * weight;
            if (Double.isNaN(newScore)) {
              score = entry.getScore();
            } else {
              score = newScore;
            }
          }
          members.put(entry.getMember(), new OrderedSetEntry(entry.getMember(), score));
          continue;
        }

        existingValue.updateScore(aggregator.getFunction().apply(existingValue.getScore(),
            entry.getScore() * weight));
      }
    }

    if (removeFromRegion()) {
      regionProvider.getDataRegion().remove(key);
    } else {
      scoreSet.addAll(members.values());
      regionProvider.getLocalDataRegion().put(key, this);
    }

    return getSortedSetSize();
  }

  private List<byte[]> zpop(Iterator<AbstractOrderedSetEntry> scoresIterator,
      Region<RedisKey, RedisData> region, RedisKey key) {
    if (!scoresIterator.hasNext()) {
      return Collections.emptyList();
    }

    List<byte[]> result = new ArrayList<>();
    RemsDeltaInfo deltaInfo = new RemsDeltaInfo();
    while (scoresIterator.hasNext()) {
      AbstractOrderedSetEntry entry = scoresIterator.next();
      scoresIterator.remove();
      members.remove(entry.getMember());

      result.add(entry.getMember());
      result.add(Coder.doubleToBytes(entry.getScore()));
      deltaInfo.add(entry.getMember());
    }

    storeChanges(region, key, deltaInfo);

    return result;
  }

  private long iteratorRangeRemove(Iterator<AbstractOrderedSetEntry> scoresIterator,
      Region<RedisKey, RedisData> region, RedisKey key) {
    if (!scoresIterator.hasNext()) {
      return 0;
    }

    int entriesRemoved = 0;

    RemsDeltaInfo deltaInfo = new RemsDeltaInfo();
    while (scoresIterator.hasNext()) {
      AbstractOrderedSetEntry entry = scoresIterator.next();
      scoresIterator.remove();
      members.remove(entry.getMember());
      entriesRemoved++;
      deltaInfo.add(entry.getMember());
    }

    storeChanges(region, key, deltaInfo);

    return entriesRemoved;
  }

  private byte[] zaddIncr(Region<RedisKey, RedisData> region, RedisKey key, byte[] member,
      double increment, ZAddOptions options) {
    // for zadd incr option, only one incrementing element pair is allowed to get here.
    if (options.isNX() && members.containsKey(member)) {
      return null;
    }
    if (options.isXX() && !members.containsKey(member)) {
      return null;
    }
    return zincrby(region, key, increment, member);
  }

  private List<byte[]> getRange(AbstractSortedSetRangeOptions<?> rangeOptions) {
    int startIndex = getStartIndex(rangeOptions);

    if (startIndex >= getSortedSetSize() && !rangeOptions.isRev()
        || startIndex < 0 && rangeOptions.isRev()) {
      return Collections.emptyList();
    }

    int maxElementsToReturn = getMaxElementsToReturn(rangeOptions, startIndex);

    if (maxElementsToReturn <= 0) {
      return Collections.emptyList();
    }

    return getElementsFromSet(rangeOptions, startIndex, maxElementsToReturn);
  }

  private long removeRange(Region<RedisKey, RedisData> region, RedisKey key,
      AbstractSortedSetRangeOptions<?> rangeOptions) {
    int startIndex = getStartIndex(rangeOptions);

    if (startIndex >= getSortedSetSize() && !rangeOptions.isRev()
        || startIndex < 0 && rangeOptions.isRev()) {
      return 0;
    }

    int maxElementsToRemove = getMaxElementsToReturn(rangeOptions, startIndex);

    if (maxElementsToRemove <= 0) {
      return 0;
    }

    Iterator<AbstractOrderedSetEntry> entryIterator =
        scoreSet.getIndexRange(startIndex, maxElementsToRemove, rangeOptions.isRev());

    return iteratorRangeRemove(entryIterator, region, key);
  }

  private int getStartIndex(AbstractSortedSetRangeOptions<?> rangeOptions) {
    int startIndex = rangeOptions.getRangeIndex(scoreSet, true);
    if (rangeOptions.hasLimit()) {
      if (rangeOptions.isRev()) {
        startIndex -= rangeOptions.getOffset();
      } else {
        startIndex += rangeOptions.getOffset();
      }
    }
    return startIndex;
  }

  private int getMaxElementsToReturn(AbstractSortedSetRangeOptions<?> rangeOptions,
      int startIndex) {
    int endIndex = rangeOptions.getRangeIndex(scoreSet, false);
    int rangeSize = rangeOptions.isRev() ? startIndex - endIndex : endIndex - startIndex;

    return Math.min(rangeOptions.getCount(), rangeSize);
  }

  private List<byte[]> getElementsFromSet(AbstractSortedSetRangeOptions<?> rangeOptions,
      int startIndex, int maxElementsToReturn) {
    Iterator<AbstractOrderedSetEntry> entryIterator =
        scoreSet.getIndexRange(startIndex, maxElementsToReturn, rangeOptions.isRev());

    if (rangeOptions.isWithScores()) {
      maxElementsToReturn *= 2;
    }

    List<byte[]> result = new ArrayList<>(maxElementsToReturn);
    while (entryIterator.hasNext()) {
      AbstractOrderedSetEntry entry = entryIterator.next();

      result.add(entry.getMember());
      if (rangeOptions.isWithScores()) {
        result.add(Coder.doubleToBytes(entry.getScore()));
      }
    }
    return result;
  }

  private void addIfMatching(GlobPattern matchPattern, List<byte[]> resultList, byte[] key,
      double score) {
    if (matchPattern != null) {
      if (matchPattern.matches(key)) {
        resultList.add(key);
        resultList.add(doubleToBytes(score));
      }
    } else {
      resultList.add(key);
      resultList.add(doubleToBytes(score));
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

  public abstract static class AbstractOrderedSetEntry
      implements Comparable<AbstractOrderedSetEntry>,
      Sizeable {
    byte[] member;
    double score = 0D;

    private AbstractOrderedSetEntry() {}

    public int compareTo(AbstractOrderedSetEntry o) {
      int comparison = compareScores(score, o.score);
      if (comparison == 0) {
        // Scores equal, try lexical ordering
        return compareMembers(member, o.member);
      }
      return comparison;
    }

    public byte[] getMember() {
      return member;
    }

    public double getScore() {
      return score;
    }

    public int compareScores(double score1, double score2) {
      return compare(score1, score2);
    }

    public abstract int compareMembers(byte[] array1, byte[] array2);

    public abstract int getSizeInBytes();
  }

  // Entry used to store data in the scoreSet
  public static class OrderedSetEntry extends AbstractOrderedSetEntry {

    public static final int ORDERED_SET_ENTRY_OVERHEAD = memoryOverhead(OrderedSetEntry.class);

    public OrderedSetEntry(byte[] member, double score) {
      this.member = member;
      this.score = score;
    }

    @Override
    public int compareMembers(byte[] array1, byte[] array2) {
      return javaImplementationOfAnsiCMemCmp(array1, array2);
    }

    @Override
    public int getSizeInBytes() {
      // don't include the member size since it is accounted
      // for as the key on the Hash.
      return ORDERED_SET_ENTRY_OVERHEAD;
    }

    public double updateScore(double newScore) {
      score = newScore;
      return newScore;
    }
  }

  // Dummy entry used to find the rank of an element with the given score for inclusive or
  // exclusive ranges
  public static class ScoreDummyOrderedSetEntry extends AbstractOrderedSetEntry {

    public ScoreDummyOrderedSetEntry(double score, boolean isExclusive, boolean isMinimum) {
      // If we are using an exclusive minimum comparison, or an inclusive maximum comparison then
      // this entry should act as if it is greater than the entry it's being compared to
      this.member = isExclusive ^ isMinimum ? bLEAST_MEMBER_NAME : bGREATEST_MEMBER_NAME;
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
  public static class MemberDummyOrderedSetEntry extends AbstractOrderedSetEntry {
    final boolean isExclusive;
    final boolean isMinimum;

    public MemberDummyOrderedSetEntry(byte[] member, boolean isExclusive, boolean isMinimum) {
      this.member = member;
      this.isExclusive = isExclusive;
      this.isMinimum = isMinimum;
    }

    @Override
    public int compareScores(double score1, double score2) {
      // Assume that all members have the same score. Behaviour is unspecified otherwise.
      return 0;
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
          // If the member names are equal but we are using an exclusive minimum comparison, or an
          // inclusive maximum comparison then this entry should act as if it is greater than the
          // entry it's being compared to
          return isMinimum ^ isExclusive ? -1 : 1;
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

  public static class MemberMap
      extends SizeableBytes2ObjectOpenCustomHashMapWithCursor<OrderedSetEntry> {

    public MemberMap(int size) {
      super(size);
    }

    public MemberMap(Map<byte[], RedisSortedSet.OrderedSetEntry> initialElements) {
      super(initialElements);
    }

    @Override
    protected int sizeValue(OrderedSetEntry value) {
      return value.getSizeInBytes();
    }

    public void toData(DataOutput out) throws IOException {
      DataSerializer.writePrimitiveInt(size(), out);
      for (Map.Entry<byte[], OrderedSetEntry> entry : entrySet()) {
        byte[] member = entry.getKey();
        double score = entry.getValue().getScore();
        DataSerializer.writeByteArray(member, out);
        DataSerializer.writePrimitiveDouble(score, out);
      }
    }
  }

  /**
   * ScoreSet does not keep track of the size of the element instances since they
   * are already accounted for by the MemberMap.
   */
  public static class ScoreSet extends OrderStatisticsTree<AbstractOrderedSetEntry> {
  }

  private enum MemberAddResult {
    CREATE,
    UPDATE,
    NO_OP
  }
}
