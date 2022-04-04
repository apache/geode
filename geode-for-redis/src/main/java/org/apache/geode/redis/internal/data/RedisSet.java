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

import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;
import static org.apache.geode.redis.internal.RedisConstants.REDIS_SET_DATA_SERIALIZABLE_ID;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.commands.executor.GlobPattern;
import org.apache.geode.redis.internal.data.collections.SizeableObjectOpenCustomHashSetWithCursor;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.RemoveByteArrays;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrays;
import org.apache.geode.redis.internal.services.RegionProvider;

public class RedisSet extends AbstractRedisData {

  static {
    Instantiator.register(new Instantiator(RedisSet.class, REDIS_SET_DATA_SERIALIZABLE_ID) {
      public DataSerializable newInstance() {
        return new RedisSet();
      }
    });
  }

  protected static final int REDIS_SET_OVERHEAD = memoryOverhead(RedisSet.class);
  private MemberSet members;

  public RedisSet(Collection<byte[]> members) {
    this.members = new MemberSet(members.size());
    for (byte[] member : members) {
      membersAdd(member);
    }
  }

  public RedisSet(MemberSet members) {
    this.members = members;
  }

  public RedisSet(RedisSet redisSet) {
    setExpirationTimestampNoDelta(redisSet.getExpirationTimestamp());
    setVersion(redisSet.getVersion());
    members = new MemberSet(redisSet.members.size());
    members.addAll(redisSet.members);
  }

  public RedisSet(int expectedSize) {
    members = new MemberSet(expectedSize);
  }

  /**
   * For deserialization only.
   */
  public RedisSet() {}

  public static int smove(RedisKey sourceKey, RedisKey destKey, byte[] member,
      RegionProvider regionProvider) {
    RedisSet source = regionProvider.getTypedRedisData(REDIS_SET, sourceKey, false);
    RedisSet destination = regionProvider.getTypedRedisData(REDIS_SET, destKey, false);

    if (!source.sismember(member)) {
      return 0;
    }

    if (sourceKey.equals(destKey)) {
      return 1;
    }

    List<byte[]> memberList = new ArrayList<>();
    memberList.add(member);
    RedisSet newSource = new RedisSet(source);
    newSource.srem(memberList, regionProvider.getDataRegion(), sourceKey);
    RedisSet newDestination = new RedisSet(destination);
    newDestination.sadd(memberList, regionProvider.getDataRegion(), destKey);
    return 1;
  }

  public static MemberSet sunion(RegionProvider regionProvider, List<RedisKey> keys,
      boolean updateStats) {
    MemberSet union = new MemberSet();
    for (RedisKey key : keys) {
      RedisSet curSet = regionProvider.getTypedRedisData(REDIS_SET, key, updateStats);
      union.addAll(curSet.members);
    }
    return union;
  }

  public static int sunionstore(RegionProvider regionProvider, List<RedisKey> keys,
      RedisKey destinationKey) {
    MemberSet union = sunion(regionProvider, keys, false);
    return setOpStoreResult(regionProvider, destinationKey, union);
  }

  public static MemberSet sdiff(RegionProvider regionProvider, List<RedisKey> keys,
      boolean updateStats) {
    RedisSet firstSet = regionProvider.getTypedRedisData(REDIS_SET, keys.get(0), updateStats);
    MemberSet diff = new MemberSet(firstSet.members);
    for (int i = 1; i < keys.size(); i++) {
      RedisSet curSet = regionProvider.getTypedRedisData(REDIS_SET, keys.get(i), updateStats);
      if (curSet == NULL_REDIS_SET) {
        continue;
      }
      diff.removeAll(curSet.members);
    }
    return diff;
  }

  public static int sdiffstore(RegionProvider regionProvider,
      List<RedisKey> keys, RedisKey destinationKey) {
    MemberSet diff = sdiff(regionProvider, keys, false);
    return setOpStoreResult(regionProvider, destinationKey, diff);
  }

  public static MemberSet sinter(RegionProvider regionProvider, List<RedisKey> keys,
      boolean updateStats) {
    List<RedisSet> sets = createRedisSetList(keys, regionProvider, updateStats);
    final RedisSet smallestSet = findSmallest(sets);
    MemberSet result = new MemberSet(smallestSet.scard());
    if (smallestSet.scard() == 0) {
      return result;
    }

    for (byte[] member : smallestSet.members) {
      boolean addToSet = true;
      for (RedisSet otherSet : sets) {
        if (otherSet == smallestSet) {
          continue;
        }
        if (!otherSet.members.contains(member)) {
          addToSet = false;
          break;
        }
      }
      if (addToSet) {
        result.add(member);
      }
    }
    return result;
  }

  private static RedisSet findSmallest(List<RedisSet> sets) {
    RedisSet smallestSet = NULL_REDIS_SET;
    for (RedisSet set : sets) {
      if (smallestSet == NULL_REDIS_SET) {
        smallestSet = set;
      } else if (smallestSet.scard() > set.scard()) {
        smallestSet = set;
      }
    }
    return smallestSet;
  }

  private static List<RedisSet> createRedisSetList(List<RedisKey> keys,
      RegionProvider regionProvider, boolean updateStats) {
    List<RedisSet> sets = new ArrayList<>(keys.size());
    for (RedisKey key : keys) {
      RedisSet redisSet = regionProvider.getTypedRedisData(REDIS_SET, key, updateStats);
      sets.add(redisSet);
    }
    return sets;
  }

  public static int sinterstore(RegionProvider regionProvider,
      List<RedisKey> keys, RedisKey destinationKey) {
    MemberSet inter = sinter(regionProvider, keys, false);
    return setOpStoreResult(regionProvider, destinationKey, inter);
  }

  @VisibleForTesting
  static int setOpStoreResult(RegionProvider regionProvider, RedisKey destinationKey,
      MemberSet result) {
    RedisSet destinationSet =
        regionProvider.getTypedRedisDataElseRemove(REDIS_SET, destinationKey, false);
    if (result.isEmpty()) {
      if (destinationSet != null) {
        regionProvider.getDataRegion().remove(destinationKey);
      }
      return 0;
    }

    if (destinationSet != null) {
      destinationSet.persistNoDelta();
      destinationSet.members = result;
      destinationSet.storeChanges(regionProvider.getDataRegion(), destinationKey,
          new ReplaceByteArrays(result));
    } else {
      regionProvider.getDataRegion().put(destinationKey, new RedisSet(result));
    }

    return result.size();
  }

  public Pair<Integer, List<byte[]>> sscan(GlobPattern matchPattern, int count,
      int cursor) {
    int maximumCapacity = Math.min(count, scard() + 1);
    List<byte[]> resultList = new ArrayList<>(maximumCapacity);

    cursor = members.scan(cursor, count,
        (list, key) -> addIfMatching(matchPattern, list, key), resultList);

    return new ImmutablePair<>(cursor, resultList);

  }

  private void addIfMatching(GlobPattern matchPattern, List<byte[]> list, byte[] key) {
    if (matchPattern != null) {
      if (matchPattern.matches(key)) {
        list.add(key);
      }
    } else {
      list.add(key);
    }
  }

  public List<byte[]> spop(int count, Region<RedisKey, RedisData> region, RedisKey key) {
    final int popMethodRatio = 5; // The ratio is based off command documentation
    List<byte[]> result = new ArrayList<>(Math.min(count, members.size()));
    if (count * popMethodRatio < members.size()) {
      /*
       * Count is small enough to add random elements to result.
       * Random indexes are generated to get members that are stored in the backing array of the
       * MemberSet. The members are then removed from the set, allowing for a unique random
       * number to be generated every time.
       */
      spopWithSmallCount(count, result);
    } else {
      /*
       * The count is close to the number of members in the set.
       * Since members are being removed from the set, there is a possibility of rehashing. With a
       * large count, there is a chance that it can rehash multiple times, copying
       * the set to the result and removing members from that help limit the amount of
       * rehashes that need to be preformed.
       */
      spopWithLargeCount(count, result);
    }
    storeChanges(region, key, new RemoveByteArrays(result));
    return result;
  }

  private void spopWithSmallCount(int count, List<byte[]> result) {
    Random rand = new Random();
    while (result.size() != count) {
      byte[] member = members.getRandomMemberFromBackingArray(rand);
      result.add(member);
      members.remove(member);
    }
  }

  private void spopWithLargeCount(int count, List<byte[]> result) {
    if (count >= members.size()) {
      members.toList(result);
      members.clear();
      return;
    }

    Random rand = new Random();
    MemberSet remainingMembers = new MemberSet(members.size() - count);
    while (members.size() != count) {
      byte[] member = members.getRandomMemberFromBackingArray(rand);
      remainingMembers.add(member);
      members.remove(member);
    }

    members.toList(result);
    members = remainingMembers;
  }

  public List<byte[]> srandmember(int count) {
    final int randMethodRatio = 3; // The ratio is based off command documentation
    List<byte[]> result;
    if (count < 0) {
      result = new ArrayList<>(-count);
      srandomDuplicateList(-count, result);
    } else if (count * randMethodRatio < members.size()) {
      /*
       * Count is small enough to add random elements to result.
       * Random indexes are generated to get members that are stored in the backing array of the
       * MemberSet.
       *
       * Since the MemberSet is not being modified, the members previously found are tracked.
       */
      result = new ArrayList<>(count);
      srandomUniqueListWithSmallCount(count, result);
    } else {
      /*
       * The count is close to the number of members in the set.
       * If the same method were used as srandomUniqueListWithSmallCount, when the result size
       * approaches the count, it gets to a point where a specific random index would need
       * to be generated to get a valid member (A member that has not been added to the result).
       *
       * Copying the members to the result and generating a random index of members we want to
       * remove from the result solves that issue.
       *
       * For example, if you have a set with 100 members, and the backing array has a length of 100.
       * If we want to get 99 random members, it would take more effort to generate 99 unique
       * random indexes to add unique members to the list than it would be generate 1 random
       * index for a 1 member to be removed.
       */
      result = new ArrayList<>(Math.min(count, members.size()));
      srandomUniqueListWithLargeCount(count, result);
    }
    return result;
  }

  private void srandomDuplicateList(int count, List<byte[]> result) {
    Random rand = new Random();
    while (result.size() != count) {
      byte[] member = members.getRandomMemberFromBackingArray(rand);
      result.add(member);
    }
  }

  private void srandomUniqueListWithSmallCount(int count, List<byte[]> result) {
    Random rand = new Random();
    Set<byte[]> membersUsed = new HashSet<>();

    while (result.size() != count) {
      byte[] member = members.getRandomMemberFromBackingArray(rand);
      if (!membersUsed.contains(member)) {
        result.add(member);
        membersUsed.add(member);
      }
    }
  }

  private void srandomUniqueListWithLargeCount(int count, List<byte[]> result) {
    if (count >= members.size()) {
      members.toList(result);
      return;
    }

    Random rand = new Random();
    MemberSet duplicateSet = new MemberSet(members);
    while (duplicateSet.size() != count) {
      byte[] member = members.getRandomMemberFromBackingArray(rand);
      duplicateSet.remove(member);
    }

    duplicateSet.toList(result);
  }

  public boolean sismember(byte[] member) {
    return members.contains(member);
  }

  public int scard() {
    return members.size();
  }

  @Override
  public void applyAddByteArrayDelta(byte[] bytes) {
    membersAdd(bytes);
  }

  @Override
  public void applyRemoveByteArrayDelta(byte[] bytes) {
    membersRemove(bytes);
  }

  @Override
  public void applyReplaceByteArraysDelta(MemberSet members) {
    persistNoDelta();
    this.members = members;
  }

  /**
   * Since GII (getInitialImage) can come in and call toData while other threads
   * are modifying this object, the striped executor will not protect toData.
   * So any methods that modify "members" needs to be thread safe with toData.
   */

  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writePrimitiveInt(members.size(), out);
    for (byte[] member : members) {
      DataSerializer.writeByteArray(member, out);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    int size = DataSerializer.readPrimitiveInt(in);
    members = new MemberSet(size);
    for (int i = 0; i < size; ++i) {
      members.add(DataSerializer.readByteArray(in));
    }
  }

  @VisibleForTesting
  synchronized boolean membersAdd(byte[] memberToAdd) {
    return members.add(memberToAdd);
  }

  @VisibleForTesting
  synchronized boolean membersRemove(byte[] memberToRemove) {
    return members.remove(memberToRemove);
  }

  /**
   * @param membersToAdd members to add to this set
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of members actually added
   */
  public long sadd(List<byte[]> membersToAdd, Region<RedisKey, RedisData> region, RedisKey key) {
    int membersAdded = 0;
    AddByteArrays delta = null;
    synchronized (this) {
      for (byte[] member : membersToAdd) {
        if (membersAdd(member)) {
          if (delta == null) {
            delta = new AddByteArrays(incrementAndGetVersion());
          }
          delta.add(member);
          membersAdded++;
        }
      }
    }
    if (membersAdded == 0) {
      return 0;
    }
    storeChanges(region, key, delta);
    return membersAdded;
  }

  /**
   * @param membersToRemove members to remove from this set
   * @param region the region this instance is stored in
   * @param key the name of the set to remove from
   * @return the number of members actually removed
   */
  public long srem(List<byte[]> membersToRemove, Region<RedisKey, RedisData> region, RedisKey key) {
    RemoveByteArrays delta = new RemoveByteArrays();
    int membersRemoved = 0;
    for (byte[] member : membersToRemove) {
      if (membersRemove(member)) {
        delta.add(member);
        membersRemoved++;
      }
    }
    if (membersRemoved == 0) {
      return 0;
    }
    storeChanges(region, key, delta);
    return membersRemoved;
  }

  /**
   * The returned set is NOT a copy and will be changed
   * by future changes to this instance.
   *
   * @return a set containing all the members in this set
   */
  public Set<byte[]> smembers() {
    return Collections.unmodifiableSet(members);
  }

  @Override
  public RedisDataType getType() {
    return REDIS_SET;
  }

  @Override
  protected boolean removeFromRegion() {
    return members.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RedisSet)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RedisSet redisSet = (RedisSet) o;

    if (redisSet.members.size() != members.size()) {
      return false;
    }
    for (byte[] member : members) {
      if (!redisSet.members.contains(member)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), members);
  }

  @Override
  public String toString() {
    return "RedisSet{" + super.toString() + ", " + "size=" + members.size() + '}';
  }

  @Override
  public int getSizeInBytes() {
    return REDIS_SET_OVERHEAD + members.getSizeInBytes();
  }

  public static class MemberSet extends SizeableObjectOpenCustomHashSetWithCursor<byte[]> {
    public MemberSet() {
      super(ByteArrays.HASH_STRATEGY);
    }

    public MemberSet(int size) {
      super(size, ByteArrays.HASH_STRATEGY);
    }

    public MemberSet(Collection<byte[]> initialElements) {
      super(initialElements, ByteArrays.HASH_STRATEGY);
    }

    @Override
    protected int sizeElement(byte[] element) {
      return memoryOverhead(element);
    }

    private void toList(List<byte[]> list) {
      for (byte[] member : this) {
        list.add(member);
      }
    }
  }

}
