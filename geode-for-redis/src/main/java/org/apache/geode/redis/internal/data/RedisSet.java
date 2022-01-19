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

import static java.util.Collections.emptyList;
import static org.apache.geode.internal.JvmSizeUtils.memoryOverhead;
import static org.apache.geode.redis.internal.data.NullRedisDataStructures.NULL_REDIS_SET;
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
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

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.commands.executor.GlobPattern;
import org.apache.geode.redis.internal.data.collections.SizeableObjectOpenCustomHashSet;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.RemoveByteArrays;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrays;
import org.apache.geode.redis.internal.services.RegionProvider;

public class RedisSet extends AbstractRedisData {
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

  public RedisSet(int expectedSize) {
    members = new MemberSet(expectedSize);
  }

  /**
   * For deserialization only.
   */
  public RedisSet() {}

  public static int smove(RedisKey sourceKey, RedisKey destKey, byte[] member,
      RegionProvider regionProvider) {
    Region<RedisKey, RedisData> region = regionProvider.getDataRegion();

    RedisSet source = regionProvider.getTypedRedisData(REDIS_SET, sourceKey, false);
    RedisSet destination = regionProvider.getTypedRedisData(REDIS_SET, destKey, false);
    if (source.members.isEmpty() || !source.members.contains(member)) {
      return 0;
    }

    List<byte[]> movedMember = new ArrayList<>();
    movedMember.add(member);
    source.srem(movedMember, region, sourceKey);
    destination.sadd(movedMember, region, destKey);
    return 1;
  }

  public static Set<byte[]> sunion(RegionProvider regionProvider, List<RedisKey> keys) {
    return calculateUnion(regionProvider, keys, true);
  }

  private static MemberSet calculateUnion(RegionProvider regionProvider, List<RedisKey> keys,
      boolean updateStats) {
    MemberSet union = new MemberSet();
    for (RedisKey key : keys) {
      RedisSet curSet = regionProvider.getTypedRedisData(REDIS_SET, key, updateStats);
      union.addAll(curSet.members);
    }
    return union;
  }

  public static Set<byte[]> sdiff(RegionProvider regionProvider, List<RedisKey> keys) {
    return calculateDiff(regionProvider, keys, true);
  }

  public static int sdiffstore(RegionProvider regionProvider, RedisKey destinationKey,
      List<RedisKey> keys) {
    MemberSet diff = calculateDiff(regionProvider, keys, false);
    return setOpStoreResult(regionProvider, destinationKey, diff);
  }

  private static MemberSet calculateDiff(RegionProvider regionProvider, List<RedisKey> keys,
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

  public static Set<byte[]> sinter(RegionProvider regionProvider, List<RedisKey> keys) {
    List<RedisSet> sets = createRedisSetList(keys, regionProvider);
    final RedisSet smallestSet = findSmallest(sets);
    if (smallestSet.scard() == 0) {
      return Collections.emptySet();
    }

    MemberSet result = new MemberSet(smallestSet.scard());
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
      RegionProvider regionProvider) {
    List<RedisSet> sets = new ArrayList<>(keys.size());
    for (RedisKey key : keys) {
      RedisSet redisSet = regionProvider.getTypedRedisData(REDIS_SET, key, true);
      sets.add(redisSet);
    }
    return sets;
  }

  @VisibleForTesting
  static int setOpStoreResult(RegionProvider regionProvider, RedisKey destinationKey,
      MemberSet diff) {
    RedisSet destinationSet =
        regionProvider.getTypedRedisDataElseRemove(REDIS_SET, destinationKey, false);
    if (diff.isEmpty()) {
      if (destinationSet != null) {
        regionProvider.getDataRegion().remove(destinationKey);
      }
      return 0;
    }

    if (destinationSet != null) {
      destinationSet.persistNoDelta();
      destinationSet.members = diff;
      destinationSet.storeChanges(regionProvider.getDataRegion(), destinationKey,
          new ReplaceByteArrays(diff));
    } else {
      regionProvider.getDataRegion().put(destinationKey, new RedisSet(diff));
    }

    return diff.size();
  }

  public Pair<BigInteger, List<Object>> sscan(GlobPattern matchPattern, int count,
      BigInteger cursor) {
    List<Object> returnList = new ArrayList<>();
    int size = members.size();
    BigInteger beforeCursor = new BigInteger("0");
    int numElements = 0;
    int i = -1;
    for (byte[] value : members) {
      i++;
      if (beforeCursor.compareTo(cursor) < 0) {
        beforeCursor = beforeCursor.add(new BigInteger("1"));
        continue;
      }

      if (matchPattern != null) {
        if (matchPattern.matches(value)) {
          returnList.add(value);
          numElements++;
        }
      } else {
        returnList.add(value);
        numElements++;
      }

      if (numElements == count) {
        break;
      }
    }

    Pair<BigInteger, List<Object>> scanResult;
    if (i >= size - 1) {
      scanResult = new ImmutablePair<>(new BigInteger("0"), returnList);
    } else {
      scanResult = new ImmutablePair<>(new BigInteger(String.valueOf(i + 1)), returnList);
    }
    return scanResult;
  }

  public Collection<byte[]> spop(Region<RedisKey, RedisData> region, RedisKey key, int popCount) {
    int originalSize = scard();
    if (originalSize == 0) {
      return emptyList();
    }

    if (popCount >= originalSize) {
      region.remove(key, this);
      return members;
    }

    List<byte[]> popped = new ArrayList<>();
    byte[][] setMembers = members.toArray(new byte[originalSize][]);
    Random rand = new Random();
    while (popped.size() < popCount) {
      int idx = rand.nextInt(originalSize);
      byte[] memberToPop = setMembers[idx];
      if (memberToPop != null) {
        setMembers[idx] = null;
        popped.add(memberToPop);
        membersRemove(memberToPop);
      }
    }
    if (!popped.isEmpty()) {
      storeChanges(region, key, new RemoveByteArrays(popped));
    }
    return popped;
  }

  public List<byte[]> srandmember(int count) {
    boolean duplicatesAllowed = count < 0;
    count = duplicatesAllowed ? -count : count;
    int membersSize = members.size();
    int memberMapSize = members.getMemberMapSize();

    Random rand = new Random();
    List<byte[]> result = new ArrayList<>(count);
    if (!duplicatesAllowed && membersSize <= count) {
      result.addAll(members);
    } else {
      Set<Integer> usedIndexes = null;
      if (!duplicatesAllowed) {
        usedIndexes = new HashSet<>(count);
      }

      for (int i = 0; i < count; i++) {
        int randIndex = rand.nextInt(memberMapSize);
        byte[] member = members.getKeyAtIndex(randIndex);

        while (member == null || (!duplicatesAllowed && usedIndexes.contains(randIndex))) {
          randIndex = rand.nextInt(memberMapSize);
          member = members.getKeyAtIndex(randIndex);
        }

        result.add(member);
        if (!duplicatesAllowed) {
          usedIndexes.add(randIndex);
        }
      }
    }
    return result;
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
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writePrimitiveInt(members.size(), out);
    for (byte[] member : members) {
      DataSerializer.writeByteArray(member, out);
    }
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = DataSerializer.readPrimitiveInt(in);
    members = new MemberSet(size);
    for (int i = 0; i < size; ++i) {
      members.add(DataSerializer.readByteArray(in));
    }
  }

  @Override
  public int getDSFID() {
    return REDIS_SET_ID;
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
   * @param membersToAdd members to add to this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of members actually added
   */
  public long sadd(List<byte[]> membersToAdd, Region<RedisKey, RedisData> region, RedisKey key) {
    membersToAdd.removeIf(memberToAdd -> !membersAdd(memberToAdd));
    int membersAdded = membersToAdd.size();
    if (membersAdded != 0) {
      storeChanges(region, key, new AddByteArrays(membersToAdd));
    }
    return membersAdded;
  }

  /**
   * @param membersToRemove members to remove from this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to remove from
   * @return the number of members actually removed
   */
  public long srem(List<byte[]> membersToRemove, Region<RedisKey, RedisData> region, RedisKey key) {
    membersToRemove.removeIf(memberToRemove -> !membersRemove(memberToRemove));
    int membersRemoved = membersToRemove.size();
    if (membersRemoved != 0) {
      storeChanges(region, key, new RemoveByteArrays(membersToRemove));
    }
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
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getSizeInBytes() {
    return REDIS_SET_OVERHEAD + members.getSizeInBytes();
  }

  public static class MemberSet extends SizeableObjectOpenCustomHashSet<byte[]> {
    public MemberSet() {
      super(ByteArrays.HASH_STRATEGY);
    }

    public MemberSet(int size) {
      super(size, ByteArrays.HASH_STRATEGY);
    }

    public MemberSet(Collection<byte[]> initialElements) {
      super(initialElements, ByteArrays.HASH_STRATEGY);
    }

    public byte[] getKeyAtIndex(int pos) {
      return getKey(pos);
    }

    @Override
    protected int sizeElement(byte[] element) {
      return memoryOverhead(element);
    }
  }

}
