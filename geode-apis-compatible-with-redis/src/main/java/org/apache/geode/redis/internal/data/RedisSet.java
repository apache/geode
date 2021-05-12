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
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;

public class RedisSet extends AbstractRedisData {
  private HashSet<ByteArrayWrapper> members;

  // the following constants were calculated using reflection and math. you can find the tests for
  // these values in RedisSetTest, which show the way these numbers were calculated. the constants
  // have the advantage of saving us a lot of computation that would happen every time a new key was
  // added. if our internal implementation changes, these values may be incorrect. the tests will
  // catch this change. an increase in overhead should be carefully considered.
  // Note: the per member overhead is known to not be constant. it changes as more members are
  // added, and/or as the members get longer
  protected static final int BASE_REDIS_SET_OVERHEAD = 112;
  protected static final int PER_MEMBER_OVERHEAD = 77;
  protected static final int INTERNAL_HASH_SET_STORAGE_OVERHEAD = 86;

  private int sizeInBytes;

  RedisSet(Collection<ByteArrayWrapper> members) {
    this();

    if (members instanceof HashSet) {
      this.members = (HashSet<ByteArrayWrapper>) members;
    } else {
      this.members = new HashSet<>(members);
    }

    if (members.size() > 0) {
      sizeInBytes += INTERNAL_HASH_SET_STORAGE_OVERHEAD;
    }

    for (ByteArrayWrapper value : this.members) {
      sizeInBytes += PER_MEMBER_OVERHEAD + value.length();
    }
  }

  // for serialization
  public RedisSet() {
    sizeInBytes += BASE_REDIS_SET_OVERHEAD;
  }

  Pair<BigInteger, List<Object>> sscan(Pattern matchPattern, int count, BigInteger cursor) {

    List<Object> returnList = new ArrayList<>();
    int size = members.size();
    BigInteger beforeCursor = new BigInteger("0");
    int numElements = 0;
    int i = -1;
    for (ByteArrayWrapper value : members) {
      i++;
      if (beforeCursor.compareTo(cursor) < 0) {
        beforeCursor = beforeCursor.add(new BigInteger("1"));
        continue;
      }

      if (matchPattern != null) {
        if (matchPattern.matcher(value.toString()).matches()) {
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

  Collection<ByteArrayWrapper> spop(Region<RedisKey, RedisData> region,
      RedisKey key, int popCount) {
    int originalSize = scard();
    if (originalSize == 0) {
      return emptyList();
    }

    if (popCount >= originalSize) {
      region.remove(key, this);
      return this.members;
    }

    ArrayList<ByteArrayWrapper> popped = new ArrayList<>();
    ByteArrayWrapper[] setMembers = members.toArray(new ByteArrayWrapper[originalSize]);
    Random rand = new Random();
    while (popped.size() < popCount) {
      int idx = rand.nextInt(originalSize);
      ByteArrayWrapper memberToPop = setMembers[idx];
      if (memberToPop != null) {
        setMembers[idx] = null;
        popped.add(memberToPop);
        membersRemove(memberToPop);
      }
    }
    if (!popped.isEmpty()) {
      storeChanges(region, key, new RemsDeltaInfo(unwrapByteArrayWrappers(popped)));
    }
    return popped;
  }

  Collection<ByteArrayWrapper> srandmember(int count) {
    int membersSize = members.size();
    boolean duplicatesAllowed = count < 0;
    if (duplicatesAllowed) {
      count = -count;
    }

    if (!duplicatesAllowed && membersSize <= count && count != 1) {
      return new ArrayList<>(members);
    }

    Random rand = new Random();

    ByteArrayWrapper[] entries = members.toArray(new ByteArrayWrapper[membersSize]);

    if (count == 1) {
      ByteArrayWrapper randEntry = entries[rand.nextInt(entries.length)];
      // Note using ArrayList because Collections.singleton has serialization issues.
      ArrayList<ByteArrayWrapper> result = new ArrayList<>(1);
      result.add(randEntry);
      return result;
    }
    if (duplicatesAllowed) {
      ArrayList<ByteArrayWrapper> result = new ArrayList<>(count);
      while (count > 0) {
        result.add(entries[rand.nextInt(entries.length)]);
        count--;
      }
      return result;
    } else {
      Set<ByteArrayWrapper> result = new HashSet<>();
      // Note that rand.nextInt can return duplicates when "count" is high
      // so we need to use a Set to collect the results.
      while (result.size() < count) {
        ByteArrayWrapper s = entries[rand.nextInt(entries.length)];
        result.add(s);
      }
      return result;
    }
  }

  public boolean sismember(ByteArrayWrapper member) {
    return members.contains(member);
  }

  public int scard() {
    return members.size();
  }

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
   * Since GII (getInitialImage) can come in and call toData while other threads
   * are modifying this object, the striped executor will not protect toData.
   * So any methods that modify "members" needs to be thread safe with toData.
   */

  @Override
  public synchronized void toData(DataOutput out, SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeHashSet(members, out);
    DataSerializer.writeInteger(sizeInBytes, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    members = DataSerializer.readHashSet(in);
    sizeInBytes = DataSerializer.readInteger(in);
  }

  @Override
  public int getDSFID() {
    return REDIS_SET_ID;
  }

  private synchronized boolean membersAdd(ByteArrayWrapper memberToAdd) {
    boolean isAdded = members.add(memberToAdd);
    if (isAdded) {
      sizeInBytes += PER_MEMBER_OVERHEAD + memberToAdd.length();
      if (members.size() == 1) {
        sizeInBytes += INTERNAL_HASH_SET_STORAGE_OVERHEAD;
      }
    }
    return isAdded;
  }

  private boolean membersRemove(ByteArrayWrapper memberToRemove) {
    boolean isRemoved = members.remove(memberToRemove);
    if (isRemoved) {
      sizeInBytes -= PER_MEMBER_OVERHEAD + memberToRemove.length();
      if (members.isEmpty()) {
        sizeInBytes = BASE_REDIS_SET_OVERHEAD;
      }
    }
    return isRemoved;
  }

  private synchronized void membersAddAll(AddsDeltaInfo addsDeltaInfo) {
    List<ByteArrayWrapper> adds = wrapByteArrays(addsDeltaInfo.getAdds());
    sizeInBytes += adds.stream().mapToInt(a -> a.length() + PER_MEMBER_OVERHEAD).sum();
    members.addAll(adds);
  }

  private synchronized void membersRemoveAll(RemsDeltaInfo remsDeltaInfo) {
    List<ByteArrayWrapper> removes = wrapByteArrays(remsDeltaInfo.getRemoves());
    sizeInBytes -= removes.stream().mapToInt(a -> a.length() + PER_MEMBER_OVERHEAD).sum();
    members.removeAll(removes);
  }

  /**
   * Unwraps all of the byte array wrappers in a list
   *
   * Temporary function to continue to use ByteArrayWrappers in RedisSet after
   * delta info classes have already switched to using plan byte[]
   */
  private ArrayList<byte[]> unwrapByteArrayWrappers(ArrayList<ByteArrayWrapper> list) {
    return list.stream()
        .map(ByteArrayWrapper::toBytes)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  /**
   * Wraps all of the byte[] values in byte array wrappers in a list
   *
   * Temporary function to continue to use ByteArrayWrappers in RedisSet after
   * delta info classes have already switched to using plan byte[]
   */
  private List<ByteArrayWrapper> wrapByteArrays(List<byte[]> list) {
    return list.stream().map(ByteArrayWrapper::new).collect(
        Collectors.toList());
  }

  /**
   * @param membersToAdd members to add to this set; NOTE this list may by
   *        modified by this call
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @return the number of members actually added
   */
  long sadd(ArrayList<ByteArrayWrapper> membersToAdd, Region<RedisKey, RedisData> region,
      RedisKey key) {

    membersToAdd.removeIf(memberToAdd -> !membersAdd(memberToAdd));
    int membersAdded = membersToAdd.size();
    if (membersAdded != 0) {
      final ArrayList<byte[]> rStream = unwrapByteArrayWrappers(membersToAdd);
      storeChanges(region, key, new AddsDeltaInfo(rStream));
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
  long srem(ArrayList<ByteArrayWrapper> membersToRemove, Region<RedisKey, RedisData> region,
      RedisKey key) {

    membersToRemove.removeIf(memberToRemove -> !membersRemove(memberToRemove));
    int membersRemoved = membersToRemove.size();
    if (membersRemoved != 0) {
      storeChanges(region, key, new RemsDeltaInfo(unwrapByteArrayWrappers(membersToRemove)));
    }
    return membersRemoved;
  }

  /**
   * The returned set is a copy and will not be changed
   * by future changes to this instance.
   *
   * @return a set containing all the members in this set
   */
  @VisibleForTesting
  Set<ByteArrayWrapper> smembers() {
    return new HashSet<>(members);
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
    return Objects.equals(members, redisSet.members);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), members);
  }

  @Override
  public String toString() {
    return "RedisSet{" + super.toString() + ", " + "members=" + members + '}';
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getSizeInBytes() {
    return sizeInBytes;
  }
}
