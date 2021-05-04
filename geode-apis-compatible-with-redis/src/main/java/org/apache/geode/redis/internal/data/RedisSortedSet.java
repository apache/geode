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
import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SORTED_SET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;

public class RedisSortedSet extends AbstractRedisData {

  private HashMap<byte[], byte[]> members;

  @SuppressWarnings("unchecked")
  RedisSortedSet(Collection<byte[]> members) {
    if (members instanceof HashSet) {
      this.members = (HashMap<byte[], byte[]>) members;
    } else {
      this.members = new HashMap<byte[], byte[]>(members);
    }
  }

  // for serialization
  public RedisSortedSet() {}

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
      storeChanges(region, key, new RemsDeltaInfo(popped));
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
    InternalDataSerializer.writeHashSet(members, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    members = InternalDataSerializer.readHashSet(in);
  }

  @Override
  public int getDSFID() {
    return REDIS_SET_ID;
  }

  private synchronized boolean membersAdd(ByteArrayWrapper memberToAdd) {
    return members.add(memberToAdd);
  }

  private boolean membersRemove(ByteArrayWrapper memberToRemove) {
    return members.remove(memberToRemove);
  }

  private synchronized boolean membersAddAll(AddsDeltaInfo addsDeltaInfo) {
    return members.addAll(addsDeltaInfo.getAdds());
  }

  private synchronized boolean membersRemoveAll(RemsDeltaInfo remsDeltaInfo) {
    return members.removeAll(remsDeltaInfo.getRemoves());
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
      storeChanges(region, key, new AddsDeltaInfo(membersToAdd));
    }
    return membersAdded;
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
    RedisSortedSet redisSet = (RedisSortedSet) o;
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
}
