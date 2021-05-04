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

import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_SORTED_SET;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;

public class RedisSortedSet extends AbstractRedisData {

  private enum AddOrChange {
    ADDED, CHANGED, NOOP;
  }

  private HashMap<byte[], byte[]> members;

  @SuppressWarnings("unchecked")
  RedisSortedSet(Map<byte[], byte[]> members) {
    this.members = (HashMap<byte[], byte[]>) members;
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

  //
  // /**
  // * Since GII (getInitialImage) can come in and call toData while other threads
  // * are modifying this object, the striped executor will not protect toData.
  // * So any methods that modify "members" needs to be thread safe with toData.
  // */
  //
  // @Override
  // public synchronized void toData(DataOutput out, SerializationContext context) throws
  // IOException {
  // super.toData(out, context);
  // InternalDataSerializer.writeHashSet(members, out);
  // }
  //
  // @Override
  // public void fromData(DataInput in, DeserializationContext context)
  // throws IOException, ClassNotFoundException {
  // super.fromData(in, context);
  // members = InternalDataSerializer.readHashSet(in);
  // }
  //
  @Override
  public int getDSFID() {
    return REDIS_SORTED_SET_ID;
  }

  //
  private synchronized AddOrChange membersAdd(byte[] scoreToAdd, byte[] memberToAdd, boolean CH) {
    byte[] oldScore = members.get(memberToAdd);
    boolean added = (members.put(scoreToAdd, memberToAdd) == null);
    if (CH && !added) {
      return oldScore.equals(scoreToAdd) ? AddOrChange.NOOP : AddOrChange.CHANGED;
    }
    return added ? AddOrChange.ADDED : AddOrChange.NOOP;
  }

  private synchronized void membersAddAll(AddsDeltaInfo addsDeltaInfo) {
    Iterator<ByteArrayWrapper> iterator = addsDeltaInfo.getAdds().iterator();
    while (iterator.hasNext()) {
      ByteArrayWrapper member = iterator.next();
      ByteArrayWrapper score = iterator.next();
      members.put(member.toBytes(), score.toBytes()); // TODO: get rid of ByteArrayWrapper
    }
  }

  private synchronized void membersRemoveAll(RemsDeltaInfo remsDeltaInfo) {
    // TODO: get rid of ByteArrayWrapper
    for (ByteArrayWrapper member : remsDeltaInfo.getRemoves()) {
      members.remove(member);
    }
  }

  /**
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @param membersToAdd members to add to this set; NOTE this list may by modified by this call
   * @return the number of members actually added
   */
  long zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToAdd) {
    int membersAdded = 0;
    long membersChanged = 0; // TODO: really implement changed

    AddsDeltaInfo deltaInfo = null;
    Iterator<byte[]> iterator = membersToAdd.iterator();
    while (iterator.hasNext()) {
      byte[] member = iterator.next();
      byte[] score = iterator.next();

      switch (membersAdd(member, score, false)) {
        case ADDED:
          membersAdded++;
          makeAddsDeltaInfo(deltaInfo, member, score);
          break;
        case CHANGED:
          membersChanged++;
          makeAddsDeltaInfo(deltaInfo, member, score);
          break;
        default:
          // do nothing
      }
    }
    if (deltaInfo != null) {
      storeChanges(region, key, deltaInfo);
    }
    return membersAdded;
  }

  private AddsDeltaInfo makeAddsDeltaInfo(AddsDeltaInfo deltaInfo, byte[] member, byte[] score) {
    if (deltaInfo == null) {
      deltaInfo = new AddsDeltaInfo();
    }
    deltaInfo.add(new ByteArrayWrapper(member)); // TODO: get rid of ByteArrayWrapper
    deltaInfo.add(new ByteArrayWrapper(score));
    return deltaInfo;
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
