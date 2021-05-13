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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenCustomHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.executor.sortedset.ZSetOptions;

public class RedisSortedSet extends AbstractRedisData {
  private Object2ObjectOpenCustomHashMap<byte[], byte[]> members;

  protected static final int BASE_REDIS_SORTED_SET_OVERHEAD = 184;
  protected static final int PER_MEMBER_OVERHEAD = 77;
  protected static final int INTERNAL_CUSTOM_HASHMAP_STORAGE_OVERHEAD = 86;

  private int sizeInBytes;

  @Override
  public int getSizeInBytes() {
    return sizeInBytes;
  }

  private enum AddOrChange {
    ADDED, CHANGED, NOOP
  }

  RedisSortedSet(List<byte[]> members) {
    sizeInBytes += BASE_REDIS_SORTED_SET_OVERHEAD;
    this.members = new Object2ObjectOpenCustomHashMap<>(members.size(), ByteArrays.HASH_STRATEGY);
    Iterator<byte[]> iterator = members.iterator();
    if (members.size() > 0) {
      sizeInBytes += INTERNAL_CUSTOM_HASHMAP_STORAGE_OVERHEAD;
    }

    while (iterator.hasNext()) {
      byte[] score = iterator.next();
      sizeInBytes += PER_MEMBER_OVERHEAD + score.length;
      byte[] member = iterator.next();
      sizeInBytes += PER_MEMBER_OVERHEAD + member.length;
      this.members.put(member, score);
    }
  }

  RedisSortedSet(List<byte[]> members, ZSetOptions options) {
    if (options.isXX()) {
      sizeInBytes += BASE_REDIS_SORTED_SET_OVERHEAD;
      this.members = new Object2ObjectOpenCustomHashMap<>(members.size(), ByteArrays.HASH_STRATEGY);
    } else {
      RedisSortedSet tempSet = new RedisSortedSet(members);
      this.members = tempSet.members;
      this.sizeInBytes = tempSet.sizeInBytes;
    }
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
    for (Map.Entry<byte[], byte[]> entry : members.entrySet()) {
      byte[] key = entry.getKey();
      byte[] value = entry.getValue();
      InternalDataSerializer.writeByteArray(key, out);
      InternalDataSerializer.writeByteArray(value, out);
    }
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    int size = InternalDataSerializer.readPrimitiveInt(in);
    members = new Object2ObjectOpenCustomHashMap<>(size, ByteArrays.HASH_STRATEGY);
    for (int i = 0; i < size; i++) {
      members.put(InternalDataSerializer.readByteArray(in),
          InternalDataSerializer.readByteArray(in));
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
    RedisSortedSet redisSortedSet = (RedisSortedSet) o;
    if (members.size() != redisSortedSet.members.size()) {
      return false;
    }

    for (Map.Entry<byte[], byte[]> entry : members.entrySet()) {
      if (!Arrays.equals(redisSortedSet.members.get(entry.getKey()), (entry.getValue()))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int getDSFID() {
    return REDIS_SORTED_SET_ID;
  }

  private synchronized AddOrChange membersAdd(byte[] memberToAdd, byte[] scoreToAdd, boolean CH) {
    byte[] oldScore = members.get(memberToAdd);
    boolean added = (members.put(memberToAdd, scoreToAdd) == null);
    if (added) {
      sizeInBytes += PER_MEMBER_OVERHEAD + memberToAdd.length;
      if (members.size() == 1) {
        sizeInBytes += INTERNAL_CUSTOM_HASHMAP_STORAGE_OVERHEAD;
      }
    }
    if (CH && !added) {
      return Arrays.equals(oldScore, scoreToAdd) ? AddOrChange.NOOP : AddOrChange.CHANGED;
    }
    return added ? AddOrChange.ADDED : AddOrChange.NOOP;
  }

  private synchronized void membersAddAll(AddsDeltaInfo addsDeltaInfo) {
    Iterator<byte[]> iterator = addsDeltaInfo.getAdds().iterator();
    while (iterator.hasNext()) {
      byte[] member = iterator.next();
      sizeInBytes += PER_MEMBER_OVERHEAD + member.length;
      byte[] score = iterator.next();
      sizeInBytes += PER_MEMBER_OVERHEAD + score.length;
      members.put(member, score);
    }
  }

  private synchronized void membersRemoveAll(RemsDeltaInfo remsDeltaInfo) {
    for (byte[] member : remsDeltaInfo.getRemoves()) {
      sizeInBytes -= (PER_MEMBER_OVERHEAD + member.length);
      members.remove(member);
    }
  }

  /**
   * @param region the region this instance is stored in
   * @param key the name of the set to add to
   * @param membersToAdd members to add to this set; NOTE this list may by modified by this call
   * @return the number of members actually added
   */
  long zadd(Region<RedisKey, RedisData> region, RedisKey key, List<byte[]> membersToAdd,
      ZSetOptions options) {
    int membersAdded = 0;
    if (options == null) {
      options = new ZSetOptions(ZSetOptions.Exists.NONE, ZSetOptions.Update.NONE);
    }
    AddsDeltaInfo deltaInfo = null;
    Iterator<byte[]> iterator = membersToAdd.iterator();
    while (iterator.hasNext()) {
      boolean delta = true;
      byte[] score = iterator.next();
      byte[] member = iterator.next();

      if (options.isNX()) {
        if (members.get(member) != null) {
          continue;
        }
      }
      if (options.isXX()) {
        if (members.get(member) == null) {
          continue;
        }
      }
      switch (membersAdd(member, score, false)) {
        case ADDED:
          membersAdded++;
          makeAddsDeltaInfo(deltaInfo, member, score);
          break;
        case CHANGED:
          // TODO: track changed
          makeAddsDeltaInfo(deltaInfo, member, score);
          break;
        default:
          delta = false;
          // do nothing
      }

      if (delta) {
        if (deltaInfo == null) {
          deltaInfo = new AddsDeltaInfo(new ArrayList<>());
        }
        deltaInfo.add(member);
        deltaInfo.add(score);
      }
    }
    if (deltaInfo != null) {
      storeChanges(region, key, deltaInfo);
    }
    return membersAdded;
  }

  byte[] zscore(byte[] member) {
    return members.get(member);
  }

  private AddsDeltaInfo makeAddsDeltaInfo(AddsDeltaInfo deltaInfo, byte[] member, byte[] score) {
    if (deltaInfo == null) {
      deltaInfo = new AddsDeltaInfo(new ArrayList<>());
    }
    deltaInfo.add(member);
    deltaInfo.add(score);
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
