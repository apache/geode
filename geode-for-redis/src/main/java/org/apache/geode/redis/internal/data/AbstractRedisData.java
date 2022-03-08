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

import static org.apache.geode.DataSerializer.readEnum;
import static org.apache.geode.internal.cache.TXManagerImpl.NOTX;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_KEY_EXISTS;
import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAYS;
import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAYS_TAIL;
import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAY_DOUBLE_PAIRS;
import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAY_PAIRS;
import static org.apache.geode.redis.internal.data.delta.DeltaType.APPEND_BYTE_ARRAY;
import static org.apache.geode.redis.internal.data.delta.DeltaType.INSERT_BYTE_ARRAY;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REMOVE_BYTE_ARRAYS;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REMOVE_ELEMENTS_BY_INDEX;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REMOVE_ELEMENTS_BY_INDEX_REVERSE_ORDER;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAYS;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAY_AT_OFFSET;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAY_DOUBLE_PAIRS;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_AT_OFFSET;
import static org.apache.geode.redis.internal.data.delta.DeltaType.SET_BYTE_ARRAY;
import static org.apache.geode.redis.internal.data.delta.DeltaType.SET_BYTE_ARRAY_AND_TIMESTAMP;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.RADISH_DUMP_HEADER;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXId;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.data.delta.AddByteArrayDoublePairs;
import org.apache.geode.redis.internal.data.delta.AddByteArrayPairs;
import org.apache.geode.redis.internal.data.delta.AddByteArrays;
import org.apache.geode.redis.internal.data.delta.AddByteArraysTail;
import org.apache.geode.redis.internal.data.delta.AppendByteArray;
import org.apache.geode.redis.internal.data.delta.DeltaInfo;
import org.apache.geode.redis.internal.data.delta.DeltaType;
import org.apache.geode.redis.internal.data.delta.InsertByteArray;
import org.apache.geode.redis.internal.data.delta.RemoveByteArrays;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndex;
import org.apache.geode.redis.internal.data.delta.RemoveElementsByIndexReverseOrder;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrayAtOffset;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrayDoublePairs;
import org.apache.geode.redis.internal.data.delta.ReplaceByteArrays;
import org.apache.geode.redis.internal.data.delta.ReplaceByteAtOffset;
import org.apache.geode.redis.internal.data.delta.SetByteArray;
import org.apache.geode.redis.internal.data.delta.SetByteArrayAndTimestamp;
import org.apache.geode.redis.internal.data.delta.SetTimestamp;
import org.apache.geode.redis.internal.services.RegionProvider;

public abstract class AbstractRedisData implements RedisData {
  private static final BucketRegion.PrimaryMoveReadLockAcquired primaryMoveReadLockAcquired =
      new BucketRegion.PrimaryMoveReadLockAcquired();

  public static final long NO_EXPIRATION = -1L;

  private byte version = 0;

  /**
   * The timestamp at which this instance should expire.
   * NO_EXPIRATION means it never expires.
   * Otherwise it is a standard Unix timestamp
   * (i.e. the number of milliseconds since midnight, Jan 1, 1970)
   */
  private volatile long expirationTimestamp = NO_EXPIRATION;
  private static final ThreadLocal<DeltaInfo> deltaInfo = new ThreadLocal<>();

  public byte incrementAndGetVersion() {
    return ++version;
  }

  public void setVersion(byte version) {
    this.version = version;
  }

  @Override
  public void setExpirationTimestamp(Region<RedisKey, RedisData> region, RedisKey key, long value) {
    expirationTimestamp = value;
    storeChanges(region, key, new SetTimestamp(value));
  }

  public void setExpirationTimestampNoDelta(long value) {
    expirationTimestamp = value;
  }

  @Override
  public int pexpireat(RegionProvider regionProvider, RedisKey key, long timestamp) {
    long now = System.currentTimeMillis();
    if (now >= timestamp) {
      // already expired
      doExpiration(regionProvider, key);
    } else {
      setExpirationTimestamp(regionProvider.getLocalDataRegion(), key, timestamp);
    }
    return 1;
  }

  @Override
  public void doExpiration(RegionProvider regionProvider, RedisKey key) {
    long start = regionProvider.getRedisStats().startExpiration();
    regionProvider.getLocalDataRegion().remove(key);
    regionProvider.getRedisStats().endExpiration(start);
  }

  @Override
  public boolean rename(Region<RedisKey, RedisData> region, RedisKey oldKey, RedisKey newKey,
      boolean ifTargetNotExists) {

    if (ifTargetNotExists) {
      try {
        region.create(newKey, this, primaryMoveReadLockAcquired);
      } catch (EntryExistsException e) {
        throw new RedisKeyExistsException(ERROR_KEY_EXISTS);
      }
    } else {
      region.put(newKey, this, primaryMoveReadLockAcquired);
    }

    try {
      region.destroy(oldKey, primaryMoveReadLockAcquired);
    } catch (EntryNotFoundException ignore) {
    }
    return true;
  }

  @Override
  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  @Override
  public long pttl(Region<RedisKey, RedisData> region, RedisKey key) {
    long expireTimestamp = getExpirationTimestamp();
    if (expireTimestamp == NO_EXPIRATION) {
      return -1;
    }
    long now = System.currentTimeMillis();
    if (now >= expireTimestamp) {
      region.remove(key);
      return -2;
    }
    return expireTimestamp - now;
  }

  @Override
  public int persist(Region<RedisKey, RedisData> region, RedisKey key) {
    if (getExpirationTimestamp() == NO_EXPIRATION) {
      return 0;
    }
    setExpirationTimestamp(region, key, NO_EXPIRATION);
    return 1;
  }

  @Override
  public String type() {
    return getType().toString();
  }

  public void persistNoDelta() {
    setExpirationTimestampNoDelta(NO_EXPIRATION);
  }

  @Override
  public boolean hasExpired() {
    long expireTimestamp = getExpirationTimestamp();
    if (expireTimestamp == NO_EXPIRATION) {
      return false;
    }
    long now = System.currentTimeMillis();
    return now >= expireTimestamp;
  }

  @Override
  public boolean hasExpired(long now) {
    long expireTimestamp = getExpirationTimestamp();
    if (expireTimestamp == NO_EXPIRATION) {
      return false;
    }
    return now >= expireTimestamp;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    out.writeByte(version);
    out.writeLong(expirationTimestamp);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    version = in.readByte();
    expirationTimestamp = in.readLong();
  }

  private void setDelta(DeltaInfo newDeltaInfo) {
    deltaInfo.set(newDeltaInfo);
  }

  @Override
  public boolean hasDelta() {
    return deltaInfo.get() != null;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    deltaInfo.get().serializeTo(out);
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    DeltaType deltaType = readEnum(DeltaType.class, in);

    if (deltaType.isVersioned()) {
      byte deltaVersion = DataSerializer.readPrimitiveByte(in);
      if (deltaVersion == version) {
        return;
      }
      setVersion(deltaVersion);
    }

    switch (deltaType) {
      case SET_TIMESTAMP:
        SetTimestamp.deserializeFrom(in, this);
        break;
      case ADD_BYTE_ARRAYS:
        synchronized (this) {
          AddByteArrays.deserializeFrom(in, this);
        }
        break;
      case ADD_BYTE_ARRAYS_TAIL:
        synchronized (this) {
          AddByteArraysTail.deserializeFrom(in, this);
        }
        break;
      case REMOVE_BYTE_ARRAYS:
        synchronized (this) {
          RemoveByteArrays.deserializeFrom(in, this);
        }
        break;
      case APPEND_BYTE_ARRAY:
        AppendByteArray.deserializeFrom(in, this);
        break;
      case ADD_BYTE_ARRAY_PAIRS:
        synchronized (this) {
          AddByteArrayPairs.deserializeFrom(in, this);
        }
        break;
      case INSERT_BYTE_ARRAY:
        synchronized (this) {
          InsertByteArray.deserializeFrom(in, this);
        }
        break;
      case ADD_BYTE_ARRAY_DOUBLE_PAIRS:
        synchronized (this) {
          AddByteArrayDoublePairs.deserializeFrom(in, this);
        }
        break;
      case SET_BYTE_ARRAY:
        SetByteArray.deserializeFrom(in, this);
        break;
      case SET_BYTE_ARRAY_AND_TIMESTAMP:
        SetByteArrayAndTimestamp.deserializeFrom(in, this);
        break;
      case REPLACE_BYTE_ARRAYS:
        ReplaceByteArrays.deserializeFrom(in, this);
        break;
      case REPLACE_BYTE_ARRAY_AT_OFFSET:
        ReplaceByteArrayAtOffset.deserializeFrom(in, this);
        break;
      case REPLACE_BYTE_ARRAY_DOUBLE_PAIRS:
        ReplaceByteArrayDoublePairs.deserializeFrom(in, this);
        break;
      case REPLACE_BYTE_AT_OFFSET:
        ReplaceByteAtOffset.deserializeFrom(in, this);
        break;
      case REMOVE_ELEMENTS_BY_INDEX:
        RemoveElementsByIndex.deserializeFrom(in, this);
        break;
      case REMOVE_ELEMENTS_BY_INDEX_REVERSE_ORDER:
        RemoveElementsByIndexReverseOrder.deserializeFrom(in, this);
        break;
    }
  }

  public void applySetTimestampDelta(long timestamp) {
    expirationTimestamp = timestamp;
  }

  public void applyAddByteArrayDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected " + ADD_BYTE_ARRAYS);
  }

  public void applyAddByteArrayTailDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected " + ADD_BYTE_ARRAYS_TAIL);
  }

  public void applyInsertByteArrayDelta(byte[] bytes, int index) {
    throw new IllegalStateException("unexpected " + INSERT_BYTE_ARRAY);
  }

  public void applyRemoveByteArrayDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected " + REMOVE_BYTE_ARRAYS);
  }

  public void applyAddByteArrayDoublePairDelta(byte[] bytes, double score) {
    throw new IllegalStateException("unexpected " + ADD_BYTE_ARRAY_DOUBLE_PAIRS);
  }

  public void applyAddByteArrayPairDelta(byte[] keyBytes, byte[] valueBytes) {
    throw new IllegalStateException("unexpected " + ADD_BYTE_ARRAY_PAIRS);
  }

  public void applySetByteArrayDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected " + SET_BYTE_ARRAY);
  }

  public void applySetByteArrayAndTimestampDelta(byte[] bytes, long timestamp) {
    throw new IllegalStateException("unexpected " + SET_BYTE_ARRAY_AND_TIMESTAMP);
  }

  public void applyAppendByteArrayDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected " + APPEND_BYTE_ARRAY);
  }

  public void applyReplaceByteArraysDelta(RedisSet.MemberSet members) {
    throw new IllegalStateException("unexpected " + REPLACE_BYTE_ARRAYS);
  }

  public void applyReplaceByteArrayAtOffsetDelta(int offset, byte[] bytes) {
    throw new IllegalStateException("unexpected " + REPLACE_BYTE_ARRAY_AT_OFFSET);
  }

  public void applyReplaceByteArrayDoublePairDelta(RedisSortedSet.MemberMap members,
      RedisSortedSet.ScoreSet scoreSet) {
    throw new IllegalStateException("unexpected " + REPLACE_BYTE_ARRAY_DOUBLE_PAIRS);
  }

  public void applyReplaceByteAtOffsetDelta(int offset, byte bits) {
    throw new IllegalStateException("unexpected " + REPLACE_BYTE_AT_OFFSET);
  }

  public void applyRemoveElementsByIndex(List<Integer> indexes) {
    throw new IllegalStateException("unexpected " + REMOVE_ELEMENTS_BY_INDEX);
  }

  public void applyRemoveElementsByIndexReverseOrder(List<Integer> indexes) {
    throw new IllegalStateException("unexpected " + REMOVE_ELEMENTS_BY_INDEX_REVERSE_ORDER);
  }

  @Override
  public byte[] dump() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(RADISH_DUMP_HEADER);
    DataOutputStream outputStream = new DataOutputStream(baos);
    outputStream.writeShort(KnownVersion.CURRENT.ordinal());

    DataSerializer.writeObject(this, outputStream);
    return baos.toByteArray();
  }

  @Override
  public RedisData restore(byte[] data, boolean replaceExisting) throws Exception {
    if (!replaceExisting) {
      throw new RedisKeyExistsException(ERROR_KEY_EXISTS);
    }

    return restore(data);
  }

  protected void storeChanges(Region<RedisKey, RedisData> region, RedisKey key,
      DeltaInfo deltaInfo) {
    if (deltaInfo != null) {
      if (removeFromRegion()) {
        region.remove(key);
      } else {
        if (!txActive(region)) {
          setDelta(deltaInfo);
        }
        try {
          region.put(key, this);
        } finally {
          setDelta(null);
        }
      }
    }
  }

  private boolean txActive(Region<RedisKey, RedisData> region) {
    TXId txId;
    if (region instanceof LocalDataSet) {
      txId = ((LocalDataSet) region).getProxy().getTXId();
    } else {
      txId = ((PartitionedRegion) region).getTXId();
    }
    return txId != null && txId.getUniqId() != NOTX;
  }

  protected abstract boolean removeFromRegion();

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AbstractRedisData)) {
      return false;
    }
    AbstractRedisData that = (AbstractRedisData) o;
    return version == that.version &&
        getExpirationTimestamp() == that.getExpirationTimestamp();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getExpirationTimestamp());
  }

  @Override
  public String toString() {
    return "version=" + version + ", expirationTimestamp=" + expirationTimestamp;
  }

}
