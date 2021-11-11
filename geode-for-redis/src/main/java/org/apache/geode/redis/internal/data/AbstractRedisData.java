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

import static org.apache.geode.DataSerializer.readByteArray;
import static org.apache.geode.DataSerializer.readEnum;
import static org.apache.geode.DataSerializer.readPrimitiveByte;
import static org.apache.geode.DataSerializer.readPrimitiveDouble;
import static org.apache.geode.DataSerializer.readPrimitiveInt;
import static org.apache.geode.DataSerializer.readPrimitiveLong;
import static org.apache.geode.internal.InternalDataSerializer.readArrayLength;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_KEY_EXISTS;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.RADISH_DUMP_HEADER;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.data.delta.DeltaInfo;
import org.apache.geode.redis.internal.data.delta.DeltaType;
import org.apache.geode.redis.internal.data.delta.TimestampDeltaInfo;
import org.apache.geode.redis.internal.services.RegionProvider;

public abstract class AbstractRedisData implements RedisData {
  private static final BucketRegion.PrimaryMoveReadLockAcquired primaryMoveReadLockAcquired =
      new BucketRegion.PrimaryMoveReadLockAcquired();

  private static final Logger logger = LogService.getLogger();
  public static final long NO_EXPIRATION = -1L;

  /**
   * The timestamp at which this instance should expire.
   * NO_EXPIRATION means it never expires.
   * Otherwise it is a standard Unix timestamp
   * (i.e. the number of milliseconds since midnight, Jan 1, 1970)
   */
  private volatile long expirationTimestamp = NO_EXPIRATION;
  private static final ThreadLocal<DeltaInfo> deltaInfo = new ThreadLocal<>();

  @Override
  public void setExpirationTimestamp(Region<RedisKey, RedisData> region, RedisKey key, long value) {
    expirationTimestamp = value;
    storeChanges(region, key, new TimestampDeltaInfo(value));
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
    out.writeLong(expirationTimestamp);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
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
    switch (deltaType) {
      case TIMESTAMP:
        expirationTimestamp = readPrimitiveLong(in);
        break;
      case ADDS:
        fromAddsDelta(in);
        break;
      case REMOVES:
        fromRemovesDelta(in);
        break;
      case APPEND:
        fromAppendDelta(in);
        break;
      case HADDS:
        fromHaddsDelta(in);
        break;
      case ZADDS:
        fromZaddsDelta(in);
        break;
      case REPLACE_BYTES:
        applyReplaceBytesDelta(readByteArray(in));
        break;
      case REPLACE_BYTES_AND_TIMESTAMP:
        fromReplaceBytesAndTimestampDelta(in);
        break;
      case SET_RANGE:
        fromSetRangeDelta(in);
        break;
      case SET_BIT:
        applySetBitDelta(readArrayLength(in), readPrimitiveByte(in));
        break;
    }
  }

  private void fromSetRangeDelta(DataInput in) throws IOException {
    int offset = readArrayLength(in);
    byte[] bytes = readByteArray(in);
    applySetRangeDelta(offset, bytes);
  }

  private void fromReplaceBytesAndTimestampDelta(DataInput in) throws IOException {
    byte[] bytes = readByteArray(in);
    long timestamp = readPrimitiveLong(in);
    applyReplaceBytesAndTimestampDelta(bytes, timestamp);
  }

  private synchronized void fromZaddsDelta(DataInput in) throws IOException {
    int size = readArrayLength(in);
    while (size > 0) {
      byte[] member = readByteArray(in);
      double score = readPrimitiveDouble(in);
      applyZaddDelta(member, score);
      size--;
    }
  }

  private synchronized void fromHaddsDelta(DataInput in) throws IOException {
    int size = readArrayLength(in);
    while (size > 0) {
      byte[] key = readByteArray(in);
      byte[] value = readByteArray(in);
      applyHaddDelta(key, value);
      size -= 2;
    }
  }

  private void fromAppendDelta(DataInput in) throws IOException {
    int sequence = readPrimitiveInt(in);
    byte[] byteArray = readByteArray(in);
    applyAppendDelta(sequence, byteArray);
  }

  private synchronized void fromRemovesDelta(DataInput in) throws IOException {
    int size = readArrayLength(in);
    while (size > 0) {
      applyRemoveDelta(readByteArray(in));
      size--;
    }
  }

  private synchronized void fromAddsDelta(DataInput in) throws IOException {
    int size = readArrayLength(in);
    while (size > 0) {
      applyAddDelta(readByteArray(in));
      size--;
    }
  }

  protected void applyAddDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected ADDS DeltaInfo");
  }

  protected void applyRemoveDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected REMOVES DeltaInfo");
  }

  protected void applyZaddDelta(byte[] bytes, double score) {
    throw new IllegalStateException("unexpected ZADDS DeltaInfo");
  }

  protected void applyHaddDelta(byte[] keyBytes, byte[] valueBytes) {
    throw new IllegalStateException("unexpected HADDS DeltaInfo");
  }

  protected void applyReplaceBytesDelta(byte[] bytes) {
    throw new IllegalStateException("unexpected REPLACE_BYTES DeltaInfo");
  }

  protected void applyReplaceBytesAndTimestampDelta(byte[] bytes, long timestamp) {
    throw new IllegalStateException("unexpected REPLACE_BYTES_AND_TIMESTAMP DeltaInfo");
  }

  protected void applyAppendDelta(int sequence, byte[] bytes) {
    throw new IllegalStateException("unexpected APPEND DeltaInfo");
  }

  protected void applySetRangeDelta(int offset, byte[] bytes) {
    throw new IllegalStateException("unexpected SET_RANGE DeltaInfo");
  }

  protected void applySetBitDelta(int offset, byte bits) {
    throw new IllegalStateException("unexpected SET_BIT DeltaInfo");
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
        setDelta(deltaInfo);
        try {
          region.put(key, this);
        } finally {
          setDelta(null);
        }
      }
    }
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
    return getExpirationTimestamp() == that.getExpirationTimestamp();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getExpirationTimestamp());
  }

  @Override
  public String toString() {
    return "expirationTimestamp=" + expirationTimestamp;
  }

}
