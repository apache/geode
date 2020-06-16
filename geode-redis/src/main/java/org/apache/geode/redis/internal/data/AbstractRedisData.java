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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.delta.AddsDeltaInfo;
import org.apache.geode.redis.internal.delta.AppendDeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaInfo;
import org.apache.geode.redis.internal.delta.DeltaType;
import org.apache.geode.redis.internal.delta.RemsDeltaInfo;
import org.apache.geode.redis.internal.delta.TimestampDeltaInfo;

public abstract class AbstractRedisData implements RedisData {
  @Override
  public String toString() {
    return "expirationTimestamp=" + expirationTimestamp;
  }

  private static final long NO_EXPIRATION = -1L;
  /**
   * The timestamp at which this instance should expire.
   * NO_EXPIRATION means it never expires.
   * Otherwise it is a standard Unix timestamp
   * (i.e. the number of milliseconds since midnight, Jan 1, 1970)
   */
  private volatile long expirationTimestamp = NO_EXPIRATION;
  private transient DeltaInfo deltaInfo;

  @Override
  public void setExpirationTimestamp(Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key, long value) {
    expirationTimestamp = value;
    storeChanges(region, key, new TimestampDeltaInfo(value));
  }

  public void setExpirationTimestampNoDelta(long value) {
    expirationTimestamp = value;
  }

  @Override
  public long getExpirationTimestamp() {
    return expirationTimestamp;
  }

  @Override
  public long pttl(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key) {
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
  public int persist(Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key) {
    if (getExpirationTimestamp() == NO_EXPIRATION) {
      return 0;
    }
    setExpirationTimestamp(region, key, NO_EXPIRATION);
    return 1;
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
    if (now < expireTimestamp) {
      return false;
    }
    return true;
  }

  @Override
  public boolean hasExpired(long now) {
    long expireTimestamp = getExpirationTimestamp();
    if (expireTimestamp == NO_EXPIRATION) {
      return false;
    }
    if (now < expireTimestamp) {
      return false;
    }
    return true;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeLong(expirationTimestamp, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    expirationTimestamp = (DataSerializer.readLong(in));
  }

  private void setDelta(DeltaInfo deltaInfo) {
    this.deltaInfo = deltaInfo;
  }

  @Override
  public boolean hasDelta() {
    return deltaInfo != null;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    try {
      deltaInfo.serializeTo(out);
    } finally {
      deltaInfo = null;
    }
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    DeltaType deltaType = DataSerializer.readEnum(DeltaType.class, in);
    switch (deltaType) {
      case TIMESTAMP:
        expirationTimestamp = DataSerializer.readLong(in);
        break;
      case ADDS:
        applyDelta(new AddsDeltaInfo(readArrayList(in)));
        break;
      case REMS:
        applyDelta(new RemsDeltaInfo(readArrayList(in)));
        break;
      case APPEND:
        applyDelta(new AppendDeltaInfo(DataSerializer.readByteArray(in)));
        break;
    }
  }

  private ArrayList<ByteArrayWrapper> readArrayList(DataInput in) throws IOException {
    try {
      return DataSerializer.readArrayList(in);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  protected void storeChanges(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      DeltaInfo deltaInfo) {
    if (deltaInfo != null) {
      if (removeFromRegion()) {
        region.remove(key);
      } else {
        setDelta(deltaInfo);
        region.put(key, this);
      }
    }
  }

  protected abstract void applyDelta(DeltaInfo deltaInfo);

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
}
