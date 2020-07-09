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

import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;

/**
 * Implements behaviour for when no instance of RedisData exists.
 */
public class NullRedisData implements RedisData {
  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public RedisDataType getType() {
    return null;
  }

  @Override
  public void setExpirationTimestamp(Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key, long value) {

  }

  @Override
  public long getExpirationTimestamp() {
    return 0;
  }

  @Override
  public int persist(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key) {
    return 0;
  }

  @Override
  public boolean hasExpired() {
    return false;
  }

  @Override
  public boolean hasExpired(long now) {
    return false;
  }

  @Override
  public long pttl(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key) {
    return -2;
  }

  @Override
  public int pexpireat(CommandHelper helper, ByteArrayWrapper key, long timestamp) {
    return 0;
  }

  @Override
  public void doExpiration(CommandHelper helper, ByteArrayWrapper key) {
    // nothing needed
  }

  @Override
  public String type() {
    return "none";
  }

  @Override
  public boolean rename(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper oldKey,
      ByteArrayWrapper newKey) {
    return false;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasDelta() {
    return false;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    throw new UnsupportedOperationException();
  }
}
