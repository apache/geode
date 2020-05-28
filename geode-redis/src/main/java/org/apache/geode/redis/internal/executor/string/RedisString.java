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
 */

package org.apache.geode.redis.internal.executor.string;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.AbstractRedisData;
import org.apache.geode.redis.internal.AppendDeltaInfo;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.DeltaInfo;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisDataType;

public class RedisString extends AbstractRedisData {
  private ByteArrayWrapper value;

  public RedisString(ByteArrayWrapper value) {
    this.value = value;
  }

  // for serialization
  public RedisString() {}

  public int append(ByteArrayWrapper appendValue,
      Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key) {
    value.append(appendValue.toBytes());
    storeChanges(region, key, new AppendDeltaInfo(appendValue.toBytes()));
    return value.length();
  }

  public ByteArrayWrapper get() {
    return new ByteArrayWrapper(value.toBytes());
  }

  public void set(ByteArrayWrapper value) {
    this.value = value;
  }

  public long incr(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key)
      throws NumberFormatException, ArithmeticException {
    long longValue = Long.parseLong(value.toString());
    if (longValue == Long.MAX_VALUE) {
      throw new ArithmeticException("overflow");
    }
    longValue++;
    String stringValue = Long.toString(longValue);
    value.setBytes(Coder.stringToBytes(stringValue));
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return longValue;
  }

  public long decr(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key)
      throws NumberFormatException, ArithmeticException {
    long longValue = Long.parseLong(value.toString());
    if (longValue == Long.MIN_VALUE) {
      throw new ArithmeticException("underflow");
    }
    longValue--;
    String stringValue = Long.toString(longValue);
    value.setBytes(Coder.stringToBytes(stringValue));
    // numeric strings are short so no need to use delta
    region.put(key, this);
    return longValue;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeByteArray(value.toBytes(), out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    value = new ByteArrayWrapper(DataSerializer.readByteArray(in));
  }

  @Override
  protected void applyDelta(DeltaInfo deltaInfo) {
    AppendDeltaInfo appendDeltaInfo = (AppendDeltaInfo) deltaInfo;
    byte[] appendBytes = appendDeltaInfo.getBytes();
    if (value == null) {
      value = new ByteArrayWrapper(appendBytes);
    } else {
      value.append(appendBytes);
    }
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_STRING;
  }

  @Override
  protected boolean removeFromRegion() {
    return false;
  }
}
