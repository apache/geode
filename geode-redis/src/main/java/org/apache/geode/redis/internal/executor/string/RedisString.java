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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisDataType;

public class RedisString implements DataSerializable, RedisData {
  private ByteArrayWrapper value;

  private transient ByteArrayWrapper delta;

  public RedisString(ByteArrayWrapper value) {
    this.value = value;
  }

  // for serialization
  public RedisString() {}

  public int append(ByteArrayWrapper appendValue,
      Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key) {
    value.append(appendValue.toBytes());
    delta = appendValue;
    region.put(key, this);
    return value.length();
  }

  public ByteArrayWrapper get() {
    return new ByteArrayWrapper(value.toBytes());
  }

  public void set(ByteArrayWrapper value) {
    this.value = value;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(value.toBytes(), out);
  }

  @Override
  public void fromData(DataInput in) throws IOException {
    value = new ByteArrayWrapper(DataSerializer.readByteArray(in));
  }

  @Override
  public RedisDataType getType() {
    return RedisDataType.REDIS_STRING;
  }

  @Override
  public boolean hasDelta() {
    return delta != null;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {
    try {
      DataSerializer.writeByteArray(delta.toBytes(), out);
    } finally {
      delta = null;
    }
  }

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    try {
      byte[] deltaBytes = DataSerializer.readByteArray(in);
      if (value == null) {
        value = new ByteArrayWrapper(deltaBytes);
      } else {
        value.append(deltaBytes);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
