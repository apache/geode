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

package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS;
import static org.apache.geode.redis.internal.RegionProvider.REDIS_SLOTS_PER_BUCKET;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.executor.cluster.CRC16;
import org.apache.geode.redis.internal.executor.cluster.RedisPartitionResolver;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisKey implements DataSerializableFixedID {

  private int crc16;
  private byte[] value;

  public RedisKey() {}

  public RedisKey(byte[] value) {
    this.value = value;
    int startHashtag = Integer.MAX_VALUE;
    int endHashtag = 0;

    for (int i = 0; i < value.length; i++) {
      if (value[i] == '{' && startHashtag == Integer.MAX_VALUE) {
        startHashtag = i;
      } else if (value[i] == '}') {
        endHashtag = i;
        break;
      }
    }

    if (endHashtag - startHashtag <= 1) {
      startHashtag = -1;
      endHashtag = value.length;
    }

    crc16 = CRC16.calculate(value, startHashtag + 1, endHashtag);
  }

  public int getBucketId() {
    // & (REDIS_SLOTS - 1) is equivalent to % REDIS_SLOTS but supposedly faster
    return (getCrc16() & (REDIS_SLOTS - 1)) / REDIS_SLOTS_PER_BUCKET;
  }

  /**
   * Hash code for byte[] wrapped by this object
   */
  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof RedisKey) {
      return Arrays.equals(value, ((RedisKey) other).value);
    }
    return false;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.REDIS_KEY;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    out.writeShort(crc16);
    DataSerializer.writeByteArray(value, out);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException {
    // Need to convert a signed short to unsigned
    crc16 = in.readShort() & 0xffff;
    value = DataSerializer.readByteArray(in);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public String toString() {
    return Coder.bytesToString(value);
  }

  public byte[] toBytes() {
    return this.value;
  }

  /**
   * Used by the {@link RedisPartitionResolver} to map slots to buckets
   */
  public int getCrc16() {
    return crc16;
  }
}
