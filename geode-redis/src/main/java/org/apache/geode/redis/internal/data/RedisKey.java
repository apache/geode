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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.redis.internal.executor.cluster.CRC16;
import org.apache.geode.redis.internal.executor.cluster.RedisPartitionResolver;

public class RedisKey extends ByteArrayWrapper implements DataSerializableFixedID {

  private int crc16;

  public RedisKey() {}

  public RedisKey(byte[] value) {
    super(value);

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

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.REDIS_KEY;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    out.writeShort(crc16);
    super.toData(out, context);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    // Need to convert a signed short to unsigned
    crc16 = in.readShort() & 0xffff;
    super.fromData(in, context);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  /**
   * Used by the {@link RedisPartitionResolver} to map slots to buckets
   */
  public int getCrc16() {
    return crc16;
  }
}
