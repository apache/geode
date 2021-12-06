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

package org.apache.geode.redis.internal.data.delta;

import static org.apache.geode.DataSerializer.readByteArray;
import static org.apache.geode.DataSerializer.readPrimitiveLong;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;

/**
 * This delta info is used by RedisString set ops to set the bytes
 * and the timestamp.
 */
public class SetByteArrayAndTimestamp implements DeltaInfo {
  private final byte[] byteArray;
  private final long timestamp;

  public SetByteArrayAndTimestamp(byte[] bytes, long timestamp) {
    byteArray = bytes;
    this.timestamp = timestamp;
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(DeltaType.SET_BYTE_ARRAY_AND_TIMESTAMP, out);
    DataSerializer.writeByteArray(byteArray, out);
    DataSerializer.writePrimitiveLong(timestamp, out);
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    byte[] byteArray = readByteArray(in);
    long timestamp = readPrimitiveLong(in);
    redisData.applySetByteArrayAndTimestampDelta(byteArray, timestamp);
  }
}
