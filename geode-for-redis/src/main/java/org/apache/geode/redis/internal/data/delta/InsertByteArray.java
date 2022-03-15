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

package org.apache.geode.redis.internal.data.delta;

import static org.apache.geode.redis.internal.data.delta.DeltaType.INSERT_BYTE_ARRAY;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;

public class InsertByteArray extends DeltaInfo {
  private final byte[] byteArray;
  private final int index;

  public InsertByteArray(byte newVersion, byte[] delta, int index) {
    super(newVersion);
    byteArray = delta;
    this.index = index;
  }

  @Override
  public DeltaType getType() {
    return INSERT_BYTE_ARRAY;
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(INSERT_BYTE_ARRAY, out);
    DataSerializer.writePrimitiveInt(index, out);
    DataSerializer.writeByteArray(byteArray, out);
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    int index = DataSerializer.readPrimitiveInt(in);
    byte[] bytearray = DataSerializer.readByteArray(in);
    redisData.applyInsertByteArrayDelta(bytearray, index);
  }
}
