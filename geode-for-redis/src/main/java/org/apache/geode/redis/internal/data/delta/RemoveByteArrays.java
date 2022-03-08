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

import static org.apache.geode.DataSerializer.readByteArray;
import static org.apache.geode.internal.InternalDataSerializer.readArrayLength;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REMOVE_BYTE_ARRAYS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;

public class RemoveByteArrays extends DeltaInfo {
  private final List<byte[]> byteArrays;

  public RemoveByteArrays() {
    this(new ArrayList<>());
  }

  public RemoveByteArrays(List<byte[]> deltas) {
    super((short) 0);
    byteArrays = deltas;
  }

  public void add(byte[] delta) {
    byteArrays.add(delta);
  }

  public void serializeTo(DataOutput out) throws IOException {
    super.serializeTo(out);
    DataSerializer.writeEnum(REMOVE_BYTE_ARRAYS, out);
    InternalDataSerializer.writeArrayLength(byteArrays.size(), out);
    for (byte[] bytes : byteArrays) {
      DataSerializer.writeByteArray(bytes, out);
    }
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    int size = readArrayLength(in);
    while (size > 0) {
      redisData.applyRemoveByteArrayDelta(readByteArray(in));
      size--;
    }
  }

  @VisibleForTesting
  public List<byte[]> getRemoves() {
    return byteArrays;
  }
}
