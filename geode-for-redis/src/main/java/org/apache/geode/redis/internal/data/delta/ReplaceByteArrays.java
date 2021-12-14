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

import static org.apache.geode.internal.InternalDataSerializer.readSet;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAYS;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;

public class ReplaceByteArrays implements DeltaInfo {
  private final Set<byte[]> byteArrays;

  public ReplaceByteArrays(Set<byte[]> deltas) {
    this.byteArrays = deltas;
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(REPLACE_BYTE_ARRAYS, out);
    InternalDataSerializer.writeSet(byteArrays, out);
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    try {
      redisData.applyReplaceByteArraysDelta(uncheckedCast(readSet(in)));
    } catch (ClassNotFoundException ignore) {
      // This should be impossible since we should always be able to find byte array class
    }
  }
}
