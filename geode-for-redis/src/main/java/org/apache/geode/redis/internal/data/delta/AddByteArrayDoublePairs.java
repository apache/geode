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
import static org.apache.geode.DataSerializer.readPrimitiveDouble;
import static org.apache.geode.internal.InternalDataSerializer.readArrayLength;
import static org.apache.geode.redis.internal.data.delta.DeltaType.ADD_BYTE_ARRAY_DOUBLE_PAIRS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;

public class AddByteArrayDoublePairs implements DeltaInfo {
  private final List<byte[]> byteArrays;
  private final double[] doubles;

  public AddByteArrayDoublePairs(int size) {
    byteArrays = new ArrayList<>(size);
    doubles = new double[size];
  }

  public void add(byte[] byteArray, double doubleValue) {
    doubles[byteArrays.size()] = doubleValue;
    byteArrays.add(byteArray);
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(ADD_BYTE_ARRAY_DOUBLE_PAIRS, out);
    InternalDataSerializer.writeArrayLength(byteArrays.size(), out);
    for (int i = 0; i < byteArrays.size(); i++) {
      DataSerializer.writeByteArray(byteArrays.get(i), out);
      DataSerializer.writePrimitiveDouble(doubles[i], out);
    }
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    int size = readArrayLength(in);
    while (size > 0) {
      byte[] byteArray = readByteArray(in);
      double doubleValue = readPrimitiveDouble(in);
      redisData.applyAddByteArrayDoublePairDelta(byteArray, doubleValue);
      size--;
    }
  }
}
