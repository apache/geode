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

import static org.apache.geode.DataSerializer.readPrimitiveInt;
import static org.apache.geode.internal.InternalDataSerializer.readArrayLength;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REMOVE_ELEMENTS_BY_INDEX_REVERSE_ORDER;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;

public class RemoveElementsByIndexReverseOrder implements DeltaInfo {
  private final List<Integer> indexes;

  public void add(int index) {
    indexes.add(index);
  }

  public RemoveElementsByIndexReverseOrder() {
    this.indexes = new ArrayList<>();
  }

  public RemoveElementsByIndexReverseOrder(List<Integer> indexes) {
    this.indexes = indexes;
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(REMOVE_ELEMENTS_BY_INDEX_REVERSE_ORDER, out);
    InternalDataSerializer.writeArrayLength(indexes.size(), out);
    for (int index : indexes) {
      DataSerializer.writePrimitiveInt(index, out);
    }
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    int size = readArrayLength(in);
    List<Integer> indexes = new ArrayList<>();
    while (size > 0) {
      indexes.add(readPrimitiveInt(in));
      size--;
    }
    redisData.applyRemoveElementsByIndexReverseOrder(indexes);
  }
}
