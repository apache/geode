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
import static org.apache.geode.redis.internal.data.delta.DeltaType.RETAIN_ELEMENTS_BY_INDEX_RANGE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;

public class RetainElementsByIndexRange extends DeltaInfo {
  private final int start;
  private final int end;

  public RetainElementsByIndexRange(byte version, int start, int end) {
    super(version);
    this.start = start;
    this.end = end;
  }

  @Override
  public DeltaType getType() {
    return RETAIN_ELEMENTS_BY_INDEX_RANGE;
  }

  public void serializeTo(DataOutput out) throws IOException {
    super.serializeTo(out);
    DataSerializer.writePrimitiveInt(start, out);
    DataSerializer.writePrimitiveInt(end, out);
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    redisData.applyRetainElementsByIndexRange(readPrimitiveInt(in), readPrimitiveInt(in));
  }
}
