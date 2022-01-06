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
import static org.apache.geode.DataSerializer.readPrimitiveDouble;
import static org.apache.geode.internal.InternalDataSerializer.readArrayLength;
import static org.apache.geode.redis.internal.data.delta.DeltaType.REPLACE_BYTE_ARRAY_DOUBLE_PAIRS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.redis.internal.data.AbstractRedisData;
import org.apache.geode.redis.internal.data.RedisSortedSet;

public class ReplaceByteArrayDoublePairs implements DeltaInfo {
  private final RedisSortedSet.MemberMap members;

  public ReplaceByteArrayDoublePairs(RedisSortedSet.MemberMap members) {
    this.members = members;
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(REPLACE_BYTE_ARRAY_DOUBLE_PAIRS, out);
    InternalDataSerializer.writeArrayLength(members.size(), out);
    for (byte[] member : members.keySet()) {
      DataSerializer.writeByteArray(member, out);
      DataSerializer.writePrimitiveDouble(members.get(member).getScore(), out);
    }
  }

  public static void deserializeFrom(DataInput in, AbstractRedisData redisData) throws IOException {
    int size = readArrayLength(in);
    RedisSortedSet.MemberMap membersMap = new RedisSortedSet.MemberMap(size);
    RedisSortedSet.ScoreSet scoreSet = new RedisSortedSet.ScoreSet();
    while (size > 0) {
      byte[] member = readByteArray(in);
      double score = readPrimitiveDouble(in);
      RedisSortedSet.OrderedSetEntry entry = new RedisSortedSet.OrderedSetEntry(member, score);
      membersMap.put(member, entry);
      scoreSet.add(entry);
      size--;
    }
    redisData.applyReplaceByteArrayDoublePairDelta(membersMap, scoreSet);

  }
}
