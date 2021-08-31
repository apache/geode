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

package org.apache.geode.redis.internal.delta;

import static org.apache.geode.redis.internal.delta.DeltaType.ZADDS;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;

public class ZAddsDeltaInfo implements DeltaInfo {
  private final ArrayList<byte[]> deltas = new ArrayList<>();
  private final ArrayList<Double> scores = new ArrayList<>();

  public ZAddsDeltaInfo() {}

  public ZAddsDeltaInfo(List<byte[]> deltas, List<Double> scores) {
    this.deltas.addAll(deltas);
    this.scores.addAll(scores);
  }

  public ZAddsDeltaInfo(byte[] delta, Double score) {
    this.deltas.add(delta);
    this.scores.add(score);
  }

  public void add(byte[] delta, double score) {
    deltas.add(delta);
    this.scores.add(score);
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(ZADDS, out);
    DataSerializer.writePrimitiveInt(deltas.size(), out);
    for (int i = 0; i < deltas.size(); i++) {
      DataSerializer.writeByteArray(deltas.get(i), out);
      DataSerializer.writeDouble(scores.get(i), out);
    }
  }

  public List<byte[]> getZAddMembers() {
    return deltas;
  }

  public List<Double> getZAddScores() {
    return scores;
  }
}
