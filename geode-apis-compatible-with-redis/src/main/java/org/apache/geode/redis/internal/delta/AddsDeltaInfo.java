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

import static org.apache.geode.redis.internal.delta.DeltaType.ADDS;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;

public class AddsDeltaInfo implements DeltaInfo {
  private final ArrayList<byte[]> deltas;

  public AddsDeltaInfo(int size) {
    this.deltas = new ArrayList<>(size);
  }

  public AddsDeltaInfo(List<byte[]> deltas) {
    this.deltas = new ArrayList<>(deltas);
  }

  public void add(byte[] delta) {
    deltas.add(delta);
  }

  public void serializeTo(DataOutput out) throws IOException {
    DataSerializer.writeEnum(ADDS, out);
    DataSerializer.writeArrayList(deltas, out);
  }

  public List<byte[]> getAdds() {
    return deltas;
  }
}
