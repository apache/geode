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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;

public class TestDelta implements Delta, DataSerializable, Cloneable {

  public boolean hasDelta;
  public String info;
  public int serializations;
  public int deserializations;
  public int deltas;
  public int clones;
  public boolean forceRecalculateSize;

  public TestDelta() {}

  public TestDelta(boolean hasDelta, String info) {
    this.hasDelta = hasDelta;
    this.info = info;
    this.forceRecalculateSize = false;
  }

  public TestDelta(boolean hasDelta, String info, boolean forceRecalculateSize) {
    this.hasDelta = hasDelta;
    this.info = info;
    this.forceRecalculateSize = forceRecalculateSize;
  }

  @Override
  public String toString() {
    return "TestDelta{" +
        "info='" + info + "'" +
        "forceRecalculateSize='" + forceRecalculateSize + "'" +
        '}';
  }

  public synchronized void checkFields(final int serializations, final int deserializations,
      final int deltas, final int clones) {
    assertEquals(serializations, this.serializations);
    assertEquals(deserializations, this.deserializations);
    assertEquals(deltas, this.deltas);
    assertEquals(clones, this.clones);
  }

  @Override
  public synchronized void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    this.hasDelta = true;
    info = DataSerializer.readString(in);
    forceRecalculateSize = DataSerializer.readBoolean(in);
    deltas++;
  }

  @Override
  public boolean hasDelta() {
    return hasDelta;
  }

  @Override
  public boolean getForceRecalculateSize() {
    return forceRecalculateSize;
  }

  @Override
  public synchronized void toDelta(DataOutput out) throws IOException {
    DataSerializer.writeString(info, out);
    DataSerializer.writeBoolean(forceRecalculateSize, out);
  }

  @Override
  public synchronized void fromData(DataInput in) throws IOException, ClassNotFoundException {
    info = DataSerializer.readString(in);
    serializations = in.readInt();
    deserializations = in.readInt();
    deltas = in.readInt();
    clones = in.readInt();
    deserializations++;
  }

  @Override
  public synchronized void toData(DataOutput out) throws IOException {
    serializations++;
    DataSerializer.writeString(info, out);
    out.writeInt(serializations);
    out.writeInt(deserializations);
    out.writeInt(deltas);
    out.writeInt(clones);
  }

  @Override
  public synchronized Object clone() throws CloneNotSupportedException {
    clones++;
    return super.clone();
  }
}
