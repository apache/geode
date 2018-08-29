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

  public TestDelta() {}

  public TestDelta(boolean hasDelta, String info) {
    this.hasDelta = hasDelta;
    this.info = info;
  }

  public synchronized void checkFields(final int serializations, final int deserializations,
      final int deltas, final int clones) {
    assertEquals(serializations, this.serializations);
    assertEquals(deserializations, this.deserializations);
    assertEquals(deltas, this.deltas);
    assertEquals(clones, this.clones);
  }

  public synchronized void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    // new Exception("DAN - From Delta Called").printStackTrace();
    this.hasDelta = true;
    info = DataSerializer.readString(in);
    deltas++;
  }

  public boolean hasDelta() {
    return hasDelta;
  }

  public synchronized void toDelta(DataOutput out) throws IOException {
    // new Exception("DAN - To Delta Called").printStackTrace();
    DataSerializer.writeString(info, out);
  }

  public synchronized void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // new Exception("DAN - From Data Called").printStackTrace();
    info = DataSerializer.readString(in);
    serializations = in.readInt();
    deserializations = in.readInt();
    deltas = in.readInt();
    clones = in.readInt();
    deserializations++;
  }

  public synchronized void toData(DataOutput out) throws IOException {
    // new Exception("DAN - To Data Called").printStackTrace();
    serializations++;
    DataSerializer.writeString(info, out);
    out.writeInt(serializations);
    out.writeInt(deserializations);
    out.writeInt(deltas);
    out.writeInt(clones);
  }

  @Override
  public synchronized Object clone() throws CloneNotSupportedException {
    // new Exception("DAN - Clone Called").printStackTrace();
    clones++;
    return super.clone();
  }
}
