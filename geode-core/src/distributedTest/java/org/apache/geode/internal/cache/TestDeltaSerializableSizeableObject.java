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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.Delta;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.internal.size.Sizeable;

public class TestDeltaSerializableSizeableObject
    implements Delta, DataSerializable, Sizeable {

  String value;

  public TestDeltaSerializableSizeableObject() {}

  public TestDeltaSerializableSizeableObject(String value) {
    this.value = value;
  }

  @Override
  public boolean hasDelta() {
    return false;
  }

  @Override
  public void toDelta(DataOutput out) throws IOException {}

  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {}

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeUTF(value);
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    value = in.readUTF();
  }

  @Override
  public int getSizeInBytes() {
    return value.length();
  }
}
