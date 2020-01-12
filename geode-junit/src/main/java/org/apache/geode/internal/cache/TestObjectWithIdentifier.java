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
import java.util.Arrays;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.size.Sizeable;

/**
 * Object used for the put() operation as key and object. The objectIdentifier is used to provide a
 * predetermined hashcode for the object.
 */
public class TestObjectWithIdentifier implements DataSerializable, Sizeable {

  private String name;
  private byte bytes[] = new byte[1024 * 4];
  private int identifier;

  public TestObjectWithIdentifier() {
    // nothing
  }

  public TestObjectWithIdentifier(String objectName, int identifier) {
    name = objectName;
    Arrays.fill(bytes, (byte) 'A');
    this.identifier = identifier;
  }

  @Override
  public int hashCode() {
    return identifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (TestObjectWithIdentifier.class.isInstance(obj)) {
      TestObjectWithIdentifier other = (TestObjectWithIdentifier) obj;
      return name.equals(other.name) && Arrays.equals(bytes, other.bytes);
    } else {
      return false;
    }
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(bytes, out);
    DataSerializer.writeString(name, out);
    out.writeInt(identifier);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    bytes = DataSerializer.readByteArray(in);
    name = DataSerializer.readString(in);
    identifier = in.readInt();
  }

  @Override
  public int getSizeInBytes() {
    return ObjectSizer.DEFAULT.sizeof(bytes) + ObjectSizer.DEFAULT.sizeof(name)
        + ObjectSizer.DEFAULT.sizeof(identifier) + Sizeable.PER_OBJECT_OVERHEAD * 3;
  }
}
