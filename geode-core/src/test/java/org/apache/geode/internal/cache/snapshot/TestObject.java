/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.snapshot;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class TestObject implements PdxSerializable {

  public int id;
  public String owner;

  public TestObject() {
  }

  public TestObject(final int id, final String owner) {
    this.id = id;
    this.owner = owner;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TestObject that = (TestObject) o;

    if (id != that.id) {
      return false;
    }
    return owner != null ? owner.equals(that.owner) : that.owner == null;

  }

  @Override
  public int hashCode() {
    int result = id;
    result = 31 * result + (owner != null ? owner.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TestObject [id=" + id + ", owner=" + owner + "]";
  }

  @Override
  public void toData(final PdxWriter writer) {
    writer.markIdentityField("id");
    writer.writeInt("id", id);
    writer.writeString("owner", owner);
  }

  @Override
  public void fromData(final PdxReader reader) {
    id = reader.readInt("id");
    owner = reader.readString("owner");
  }
}