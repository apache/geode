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

package org.apache.geode.cache.query.dunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

public class TestObject implements DataSerializable {
  protected String _ticker;
  protected int _price;
  public int id;
  public int important;
  public int selection;
  public int select;

  public TestObject() {}

  public TestObject(int id, String ticker) {
    this.id = id;
    _ticker = ticker;
    _price = id;
    important = id;
    selection = id;
    select = id;
  }

  public int getId() {
    return id;
  }

  public String getTicker() {
    return _ticker;
  }

  public int getPrice() {
    return _price;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(id);
    DataSerializer.writeString(_ticker, out);
    out.writeInt(_price);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    id = in.readInt();
    _ticker = DataSerializer.readString(in);
    _price = in.readInt();
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("TestObject [").append("id=").append(id).append("; ticker=")
        .append(_ticker).append("; price=").append(_price).append("]");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TestObject)) {
      return false;
    }
    TestObject other = (TestObject) o;
    return (id == other.id) && (_ticker.equals(other._ticker));
  }

  @Override
  public int hashCode() {
    return id;
  }

}
