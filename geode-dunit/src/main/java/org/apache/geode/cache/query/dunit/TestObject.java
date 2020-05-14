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
    this._ticker = ticker;
    this._price = id;
    this.important = id;
    this.selection = id;
    this.select = id;
  }

  public int getId() {
    return this.id;
  }

  public String getTicker() {
    return this._ticker;
  }

  public int getPrice() {
    return this._price;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    DataSerializer.writeString(this._ticker, out);
    out.writeInt(this._price);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this._ticker = DataSerializer.readString(in);
    this._price = in.readInt();
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("TestObject [").append("id=").append(this.id).append("; ticker=")
        .append(this._ticker).append("; price=").append(this._price).append("]");
    return buffer.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TestObject)) {
      return false;
    }
    TestObject other = (TestObject) o;
    if ((id == other.id) && (_ticker.equals(other._ticker))) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return this.id;
  }

}
