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
package org.apache.geode.security.query.data;

import java.io.Serializable;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

public class PdxQueryTestObject
    implements PdxSerializable, Serializable /* just to pass around in test code */ {
  public int id = -1;
  private String name;
  private int age = 1;

  private boolean shouldThrowException = true;


  public PdxQueryTestObject() {

  }

  public PdxQueryTestObject(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "Test_Object";
  }

  public int getAge() {
    return age;
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeInt("id", id);
    writer.writeString("getName", name);
  }

  @Override
  public void fromData(PdxReader reader) {
    id = reader.readInt("id");
    name = reader.readString("getName");
  }

  public boolean equals(Object o) {
    if (o instanceof PdxQueryTestObject) {
      PdxQueryTestObject other = (PdxQueryTestObject) o;
      return other.id == this.id && other.name.equals(this.name);
    }
    return false;
  }
}
