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
package org.apache.geode.connectors.jdbc;

import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;

@SuppressWarnings("unused")
public class Employee implements PdxSerializable {
  private String id;
  private String name;
  private int age;

  public Employee() {
    // nothing
  }

  Employee(String id, String name, int age) {
    this.id = id;
    this.name = name;
    this.age = age;
  }

  String getId() {
    return id;
  }

  String getName() {
    return name;
  }

  int getAge() {
    return age;
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeString("id", this.id);
    writer.writeString("name", this.name);
    writer.writeInt("age", this.age);
  }

  @Override
  public void fromData(PdxReader reader) {
    this.id = reader.readString("id");
    this.name = reader.readString("name");
    this.age = reader.readInt("age");
  }
}
