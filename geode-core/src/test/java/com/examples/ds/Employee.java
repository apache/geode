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
package com.examples.ds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

public class Employee implements DataSerializable {
  private int id;
  private String name;
  private Date birthday;
  private Company employer;

  public Employee(int id, String name, Date birthday, Company employer) {
    this.id = id;
    this.name = name;
    this.birthday = birthday;
    this.employer = employer;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeUTF(this.name);
    DataSerializer.writeDate(this.birthday, out);
    DataSerializer.writeObject(this.employer, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    this.id = in.readInt();
    this.name = in.readUTF();
    this.birthday = DataSerializer.readDate(in);
    this.employer = (Company) DataSerializer.readObject(in);
  }
}
