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

import org.apache.geode.DataSerializable;
import org.apache.geode.Instantiator;

public class User implements DataSerializable {
  private String name;
  private int userId;

  static {
    Instantiator.register(new Instantiator(User.class, (byte) 45) {
      public DataSerializable newInstance() {
        return new User();
      }
    });
  }

  public User(String name, int userId) {
    this.name = name;
    this.userId = userId;
  }

  /**
   * Creates an "empty" User whose contents are filled in by invoking its toData() method
   */
  protected User() {

  }

  public void toData(DataOutput out) throws IOException {
    out.writeUTF(this.name);
    out.writeInt(this.userId);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.name = in.readUTF();
    this.userId = in.readInt();
  }
}
