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

import org.apache.geode.DataSerializer;

public class CompanySerializer extends DataSerializer {

  static {
    DataSerializer.register(CompanySerializer.class);
  }

  /**
   * May be invoked reflectively if instances of Company are distributed to other VMs.
   */
  public CompanySerializer() {

  }

  public int getId() {
    return 42;
  }

  public Class[] getSupportedClasses() {
    return new Class[] {Company.class};
  }

  public boolean toData(Object o, DataOutput out) throws IOException {
    if (o instanceof Company) {
      Company company = (Company) o;
      out.writeUTF(company.getName());

      // Let's assume that Address is java.io.Serializable
      Address address = company.getAddress();
      writeObject(address, out);
      return true;

    } else {
      return false;
    }
  }

  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {

    String name = in.readUTF();
    Address address = (Address) readObject(in);
    return new Company(name, address);
  }
}
