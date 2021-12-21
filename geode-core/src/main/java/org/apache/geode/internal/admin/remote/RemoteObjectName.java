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


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

/**
 * Used to name an object in a region. This class is needed so that the console will not need to
 * load the user defined classes.
 */
public class RemoteObjectName implements DataSerializable {
  private static final long serialVersionUID = 5076319310507575418L;
  private String className;
  private String value;
  private int hashCode;

  public RemoteObjectName(Object name) {
    className = name.getClass().getName();
    value = name.toString();
    hashCode = name.hashCode();
  }

  /**
   * This constructor is only for use by the DataSerializable mechanism
   */
  public RemoteObjectName() {}

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o instanceof RemoteObjectName) {
      RemoteObjectName n = (RemoteObjectName) o;
      return (hashCode == n.hashCode) && className.equals(n.className) && value.equals(n.value);
    } else {
      // this should only happen on the server side when we are trying
      // to find the original object
      if (hashCode != o.hashCode()) {
        return false;
      }
      if (!className.equals(o.getClass().getName())) {
        return false;
      }
      return value.equals(o.toString());
    }
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return className + " \"" + value + "\"";
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(className, out);
    DataSerializer.writeString(value, out);
    out.writeInt(hashCode);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    className = DataSerializer.readString(in);
    value = DataSerializer.readString(in);
    hashCode = in.readInt();
  }

}
