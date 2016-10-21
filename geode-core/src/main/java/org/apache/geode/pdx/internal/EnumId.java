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
package org.apache.geode.pdx.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;

public class EnumId implements DataSerializableFixedID {

  private int id;

  public EnumId(int id) {
    this.id = id;
  }

  public EnumId() {}

  public int getDSFID() {
    return ENUM_ID;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.id = in.readInt();
  }

  public int intValue() {
    return this.id;
  }

  public int getDSId() {
    return this.id >> 24;
  }

  public int getEnumNum() {
    return this.id & 0x00FFFFFF;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    EnumId other = (EnumId) obj;
    if (id != other.id)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "EnumId[dsid=" + getDSId() + ", enumnum=" + getEnumNum() + "]";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }



}
