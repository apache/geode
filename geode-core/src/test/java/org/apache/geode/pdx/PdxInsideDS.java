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
package org.apache.geode.pdx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;

class PdxInsideDS implements DataSerializable {
  private String myString1;
  private long myLong;
  private PdxSerializable myPdx;
  private String myString2;

  public PdxInsideDS() {}


  public PdxInsideDS(String str1, long myLong, PdxSerializable myPdx, String str2) {
    this.myString1 = str1;
    this.myLong = myLong;
    this.myPdx = myPdx;
    this.myString2 = str2;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.myString1, out);
    DataSerializer.writePrimitiveLong(this.myLong, out);
    DataSerializer.writeObject(this.myPdx, out);
    DataSerializer.writeString(this.myString2, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.myString1 = DataSerializer.readString(in);
    this.myLong = DataSerializer.readPrimitiveLong(in);
    this.myPdx = (PdxSerializable) DataSerializer.readObject(in);
    this.myString2 = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "PdxInsideDS [myString1=" + myString1 + ", myLong=" + myLong + ", myPdx=" + myPdx
        + ", myString2=" + myString2 + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (myLong ^ (myLong >>> 32));
    result = prime * result + ((myString1 == null) ? 0 : myString1.hashCode());
    result = prime * result + ((myString2 == null) ? 0 : myString2.hashCode());
    result = prime * result + ((myPdx == null) ? 0 : myPdx.hashCode());
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
    PdxInsideDS other = (PdxInsideDS) obj;
    if (myLong != other.myLong)
      return false;
    if (myString1 == null) {
      if (other.myString1 != null)
        return false;
    } else if (!myString1.equals(other.myString1))
      return false;
    if (myString2 == null) {
      if (other.myString2 != null)
        return false;
    } else if (!myString2.equals(other.myString2))
      return false;
    if (myPdx == null) {
      if (other.myPdx != null)
        return false;
    } else if (!myPdx.equals(other.myPdx))
      return false;
    return true;
  }
}
