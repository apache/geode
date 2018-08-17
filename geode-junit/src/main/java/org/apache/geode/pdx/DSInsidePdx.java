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


import org.apache.geode.DataSerializable;

public class DSInsidePdx implements PdxSerializable {

  private String myString1;
  private long myLong;
  private DataSerializable myDS;
  private String myString2;
  private float myFloat;

  public DSInsidePdx() {}

  public DSInsidePdx(String str1, long myLong, DataSerializable myDS, String str2, float myFloat) {
    this.myString1 = str1;
    this.myLong = myLong;
    this.myDS = myDS;
    this.myString2 = str2;
    this.myFloat = myFloat;
  }

  public void toData(PdxWriter out) {
    out.writeString("myString1", this.myString1);
    out.writeLong("myLong", this.myLong);
    out.writeObject("myDS", this.myDS);
    out.writeString("myString2", this.myString2);
    out.writeFloat("myFloat", this.myFloat);
  }

  public void fromData(PdxReader in) {
    this.myString1 = in.readString("myString1");
    this.myLong = in.readLong("myLong");
    this.myDS = (DataSerializable) in.readObject("myDS");
    this.myString2 = in.readString("myString2");
    this.myFloat = in.readFloat("myFloat");
  }

  @Override
  public String toString() {
    return "DSInsidePdx [myString1=" + myString1 + ", myLong=" + myLong + ", myDS=" + myDS
        + ", myString2=" + myString2 + ", myFloat=" + myFloat + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((myDS == null) ? 0 : myDS.hashCode());
    result = prime * result + Float.floatToIntBits(myFloat);
    result = prime * result + (int) (myLong ^ (myLong >>> 32));
    result = prime * result + ((myString1 == null) ? 0 : myString1.hashCode());
    result = prime * result + ((myString2 == null) ? 0 : myString2.hashCode());
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
    DSInsidePdx other = (DSInsidePdx) obj;
    if (myDS == null) {
      if (other.myDS != null)
        return false;
    } else if (!myDS.equals(other.myDS))
      return false;
    if (Float.floatToIntBits(myFloat) != Float.floatToIntBits(other.myFloat))
      return false;
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
    return true;
  }
}
