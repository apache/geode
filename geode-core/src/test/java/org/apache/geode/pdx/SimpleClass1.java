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



public class SimpleClass1 implements PdxSerializable {

  public SimpleClass1() {}

  private boolean myFlag;
  private short myShort;
  private String myString1;
  private long myLong;
  private String myString2;
  private String myString3;
  private int myInt;
  private float myFloat;

  public SimpleClass1(boolean myFlag, short myShort, String str1, long myLong, String str2,
      String str3, int myInt, float myFloat) {
    this.myFlag = myFlag;
    this.myShort = myShort;
    this.myString1 = str1;
    this.myLong = myLong;
    this.myString2 = str2;
    this.myString3 = str3;
    this.myInt = myInt;
    this.myFloat = myFloat;
  }

  public void toData(PdxWriter out) {
    out.writeBoolean("myFlag", this.myFlag);
    out.writeShort("myShort", this.myShort);
    out.writeString("myString1", this.myString1);
    out.writeLong("myLong", this.myLong);
    out.writeString("myString2", this.myString2);
    out.writeString("myString3", this.myString3);
    out.writeInt("myInt", this.myInt);
    out.writeFloat("myFloat", this.myFloat);
  }

  public void fromData(PdxReader in) {
    this.myFlag = in.readBoolean("myFlag");
    this.myShort = in.readShort("myShort");
    this.myString1 = in.readString("myString1");
    this.myLong = in.readLong("myLong");
    this.myString2 = in.readString("myString2");
    this.myString3 = in.readString("myString3");
    this.myInt = in.readInt("myInt");
    this.myFloat = in.readFloat("myFloat");
  }

  public String toString() {
    return "SimpleClass1 [myFlag=" + myFlag + ", myShort=" + myShort + ", myString1=" + myString1
        + ", myLong=" + myLong + ", myString2=" + myString2 + ", myString3=" + myString3
        + ", myInt=" + myInt + ", myFloat=" + myFloat + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (myFlag ? 1231 : 1237);
    result = prime * result + Float.floatToIntBits(myFloat);
    result = prime * result + myInt;
    result = prime * result + (int) (myLong ^ (myLong >>> 32));
    result = prime * result + myShort;
    result = prime * result + ((myString1 == null) ? 0 : myString1.hashCode());
    result = prime * result + ((myString2 == null) ? 0 : myString2.hashCode());
    result = prime * result + ((myString3 == null) ? 0 : myString3.hashCode());
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
    SimpleClass1 other = (SimpleClass1) obj;
    if (myFlag != other.myFlag)
      return false;
    if (Float.floatToIntBits(myFloat) != Float.floatToIntBits(other.myFloat))
      return false;
    if (myInt != other.myInt)
      return false;
    if (myLong != other.myLong)
      return false;
    if (myShort != other.myShort)
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
    if (myString3 == null) {
      if (other.myString3 != null)
        return false;
    } else if (!myString3.equals(other.myString3))
      return false;
    return true;
  }

  public boolean isMyFlag() {
    return this.myFlag;
  }

  public short getMyShort() {
    return this.myShort;
  }

  public String getMyString1() {
    return this.myString1;
  }

  public long getMyLong() {
    return this.myLong;
  }

  public String getMyString2() {
    return this.myString2;
  }

  public String getMyString3() {
    return this.myString3;
  }

  public int getMyInt() {
    return this.myInt;
  }

  public float getMyFloat() {
    return this.myFloat;
  }
}
