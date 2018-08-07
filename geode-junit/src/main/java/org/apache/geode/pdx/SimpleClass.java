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


public class SimpleClass implements PdxSerializable {
  public static enum SimpleEnum {
    ONE, TWO
  };

  private int myInt;
  private byte myByte;
  private SimpleEnum myEnum;

  public SimpleClass() {}

  public SimpleClass(int intVal, byte byteVal) {
    this.myInt = intVal;
    this.myByte = byteVal;
    this.myEnum = SimpleEnum.TWO;
  }

  public SimpleClass(int intVal, byte byteVal, SimpleEnum enumVal) {
    this.myInt = intVal;
    this.myByte = byteVal;
    this.myEnum = enumVal;
  }

  public int getMyInt() {
    return myInt;
  }

  public byte getMyByte() {
    return myByte;
  }

  public SimpleEnum getMyEnum() {
    return myEnum;
  }

  public void toData(PdxWriter out) {
    out.writeInt("myInt", this.myInt);
    out.writeByte("myByte", this.myByte);
    out.writeObject("myEnum", this.myEnum);
  }

  public void fromData(PdxReader in) {
    this.myInt = in.readInt("myInt");
    this.myByte = in.readByte("myByte");
    this.myEnum = (SimpleEnum) in.readObject("myEnum");
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [myInt=" + myInt + ", myByte=" + myByte + ", myEnum="
        + myEnum + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + myByte;
    result = prime * result + ((myEnum == null) ? 0 : myEnum.hashCode());
    result = prime * result + myInt;
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
    SimpleClass other = (SimpleClass) obj;
    if (myByte != other.myByte)
      return false;
    if (myEnum != other.myEnum)
      return false;
    if (myInt != other.myInt)
      return false;
    return true;
  }
}
