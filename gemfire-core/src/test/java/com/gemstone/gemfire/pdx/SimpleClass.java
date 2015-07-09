/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;


public class SimpleClass implements PdxSerializable {
  public static enum SimpleEnum {ONE, TWO};
  private int myInt;
  private byte myByte;
  private SimpleEnum myEnum;

  public SimpleClass() {
  }
  
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
  public void toData(PdxWriter out) {
    out.writeInt("myInt", this.myInt);
    out.writeByte("myByte", this.myByte);
    out.writeObject("myEnum", this.myEnum);
  }
  
  public void fromData(PdxReader in){
    this.myInt = in.readInt("myInt");
    this.myByte = in.readByte("myByte");
    this.myEnum = (SimpleEnum) in.readObject("myEnum");
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + " [myInt=" + myInt + ", myByte=" + myByte + ", myEnum=" + myEnum + "]";
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