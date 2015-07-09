package com.examples.snapshot;

public class MyObjectPdx extends MyObject {
  public enum MyEnumPdx {
    const1, const2, const3, const4, const5;
  }
  
  private MyEnumPdx enumVal;
  
  public MyObjectPdx() {
  }

  public MyObjectPdx(long number, String s, MyEnumPdx enumVal) {
    super(number, s);
    this.enumVal = enumVal;
  }
}
