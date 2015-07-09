/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.snapshot;

import java.io.Serializable;

import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * Data class for testing snapshots, cannot be located in com.gemstone.*.
 * 
 * @author bakera
 */
public class MyObject implements Serializable {
  protected long f1;
  protected String f2;
  
  public MyObject() {
  }

  public MyObject(long number, String s) {
    f1 = number;
    f2 = s;
  }
  
  public long getF1() {
    return f1;
  }
  
  public String getF2() {
    return f2;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof MyObject) {
      MyObject obj = (MyObject) o;
      return f1 == obj.f1 && f2.equals(obj.f2);

    } else if (o instanceof PdxInstance) {
      PdxInstance pdx = (PdxInstance) o;
      return pdx.getField("f1").equals(f1) && pdx.getField("f2").equals(f2);
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    return (int) (17 * f1 ^ f2.hashCode());
  }
  
  public String toString() {
    return f1 + "-" + f2;
  }
}