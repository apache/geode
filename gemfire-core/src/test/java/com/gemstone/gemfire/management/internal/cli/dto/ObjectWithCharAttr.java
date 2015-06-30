/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.dto;

import java.io.Serializable;



public class ObjectWithCharAttr implements Serializable {
  
  String name;
  char c;
  int t;
  
  public int getT() {
    return t;
  }
  public void setT(int t) {
    this.t = t;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  
  public char getC() {
    return c;
  }
  public void setC(char c) {
    this.c = c;
  }
  
  public boolean equals(Object t){
    if(t instanceof ObjectWithCharAttr) {
      ObjectWithCharAttr otherKey = (ObjectWithCharAttr)t;
      return otherKey.t==this.t;
    } else return false;
  }
  
  public int hashCode(){
    return t;
  }

}
