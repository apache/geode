/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.transaction;

import java.io.Serializable;

public class Person implements Serializable {
  private String name;
  private int age;

  public Person(String name, int age) {
    this.name = name;
    this.age = age;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public int getAge() {
    return age;
  }
  //@Override
  public String toString() {
    return "Name:"+name+" age:"+age;
  }

  public static boolean THROW_ON_INDEX = false;
  public boolean index() {
    if(THROW_ON_INDEX) {
      throw new RuntimeException("sup dawg");
    }
    return true;
  }
}
