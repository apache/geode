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

package org.apache.geode.cache.query.facets.lang;

import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Employee {
  static Random rand = new Random();

  public int salary;
  public String name;
  public Date hireDate;
  public Address fieldAddress;
  public float f;
  public double d;
  public Set collect;

  public long empId;

  public Employee() {
    f = 0.9f;
    d = 0.1;
    salary = (int) (rand.nextFloat() * 200000);
    name = genName();
    if (rand.nextFloat() < 0.2) {
      hireDate = null;
    } else {
      hireDate = new Date(Math.abs(rand.nextLong()));
    }

    if (rand.nextFloat() < 0.2) {
      fieldAddress = null;
    } else {
      fieldAddress = new Address();
    }

    collect = new HashSet();
  }



  public String toString() {
    return getClass().getName() + ": " + name + "/" + salary + '/' + fieldAddress;
  }

  public int testMethod(double p1, String p2) {
    System.out.println("invoking testMethod: double");
    return salary;
  }

  public int testMethod(float p1, String p2) {
    System.out.println("invoking testMethod: float");
    return salary;
  }

  public int testMethod(long p1, String p2) {
    System.out.println("invoking testMethod: long");
    return salary;
  }

  public int testMethod(int p1, String p2) {
    System.out.println("invoking testMethod: int");
    return salary;
  }

  public int testMethod(short p1, String p2) {
    System.out.println("invoking testMethod: short");
    return salary;
  }

  public int testMethod(char p1, String p2) {
    System.out.println("invoking testMethod: char");
    return salary;
  }

  public int testMethod(byte p1, String p2) {
    System.out.println("invoking testMethod: byte");
    return salary;
  }

  public int testMethod(boolean p1, String p2) {
    System.out.println("invoking testMethod: boolean");
    return salary;
  }

  public int testMethod(java.sql.Date p1, String p2) {
    System.out.println("invoking testMethod: java.sql.Date: " + p1);
    return salary;
  }

  public int testMethod(java.sql.Time p1, String p2) {
    System.out.println("invoking testMethod: Time: " + p1);
    return salary;
  }

  public int testMethod(java.sql.Timestamp p1, String p2) {
    System.out.println("invoking testMethod: Timestamp: " + p1);
    return salary;
  }


  private static String genName() {
    switch ((Math.abs(rand.nextInt()) % 5)) {
      case 0:
        return "Adam";
      case 1:
        return "Bob";
      case 2:
        return "Charles";
      case 3:
        return "David";
      case 4:
        return "Earnest";
    }
    throw new IllegalStateException();
  }


  public Address address() {
    System.out.println("In address() method");
    return fieldAddress;
  }



}
