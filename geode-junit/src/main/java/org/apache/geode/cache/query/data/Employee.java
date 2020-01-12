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
package org.apache.geode.cache.query.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Employee implements Serializable {
  private String name;
  private int age;
  private int empId;
  private Set addresses;
  private String title;
  private int salary;
  private PhoneNo phoneNo;

  public String name() {
    return name;
  }

  public int getAge() {
    return age;
  }

  public int empId() {
    return empId;
  }

  public String getTitle() {
    return title;
  }

  public int salary() {
    return salary;
  }

  /** Creates a new instance of Employee */
  public Employee(String name, int age, int empId, String title, int salary, Set addresses) {
    this.name = name;
    this.age = age;
    this.empId = empId;
    this.title = title;
    this.salary = salary;
    this.addresses = addresses;
    this.phoneNo = new PhoneNo(111, 222, 333, 444);

  }

  // Added for the Test IUMRCompositeIteratorTest
  public Set getPhoneNo(String zipCode) {
    Set ph = new HashSet();
    ph.add(this.phoneNo);
    return ph;
  }

  public Set getPh(int empId) {
    Set ph = new HashSet();
    ph.add(this.phoneNo);
    return ph;
  }
}
