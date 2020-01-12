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
package org.apache.geode.management.internal.cli.dto;

import java.io.Serializable;

/**
 * Sample class for Data DUnit tests with JSON keys and values
 *
 */

public class Value1 implements Serializable {

  private String name;
  private String lastName;
  private String department;
  private int age;
  private int employeeId;

  public Value1(int i) {
    this.employeeId = i;
    this.name = "Name" + i;
    this.lastName = "lastName" + i;
    this.department = "department" + i;
    this.age = i;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getDepartment() {
    return department;
  }

  public void setDepartment(String department) {
    this.department = department;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public int getEmployeeId() {
    return employeeId;
  }

  public void setEmployeeId(int employeeId) {
    this.employeeId = employeeId;
  }

  public boolean equals(Object other) {
    if (other instanceof Value1) {
      Value1 v = (Value1) other;
      return v.employeeId == employeeId;
    } else
      return false;
  }

  public int hashCode() {
    return employeeId % 7;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" Value1 [ Name : ").append(name).append(" lastName : ").append(lastName)
        .append(" department : ").append(department).append(" age : ").append(age)
        .append(" employeeId : ").append(employeeId).append(" ]");
    return sb.toString();
  }


}
