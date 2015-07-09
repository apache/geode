/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.dto;

import java.io.Serializable;

/**
 * Sample class for Data DUnit tests with JSON keys and values
 * @author tushark
 *
 */

public class Value1 implements Serializable{
  
  private String name;
  private String lastName;
  private String department;
  private int age;
  private int employeeId;
  
  public Value1(int i){
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
  
  public boolean equals(Object other){
    if(other instanceof Value1){
      Value1 v = (Value1)other;
      return v.employeeId==employeeId;
    }
    else return false;
  }
  
  public int hashCode(){
    return employeeId%7;
  }
  
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append(" Value1 [ Name : ").append(name)
      .append(" lastName : ").append(lastName)
      .append(" department : ").append(department)
      .append(" age : ").append(age)     
      .append(" employeeId : ").append(employeeId).append(" ]");
    return sb.toString();
  }
  

}
