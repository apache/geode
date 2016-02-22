/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gemstone.gemfire.modules;

import java.util.HashSet;
import java.util.Set;

public class Person {
  private Long id;
  private int age;
  private String firstname;
  private String lastname;

  private Set<Event> e = new HashSet<Event>();
  
  public Person() {}

  public void setId(Long id) {
    this.id = id;
  }

  public Long getId() {
    return id;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public int getAge() {
    return age;
  }

  public void setFirstname(String firstname) {
    this.firstname = firstname;
  }

  public String getFirstname() {
    return firstname;
  }

  public void setLastname(String lastname) {
    this.lastname = lastname;
  }

  public String getLastname() {
    return lastname;
  }

  public void setE(Set<Event> events) {
    this.e = events;
  }

  public Set<Event> getE() {
    return e;
  }
  
}
