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


import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;



public class Faculty extends Person {
  private String _rank;
  private Department _dept;
  private int _salary;
  // private IndexableSet _hobbies;
  // private IndexableSet _advisees;
  private Set _hobbies;
  private Set _advisees;

  public Faculty() {}

  public Faculty(String ssn, String name, Date bd, String rank, Department dept, int salary,
      Collection hobbies, Collection advisees) {
    super(ssn, name, bd);
    _rank = rank;
    _dept = dept;
    _salary = salary;
    if (hobbies != null) {
      initHobbies();
      _hobbies.addAll(hobbies);
    }

    if (advisees != null) {
      initAdvisees();
      _advisees.addAll(advisees);
    }
  }

  public String getRank() {
    return _rank;
  }

  public Department getDepartment() {
    return _dept;
  }

  public int getSalary() {
    return _salary;
  }

  public Set getHobbies() {
    if (_hobbies == null) {
      return Collections.EMPTY_SET;
    }
    return _hobbies;
  }


  public Set getAdvisees() {
    if (_advisees == null) {
      return Collections.EMPTY_SET;
    }
    return _advisees;
  }


  public void setRank(String rank) {
    _rank = rank;
  }


  public void setDepartment(Department dept) {
    _dept = dept;
  }


  public void setSalary(int salary) {
    _salary = salary;
  }


  public void addHobby(String hobby) {
    if (_hobbies == null) {
      initHobbies();
    }
    _hobbies.add(hobby);
  }

  public void removeHobby(String hobby) {
    if (_hobbies == null) {
      return;
    }
    _hobbies.remove(hobby);
    if (_hobbies.isEmpty()) {
      _hobbies = null;
    }
  }

  public void setAdvisees(Set set) {
    _advisees = new HashSet();// Utils.getQueryService().newIndexableSet(Student.class, set);
  }


  public void addAdvisee(Student stud) {
    if (_advisees == null) {
      initAdvisees();
    }
    _advisees.add(stud);
  }

  private void initHobbies() {
    // _hobbies = Utils.getQueryService().newIndexableSet(String.class);
    _hobbies = new HashSet();
  }


  private void initAdvisees() {
    // _advisees = Utils.getQueryService().newIndexableSet(G_Student.class);
    _advisees = new HashSet();
  }



}
