/*
 *========================================================================
 * Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */


package com.gemstone.gemfire.cache.query.facets.lang;


import java.util.*;



public class Faculty extends Person {
  private String _rank;
  private Department _dept;
  private int _salary;
  //private IndexableSet _hobbies;
  //private IndexableSet _advisees;
  private Set _hobbies;
  private Set _advisees;
  
  public Faculty() {
  }
  
  public Faculty(String ssn, String name, Date bd,
          String rank, Department dept, int salary,
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
    if (_hobbies == null)
      return Collections.EMPTY_SET;
    return _hobbies;
  }
  
  
  public Set getAdvisees() {
    if (_advisees == null)
      return Collections.EMPTY_SET;
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
    if (_hobbies == null)
      initHobbies();
    _hobbies.add(hobby);
  }
  
  public void removeHobby(String hobby) {
    if (_hobbies == null)
      return;
    _hobbies.remove(hobby);
    if (_hobbies.isEmpty())
      _hobbies = null;
  }
  
  public void setAdvisees(Set set) {
    _advisees = new HashSet();//Utils.getQueryService().newIndexableSet(Student.class, set);
  }
  
  
  public void addAdvisee(Student stud) {
    if (_advisees == null)
      initAdvisees();
    _advisees.add(stud);
  }
  
  private void initHobbies() {
    //_hobbies = Utils.getQueryService().newIndexableSet(String.class);
    _hobbies = new HashSet();
  }
  
  
  private void initAdvisees() {
    //_advisees = Utils.getQueryService().newIndexableSet(G_Student.class);
    _advisees = new HashSet();
  }
  
  
  
}
