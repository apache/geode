/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package hibe;

import java.util.HashSet;
import java.util.Set;

public class Person {
  private Long id;
  private int age;
  private String firstname;
  private String lastname;

  private Set<Event> events = new HashSet<Event>();
  
  public Person() {}

  private void setId(Long id) {
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

  public void setEvents(Set<Event> events) {
    this.events = events;
  }

  public Set<Event> getEvents() {
    return events;
  }
  
}
