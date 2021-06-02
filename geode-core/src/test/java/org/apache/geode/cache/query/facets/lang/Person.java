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

import java.util.Calendar;
import java.util.Date;

public class Person {
  private String _ssn;
  private String _name;
  private java.sql.Date _birthdate;

  public Person() {}


  public Person(String ssn, String name, java.util.Date birthdate) {
    _ssn = ssn;
    _name = name;
    _birthdate = new java.sql.Date(birthdate.getTime());
  }

  public String toString() {
    return getName();
  }

  public String getName() {
    return _name;
  }


  public String getSSN() {
    return _ssn;
  }

  public java.sql.Date getBirthdate() {
    return _birthdate;
  }


  public int getAge() {
    Calendar now = Calendar.getInstance();
    Calendar bd = Calendar.getInstance();
    bd.setTime(_birthdate);

    Calendar bdThisYear = Calendar.getInstance();
    bdThisYear.setTime(_birthdate);
    bdThisYear.set(Calendar.YEAR, now.get(Calendar.YEAR));

    int age = now.get(Calendar.YEAR) - bd.get(Calendar.YEAR);

    if (bdThisYear.after(now)) {
      age--;
    }

    return age;
  }

  public void setBirthdate(Date bd) {
    _birthdate = new java.sql.Date(bd.getTime());
  }


  public void setName(String name) {
    _name = name;
  }

  public void setSSN(String ssn) {
    _ssn = ssn;
  }
}
