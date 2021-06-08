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



public class Student extends Person {
  private Set _courses;
  private float _gpa;
  private Department _dept;


  public Student() {}

  public Student(String ssn, String name, Date birthdate, Collection courses, float gpa,
      Department dept) {
    super(ssn, name, birthdate);
    initCourses();
    _courses.addAll(courses);
    _gpa = gpa;
    _dept = dept;
  }

  public Set getCourses() {
    if (_courses.isEmpty()) {
      return Collections.EMPTY_SET;
    }
    return _courses;
  }

  public float getGPA() {
    return _gpa;
  }

  public Department getDepartment() {
    return _dept;
  }

  public void addCourse(Course course) {
    if (_courses == null) {
      initCourses();
    }
    _courses.add(course);
  }

  public void removeCourse(Course course) {
    if (_courses == null) {
      return;
    }
    _courses.remove(course);
    if (_courses.isEmpty()) {
      _courses = null;
    }
  }

  public void setGPA(float gpa) {
    _gpa = gpa;
  }


  public void setDepartment(Department dept) {
    _dept = dept;
  }

  private void initCourses() {
    // _courses = Utils.getQueryService().newIndexableSet(Course.class);
    _courses = new HashSet();
  }



}
