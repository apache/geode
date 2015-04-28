/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Sep 28, 2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package com.gemstone.gemfire.cache.query.data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author ashahid
 * 
 *  
 */
public class Student {

  private static int counter = 0;
  static String[] names = { "English", "Hindi","Maths", "Bio"};
  static String[] teacher_names = { "X", "Y","Z", "A"};
  public String name;
  public int rollnum;
  public List subjects = new ArrayList();
  public List teachers = new ArrayList();
  
  public static  void initializeCounter() {
    counter = 0;
  }

  public Student(String name) {
    this.name = name;
    synchronized (Student.class) {
      rollnum = ++counter;
    }
    int rem = rollnum % names.length;
    if (rem == 0) rem = 4;
    for (int j = 0; j < rem; ++j) {
      subjects.add(this.new Subject(names[j]));
      teachers.add(new Teacher(teacher_names[j]));
    }
  }

  public class Subject {

    public String subject;

    public Subject(String sub) {
      Subject.this.subject = sub;
    }
  }
  
  public static class Teacher {
    public String teacher ;
    public Teacher(String teacher) {
      this.teacher = teacher;
    }
    
  }
  
}