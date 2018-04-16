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
/*
 * Created on Sep 28, 2005
 *
 * TODO To change the template for this generated file go to Window - Preferences - Java - Code
 * Style - Code Templates
 */
package org.apache.geode.cache.query.data;

import java.util.ArrayList;
import java.util.List;

public class Student {

  private static int counter = 0;
  static String[] names = {"English", "Hindi", "Maths", "Bio"};
  static String[] teacher_names = {"X", "Y", "Z", "A"};
  public String name;
  public int rollnum;
  public List subjects = new ArrayList();
  public List teachers = new ArrayList();

  public static void initializeCounter() {
    counter = 0;
  }

  public Student(String name) {
    this.name = name;
    synchronized (Student.class) {
      rollnum = ++counter;
    }
    int rem = rollnum % names.length;
    if (rem == 0)
      rem = 4;
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
    public String teacher;

    public Teacher(String teacher) {
      this.teacher = teacher;
    }

  }

}
