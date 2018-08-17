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



public class ObjectWithCharAttr implements Serializable {

  String name;
  char c;
  int t;

  public int getT() {
    return t;
  }

  public void setT(int t) {
    this.t = t;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public char getC() {
    return c;
  }

  public void setC(char c) {
    this.c = c;
  }

  public boolean equals(Object t) {
    if (t instanceof ObjectWithCharAttr) {
      ObjectWithCharAttr otherKey = (ObjectWithCharAttr) t;
      return otherKey.t == this.t;
    } else
      return false;
  }

  public int hashCode() {
    return t;
  }

}
