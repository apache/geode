/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.test;

import java.io.Serializable;

public class TestObject implements Serializable {

  private String field1 = "hello world";
  private String field2 = "this is a field";

  public TestObject() {

  }

  public TestObject(final String field1, final String field2) {
    this.field1 = field1;
    this.field2 = field2;
  }

  public String getField1() {
    return field1;
  }

  public void setField1(final String field1) {
    this.field1 = field1;
  }

  public String getField2() {
    return field2;
  }

  public void setField2(final String field2) {
    this.field2 = field2;
  }

  @Override
  public boolean equals(Object obj) {
    TestObject testObject = (TestObject) obj;
    return (testObject.field1.equals(field1) && testObject.field2.equals(field2));
  }

  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName()).append("[").append("field1=")
        .append(field1).append("; field2=").append(field2).append("]").toString();
  }
}
