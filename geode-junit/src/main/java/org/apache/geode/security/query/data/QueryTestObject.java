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
package org.apache.geode.security.query.data;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class QueryTestObject implements Serializable {
  public int id = -1;

  private String name;

  public Date dateField;

  public Map<Object, Object> mapField;

  public QueryTestObject(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String someMethod() {
    return name + ":" + id;
  }

  public void setDateField(String dateString) {
    try {
      SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
      dateField = sdf.parse(dateString);
    } catch (ParseException e) {

    }
  }

  @Override
  public String toString() {
    return "Test_Object";
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof QueryTestObject)) {
      return false;
    }
    QueryTestObject qto = (QueryTestObject) obj;
    return (this.id == qto.id && this.name.equals(qto.getName()));
  }
}
