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


/**
 * Sample class for Data DUnit tests with JSON keys and values
 *
 */

public class Key1 implements Serializable {

  private String id;
  private String name;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean equals(Object other) {
    if (other instanceof Key1) {
      Key1 k1 = (Key1) other;
      return k1.id.equals(id);
    } else
      return false;
  }

  public int hashCode() {
    return id.hashCode();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" Key1 [ id : ").append(id).append(" name : ").append(name).append(" ]");
    return sb.toString();
  }



}
