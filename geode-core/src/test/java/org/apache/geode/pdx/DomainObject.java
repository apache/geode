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
package org.apache.geode.pdx;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.geode.internal.PdxSerializerObject;

public abstract class DomainObject implements PdxSerializerObject {
  private String string_0;
  private long long_0;

  private String[] string_array;

  private ArrayList<String> string_list;

  public DomainObject() {
    this(0);
  }

  public DomainObject(int size) {
    string_array = new String[size];
    string_list = new ArrayList<String>(size);
  }

  public Object get(String varName) throws Exception {
    Field f;
    try {
      f = this.getClass().getDeclaredField(varName);
    } catch (NoSuchFieldException fex) {
      f = this.getClass().getSuperclass().getDeclaredField(varName);
    }
    Object o = f.get(this);
    return o;
  }

  public void set(String varName, Object value) throws Exception {
    Field f;
    try {
      f = this.getClass().getDeclaredField(varName);
    } catch (NoSuchFieldException fex) {
      try {
        f = this.getClass().getSuperclass().getDeclaredField(varName);
      } catch (NoSuchFieldException nex) {
        f = this.getClass().getSuperclass().getSuperclass().getDeclaredField(varName);
      }
    }

    f.set(this, value);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (long_0 ^ (long_0 >>> 32));
    result = prime * result + ((string_0 == null) ? 0 : string_0.hashCode());
    result = prime * result + Arrays.hashCode(string_array);
    result = prime * result + ((string_list == null) ? 0 : string_list.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DomainObject other = (DomainObject) obj;
    if (long_0 != other.long_0)
      return false;
    if (string_0 == null) {
      if (other.string_0 != null)
        return false;
    } else if (!string_0.equals(other.string_0))
      return false;
    if (!Arrays.equals(string_array, other.string_array))
      return false;
    if (string_list == null) {
      if (other.string_list != null)
        return false;
    } else if (!string_list.equals(other.string_list))
      return false;
    return true;
  }
}
