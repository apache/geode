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
package org.apache.geode.internal.serialization.filter;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;

/**
 * For use by {@link LocatorLauncherGlobalSerialFilterDistributedTest}.
 */
@SuppressWarnings("all")
class SerializableClass implements Serializable {

  private final String value;

  SerializableClass(String value) {
    this.value = requireNonNull(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SerializableClass that = (SerializableClass) o;
    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return hash(value);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SerializableClass{");
    sb.append("value='").append(value).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
