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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;

// TODO:KIRK: delete this
public class SerialFilterAssertions {

  private static final ObjectInputFilterApi API = new ReflectionObjectInputFilterApiFactory()
      .createObjectInputFilterApi();

  private SerialFilterAssertions() {
    // do not instantiate
  }

  public static void assertThatSerialFilterIsNull()
      throws InvocationTargetException, IllegalAccessException {
    boolean exists = API.getSerialFilter() != null;
    assertThat(exists)
        .as("ObjectInputFilter$Config.getSerialFilter() is null")
        .isFalse();
  }

  public static void assertThatSerialFilterIsNotNull()
      throws InvocationTargetException, IllegalAccessException {
    boolean exists = API.getSerialFilter() != null;
    assertThat(exists)
        .as("ObjectInputFilter$Config.getSerialFilter() is not null")
        .isTrue();
  }

  public static void assertThatSerialFilterIsSameAs(Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException {
    Object currentFilter = API.getSerialFilter();
    boolean sameIdentity = currentFilter == objectInputFilter;
    assertThat(sameIdentity)
        .as("ObjectInputFilter$Config.getSerialFilter() is same as parameter")
        .isTrue();
  }

  public static void assertThatSerialFilterIsNotSameAs(Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException {
    Object currentFilter = API.getSerialFilter();
    boolean sameIdentity = currentFilter == objectInputFilter;
    assertThat(sameIdentity)
        .as("ObjectInputFilter$Config.getSerialFilter() is same as parameter")
        .isFalse();
  }
}
