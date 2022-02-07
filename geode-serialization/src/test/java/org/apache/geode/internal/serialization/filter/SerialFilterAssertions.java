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

@SuppressWarnings("unused")
class SerialFilterAssertions {

  private static final ObjectInputFilterApi API = new ReflectiveObjectInputFilterApiFactory()
      .createObjectInputFilterApi();

  private SerialFilterAssertions() {
    // do not instantiate
  }

  static void assertThatSerialFilterIsNull()
      throws InvocationTargetException, IllegalAccessException {
    assertThat(API.getSerialFilter())
        .as("ObjectInputFilter$Config.getSerialFilter()")
        .isNull();
  }

  static void assertThatSerialFilterIsNotNull()
      throws InvocationTargetException, IllegalAccessException {
    assertThat(API.getSerialFilter())
        .as("ObjectInputFilter$Config.getSerialFilter()")
        .isNotNull();
  }

  static void assertThatSerialFilterIsSameAs(Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException {
    assertThat(API.getSerialFilter())
        .as("ObjectInputFilter$Config.getSerialFilter()")
        .isSameAs(objectInputFilter);
  }

  static void assertThatSerialFilterIsNotSameAs(Object objectInputFilter)
      throws InvocationTargetException, IllegalAccessException {
    assertThat(API.getSerialFilter())
        .as("ObjectInputFilter$Config.getSerialFilter()")
        .isNotSameAs(objectInputFilter);
  }
}
