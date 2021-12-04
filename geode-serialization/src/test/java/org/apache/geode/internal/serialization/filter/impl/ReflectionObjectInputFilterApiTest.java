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
package org.apache.geode.internal.serialization.filter.impl;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.JavaVersion.JAVA_1_8;
import static org.apache.commons.lang3.JavaVersion.JAVA_9;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.junit.Before;
import org.junit.Test;

public class ReflectionObjectInputFilterApiTest {

  private ObjectInputFilterApi api;

  @Before
  public void setUp() throws ClassNotFoundException, NoSuchMethodException {
    if (isJavaVersionAtLeast(JAVA_9)) {
      api = new Java9ReflectionObjectInputFilterApi(ApiPackage.JAVA_IO);
    } else if (isJavaVersionAtLeast(JAVA_1_8)) {
      api = new ReflectionObjectInputFilterApi(ApiPackage.SUN_MISC);
    }
  }

  @Test
  public void createFilterGivenValidPatternReturnsNewFilter()
      throws IllegalAccessException, InvocationTargetException {

    Object filter = api.createFilter("!*");

    assertThat(filter)
        .as("ObjectInputFilter$Config.createFilter(\"!*\")")
        .isNotNull()
        .isInstanceOf(api.getObjectInputFilterClass());
  }

  @Test
  public void createFilterGivenEmptyPatternReturnsNull()
      throws IllegalAccessException, InvocationTargetException {

    Object filter = api.createFilter("");

    assertThat(filter)
        .as("ObjectInputFilter$Config.createFilter(\"\")")
        .isNull();
  }

  @Test
  public void createFilterGivenBlankPatternReturnsNewFilter()
      throws IllegalAccessException, InvocationTargetException {

    Object filter = api.createFilter(" ");

    assertThat(filter)
        .as("ObjectInputFilter$Config.createFilter(\" \")")
        .isNotNull()
        .isInstanceOf(api.getObjectInputFilterClass());
  }

  @Test
  public void getSerialFilterReturnsNullWhenFilterDoesNotExist()
      throws IllegalAccessException, InvocationTargetException {

    Object filter = api.getSerialFilter();

    assertThat(filter)
        .as("ObjectInputFilter$Config.getSerialFilter()")
        .isNull();
  }

  @Test
  public void getObjectInputFilterReturnsNullWhenFilterDoesNotExist()
      throws ClassNotFoundException, IllegalAccessException, InvocationTargetException, IOException,
      NoSuchMethodException {
    // ObjectInputFilterApi api = new ReflectionObjectInputFilterApi(apiPackage);
    Serializable object = "hello";
    ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream(object));

    Object filter = api.getObjectInputFilter(inputStream);

    assertThat(filter).isNull();
  }

  @Test
  public void getObjectInputFilterReturnsExistingFilter()
      throws IllegalAccessException, InvocationTargetException, IOException {
    Object existingFilter = api.createFilter("*");
    Serializable object = "hello";
    ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream(object));
    api.setObjectInputFilter(inputStream, existingFilter);

    Object filter = api.getObjectInputFilter(inputStream);

    assertThat(filter).isSameAs(existingFilter);
  }

  @Test
  public void setObjectInputFilterGivenAcceptFilterReadsObject()
      throws ClassNotFoundException, IllegalAccessException, InvocationTargetException,
      IOException {
    Serializable object = new SerializableClass("hello");
    Object filter = api.createFilter(object.getClass().getName() + ";!*");

    try (ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream(object))) {
      api.setObjectInputFilter(inputStream, filter);

      assertThat(inputStream.readObject()).isEqualTo(object);
    }
  }

  @Test
  public void setObjectInputFilterGivenDenyFilterThrowsInvalidClassException()
      throws IllegalAccessException, InvocationTargetException, IOException {
    Serializable object = new SerializableClass("hello");
    Object filter = api.createFilter("!" + object.getClass().getName());

    try (ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream(object))) {
      api.setObjectInputFilter(inputStream, filter);

      Throwable thrown = catchThrowable(() -> {
        assertThat(inputStream.readObject()).isEqualTo(object);
      });

      assertThat(thrown).isInstanceOf(InvalidClassException.class);
    }
  }

  @Test
  public void createObjectInputFilterProxyGivenAcceptFilterReadsObject()
      throws ClassNotFoundException, IllegalAccessException, InvocationTargetException,
      IOException {
    Serializable object = new SerializableClass("hello");
    String className = object.getClass().getName();
    Object filter = api.createObjectInputFilterProxy(className + ";!*", singleton(className));

    try (ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream(object))) {
      api.setObjectInputFilter(inputStream, filter);

      assertThat(inputStream.readObject()).isEqualTo(object);
    }
  }

  @Test
  public void createObjectInputFilterProxyGivenRejectFilterThrowsInvalidClassException()
      throws IllegalAccessException, InvocationTargetException, IOException {
    Serializable object = new SerializableClass("hello");
    String className = object.getClass().getName();
    Object filter = api.createObjectInputFilterProxy("!" + className + ";*", emptyList());

    try (ObjectInputStream inputStream = new ObjectInputStream(byteArrayInputStream(object))) {
      api.setObjectInputFilter(inputStream, filter);

      Throwable thrown = catchThrowable(() -> {
        assertThat(inputStream.readObject()).isEqualTo(object);
      });

      assertThat(thrown).isInstanceOf(InvalidClassException.class);
    }
  }

  private static ByteArrayInputStream byteArrayInputStream(Serializable object) {
    return new ByteArrayInputStream(serialize(object));
  }

  private static class SerializableClass implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String value;

    private SerializableClass(String value) {
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
}
