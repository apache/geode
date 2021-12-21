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
package org.apache.geode.test.junit.rules.serializable;

import static org.apache.geode.test.junit.rules.serializable.FieldsOfTestName.FIELD_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.Description;


/**
 * Unit tests for {@link SerializableTestName}.
 */
public class SerializableTestNameTest {

  @Test
  public void hasOneFields() throws Exception {
    Field[] fields = TestName.class.getDeclaredFields();
    assertThat(fields.length).as("Fields: " + Arrays.asList(fields)).isEqualTo(1);
  }

  @Test
  public void fieldNameShouldExist() throws Exception {
    Field field = TestName.class.getDeclaredField(FIELD_NAME);
    assertThat(field.getType()).isEqualTo(String.class);
  }

  @Test
  public void fieldsCanBeRead() throws Exception {
    String name = "foo";
    Description mockDescription = mock(Description.class);
    when(mockDescription.getMethodName()).thenReturn(name);

    SerializableTestName instance = new SerializableTestName();
    instance.starting(mockDescription);

    assertThat(instance.getMethodName()).isEqualTo(name);
  }

  @Test
  public void isSerializable() throws Exception {
    assertThat(SerializableTestName.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void canBeSerialized() throws Exception {
    String name = "bar";
    Description mockDescription = mock(Description.class);
    when(mockDescription.getMethodName()).thenReturn(name);

    SerializableTestName instance = new SerializableTestName();
    instance.starting(mockDescription);

    assertThat(instance.getMethodName()).isEqualTo(name);

    SerializableTestName cloned = SerializationUtils.clone(instance);

    assertThat(cloned.getMethodName()).isEqualTo(name);
  }
}
