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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.rules.ExternalResource;


/**
 * Unit tests for {@link SerializableExternalResource}.
 */
public class SerializableExternalResourceTest {

  @Test
  public void hasZeroFields() throws Exception {
    Field[] fields = ExternalResource.class.getDeclaredFields();
    assertThat(fields.length).as("Fields: " + Arrays.asList(fields)).isEqualTo(0);
  }

  @Test
  public void isSerializable() throws Exception {
    assertThat(SerializableExternalResource.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void canBeSerialized() throws Throwable {
    FakeSerializableExternalResource instance = new FakeSerializableExternalResource().value(1);

    FakeSerializableExternalResource cloned =
        SerializationUtils.clone(instance);

    assertThat(instance.value()).isEqualTo(1);
    assertThat(cloned.value()).isEqualTo(1);

    instance.value(2);

    assertThat(instance.value()).isEqualTo(2);
    assertThat(cloned.value()).isEqualTo(1);
  }

  /**
   * Fake SerializableExternalResource with a simple int field.
   */
  private static class FakeSerializableExternalResource extends SerializableExternalResource {

    private int value = -1;

    public FakeSerializableExternalResource value(final int value) {
      this.value = value;
      return this;
    }

    public int value() {
      return value;
    }
  }
}
