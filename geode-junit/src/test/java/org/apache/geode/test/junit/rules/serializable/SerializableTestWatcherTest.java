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
import org.junit.rules.TestWatcher;


/**
 * Unit tests for {@link SerializableTestWatcher}.
 */
public class SerializableTestWatcherTest {

  @Test
  public void hasZeroFields() throws Exception {
    Field[] fields = TestWatcher.class.getDeclaredFields();
    assertThat(fields.length).as("Fields: " + Arrays.asList(fields)).isEqualTo(0);
  }

  @Test
  public void isSerializable() throws Exception {
    assertThat(SerializableTestWatcher.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void canBeSerialized() throws Exception {
    FakeSerializableTestWatcher instance = new FakeSerializableTestWatcher().value(1);

    FakeSerializableTestWatcher cloned =
        SerializationUtils.clone(instance);

    assertThat(instance.value()).isEqualTo(1);
    assertThat(cloned.value()).isEqualTo(1);

    instance.value(2);

    assertThat(instance.value()).isEqualTo(2);
    assertThat(cloned.value()).isEqualTo(1);
  }

  /**
   * Fake SerializableTestWatcher with a simple int field.
   */
  private static class FakeSerializableTestWatcher extends SerializableTestWatcher {

    private int value = -1;

    private FakeSerializableTestWatcher value(final int value) {
      this.value = value;
      return this;
    }

    private int value() {
      return value;
    }
  }
}
