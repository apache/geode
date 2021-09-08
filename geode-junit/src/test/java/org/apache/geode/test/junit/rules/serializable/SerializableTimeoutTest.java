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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.junit.rules.serializable.FieldSerializationUtils.readField;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTimeout.FIELD_LOOK_FOR_STUCK_THREAD;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTimeout.FIELD_TIMEOUT;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTimeout.FIELD_TIME_UNIT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.rules.Timeout;


/**
 * Unit tests for {@link SerializableTimeout}.
 */
public class SerializableTimeoutTest {

  @Test
  public void hasThreeFields() {
    Field[] fields = Timeout.class.getDeclaredFields();
    assertThat(fields.length).as("Fields: " + Arrays.asList(fields)).isEqualTo(3);
  }

  @Test
  public void fieldTimeoutShouldExist() throws Exception {
    Field field = Timeout.class.getDeclaredField(FIELD_TIMEOUT);
    assertThat(field.getType()).isEqualTo(Long.TYPE);
  }

  @Test
  public void fieldTimeUnitShouldExist() throws Exception {
    Field field = Timeout.class.getDeclaredField(FIELD_TIME_UNIT);
    assertThat(field.getType()).isEqualTo(TimeUnit.class);
  }

  @Test
  public void fieldLookForStuckThreadShouldExist() throws Exception {
    Field field = Timeout.class.getDeclaredField(FIELD_LOOK_FOR_STUCK_THREAD);
    assertThat(field.getType()).isEqualTo(Boolean.TYPE);
  }

  @Test
  public void fieldsCanBeRead() {
    long timeout = 1000;
    TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    boolean lookingForStuckThread = false;

    SerializableTimeout instance = SerializableTimeout.builder().withTimeout(timeout, timeUnit)
        .withLookingForStuckThread(lookingForStuckThread).build();

    assertThat(readField(Timeout.class, instance, FIELD_TIMEOUT)).isEqualTo(timeout);
    assertThat(readField(Timeout.class, instance, FIELD_TIME_UNIT)).isEqualTo(timeUnit);
    assertThat(readField(Timeout.class, instance, FIELD_LOOK_FOR_STUCK_THREAD))
        .isEqualTo(lookingForStuckThread);
  }

  @Test
  public void isSerializable() {
    SerializableTimeout serializableTimeout =
        new SerializableTimeout(SerializableTimeout.builder());

    assertThat(serializableTimeout).isInstanceOf(Serializable.class);
  }

  @Test
  public void serializes() {
    SerializableTimeout serializableTimeout =
        new SerializableTimeout(SerializableTimeout.builder());

    SerializableTimeout clone = SerializationUtils.clone(serializableTimeout);

    assertThat(clone.timeout(SECONDS)).isEqualTo(serializableTimeout.timeout(SECONDS));
  }

  @Test
  public void canBeSerialized() {
    long timeout = 2;
    TimeUnit timeUnit = SECONDS;
    boolean lookingForStuckThread = true;

    SerializableTimeout instance = SerializableTimeout.builder().withTimeout(timeout, timeUnit)
        .withLookingForStuckThread(lookingForStuckThread).build();

    assertThat(readField(Timeout.class, instance, FIELD_TIMEOUT)).isEqualTo(timeout);
    assertThat(readField(Timeout.class, instance, FIELD_TIME_UNIT)).isEqualTo(timeUnit);
    assertThat(readField(Timeout.class, instance, FIELD_LOOK_FOR_STUCK_THREAD))
        .isEqualTo(lookingForStuckThread);

    SerializableTimeout cloned = (SerializableTimeout) SerializationUtils.clone(instance);

    assertThat(readField(Timeout.class, cloned, FIELD_TIMEOUT)).isEqualTo(timeout);
    assertThat(readField(Timeout.class, cloned, FIELD_TIME_UNIT)).isEqualTo(timeUnit);
    assertThat(readField(Timeout.class, cloned, FIELD_LOOK_FOR_STUCK_THREAD))
        .isEqualTo(lookingForStuckThread);
  }
}
