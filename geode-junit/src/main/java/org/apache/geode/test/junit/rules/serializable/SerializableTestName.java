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

import static org.apache.geode.test.junit.rules.serializable.FieldSerializationUtils.readField;
import static org.apache.geode.test.junit.rules.serializable.FieldSerializationUtils.writeField;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTestName.FIELD_NAME;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import org.junit.rules.TestName;
import org.junit.runner.Description;

/**
 * Serializable subclass of {@link TestName TestName}. All instance variables of
 * {@code TestName} are serialized by reflection.
 */
public class SerializableTestName extends TestName implements SerializableTestRule {

  @Override
  protected void starting(final Description description) {
    super.starting(description);
  }

  private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("SerializationProxy required");
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  /**
   * Serialization proxy for {@code SerializableTestName}.
   */
  private static class SerializationProxy implements Serializable {

    private final String name;

    SerializationProxy(final SerializableTestName instance) {
      name = (String) readField(TestName.class, instance, FIELD_NAME);
    }

    private Object readResolve() {
      SerializableTestName instance = new SerializableTestName();
      writeField(TestName.class, instance, FIELD_NAME, name);
      return instance;
    }
  }
}
