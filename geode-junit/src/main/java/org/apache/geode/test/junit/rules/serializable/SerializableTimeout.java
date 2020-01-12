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
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTimeout.FIELD_LOOK_FOR_STUCK_THREAD;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTimeout.FIELD_TIMEOUT;
import static org.apache.geode.test.junit.rules.serializable.FieldsOfTimeout.FIELD_TIME_UNIT;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.junit.rules.Timeout;

/**
 * Serializable subclass of {@link org.junit.rules.Timeout Timeout}. All instance variables of
 * {@code Timeout} are serialized by reflection.
 */
public class SerializableTimeout extends Timeout implements SerializableTestRule {

  public static Builder builder() {
    return new Builder();
  }

  public SerializableTimeout(final long timeout, final TimeUnit timeUnit) {
    super(timeout, timeUnit);
  }

  protected SerializableTimeout(final Builder builder) {
    super(builder);
  }

  private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("SerializationProxy required");
  }

  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  /**
   * Builder for {@code SerializableTimeout}.
   */
  public static class Builder extends Timeout.Builder {

    protected Builder() {
      super();
    }

    @Override
    public Builder withTimeout(final long timeout, final TimeUnit unit) {
      super.withTimeout(timeout, unit);
      return this;
    }

    @Override
    public Builder withLookingForStuckThread(final boolean enable) {
      super.withLookingForStuckThread(enable);
      return this;
    }

    @Override
    public SerializableTimeout build() {
      return new SerializableTimeout(this);
    }
  }

  /**
   * Serialization proxy for {@code SerializableTimeout}.
   */
  private static class SerializationProxy implements Serializable {

    private final long timeout;
    private final TimeUnit timeUnit;
    private final boolean lookForStuckThread;

    SerializationProxy(final SerializableTimeout instance) {
      this.timeout = (long) readField(Timeout.class, instance, FIELD_TIMEOUT);
      this.timeUnit = (TimeUnit) readField(Timeout.class, instance, FIELD_TIME_UNIT);
      this.lookForStuckThread =
          (boolean) readField(Timeout.class, instance, FIELD_LOOK_FOR_STUCK_THREAD);
    }

    private Object readResolve() {
      return new SerializableTimeout.Builder().withTimeout(this.timeout, this.timeUnit)
          .withLookingForStuckThread(this.lookForStuckThread).build();
    }
  }
}
