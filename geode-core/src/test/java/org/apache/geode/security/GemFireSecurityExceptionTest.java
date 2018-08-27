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
package org.apache.geode.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.NotSerializableException;
import java.io.Serializable;

import javax.naming.NamingException;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Unit tests for {@link GemFireSecurityException}.
 */
@Category(SecurityTest.class)
public class GemFireSecurityExceptionTest {

  private String message;
  private String causeMessage;
  private Object nonSerializableResolvedObj;
  private NamingException nonSerializableNamingException;
  private SerializableObject serializableResolvedObj;
  private NamingException serializableNamingException;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    message = testName.getMethodName() + " message";
    causeMessage = testName.getMethodName() + " cause message";

    nonSerializableResolvedObj = new Object();
    nonSerializableNamingException = new NamingException(causeMessage);
    nonSerializableNamingException.setResolvedObj(nonSerializableResolvedObj);

    serializableResolvedObj = new SerializableObject(testName.getMethodName());
    serializableNamingException = new NamingException(causeMessage);
    serializableNamingException.setResolvedObj(serializableResolvedObj);

    assertPreConditions();
  }

  private void assertPreConditions() {
    Throwable thrown =
        catchThrowable(() -> SerializationUtils.clone(nonSerializableNamingException));
    assertThat(thrown).isNotNull();
    assertThat(thrown.getCause())
        .isInstanceOf(NotSerializableException.class);

    thrown = catchThrowable(() -> SerializationUtils.clone(serializableNamingException));
    assertThat(thrown).isNull();

    assertThat(nonSerializableResolvedObj).isNotInstanceOf(Serializable.class);

    thrown = catchThrowable(() -> SerializationUtils.clone(serializableResolvedObj));
    assertThat(thrown).isNull();
  }

  @Test
  public void isSerializable() {
    assertThat(GemFireSecurityException.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void serializes() {
    GemFireSecurityException instance = new GemFireSecurityException(message);

    GemFireSecurityException cloned = (GemFireSecurityException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message);
  }

  @Test
  public void serializesWithThrowable() {
    Throwable cause = new Exception(causeMessage);
    GemFireSecurityException instance = new GemFireSecurityException(message, cause);

    GemFireSecurityException cloned = (GemFireSecurityException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message).hasCause(cause);
    assertThat(cloned.getCause()).hasMessage(causeMessage);
  }

  @Test
  public void serializesWithNonSerializableNamingException() {
    GemFireSecurityException instance =
        new GemFireSecurityException(message, nonSerializableNamingException);

    GemFireSecurityException cloned = (GemFireSecurityException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message).hasCause(nonSerializableNamingException);
    NamingException cause = (NamingException) cloned.getCause();
    assertThat(cause).hasMessage(causeMessage);
    assertThat(cause.getResolvedObj()).isNull();
  }

  @Test
  public void serializesWithSerializableNamingException() {
    GemFireSecurityException instance =
        new GemFireSecurityException(message, serializableNamingException);

    GemFireSecurityException cloned = (GemFireSecurityException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message).hasCause(serializableNamingException);
    NamingException cause = (NamingException) cloned.getCause();
    assertThat(cause).hasMessage(causeMessage);
    assertThat(cause.getResolvedObj()).isNotNull().isEqualTo(serializableResolvedObj);
  }

  @Test
  public void isSerializableReturnsTrueForSerializableClass() {
    assertThat(new GemFireSecurityException("").isSerializable(serializableResolvedObj))
        .isTrue();
  }

  @Test
  public void isSerializableReturnsFalseForNonSerializableClass() {
    assertThat(new GemFireSecurityException("").isSerializable(nonSerializableResolvedObj))
        .isFalse();
  }

  private static class SerializableObject implements Serializable {

    private final String name;

    SerializableObject(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      SerializableObject that = (SerializableObject) o;

      return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }
}
