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
import static org.mockito.Mockito.mock;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.security.Principal;

import javax.naming.NamingException;

import org.apache.commons.lang.SerializationUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Unit tests for {@link NotAuthorizedException}.
 */
@Category(SecurityTest.class)
public class NotAuthorizedExceptionTest {

  private String message;
  private String causeMessage;
  private Object nonSerializableResolvedObj;
  private NamingException nonSerializableNamingException;
  private SerializableObject serializableResolvedObj;
  private NamingException serializableNamingException;
  private String principalName;
  private Principal nonSerializablePrincipal;
  private SerializablePrincipal serializablePrincipal;

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

    principalName = "jsmith";
    nonSerializablePrincipal = mock(Principal.class);
    serializablePrincipal = new SerializablePrincipal(principalName);

    assertPreconditions();
  }

  private void assertPreconditions() {
    Throwable thrown =
        catchThrowable(() -> SerializationUtils.clone(nonSerializableNamingException));
    assertThat(thrown).isNotNull();
    assertThat(thrown.getCause()).isInstanceOf(NotSerializableException.class);

    thrown = catchThrowable(() -> SerializationUtils.clone(serializableNamingException));
    assertThat(thrown).isNull();

    assertThat(nonSerializableResolvedObj).isNotInstanceOf(Serializable.class);

    thrown = catchThrowable(() -> SerializationUtils.clone(serializableResolvedObj));
    assertThat(thrown).isNull();

    assertThat(nonSerializablePrincipal).isNotInstanceOf(Serializable.class);

    thrown = catchThrowable(() -> SerializationUtils.clone(serializablePrincipal));
    assertThat(thrown).isNull();
  }

  @Test
  public void isSerializable() {
    assertThat(NotAuthorizedException.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void serializes() {
    NotAuthorizedException instance = new NotAuthorizedException(message);

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message);
  }

  @Test
  public void serializesWithThrowable() {
    Throwable cause = new Exception(causeMessage);
    NotAuthorizedException instance = new NotAuthorizedException(message, cause);

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message);
    assertThat(cloned).hasCause(cause);
  }

  @Test
  public void serializesWithNonSerializablePrincipal() {
    NotAuthorizedException instance =
        new NotAuthorizedException(message, nonSerializablePrincipal);
    assertThat(instance.getPrincipal()).isNotNull();

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message);
    assertThat(cloned.getPrincipal()).isNull();
  }

  @Test
  public void serializesWithSerializablePrincipal() {
    NotAuthorizedException instance =
        new NotAuthorizedException(message, serializablePrincipal);

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(message);
    assertThat(cloned.getPrincipal()).isNotNull().isEqualTo(serializablePrincipal);
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

  private static class SerializablePrincipal implements Principal, Serializable {

    private final String name;

    SerializablePrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      SerializablePrincipal that = (SerializablePrincipal) o;

      return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }
}
