/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.security;

import static com.googlecode.catchexception.CatchException.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

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

import com.gemstone.gemfire.test.junit.categories.SecurityTest;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link NotAuthorizedException}.
 */
@Category({ UnitTest.class, SecurityTest.class })
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
    this.message = testName.getMethodName() + " message";
    this.causeMessage = testName.getMethodName() + " cause message";

    this.nonSerializableResolvedObj = new Object();
    this.nonSerializableNamingException = new NamingException(this.causeMessage);
    this.nonSerializableNamingException.setResolvedObj(this.nonSerializableResolvedObj);

    this.serializableResolvedObj = new SerializableObject(this.testName.getMethodName());
    this.serializableNamingException = new NamingException(this.causeMessage);
    this.serializableNamingException.setResolvedObj(this.serializableResolvedObj);

    this.principalName = "jsmith";
    this.nonSerializablePrincipal = mock(Principal.class);
    this.serializablePrincipal = new SerializablePrincipal(this.principalName);

    assertPreconditions();
  }

  private void assertPreconditions() {
    catchException(this).clone(this.nonSerializableNamingException);
    assertThat((Throwable)caughtException()).isNotNull();
    assertThat((Throwable)caughtException().getCause()).isInstanceOf(NotSerializableException.class);

    catchException(this).clone(this.serializableNamingException);
    assertThat((Throwable)caughtException()).isNull();

    assertThat(this.nonSerializableResolvedObj).isNotInstanceOf(Serializable.class);

    catchException(this).clone(this.serializableResolvedObj);
    assertThat((Throwable)caughtException()).isNull();

    assertThat(this.nonSerializablePrincipal).isNotInstanceOf(Serializable.class);

    catchException(this).clone(this.serializablePrincipal);
    assertThat((Throwable)caughtException()).isNull();
  }

  @Test
  public void isSerializable() throws Exception {
    assertThat(NotAuthorizedException.class).isInstanceOf(Serializable.class);
  }

  @Test
  public void serializes() throws Exception {
    NotAuthorizedException instance = new NotAuthorizedException(this.message);

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(this.message);
  }

  @Test
  public void serializesWithThrowable() throws Exception {
    Throwable cause = new Exception(this.causeMessage);
    NotAuthorizedException instance = new NotAuthorizedException(this.message, cause);

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(this.message);
    assertThat(cloned).hasCause(cause);
  }

  @Test
  public void serializesWithNonSerializablePrincipal() throws Exception {
    NotAuthorizedException instance = new NotAuthorizedException(this.message, this.nonSerializablePrincipal);
    assertThat(instance.getPrincipal()).isNotNull();

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(this.message);
    assertThat(cloned.getPrincipal()).isNull();
  }

  @Test
  public void serializesWithSerializablePrincipal() throws Exception {
    NotAuthorizedException instance = new NotAuthorizedException(this.message, this.serializablePrincipal);

    NotAuthorizedException cloned = (NotAuthorizedException) SerializationUtils.clone(instance);

    assertThat(cloned).hasMessage(this.message);
    assertThat(cloned.getPrincipal()).isNotNull().isEqualTo(this.serializablePrincipal);
  }

  public Object clone(final Serializable object) {
    return SerializationUtils.clone(object);
  }

  public static class SerializableObject implements Serializable {

    private String name;

    SerializableObject(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SerializableObject that = (SerializableObject) o;

      return name != null ? name.equals(that.name) : that.name == null;

    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }

  public static class SerializablePrincipal implements Principal, Serializable {

    private String name;

    SerializablePrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return this.name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SerializablePrincipal that = (SerializablePrincipal) o;

      return name != null ? name.equals(that.name) : that.name == null;

    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }
}
