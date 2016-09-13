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
package com.gemstone.gemfire.internal.lang;

import static org.assertj.core.api.Assertions.*;
import static com.googlecode.catchexception.CatchException.*;
import static com.googlecode.catchexception.CatchException.caughtException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link ThrowableUtils}
 */
@Category(UnitTest.class)
public class ThrowableUtilsTest {

  @Test
  public void getRootCauseOfNullShouldThrowNullPointerException() {
    catchException(this).getRootCause(null);

    assertThat((Exception)caughtException()).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void getRootCauseOfLeafExceptionShouldReturnSameInstance() {
    Throwable exception = new Exception();
    Throwable rootCause = getRootCause(exception);

    assertThat(rootCause).isSameAs(exception);
  }

  @Test
  public void getRootCauseOfExceptionShouldReturnCause() {
    Throwable cause = new Exception();
    Throwable rootCause = getRootCause(new Exception(cause));

    assertThat(rootCause).isSameAs(cause);
  }

  @Test
  public void getRootCauseOfExceptionTreeShouldReturnCause() {
    Throwable cause = new Exception();
    Throwable rootCause = getRootCause(new Exception(new Exception(cause)));

    assertThat(rootCause).isSameAs(cause);
  }

  @Test
  public void getRootCauseOfErrorTreeShouldReturnCause() {
    Throwable cause = new Error();
    Throwable rootCause = getRootCause(new Error(new Error(cause)));

    assertThat(rootCause).isSameAs(cause);
  }

  @Test
  public void hasCauseTypeOfNullClassShouldThrowNullPointerException() {
    catchException(this).hasCauseType(new Exception(), null);

    assertThat((Exception)caughtException()).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void hasCauseTypeOfNullThrowableShouldThrowNullPointerException() {
    catchException(this).hasCauseType(null, Exception.class);

    assertThat((Exception)caughtException()).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void hasCauseTypeOfNonMatchingShouldReturnFalse() {
    assertThat(hasCauseType(new OneException(), OtherException.class)).isFalse();
  }

  @Test
  public void hasCauseTypeOfSameClassShouldReturnTrue() {
    assertThat(hasCauseType(new OneException(), OneException.class)).isTrue();
  }

  @Test
  public void hasCauseTypeOfSuperClassShouldReturnFalse() {
    assertThat(hasCauseType(new OneException(), SubException.class)).isFalse();
  }

  @Test
  public void hasCauseTypeOfSubClassShouldReturnTrue() {
    assertThat(hasCauseType(new SubException(), OneException.class)).isTrue();
  }

  @Test
  public void hasCauseTypeOfWrappedClassShouldReturnTrue() {
    assertThat(hasCauseType(new OneException(new TwoException()), TwoException.class)).isTrue();
  }

  @Test
  public void hasCauseTypeOfWrappingClassShouldReturnTrue() {
    assertThat(hasCauseType(new OneException(new TwoException()), OneException.class)).isTrue();
  }

  @Test
  public void hasCauseTypeOfNestedClassShouldReturnTrue() {
    assertThat(hasCauseType(new OneException(new TwoException(new OtherException())), OtherException.class)).isTrue();
  }

  @Test
  public void hasCauseMessageForNullShouldThrowNullPointerException() {
    catchException(this).hasCauseMessage(null, "message");

    assertThat((Exception)caughtException()).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void hasCauseMessageOfNullShouldThrowNullPointerException() {
    catchException(this).hasCauseMessage(new OneException(), null);

    assertThat((Exception)caughtException()).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void hasCauseMessageForNullMessageShouldThrowNullPointerException() {
    catchException(this).hasCauseMessage(new OneException((String)null), null);

    assertThat((Exception)caughtException()).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void hasCauseMessageOfNonMatchingNullMessageShouldThrowNullPointerException() {
    catchException(this).hasCauseMessage(new OneException("message"), null);

    assertThat((Exception)caughtException()).isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void hasCauseMessageOfEmptyMessageShouldReturnTrue() {
    assertThat(hasCauseMessage(new OneException(""), "")).isTrue();
  }

  @Test
  public void hasCauseMessageOfMatchingMessageShouldReturnTrue() {
    assertThat(hasCauseMessage(new OneException("message"), "message")).isTrue();
  }

  @Test
  public void hasCauseMessageOfNonMatchingMessageShouldReturnFalse() {
    assertThat(hasCauseMessage(new OneException("non-matching"), "message")).isFalse();
  }

  @Test
  public void hasCauseMessageOfContainedMessageShouldReturnTrue() {
    assertThat(hasCauseMessage(new OneException("this is the message"), "message")).isTrue();
  }

  @Test
  public void hasCauseMessageOfPartialMatchingMessageShouldReturnFalse() {
    assertThat(hasCauseMessage(new OneException("message"), "this is the message")).isFalse();
  }

  public Throwable getRootCause(final Throwable throwable) {
    return ThrowableUtils.getRootCause(throwable);
  }

  public boolean hasCauseType(final Throwable throwable, final Class<? extends Throwable> causeClass) {
    return ThrowableUtils.hasCauseType(throwable, causeClass);
  }

  public boolean hasCauseMessage(final Throwable throwable, final String message) {
    return ThrowableUtils.hasCauseMessage(throwable, message);
  }

  private static class OneException extends Exception {
    public OneException() {
    }
    public OneException(String message) {
      super(message);
    }
    public OneException(Throwable cause) {
      super(cause);
    }
    public OneException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class SubException extends OneException {
    public SubException() {
    }
    public SubException(String message) {
      super(message);
    }
    public SubException(Throwable cause) {
      super(cause);
    }
    public SubException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class TwoException extends Exception {
    public TwoException() {
    }
    public TwoException(String message) {
      super(message);
    }
    public TwoException(Throwable cause) {
      super(cause);
    }
    public TwoException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class OtherException extends Exception {
    public OtherException() {
    }
    public OtherException(String message) {
      super(message);
    }
    public OtherException(Throwable cause) {
      super(cause);
    }
    public OtherException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
