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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Result;

import org.apache.geode.test.junit.runners.TestRunner;

public class CloseableReferenceTest {

  private static final AtomicReference<CloseableReference<?>> RULE = new AtomicReference<>();
  private static final AtomicReference<Object> VALUE = new AtomicReference<>();

  @After
  public void tearDown() {
    RULE.set(null);
    VALUE.set(null);
  }

  @Test
  public void closesAutoCloseable() throws Exception {
    VALUE.set(mock(AutoCloseable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    AutoCloseable value = uncheckedCast(VALUE.get());
    verify(value).close();
  }

  @Test
  public void nullsOutAutoCloseable() {
    VALUE.set(mock(AutoCloseable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    CloseableReference<AutoCloseable> reference = uncheckedCast(RULE.get());
    assertThat(reference.get()).isNull();
  }

  @Test
  public void closesCloseable() throws IOException {
    VALUE.set(mock(Closeable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    Closeable value = uncheckedCast(VALUE.get());
    verify(value).close();
  }

  @Test
  public void nullsOutCloseable() {
    VALUE.set(mock(Closeable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    CloseableReference<Closeable> reference = uncheckedCast(RULE.get());
    assertThat(reference.get()).isNull();
  }

  @Test
  public void disconnectDisconnectable() {
    VALUE.set(mock(Disconnectable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    Disconnectable value = uncheckedCast(VALUE.get());
    verify(value).disconnect();
  }

  @Test
  public void nullsOutDisconnectable() {
    VALUE.set(mock(Disconnectable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    CloseableReference<Disconnectable> reference = uncheckedCast(RULE.get());
    assertThat(reference.get()).isNull();
  }

  @Test
  public void disconnectStoppable() {
    VALUE.set(mock(Stoppable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    Stoppable value = uncheckedCast(VALUE.get());
    verify(value).stop();
  }

  @Test
  public void nullsOutStoppable() {
    VALUE.set(mock(Stoppable.class));

    Result result = TestRunner.runTest(WithCloseableReference.class);
    assertThat(result.wasSuccessful()).isTrue();

    CloseableReference<Stoppable> reference = uncheckedCast(RULE.get());
    assertThat(reference.get()).isNull();
  }

  @Test
  public void skipsCloseIfAutoCloseIsFalse() {
    VALUE.set(mock(AutoCloseable.class));

    Result result = TestRunner.runTest(WithAutoCloseFalse.class);
    assertThat(result.wasSuccessful()).isTrue();

    AutoCloseable value = uncheckedCast(VALUE.get());
    verifyZeroInteractions(value);
  }

  @Test
  public void nullsOutValueEvenIfAutoCloseIsFalse() {
    VALUE.set(mock(AutoCloseable.class));

    Result result = TestRunner.runTest(WithAutoCloseFalse.class);
    assertThat(result.wasSuccessful()).isTrue();

    CloseableReference<AutoCloseable> reference = uncheckedCast(RULE.get());
    assertThat(reference.get()).isNull();
  }

  private static void capture(CloseableReference<?> closeableReference) {
    RULE.set(closeableReference);
    closeableReference.set(uncheckedCast(VALUE.get()));
  }

  public static class WithCloseableReference {

    @Rule
    public CloseableReference<?> closeableReference = new CloseableReference<>();

    @Before
    public void setUp() {
      capture(closeableReference);
    }

    @Test
    public void notClosed() {
      verifyZeroInteractions(closeableReference.get());
    }
  }

  public static class WithAutoCloseFalse {

    @Rule
    public CloseableReference<?> closeableReference = new CloseableReference<>().autoClose(false);

    @Before
    public void setUp() {
      capture(closeableReference);
    }

    @Test
    public void notClosed() {
      verifyZeroInteractions(closeableReference.get());
    }
  }

  @FunctionalInterface
  private interface Disconnectable {

    void disconnect();
  }

  @FunctionalInterface
  private interface Stoppable {

    void stop();
  }
}
