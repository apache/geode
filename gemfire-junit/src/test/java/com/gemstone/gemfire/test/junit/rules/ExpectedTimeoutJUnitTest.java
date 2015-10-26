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
package com.gemstone.gemfire.test.junit.rules;

import static org.hamcrest.core.StringContains.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for ExpectedTimeout JUnit Rule.
 * 
 * @author Kirk Lund
 * @since 8.2
 */
@Category(UnitTest.class)
public class ExpectedTimeoutJUnitTest {

  @Test
  public void passesUnused() {
    Result result = runTest(PassesUnused.class);
    assertTrue(result.wasSuccessful());
  }
  
  @Test
  public void failsWithoutExpectedException() {
    Result result = runTest(FailsWithoutExpectedException.class);
    assertFalse(result.wasSuccessful());
    List<Failure> failures = result.getFailures();
    assertEquals(1, failures.size());
    Failure failure = failures.get(0);
    assertThat(failure.getException(), is(instanceOf(AssertionError.class)));
    assertThat(failure.getException().getMessage(), containsString("Expected test to throw an instance of " + TimeoutException.class.getName()));
  }
  
  @Test
  public void failsWithoutExpectedTimeoutException() {
    Result result = runTest(FailsWithoutExpectedTimeoutException.class);
    assertFalse(result.wasSuccessful());
    List<Failure> failures = result.getFailures();
    assertEquals(1, failures.size());
    Failure failure = failures.get(0);
    assertThat(failure.getException(), is(instanceOf(AssertionError.class)));
    assertThat(failure.getException().getMessage(), containsString("Expected test to throw (an instance of " + TimeoutException.class.getName() + " and exception with message a string containing \"" + FailsWithoutExpectedTimeoutException.message + "\")"));
  }
  
  @Test
  public void failsWithExpectedTimeoutButWrongError() {
    Result result = runTest(FailsWithExpectedTimeoutButWrongError.class);
    assertFalse(result.wasSuccessful());
    List<Failure> failures = result.getFailures();
    assertEquals(1, failures.size());
    Failure failure = failures.get(0);
    assertThat(failure.getException(), is(instanceOf(AssertionError.class)));
    assertThat(failure.getException().getMessage(), containsString(NullPointerException.class.getName()));
  }
  
  @Test
  public void passesWithExpectedTimeoutAndTimeoutException() {
    Result result = runTest(PassesWithExpectedTimeoutAndTimeoutException.class);
    assertTrue(result.wasSuccessful());
  }
  
  @Test
  public void failsWhenTimeoutIsEarly() {
    Result result = runTest(FailsWhenTimeoutIsEarly.class);
    assertFalse(result.wasSuccessful());
    List<Failure> failures = result.getFailures();
    assertEquals(1, failures.size());
    Failure failure = failures.get(0);
    assertThat(failure.getException(), is(instanceOf(AssertionError.class)));
    assertThat(failure.getException().getMessage(), containsString("Expected test to throw (an instance of " + TimeoutException.class.getName() + " and exception with message a string containing \"" + FailsWhenTimeoutIsEarly.message + "\")"));
  }
  
  @Test
  public void failsWhenTimeoutIsLate() {
    Result result = runTest(FailsWhenTimeoutIsLate.class);
    assertFalse(result.wasSuccessful());
    List<Failure> failures = result.getFailures();
    assertEquals(1, failures.size());
    Failure failure = failures.get(0);
    assertThat(failure.getException(), is(instanceOf(AssertionError.class)));
    assertThat(failure.getException().getMessage(), containsString("Expected test to throw (an instance of " + TimeoutException.class.getName() + " and exception with message a string containing \"" + FailsWhenTimeoutIsLate.message + "\")"));
  }
  
  private static Result runTest(Class<?> test) {
    JUnitCore junitCore = new JUnitCore();
    return junitCore.run(Request.aClass(test).getRunner());
  }
  
  public static class AbstractExpectedTimeoutTest {
    @Rule
    public ExpectedTimeout timeout = ExpectedTimeout.none();
  }
  
  public static class PassesUnused extends AbstractExpectedTimeoutTest {
    @Test
    public void passesUnused() throws Exception {
    }
  }
  
  public static class FailsWithoutExpectedException extends AbstractExpectedTimeoutTest {
    @Test
    public void failsWithoutExpectedException() throws Exception {
      timeout.expect(TimeoutException.class);
    }
  }
  
  public static class FailsWithoutExpectedTimeoutException extends AbstractExpectedTimeoutTest {
    public static final String message = "this is a message for FailsWithoutExpectedTimeoutException";
    @Test
    public void failsWithoutExpectedTimeoutAndTimeoutException() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(1000);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
    }
  }
  
  public static class FailsWithExpectedTimeoutButWrongError extends AbstractExpectedTimeoutTest {
    public static final String message = "this is a message for FailsWithExpectedTimeoutButWrongError";
    @Test
    public void failsWithExpectedTimeoutButWrongError() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(1000);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
      throw new NullPointerException();
    }
  }

  public static class PassesWithExpectedTimeoutAndTimeoutException extends AbstractExpectedTimeoutTest {
    public static final String message = "this is a message for PassesWithExpectedTimeoutAndTimeoutException";
    public static final Class<TimeoutException> exceptionClass = TimeoutException.class;
    @Test
    public void passesWithExpectedTimeoutAndTimeoutException() throws Exception {
      timeout.expect(exceptionClass);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(1000);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
      throw new TimeoutException(message);
    }
  }

  public static class FailsWhenTimeoutIsEarly extends AbstractExpectedTimeoutTest {
    public static final String message = "this is a message for FailsWhenTimeoutIsEarly";
    @Test
    public void failsWhenTimeoutIsEarly() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(1000);
      timeout.expectMaximumDuration(2000);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(10);
    }
  }

  public static class FailsWhenTimeoutIsLate extends AbstractExpectedTimeoutTest {
    public static final String message = "this is a message for FailsWhenTimeoutIsLate";
    @Test
    public void failsWhenTimeoutIsLate() throws Exception {
      timeout.expect(TimeoutException.class);
      timeout.expectMessage(message);
      timeout.expectMinimumDuration(10);
      timeout.expectMaximumDuration(20);
      timeout.expectTimeUnit(TimeUnit.MILLISECONDS);
      Thread.sleep(100);
    }
  }
}
