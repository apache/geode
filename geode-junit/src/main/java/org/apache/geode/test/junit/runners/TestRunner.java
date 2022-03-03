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
package org.apache.geode.test.junit.runners;

import static java.util.Arrays.asList;
import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.assertj.core.util.Streams;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runners.model.MultipleFailureException;


/**
 * Used by JUnit rule unit tests to execute inner test cases.
 */
public class TestRunner {

  private TestRunner() {
    // do not instantiate
  }

  public static Result runTest(Class<?> test) {
    JUnitCore junitCore = new JUnitCore();
    return junitCore.run(Request.aClass(test).getRunner());
  }

  public static Result runTestWithValidation(Class<?> test) {
    JUnitCore junitCore = new JUnitCore();
    Result result = junitCore.run(Request.aClass(test).getRunner());

    List<Failure> failures = result.getFailures();
    if (!failures.isEmpty()) {
      List<Throwable> errors = new ArrayList<>();
      for (Failure failure : failures) {
        errors.add(failure.getException());
      }
      try {
        MultipleFailureException.assertEmpty(errors);
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }

    assertThat(result.wasSuccessful()).isTrue();

    return result;
  }

  public static List<Failure> runTestWithExpectedFailures(Class<?> test,
      Throwable... expectedThrowables) {
    return runTestWithExpectedFailures(test, asList(expectedThrowables));
  }

  public static List<Failure> runTestWithExpectedFailures(Class<?> test,
      List<Throwable> expectedThrowables) {
    List<FailureInfo> expectedFailures = Streams.stream(expectedThrowables)
        .map(t -> new FailureInfo(t.getClass(), t.getMessage()))
        .collect(Collectors.toList());

    JUnitCore junitCore = new JUnitCore();
    Result result = junitCore.run(Request.aClass(test).getRunner());

    List<Failure> failures = result.getFailures();
    assertThat(failures)
        .as("Actual failures")
        .hasSameSizeAs(expectedFailures);

    List<FailureInfo> actualFailures = Streams.stream(failures)
        .map(t -> new FailureInfo(t.getException().getClass(), t.getMessage()))
        .collect(Collectors.toList());

    assertThat(actualFailures)
        .as("Actual failures info (Throwable and message)")
        .hasSameElementsAs(expectedFailures);

    return failures;
  }

  @SafeVarargs
  public static List<Failure> runTestWithExpectedFailureTypes(Class<?> test,
      Class<? extends Throwable>... expectedThrowables) {
    return runTestWithExpectedFailureTypes(test, asList(expectedThrowables));
  }

  public static List<Failure> runTestWithExpectedFailureTypes(Class<?> test,
      List<Class<? extends Throwable>> expectedThrowables) {
    List<FailureTypeInfo> expectedFailures = Streams.stream(expectedThrowables)
        .map(FailureTypeInfo::new)
        .collect(Collectors.toList());

    JUnitCore junitCore = new JUnitCore();
    Result result = junitCore.run(Request.aClass(test).getRunner());

    List<Failure> failures = result.getFailures();
    assertThat(failures)
        .as("Actual failures")
        .hasSameSizeAs(expectedFailures);

    List<FailureTypeInfo> actualFailures = Streams.stream(failures)
        .map(t -> new FailureTypeInfo(t.getException().getClass()))
        .collect(Collectors.toList());

    assertThat(actualFailures)
        .as("Actual failures info (Throwable and message)")
        .hasSameElementsAs(expectedFailures);

    return failures;
  }

  private static class FailureInfo {

    private final Class<? extends Throwable> thrownClass;
    private final String expectedMessage;

    private FailureInfo(Class<? extends Throwable> thrownClass, String expectedMessage) {
      this.thrownClass = requireNonNull(thrownClass);
      this.expectedMessage = requireNonNull(expectedMessage);
    }

    @Override
    public int hashCode() {
      return hash(thrownClass, expectedMessage);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      FailureInfo that = (FailureInfo) obj;
      return thrownClass.equals(that.thrownClass) &&
          (expectedMessage.contains(that.expectedMessage) ||
              that.expectedMessage.contains(expectedMessage));
    }

    @Override
    public String toString() {
      return "FailureInfo{" +
          "thrownClass=" + thrownClass +
          ", expectedMessage=" + expectedMessage +
          '}';
    }
  }

  private static class FailureTypeInfo {

    private final Class<? extends Throwable> thrownClass;

    private FailureTypeInfo(Class<? extends Throwable> thrownClass) {
      this.thrownClass = thrownClass;
    }

    @Override
    public int hashCode() {
      return hash(thrownClass);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      FailureTypeInfo that = (FailureTypeInfo) obj;
      return thrownClass.equals(that.thrownClass);
    }

    @Override
    public String toString() {
      return "FailureTypeInfo{" +
          "thrownClass=" + thrownClass +
          '}';
    }
  }
}
