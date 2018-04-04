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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Used by JUnit rule unit tests to execute inner test cases.
 */
public class TestRunner {

  protected TestRunner() {}

  public static Result runTest(final Class<?> test) {
    JUnitCore junitCore = new JUnitCore();
    return junitCore.run(Request.aClass(test).getRunner());
  }

  public static Result runTestWithValidation(final Class<?> test) {
    JUnitCore junitCore = new JUnitCore();
    Result result = junitCore.run(Request.aClass(test).getRunner());

    List<Failure> failures = result.getFailures();
    if (!failures.isEmpty()) {
      Failure firstFailure = failures.get(0);
      throw new AssertionError(firstFailure.getException());
    }

    assertThat(result.wasSuccessful()).isTrue();

    return result;
  }

  public static Failure runTestWithExpectedFailure(final Class<?> test) {
    JUnitCore junitCore = new JUnitCore();
    Result result = junitCore.run(Request.aClass(test).getRunner());

    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);

    return failures.get(0);
  }

  public static List<Failure> runTestWithExpectedFailures(final Class<?> test) {
    JUnitCore junitCore = new JUnitCore();
    Result result = junitCore.run(Request.aClass(test).getRunner());

    List<Failure> failures = result.getFailures();
    assertThat(failures).isNotEmpty();

    return failures;
  }
}
