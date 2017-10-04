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
package org.apache.geode.test.dunit.rules.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.TestRunner;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class SharedErrorCollectorTest {

  static final String MESSAGE = "Failure message";

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Test
  public void errorCollectorHasExpectedField() throws Exception {
    Field errorsField = ErrorCollector.class.getDeclaredField("errors");
    assertThat(errorsField.getDeclaringClass()).isEqualTo(ErrorCollector.class);
  }

  @Test
  public void checkThatFailureInControllerIsReported() throws Exception {
    Result result = TestRunner.runTest(CheckThatFailsInController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(AssertionError.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  public void addErrorInControllerIsReported() throws Exception {
    Result result = TestRunner.runTest(AddErrorInController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(NullPointerException.class)
        .hasMessage(MESSAGE);
  }

  @Test
  public void checkThatFailureInDUnitVMIsReported() throws Exception {
    Result result = TestRunner.runTest(CheckThatFailsInDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(AssertionError.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  public void checkThatFailureInEveryDUnitVMIsReported() throws Exception {
    Result result = TestRunner.runTest(CheckThatFailsInEveryDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(4);
    int i = 0;
    for (Failure failure : failures) {
      assertThat(failure.getException()).isInstanceOf(AssertionError.class)
          .hasMessageContaining(MESSAGE + " in VM-" + i++);
    }
  }

  @Test
  public void checkThatFailureInEveryDUnitVMAndControllerIsReported() throws Exception {
    Result result = TestRunner.runTest(CheckThatFailsInEveryDUnitVMAndController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(5);
    boolean first = true;
    int i = 0;
    for (Failure failure : failures) {
      if (first) {
        assertThat(failure.getException()).isInstanceOf(AssertionError.class)
            .hasMessageContaining(MESSAGE + " in VM-CONTROLLER");
        first = false;
      } else {
        assertThat(failure.getException()).isInstanceOf(AssertionError.class)
            .hasMessageContaining(MESSAGE + " in VM-" + i++);
      }
    }
  }

  @Test
  public void checkThatFailureInMethodInDUnitVMIsReported() throws Exception {
    Result result = TestRunner.runTest(CheckThatFailsInMethodInDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(AssertionError.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  public void addErrorInDUnitVMIsReported() throws Exception {
    Result result = TestRunner.runTest(AddErrorInDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(NullPointerException.class)
        .hasMessage(MESSAGE);
  }

  @Test
  public void addErrorInEveryDUnitVMIsReported() throws Exception {
    Result result = TestRunner.runTest(AddErrorInEveryDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(4);
    int i = 0;
    for (Failure failure : failures) {
      assertThat(failure.getException()).isInstanceOf(NullPointerException.class)
          .hasMessageContaining(MESSAGE + " in VM-" + i++);
    }
  }

  @Test
  public void addErrorInEveryDUnitVMAndControllerIsReported() throws Exception {
    Result result = TestRunner.runTest(AddErrorInEveryDUnitVMAndController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(5);
    boolean first = true;
    int i = 0;
    for (Failure failure : failures) {
      if (first) {
        assertThat(failure.getException()).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(MESSAGE + " in VM-CONTROLLER");
        first = false;
      } else {
        assertThat(failure.getException()).isInstanceOf(NullPointerException.class)
            .hasMessageContaining(MESSAGE + " in VM-" + i++);
      }
    }
  }

  @Test
  public void addErrorInMethodInDUnitVMIsReported() throws Exception {
    Result result = TestRunner.runTest(AddErrorInMethodInDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(NullPointerException.class)
        .hasMessage(MESSAGE);
  }

  /**
   * Used by test {@link #checkThatFailureInControllerIsReported()}
   */
  public static class CheckThatFailsInController {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void assertionFailsInController() throws Exception {
      errorCollector.checkThat(MESSAGE, false, is(true));
    }
  }

  /**
   * Used by test {@link #addErrorInControllerIsReported()}
   */
  public static class AddErrorInController {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void exceptionInController() throws Exception {
      errorCollector.addError(new NullPointerException(MESSAGE));
    }
  }

  /**
   * Used by test {@link #checkThatFailureInDUnitVMIsReported()}
   */
  public static class CheckThatFailsInDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void assertionFailsInDUnitVM() throws Exception {
      Host.getHost(0).getVM(0).invoke(() -> errorCollector.checkThat(MESSAGE, false, is(true)));
    }
  }

  /**
   * Used by test {@link #checkThatFailureInEveryDUnitVMIsReported()}
   */
  public static class CheckThatFailsInEveryDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void assertionFailsInEveryDUnitVM() throws Exception {
      for (VM vm : Host.getHost(0).getAllVMs()) {
        vm.invoke(
            () -> errorCollector.checkThat(MESSAGE + " in VM-" + vm.getId(), false, is(true)));
      }
    }
  }

  /**
   * Used by test {@link #checkThatFailureInEveryDUnitVMAndControllerIsReported()}
   */
  public static class CheckThatFailsInEveryDUnitVMAndController implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void assertionFailsInEveryDUnitVM() throws Exception {
      errorCollector.checkThat(MESSAGE + " in VM-CONTROLLER", false, is(true));
      for (VM vm : Host.getHost(0).getAllVMs()) {
        vm.invoke(
            () -> errorCollector.checkThat(MESSAGE + " in VM-" + vm.getId(), false, is(true)));
      }
    }
  }

  /**
   * Used by test {@link #checkThatFailureInMethodInDUnitVMIsReported()}
   */
  public static class CheckThatFailsInMethodInDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void assertionFailsInDUnitVM() throws Exception {
      Host.getHost(0).getVM(0).invoke(() -> checkThat());
    }

    private void checkThat() {
      errorCollector.checkThat(MESSAGE, false, is(true));
    }
  }

  /**
   * Used by test {@link #addErrorInDUnitVMIsReported()}
   */
  public static class AddErrorInDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void exceptionInDUnitVM() throws Exception {
      Host.getHost(0).getVM(0)
          .invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE)));
    }
  }

  /**
   * Used by test {@link #addErrorInEveryDUnitVMIsReported()}
   */
  public static class AddErrorInEveryDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void exceptionInEveryDUnitVM() throws Exception {
      for (VM vm : Host.getHost(0).getAllVMs()) {
        vm.invoke(() -> errorCollector
            .addError(new NullPointerException(MESSAGE + " in VM-" + vm.getId())));
      }
    }
  }

  /**
   * Used by test {@link #addErrorInEveryDUnitVMAndControllerIsReported()}
   */
  public static class AddErrorInEveryDUnitVMAndController implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void exceptionInEveryDUnitVM() throws Exception {
      errorCollector.addError(new NullPointerException(MESSAGE + " in VM-CONTROLLER"));
      for (VM vm : Host.getHost(0).getAllVMs()) {
        vm.invoke(() -> errorCollector
            .addError(new NullPointerException(MESSAGE + " in VM-" + vm.getId())));
      }
    }
  }

  /**
   * Used by test {@link #addErrorInMethodInDUnitVMIsReported()}
   */
  public static class AddErrorInMethodInDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void exceptionInDUnitVM() throws Exception {
      Host.getHost(0).getVM(0).invoke(() -> addError());
    }

    private void addError() {
      errorCollector.addError(new NullPointerException(MESSAGE));
    }
  }
}
