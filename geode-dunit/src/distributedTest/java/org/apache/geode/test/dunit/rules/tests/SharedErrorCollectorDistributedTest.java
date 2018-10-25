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

import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.runners.TestRunner;

@SuppressWarnings("serial")
public class SharedErrorCollectorDistributedTest {

  static final String MESSAGE = "Failure message";

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Before
  public void setUp() {
    assertThat(getVMCount()).isGreaterThanOrEqualTo(DEFAULT_VM_COUNT);
  }

  @Test
  public void errorCollectorHasExpectedField() throws Exception {
    Field errorsField = ErrorCollector.class.getDeclaredField("errors");
    assertThat(errorsField.getDeclaringClass()).isEqualTo(ErrorCollector.class);
  }

  @Test
  public void checkThatFailureInControllerIsReported() {
    Result result = TestRunner.runTest(CheckThatFailsInController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(AssertionError.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  public void addErrorInControllerIsReported() {
    Result result = TestRunner.runTest(AddErrorInController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(NullPointerException.class)
        .hasMessage(MESSAGE);
  }

  @Test
  public void checkThatFailureInDUnitVMIsReported() {
    Result result = TestRunner.runTest(CheckThatFailsInDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(AssertionError.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  public void checkThatFailureInEveryDUnitVMIsReported() {
    Result result = TestRunner.runTest(CheckThatFailsInEveryDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(getVMCount());
    int i = 0;
    for (Failure failure : failures) {
      assertThat(failure.getException()).isInstanceOf(AssertionError.class)
          .hasMessageContaining(MESSAGE + " in VM-" + i++);
    }
  }

  @Test
  public void checkThatFailureInEveryDUnitVMAndControllerIsReported() {
    Result result = TestRunner.runTest(CheckThatFailsInEveryDUnitVMAndController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(getVMCount() + 1);
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
  public void checkThatFailureInMethodInDUnitVMIsReported() {
    Result result = TestRunner.runTest(CheckThatFailsInMethodInDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(AssertionError.class)
        .hasMessageContaining(MESSAGE);
  }

  @Test
  public void addErrorInDUnitVMIsReported() {
    Result result = TestRunner.runTest(AddErrorInDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(1);
    assertThat(failures.get(0).getException()).isInstanceOf(NullPointerException.class)
        .hasMessage(MESSAGE);
  }

  @Test
  public void addErrorInEveryDUnitVMIsReported() {
    Result result = TestRunner.runTest(AddErrorInEveryDUnitVM.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(getVMCount());
    int i = 0;
    for (Failure failure : failures) {
      assertThat(failure.getException()).isInstanceOf(NullPointerException.class)
          .hasMessageContaining(MESSAGE + " in VM-" + i++);
    }
  }

  @Test
  public void addErrorInEveryDUnitVMAndControllerIsReported() {
    Result result = TestRunner.runTest(AddErrorInEveryDUnitVMAndController.class);

    assertThat(result.wasSuccessful()).isFalse();
    List<Failure> failures = result.getFailures();
    assertThat(failures).hasSize(getVMCount() + 1);
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
  public void addErrorInMethodInDUnitVMIsReported() {
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
    public void assertionFailsInController() {
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
    public void exceptionInController() {
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
    public void assertionFailsInDUnitVM() {
      getVM(0).invoke(() -> errorCollector.checkThat(MESSAGE, false, is(true)));
    }
  }

  /**
   * Used by test {@link #checkThatFailureInEveryDUnitVMIsReported()}
   */
  public static class CheckThatFailsInEveryDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void assertionFailsInEveryDUnitVM() {
      for (VM vm : getAllVMs()) {
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
    public void assertionFailsInEveryDUnitVM() {
      errorCollector.checkThat(MESSAGE + " in VM-CONTROLLER", false, is(true));
      for (VM vm : getAllVMs()) {
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
    public void assertionFailsInDUnitVM() {
      getVM(0).invoke(() -> checkThat());
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
    public void exceptionInDUnitVM() {
      getVM(0).invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE)));
    }
  }

  /**
   * Used by test {@link #addErrorInEveryDUnitVMIsReported()}
   */
  public static class AddErrorInEveryDUnitVM implements Serializable {

    @Rule
    public SharedErrorCollector errorCollector = new SharedErrorCollector();

    @Test
    public void exceptionInEveryDUnitVM() {
      for (VM vm : getAllVMs()) {
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
    public void exceptionInEveryDUnitVM() {
      errorCollector.addError(new NullPointerException(MESSAGE + " in VM-CONTROLLER"));
      for (VM vm : getAllVMs()) {
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
    public void exceptionInDUnitVM() {
      getVM(0).invoke(() -> addError());
    }

    private void addError() {
      errorCollector.addError(new NullPointerException(MESSAGE));
    }
  }
}
