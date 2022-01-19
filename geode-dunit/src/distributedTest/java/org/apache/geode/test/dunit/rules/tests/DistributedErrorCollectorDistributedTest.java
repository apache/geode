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

import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithExpectedFailures;
import static org.apache.geode.test.junit.runners.TestRunner.runTestWithValidation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Distributed tests for {@link DistributedErrorCollector}.
 */
@SuppressWarnings("serial")
public class DistributedErrorCollectorDistributedTest {

  private static final String MESSAGE = "Failure message";

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Test
  public void errorCollectorHasFieldNamedErrors() throws Exception {
    Field errorsField = ErrorCollector.class.getDeclaredField("errors");

    assertThat(errorsField.getDeclaringClass()).isEqualTo(ErrorCollector.class);
  }

  @Test
  public void whenCheckThatFailsInController_resultIncludesError() {
    assertFailsWithCollectedErrors(CheckThatFailsInController.class, new AssertionError(MESSAGE));
  }

  @Test
  public void whenControllerAddsError_resultIncludesError() {
    assertFailsWithCollectedErrors(AddErrorInController.class, new NullPointerException(MESSAGE));
  }

  @Test
  public void whenCheckThatFailsInDUnitVM_resultIncludesError() {
    assertFailsWithCollectedErrors(CheckThatFailsInDUnitVM.class, new AssertionError(MESSAGE));
  }

  @Test
  public void whenCheckThatFailsInEveryDUnitVM_resultIncludesAllErrors() {
    List<Throwable> errors = new ArrayList<>();
    for (VM vm : getAllVMs()) {
      errors.add(new AssertionError(MESSAGE + " in VM-" + vm.getId()));
    }

    assertFailsWithCollectedErrors(CheckThatFailsInEveryDUnitVM.class, errors);
  }

  @Test
  public void whenCheckThatFailsInEveryDUnitVMAndController_resultIncludesAllErrors() {
    List<Throwable> errors = new ArrayList<>();
    errors.add(new AssertionError(MESSAGE + " in VM-CONTROLLER"));
    for (VM vm : getAllVMs()) {
      errors.add(new AssertionError(MESSAGE + " in VM-" + vm.getId()));
    }

    assertFailsWithCollectedErrors(CheckThatFailsInEveryDUnitVMAndController.class, errors);
  }

  @Test
  public void whenCheckThatFailsInMethodInDUnitVM_resultIncludesError() {
    assertFailsWithCollectedErrors(CheckThatFailsInMethodInDUnitVM.class,
        new AssertionError(MESSAGE));
  }

  @Test
  public void whenDUnitVMAddsError_resultIncludesError() {
    assertFailsWithCollectedErrors(AddErrorInDUnitVM.class, new NullPointerException(MESSAGE));
  }

  @Test
  public void whenEveryDUnitVMAddsError_resultIncludesAllErrors() {
    List<Throwable> errors = new ArrayList<>();
    for (VM vm : getAllVMs()) {
      errors.add(new NullPointerException(MESSAGE + " in VM-" + vm.getId()));
    }

    assertFailsWithCollectedErrors(AddErrorInEveryDUnitVM.class, errors);
  }

  @Test
  public void whenEveryDUnitVMAndControllerAddsError_resultIncludesAllErrors() {
    List<Throwable> errors = new ArrayList<>();
    errors.add(new NullPointerException(MESSAGE + " in VM-CONTROLLER"));
    for (VM vm : getAllVMs()) {
      errors.add(new NullPointerException(MESSAGE + " in VM-" + vm.getId()));
    }

    assertFailsWithCollectedErrors(AddErrorInEveryDUnitVMAndController.class, errors);
  }

  @Test
  public void whenDUnitVMAddsErrorInMethod_resultIncludesError() {
    assertFailsWithCollectedErrors(AddErrorInMethodInDUnitVM.class,
        new NullPointerException(MESSAGE));
  }

  @Test
  public void whenNewDUnitVMDoesNotAddError_resultIsSuccessful() {
    assertPasses(AddDUnitVM.class);
  }

  @Test
  public void whenNewDUnitVMAddsError_resultIncludesError() {
    assertFailsWithCollectedErrors(AddErrorInNewDUnitVM.class, new NullPointerException(MESSAGE));
  }

  @Test
  public void whenBouncedDUnitVMDoesNotAddError_resultIsSuccessful() {
    assertPasses(BounceDUnitVM.class);
  }

  @Test
  public void whenBouncedDUnitVMAddsErrorAfterBounce_resultIncludesError() {
    assertFailsWithCollectedErrors(AddErrorInBouncedDUnitVM.class,
        new NullPointerException(MESSAGE));
  }

  @Test
  public void whenBouncedDUnitVMAddsErrorBeforeBounce_resultIncludesError() {
    assertFailsWithCollectedErrors(AddErrorBeforeBouncingDUnitVM.class,
        new NullPointerException(MESSAGE));
  }

  @Test
  public void whenBouncedDUnitVMAddsErrorBeforeAndAfterBounce_resultIncludesError() {
    assertFailsWithCollectedErrors(AddErrorBeforeAndAfterBouncingDUnitVM.class,
        new NullPointerException(MESSAGE + "-before"),
        new NullPointerException(MESSAGE + "-after"));
  }

  @Test
  public void whenCheckSucceedsDoesNotThrow_resultIsSuccessful() {
    assertPasses(CheckSucceedsDoesNotThrow.class);
  }

  @Test
  public void whenCheckSucceedsThrows_resultIncludesError() {
    assertFailsWithCollectedErrors(CheckSucceedsThrows.class, new NullPointerException(MESSAGE));
  }

  private void assertPasses(final Class<?> test) {
    runTestWithValidation(test);
  }

  private void assertFailsWithCollectedErrors(final Class<?> test,
      final Throwable... collectedErrors) {
    runTestWithExpectedFailures(test, collectedErrors);
  }

  private void assertFailsWithCollectedErrors(final Class<?> test,
      final List<Throwable> collectedErrors) {
    runTestWithExpectedFailures(test, collectedErrors);
  }

  /**
   * Used by test {@link #whenCheckThatFailsInController_resultIncludesError()}
   */
  public static class CheckThatFailsInController {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void assertionFailsInController() {
      errorCollector.checkThat(MESSAGE, false, is(true));
    }
  }

  /**
   * Used by test {@link #whenControllerAddsError_resultIncludesError()}
   */
  public static class AddErrorInController {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInController() {
      errorCollector.addError(new NullPointerException(MESSAGE));
    }
  }

  /**
   * Used by test {@link #whenCheckThatFailsInDUnitVM_resultIncludesError()}
   */
  public static class CheckThatFailsInDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void assertionFailsInDUnitVM() {
      getVM(0).invoke(() -> errorCollector.checkThat(MESSAGE, false, is(true)));
    }
  }

  /**
   * Used by test {@link #whenCheckThatFailsInEveryDUnitVM_resultIncludesAllErrors()}
   */
  public static class CheckThatFailsInEveryDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void assertionFailsInEveryDUnitVM() {
      for (VM vm : getAllVMs()) {
        vm.invoke(
            () -> errorCollector.checkThat(MESSAGE + " in VM-" + vm.getId(), false, is(true)));
      }
    }
  }

  /**
   * Used by test {@link #whenCheckThatFailsInEveryDUnitVMAndController_resultIncludesAllErrors()}
   */
  public static class CheckThatFailsInEveryDUnitVMAndController implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

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
   * Used by test {@link #whenCheckThatFailsInMethodInDUnitVM_resultIncludesError()}
   */
  public static class CheckThatFailsInMethodInDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void assertionFailsInDUnitVM() {
      getVM(0).invoke(this::checkThat);
    }

    private void checkThat() {
      errorCollector.checkThat(MESSAGE, false, is(true));
    }
  }

  /**
   * Used by test {@link #whenDUnitVMAddsError_resultIncludesError()}
   */
  public static class AddErrorInDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInDUnitVM() {
      getVM(0).invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE)));
    }
  }

  /**
   * Used by test {@link #whenEveryDUnitVMAddsError_resultIncludesAllErrors()}
   */
  public static class AddErrorInEveryDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInEveryDUnitVM() {
      for (VM vm : getAllVMs()) {
        vm.invoke(() -> errorCollector
            .addError(new NullPointerException(MESSAGE + " in VM-" + vm.getId())));
      }
    }
  }

  /**
   * Used by test {@link #whenEveryDUnitVMAndControllerAddsError_resultIncludesAllErrors()}
   */
  public static class AddErrorInEveryDUnitVMAndController implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

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
   * Used by test {@link #whenDUnitVMAddsErrorInMethod_resultIncludesError()}
   */
  public static class AddErrorInMethodInDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInDUnitVM() {
      getVM(0).invoke(this::addError);
    }

    private void addError() {
      errorCollector.addError(new NullPointerException(MESSAGE));
    }
  }

  /**
   * Used by test {@link #whenNewDUnitVMDoesNotAddError_resultIsSuccessful()}
   */
  public static class AddDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void addDUnitVM() {
      int startingVMCount = getVMCount();

      getVM(getVMCount());

      assertThat(getVMCount()).isGreaterThan(startingVMCount);
    }
  }

  /**
   * Used by test {@link #whenNewDUnitVMAddsError_resultIncludesError()}
   */
  public static class AddErrorInNewDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInNewDUnitVM() {
      getVM(getVMCount()).invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE)));
    }
  }

  /**
   * Used by test {@link #whenBouncedDUnitVMDoesNotAddError_resultIsSuccessful()}
   */
  public static class BounceDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void addDUnitVM() {
      getVM(0).bounce();
    }
  }

  /**
   * Used by test {@link #whenBouncedDUnitVMAddsErrorAfterBounce_resultIncludesError()}
   */
  public static class AddErrorInBouncedDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInBouncedDUnitVM() {
      getVM(0).bounce();
      getVM(0).invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE)));
    }
  }

  /**
   * Used by test {@link #whenBouncedDUnitVMAddsErrorBeforeBounce_resultIncludesError()}
   */
  public static class AddErrorBeforeBouncingDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInBouncedDUnitVM() {
      getVM(0).invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE)));
      getVM(0).bounce();
    }
  }

  /**
   * Used by test {@link #whenBouncedDUnitVMAddsErrorBeforeAndAfterBounce_resultIncludesError()}
   */
  public static class AddErrorBeforeAndAfterBouncingDUnitVM implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void exceptionInBouncedDUnitVM() {
      getVM(0).invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE + "-before")));
      getVM(0).bounce();
      getVM(0).invoke(() -> errorCollector.addError(new NullPointerException(MESSAGE + "-after")));
    }
  }

  /**
   * Used by test {@link #whenCheckSucceedsDoesNotThrow_resultIsSuccessful()}
   */
  public static class CheckSucceedsDoesNotThrow implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void checkSucceeds() {
      Object result = errorCollector.checkSucceeds(Object::new);

      assertThat(result).isNotNull();
    }
  }

  /**
   * Used by test {@link #whenCheckSucceedsThrows_resultIncludesError()}
   */
  public static class CheckSucceedsThrows implements Serializable {

    @Rule
    public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

    @Test
    public void checkSucceeds() {
      errorCollector.checkSucceeds(() -> {
        throw new NullPointerException(MESSAGE);
      });
    }
  }
}
