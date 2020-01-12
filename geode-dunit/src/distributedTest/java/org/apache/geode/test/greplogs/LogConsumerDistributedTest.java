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
package org.apache.geode.test.greplogs;

import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getLocator;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.apache.geode.test.greplogs.LogConsumerDistributedTest.TargetVM.ANY_VM;
import static org.apache.geode.test.greplogs.LogConsumerDistributedTest.TargetVM.CONTROLLER;
import static org.apache.geode.test.greplogs.LogConsumerDistributedTest.TargetVM.LOCATOR;
import static org.apache.geode.test.junit.runners.TestRunner.runTest;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.Result;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class LogConsumerDistributedTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  private static final String LOG_MESSAGE = "just a message";
  private static final String EXCEPTION_MESSAGE =
      "java.lang.ClassNotFoundException: does.not.Exist";

  private static final AtomicReference<TargetVM> targetVM = new AtomicReference<>();
  private static final AtomicReference<SerializableRunnableIF> task = new AtomicReference<>();
  private static final AtomicReference<AsyncInvocation<Void>> asyncInvocation =
      new AtomicReference<>();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @After
  public void tearDown() {
    for (VM vm : toArray(getAllVMs(), getController(), getLocator())) {
      vm.invoke(() -> {
        targetVM.set(null);
        task.set(null);
        asyncInvocation.set(null);
      });
    }
  }

  @Test
  public void traceLevel_logMessage_passes() {
    given(CONTROLLER, () -> logger.trace(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void debugLevel_logMessage_passes() {
    given(CONTROLLER, () -> logger.debug(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void infoLevel_logMessage_passes() {
    given(CONTROLLER, () -> logger.info(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void warnLevel_logMessage_passes() {
    given(CONTROLLER, () -> logger.warn(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isTrue();
  }

  @Test
  public void errorLevel_logMessage_fails() {
    given(CONTROLLER, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void fatalLevel_logMessage_fails() {
    given(CONTROLLER, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void errorLevel_logMessage_loggedAsync_fails() {
    given(CONTROLLER, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void fatalLevel_logMessage_loggedAsync_fails() {
    given(CONTROLLER, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(result.wasSuccessful()).isFalse();
  }

  @Test
  public void errorLevel_logMessage_includedInFailure() {
    given(CONTROLLER, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void fatalLevel_logMessage_includedInFailure() {
    given(CONTROLLER, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void errorLevel_logMessage_failsInVm() {
    given(ANY_VM, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void fatalLevel_logMessage_failsInVm() {
    given(ANY_VM, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void errorLevel_logMessage_loggedAsync_failsInVm() {
    given(ANY_VM, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void fatalLevel_logMessage_loggedAsync_failsInVm() {
    given(ANY_VM, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void errorLevel_logMessage_failsInController() {
    given(CONTROLLER, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void fatalLevel_logMessage_failsInController() {
    given(CONTROLLER, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void errorLevel_logMessage_loggedAsync_failsInController() {
    given(CONTROLLER, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void fatalLevel_logMessage_loggedAsync_failsInController() {
    given(CONTROLLER, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void errorLevel_logMessage_failsInLocator() {
    given(LOCATOR, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void fatalLevel_logMessage_failsInLocator() {
    given(LOCATOR, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void errorLevel_logMessage_loggedAsync_failsInLocator() {
    given(LOCATOR, () -> logger.error(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void fatalLevel_logMessage_loggedAsync_failsInLocator() {
    given(LOCATOR, () -> logger.fatal(LOG_MESSAGE));

    Result result = runTest(ExecuteTaskAsync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("fatal")
        .hasMessageContaining(LOG_MESSAGE);
  }

  @Test
  public void errorLevel_exceptionMessage_failsInLocator() {
    given(LOCATOR, () -> logger.error(EXCEPTION_MESSAGE));

    Result result = runTest(ExecuteTaskSync.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(EXCEPTION_MESSAGE);
  }

  @Test
  public void errorLevel_exceptionMessage_loggedInBefore_failsInLocator() {
    given(LOCATOR, () -> logger.error(EXCEPTION_MESSAGE));

    Result result = runTest(ExecuteTaskBefore.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(EXCEPTION_MESSAGE);
  }

  @Test
  public void errorLevel_exceptionMessage_loggedInBeforeClass_failsInLocator() {
    given(LOCATOR, () -> logger.error(EXCEPTION_MESSAGE));

    Result result = runTest(ExecuteTaskBeforeClass.class);

    assertThat(getFailure(result))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("error")
        .hasMessageContaining(EXCEPTION_MESSAGE);
  }

  private static void given(TargetVM targetVM, SerializableRunnableIF task) {
    for (VM vm : toArray(getAllVMs(), getController(), getLocator())) {
      vm.invoke(() -> {
        LogConsumerDistributedTest.targetVM.set(targetVM);
        LogConsumerDistributedTest.task.set(task);
      });
    }
  }

  private static Throwable getFailure(Result result) {
    assertThat(result.getFailures()).hasSize(1);
    return result.getFailures().get(0).getException();
  }

  private static VM getTargetVm() {
    TargetVM vm = targetVM.get();
    switch (vm) {
      case CONTROLLER:
        return getController();
      case ANY_VM:
        return getVM(0);
      case LOCATOR:
        return getLocator();
      default:
        throw new IllegalStateException("VM for " + vm + " not found");
    }
  }

  enum TargetVM {
    CONTROLLER,
    LOCATOR,
    ANY_VM
  }

  public static class ExecuteTaskSync implements Serializable {

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @Test
    public void invokeTaskInTargetVm() {
      getTargetVm().invoke(() -> task.get().run());
    }
  }

  public static class ExecuteTaskAsync implements Serializable {

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @After
    public void tearDown() throws Exception {
      asyncInvocation.get().await();
    }

    @Test
    public void invokeTaskInTargetVm() {
      asyncInvocation.set(getTargetVm().invokeAsync(() -> task.get().run()));
    }
  }

  public static class ExecuteTaskBefore implements Serializable {

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @Before
    public void invokeTaskInBefore() {
      asyncInvocation.set(getTargetVm().invokeAsync(() -> task.get().run()));
    }

    @Test
    public void doNothing() throws Exception {
      asyncInvocation.get().await();
    }
  }

  public static class ExecuteTaskBeforeClass implements Serializable {

    @BeforeClass
    public static void invokeTaskInBeforeClass() {
      asyncInvocation.set(getTargetVm().invokeAsync(() -> task.get().run()));
    }

    @Rule
    public DistributedRule distributedRule = new DistributedRule();

    @Test
    public void doNothing() throws Exception {
      asyncInvocation.get().await();
    }
  }
}
