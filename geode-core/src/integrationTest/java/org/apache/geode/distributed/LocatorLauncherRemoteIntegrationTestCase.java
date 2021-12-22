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
package org.apache.geode.distributed;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.internal.DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.Command;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessStreamReader.InputListener;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Abstract base class for integration tests of {@link LocatorLauncher} as an application main in a
 * forked JVM.
 *
 * @since GemFire 8.0
 */
public abstract class LocatorLauncherRemoteIntegrationTestCase
    extends LocatorLauncherIntegrationTestCase implements UsesLocatorCommand {

  private final AtomicBoolean threwBindException = new AtomicBoolean();

  private volatile Process process;
  private volatile ProcessStreamReader processOutReader;
  private volatile ProcessStreamReader processErrReader;

  private LocatorCommand locatorCommand;

  @Before
  public void setUp() throws Exception {
    locatorCommand = new LocatorCommand(this);
  }

  @After
  public void tearDownAbstractLocatorLauncherRemoteIntegrationTestCase() {
    if (process != null) {
      process.destroy();
    }
    if (processOutReader != null && processOutReader.isRunning()) {
      processOutReader.stop();
    }
    if (processErrReader != null && processErrReader.isRunning()) {
      processErrReader.stop();
    }
  }

  @Override
  public List<String> getJvmArguments() {
    List<String> jvmArguments = new ArrayList<>();
    jvmArguments.add("-D" + GeodeGlossary.GEMFIRE_PREFIX + "log-level=config");
    jvmArguments.add("-D" + TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + defaultLocatorPort);
    return jvmArguments;
  }

  @Override
  public String getName() {
    return getUniqueName();
  }

  protected void assertStopOf(final Process process) {
    await()
        .atMost(AWAIT_MILLIS, MILLISECONDS)
        .untilAsserted(() -> assertThat(process.isAlive()).isFalse());
  }

  /**
   * Please leave unused parameter throwableClass for improved readability.
   */
  protected void assertThatLocatorThrew(Class<? extends Throwable> throwableClass) {
    assertThat(threwBindException.get()).isTrue();
  }

  protected void assertThatPidIsAlive(final int pid) {
    assertThat(pid).isGreaterThan(0);
    assertThat(isProcessAlive(pid)).isTrue();
  }

  protected void assertThatProcessIsNotAlive(final Process process) {
    assertThat(process.isAlive()).isFalse();
  }

  @Override
  protected LocatorLauncher givenRunningLocator() {
    return givenRunningLocator(new LocatorCommand(this).withCommand(Command.START));
  }

  protected LocatorLauncher givenRunningLocator(final LocatorCommand command) {
    return awaitStart(command);
  }

  protected LocatorCommand addJvmArgument(final String arg) {
    return locatorCommand.addJvmArgument(arg);
  }

  protected LocatorCommand withForce() {
    return withForce(true);
  }

  protected LocatorCommand withForce(final boolean value) {
    return locatorCommand.force(value);
  }

  protected LocatorCommand withPort(final int port) {
    return locatorCommand.withPort(port);
  }

  protected Process getLocatorProcess() {
    return process;
  }

  @Override
  protected LocatorLauncher startLocator() {
    return awaitStart(locatorCommand);
  }

  protected LocatorLauncher startLocator(final LocatorCommand command) {
    return awaitStart(command);
  }

  protected LocatorLauncher startLocator(final LocatorCommand command,
      final InputListener outListener, final InputListener errListener) {
    executeCommandWithReaders(command.create(), outListener, errListener);
    LocatorLauncher launcher = awaitStart(getWorkingDirectory());
    assertThat(process.isAlive()).isTrue();
    return launcher;
  }

  protected void startLocatorShouldFail(final LocatorCommand command) throws InterruptedException {
    awaitStartFail(command, createBindExceptionListener("sysout", threwBindException),
        createBindExceptionListener("syserr", threwBindException));
  }

  protected void startLocatorShouldFail() throws InterruptedException {
    startLocatorShouldFail(locatorCommand);
  }

  private void assertThatProcessIsNotAlive() {
    assertThatProcessIsNotAlive(process);
  }

  private void awaitStartFail(final LocatorCommand command, final InputListener outListener,
      final InputListener errListener) throws InterruptedException {
    executeCommandWithReaders(command.create(), outListener, errListener);
    process.waitFor(AWAIT_MILLIS, MILLISECONDS);
    assertThatProcessIsNotAlive();
    assertThat(process.exitValue()).isEqualTo(1);
  }

  private LocatorLauncher awaitStart(final File workingDirectory) {
    try {
      launcher = new Builder().setWorkingDirectory(workingDirectory.getCanonicalPath()).build();
      awaitStart(launcher);
      assertThat(process.isAlive()).isTrue();
      return launcher;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private LocatorLauncher awaitStart(final LocatorCommand command) {
    executeCommandWithReaders(command);
    LocatorLauncher launcher = awaitStart(getWorkingDirectory());
    assertThat(process.isAlive()).isTrue();
    return launcher;
  }

  @Override
  protected LocatorLauncher awaitStart(final LocatorLauncher launcher) {
    await()
        .atMost(AWAIT_MILLIS, MILLISECONDS)
        .untilAsserted(() -> {
          try {
            assertThat(launcher.status().getStatus()).isEqualTo(Status.ONLINE);
          } catch (Error | Exception e) {
            throw new AssertionError(statusFailedWithException(e), e);
          }
        });
    assertThat(process.isAlive()).isTrue();
    return launcher;
  }

  protected String statusFailedWithException(Throwable cause) {
    return "Status failed with exception: "
        + "process.isAlive()=" + process.isAlive()
        + ", processErrReader" + processErrReader
        + ", processOutReader" + processOutReader
        + ", message" + cause.getMessage();
  }

  private InputListener createBindExceptionListener(final String name,
      final AtomicBoolean threwBindException) {
    return createExpectedListener(name, BindException.class.getName(), threwBindException);
  }

  private void executeCommandWithReaders(final List<String> command) {
    try {
      process = new ProcessBuilder(command).directory(getWorkingDirectory()).start();
      processOutReader = new ProcessStreamReader.Builder(process)
          .inputStream(process.getInputStream())
          .build()
          .start();
      processErrReader = new ProcessStreamReader.Builder(process)
          .inputStream(process.getErrorStream())
          .build()
          .start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void executeCommandWithReaders(final List<String> command,
      final InputListener outListener, final InputListener errListener) {
    try {
      process = new ProcessBuilder(command).directory(getWorkingDirectory()).start();
      processOutReader = new ProcessStreamReader.Builder(process)
          .inputStream(process.getInputStream())
          .inputListener(outListener)
          .build()
          .start();
      processErrReader = new ProcessStreamReader.Builder(process)
          .inputStream(process.getErrorStream())
          .inputListener(errListener)
          .build()
          .start();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void executeCommandWithReaders(final LocatorCommand command) {
    executeCommandWithReaders(command.create());
  }
}
