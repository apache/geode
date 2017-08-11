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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.internal.cache.AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
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
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessStreamReader.InputListener;

/**
 * Abstract base class for integration tests of {@link ServerLauncher} as an application main in a
 * forked JVM.
 *
 * @since GemFire 8.0
 */
public abstract class ServerLauncherRemoteIntegrationTestCase
    extends ServerLauncherIntegrationTestCase implements UsesServerCommand {

  private final AtomicBoolean threwBindException = new AtomicBoolean();

  private volatile Process process;
  private volatile ProcessStreamReader processOutReader;
  private volatile ProcessStreamReader processErrReader;

  private ServerCommand serverCommand;

  @Before
  public void setUpServerLauncherRemoteIntegrationTestCase() throws Exception {
    serverCommand = new ServerCommand(this);
  }

  @After
  public void tearDownServerLauncherRemoteIntegrationTestCase() throws Exception {
    if (process != null) {
      process.destroy();
      process = null;
    }
    if (processOutReader != null && processOutReader.isRunning()) {
      processOutReader.stop();
    }
    if (processErrReader != null && processErrReader.isRunning()) {
      processErrReader.stop();
    }
  }

  @Override
  public boolean getDisableDefaultServer() {
    return false;
  }

  @Override
  public List<String> getJvmArguments() {
    List<String> jvmArguments = new ArrayList<>();
    jvmArguments.add("-D" + GEMFIRE_PREFIX + LOG_LEVEL + "=config");
    jvmArguments.add("-D" + GEMFIRE_PREFIX + MCAST_PORT + "=0");
    jvmArguments
        .add("-D" + TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + String.valueOf(defaultServerPort));
    return jvmArguments;
  }

  @Override
  public String getName() {
    return getUniqueName();
  }

  protected void assertThatServerThrew(final Class<? extends Throwable> throwableClass) {
    assertThat(threwBindException.get()).isTrue();
  }

  protected void assertStopOf(final Process process) {
    await().until(() -> assertThat(process.isAlive()).isFalse());
  }

  protected void assertThatPidIsAlive(final int pid) {
    assertThat(pid).isGreaterThan(0);
    assertThat(isProcessAlive(pid)).isTrue();
  }

  protected ServerLauncher givenRunningServer() {
    return givenRunningServer(serverCommand);
  }

  protected ServerLauncher givenRunningServer(final ServerCommand command) {
    return awaitStart(command);
  }

  protected Process getServerProcess() {
    return process;
  }

  @Override
  protected ServerLauncher startServer() {
    return startServer(serverCommand);
  }

  protected ServerLauncher startServer(final ServerCommand command) {
    return awaitStart(command);
  }

  protected ServerLauncher startServer(final ServerCommand command,
      final ProcessStreamReader.InputListener outListener,
      final ProcessStreamReader.InputListener errListener) throws Exception {
    executeCommandWithReaders(command.create(), outListener, errListener);
    ServerLauncher launcher = awaitStart(getWorkingDirectory());
    assertThat(process.isAlive()).isTrue();
    return launcher;
  }

  protected void startServerShouldFail() throws IOException, InterruptedException {
    startServerShouldFail(serverCommand);
  }

  protected void startServerShouldFail(final ServerCommand command)
      throws IOException, InterruptedException {
    startServerShouldFail(command, createBindExceptionListener("sysout", threwBindException),
        createBindExceptionListener("syserr", threwBindException));
  }

  protected ServerCommand addJvmArgument(final String arg) {
    return serverCommand.addJvmArgument(arg);
  }

  protected ServerCommand withDisableDefaultServer() {
    return withDisableDefaultServer(true);
  }

  protected ServerCommand withDisableDefaultServer(final boolean value) {
    return serverCommand.disableDefaultServer(value);
  }

  protected ServerCommand withForce() {
    return withForce(true);
  }

  protected ServerCommand withForce(final boolean value) {
    return serverCommand.force(value);
  }

  protected ServerCommand withServerPort(final int port) {
    return serverCommand.withServerPort(port);
  }

  private ServerLauncher awaitStart(final File workingDirectory) {
    try {
      launcher = new ServerLauncher.Builder()
          .setWorkingDirectory(workingDirectory.getCanonicalPath()).build();
      awaitStart(launcher);
      assertThat(process.isAlive()).isTrue();
      return launcher;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private ServerLauncher awaitStart(final ServerCommand command) {
    try {
      executeCommandWithReaders(command);
      ServerLauncher launcher = awaitStart(getWorkingDirectory());
      assertThat(process.isAlive()).isTrue();
      return launcher;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected ServerLauncher awaitStart(final ServerLauncher launcher) {
    await().until(() -> assertThat(launcher.status().getStatus()).isEqualTo(Status.ONLINE));
    assertThat(process.isAlive()).isTrue();
    return launcher;
  }

  private InputListener createBindExceptionListener(final String name,
      final AtomicBoolean threwBindException) {
    return createExpectedListener(name, BindException.class.getName(), threwBindException);
  }

  private void executeCommandWithReaders(final List<String> command) throws IOException {
    process = new ProcessBuilder(command).directory(getWorkingDirectory()).start();
    processOutReader = new ProcessStreamReader.Builder(process)
        .inputStream(process.getInputStream()).build().start();
    processErrReader = new ProcessStreamReader.Builder(process)
        .inputStream(process.getErrorStream()).build().start();
  }

  private void executeCommandWithReaders(final List<String> command,
      final InputListener outListener, final InputListener errListener) throws IOException {
    process = new ProcessBuilder(command).directory(getWorkingDirectory()).start();
    processOutReader = new ProcessStreamReader.Builder(process)
        .inputStream(process.getInputStream()).inputListener(outListener).build().start();
    processErrReader = new ProcessStreamReader.Builder(process)
        .inputStream(process.getErrorStream()).inputListener(errListener).build().start();
  }

  private void executeCommandWithReaders(final ServerCommand command) throws IOException {
    executeCommandWithReaders(command.create());
  }

  private void startServerShouldFail(final ServerCommand command, final InputListener outListener,
      final InputListener errListener) throws IOException, InterruptedException {
    executeCommandWithReaders(command.create(), outListener, errListener);
    process.waitFor(2, MINUTES);
    assertThat(process.isAlive()).isFalse();
    assertThat(process.exitValue()).isEqualTo(1);
  }
}
