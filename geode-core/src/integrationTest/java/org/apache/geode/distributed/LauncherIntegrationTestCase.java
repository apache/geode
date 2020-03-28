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

import static java.lang.System.lineSeparator;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.isPortAvailable;
import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.process.ProcessStreamReader.InputListener;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.lang.AvailablePid;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Abstract base class for integration tests of both {@link LocatorLauncher} and
 * {@link ServerLauncher}.
 *
 * @since GemFire 8.0
 */
public abstract class LauncherIntegrationTestCase {

  protected static final long AWAIT_MILLIS = getTimeout().toMillis() * 2;

  private static final int PREFERRED_FAKE_PID = 42;

  private static final String EXPECTED_EXCEPTION_MBEAN_NOT_REGISTERED =
      "MBean Not Registered In GemFire Domain";

  protected volatile int localPid;
  protected volatile int fakePid;

  private volatile ServerSocket socket;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUpAbstractLauncherIntegrationTestCase() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + MCAST_PORT, Integer.toString(0));
    addIgnoredException(EXPECTED_EXCEPTION_MBEAN_NOT_REGISTERED);
    localPid = identifyPid();
    fakePid = new AvailablePid().findAvailablePid(PREFERRED_FAKE_PID);
  }

  @After
  public void tearDownAbstractLauncherIntegrationTestCase() throws Exception {
    if (socket != null) {
      socket.close();
    }
  }

  protected abstract ProcessType getProcessType();

  protected void assertDeletionOf(final File file) {
    await()
        .atMost(AWAIT_MILLIS, MILLISECONDS)
        .untilAsserted(() -> assertThat(file).doesNotExist());
  }

  protected void assertThatServerPortIsFree(final int serverPort) {
    assertThatPortIsFree(serverPort);
  }

  protected void assertThatServerPortIsInUse(final int serverPort) {
    assertThatPortIsInUse(serverPort);
  }

  protected void assertThatServerPortIsInUseBySocket(final int serverPort) {
    assertThatPortIsInUseBySocket(serverPort);
  }

  protected File givenControlFile(final String name) {
    try {
      File file = new File(getWorkingDirectory(), name);
      assertThat(file.createNewFile()).isTrue();
      assertThat(file).exists();
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected File givenEmptyPidFile() {
    return givenPidFile(null);
  }

  protected void givenEmptyWorkingDirectory() {
    assertThat(getWorkingDirectory().listFiles()).hasSize(0);
  }

  protected void givenLocatorPortInUse(final int locatorPort) {
    givenPortInUse(locatorPort);
  }

  protected File givenPidFile(final Object content) {
    try {
      File file = new File(getWorkingDirectory(), getProcessType().getPidFileName());
      FileWriter writer = new FileWriter(file);
      writer.write(String.valueOf(content));
      writer.write(lineSeparator());
      writer.flush();
      writer.close();
      assertTrue(file.exists());
      return file;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected void givenServerPortIsFree(final int serverPort) {
    assertThatPortIsFree(serverPort);
  }

  protected void givenServerPortInUse(final int serverPort) {
    givenPortInUse(serverPort);
  }

  protected InputListener createExpectedListener(final String name, final String expected,
      final AtomicBoolean atomic) {
    return new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        if (line.contains(expected)) {
          atomic.set(true);
        }
      }

      @Override
      public String toString() {
        return name;
      }
    };
  }

  protected void disconnectFromDS() {
    InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  public String getClassPath() {
    // alternative: ManagementFactory.getRuntimeMXBean().getClassPath()
    return System.getProperty("java.class.path");
  }

  public String getJavaPath() {
    try {
      return new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected List<String> getJvmArguments() {
    return ManagementFactory.getRuntimeMXBean().getInputArguments();
  }

  protected int getLocatorPid() {
    return readPidFileWithValidation();
  }

  protected File getLogFile() {
    return new File(getWorkingDirectory(), getUniqueName() + ".log");
  }

  protected String getLogFilePath() {
    try {
      return getLogFile().getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected File getPidFile() {
    return new File(getWorkingDirectory(), getProcessType().getPidFileName());
  }

  protected String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  protected int getServerPid() {
    return readPidFileWithValidation();
  }

  protected String getStatusFileName() {
    return getProcessType().getStatusFileName();
  }

  protected String getStatusRequestFileName() {
    return getProcessType().getStatusRequestFileName();
  }

  protected String getStopRequestFileName() {
    return getProcessType().getStopRequestFileName();
  }

  protected File getWorkingDirectory() {
    return temporaryFolder.getRoot();
  }

  protected String getWorkingDirectoryPath() {
    try {
      return getWorkingDirectory().getCanonicalPath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected int readPidFile() {
    return readPidFile(getPidFile());
  }

  private void assertThatPortIsFree(final int port) {
    assertThat(isPortAvailable(port, SOCKET)).isTrue();
  }

  private void assertThatPortIsInUse(final int port) {
    assertThat(isPortAvailable(port, SOCKET)).isFalse();
  }

  private void assertThatPortIsInUseBySocket(final int port) {
    assertThat(socket.isBound()).isTrue();
    assertThat(socket.isClosed()).isFalse();
    assertThat(socket.getLocalPort()).isEqualTo(port);
    assertThatServerPortIsInUse(port);
  }

  private void delete(final File file) {
    assertThat(file.delete()).isTrue();
  }

  private void givenPortInUse(final int port) {
    try {
      givenPortInUse(port, InetAddress.getLocalHost());
    } catch (UnknownHostException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void givenPortInUse(final int port, InetAddress bindAddress) {
    try {
      socket = SocketCreatorFactory
          .createNonDefaultInstance(false, false, null, null, System.getProperties())
          .forCluster()
          .createServerSocket(port, 50, bindAddress);
      assertThat(socket.isBound()).isTrue();
      assertThat(socket.isClosed()).isFalse();
      assertThat(isPortAvailable(port, SOCKET)).isFalse();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private int readPidFile(final File pidFile) {
    try {
      assertTrue(pidFile.exists());
      try (BufferedReader reader = new BufferedReader(new FileReader(pidFile))) {
        return Integer.parseInt(StringUtils.trim(reader.readLine()));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private int readPidFileWithValidation() {
    int pid = readPidFile(getPidFile());
    assertThat(pid).isGreaterThan(0);
    assertThat(isProcessAlive(pid)).isTrue();
    return pid;
  }
}
