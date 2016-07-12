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
package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.DistributionLocator;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.net.SocketCreatorFactory;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessStreamReader;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Integration tests for launching a Locator in a forked process.
 *
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LocatorLauncherRemoteIntegrationTest extends AbstractLocatorLauncherRemoteIntegrationTestCase {

  protected volatile Process process;
  protected volatile ProcessStreamReader processOutReader;
  protected volatile ProcessStreamReader processErrReader;

  @Before
  public final void setUpLocatorLauncherRemoteTest() throws Exception {
  }

  @After
  public final void tearDownLocatorLauncherRemoteTest() throws Exception {
    if (this.process != null) {
      this.process.destroy();
      this.process = null;
    }
    if (this.processOutReader != null && this.processOutReader.isRunning()) {
      this.processOutReader.stop();
    }
    if (this.processErrReader != null && this.processErrReader.isRunning()) {
      this.processErrReader.stop();
    }
  }

  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertTrue(factory.isAttachAPIFound());
  }

  @Test
  @Ignore("TRAC bug #52304: test is broken and needs to be reworked")
  public void testRunningLocatorOutlivesForkingProcess() throws Exception {
  }/*
    // TODO: fix up this test
    
    this.temporaryFolder.getRoot() = new File(getUniqueName());
    this.temporaryFolder.getRoot().mkdir();
    assertTrue(this.temporaryFolder.getRoot().isDirectory() && this.temporaryFolder.getRoot().canWrite());

    // launch LocatorLauncherForkingProcess which then launches the GemFire Locator
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncherRemoteDUnitTest.class.getName().concat("$").concat(LocatorLauncherForkingProcess.class.getSimpleName()));
    command.add(String.valueOf(this.locatorPort));

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    Thread waiting = new Thread(new Runnable() {
      public void run() {
        try {
          assertIndexDetailsEquals(0, process.waitFor());
        }
        catch (InterruptedException ignore) {
          logger.error("Interrupted while waiting for process!", ignore);
        }
      }
    });

    try {
      waiting.start();
      waiting.join(TIMEOUT_MILLISECONDS);
      assertFalse("Process took too long and timed out!", waiting.isAlive());
    }
    finally {
      if (waiting.isAlive()) {
        waiting.interrupt();
      }
    }

    LocatorLauncher locatorLauncher = new Builder().setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath()).build();

    assertIndexDetailsEquals(Status.ONLINE, locatorLauncher.status().getStatus());
    assertIndexDetailsEquals(Status.STOPPED, locatorLauncher.stop().getStatus());
  }
  */

  @Category(FlakyTest.class) // GEODE-473: random ports, BindException, forks JVM, uses ErrorCollector
  @Test
  public void testStartCreatesPidFile() throws Throwable {
    // build and start the locator
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // check the status
      final LocatorState locatorState = this.launcher.status();
      assertNotNull(locatorState);
      assertEquals(Status.ONLINE, locatorState.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Category(FlakyTest.class) // GEODE-530: BindException, random ports
  @Test
  public void testStartDeletesStaleControlFiles() throws Throwable {
    // create existing control files
    this.stopRequestFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getStopRequestFileName());
    this.stopRequestFile.createNewFile();
    assertTrue(this.stopRequestFile.exists());

    this.statusRequestFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getStatusRequestFileName());
    this.statusRequestFile.createNewFile();
    assertTrue(this.statusRequestFile.exists());

    this.statusFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getStatusFileName());
    this.statusFile.createNewFile();
    assertTrue(this.statusFile.exists());

    // build and start the locator
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for locator to start
    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate stale control files were deleted
      waitForFileToDelete(this.stopRequestFile);
      waitForFileToDelete(this.statusRequestFile);
      waitForFileToDelete(this.statusFile);

      // validate log file was created
      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Category(FlakyTest.class) // GEODE-1229: BindException
  @Test
  public void testStartOverwritesStalePidFile() throws Throwable {
    // create existing pid file
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the locator
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertFalse(pid == Integer.MAX_VALUE);

      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Category(FlakyTest.class) // GEODE-764: BindException
  @Test
  public void testStartUsingForceOverwritesExistingPidFile() throws Throwable {
    // create existing pid file
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
    final int otherPid = getPid();
    assertTrue("Pid " + otherPid + " should be alive", ProcessUtils.isProcessAlive(otherPid));
    writePid(this.pidFile, otherPid);

    // build and start the locator
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");
    command.add("--force");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for locator to start
    int pid = 0;
    this.launcher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertTrue(pid != otherPid);

      // validate log file was created
      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testStartUsingPortInUseFails() throws Throwable {
    this.socket = SocketCreatorFactory.getClusterSSLSocketCreator().createServerSocket(this.locatorPort, 50, null, -1);
    this.locatorPort = this.socket.getLocalPort();

    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--port=" + this.locatorPort);

    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream())
        .inputListener(createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream())
        .inputListener(createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).build().start();

    // wait for locator to start and fail
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      int code = process.waitFor();
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      // check the status
      final LocatorState locatorState = dirLauncher.status();
      assertNotNull(locatorState);
      assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());

      final String logFileName = getUniqueName() + ".log";
      assertFalse("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // if the following fails, then the SHORTER_TIMEOUT is too short for slow machines
    // or this test needs to use MainLauncher in ProcessWrapper

    // validate that output contained BindException 
    this.errorCollector.checkThat(outputContainedExpectedString.get(), is(equalTo(true)));

    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) {
      // ignore
    }

    this.errorCollector.checkThat(status.getStatus(), is(equalTo(getExpectedStopStatusForNotRunning())));
  }

  @Test
  public void testStartWithDefaultPortInUseFails() throws Throwable {
    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();

    this.socket = SocketCreatorFactory.getClusterSSLSocketCreator().createServerSocket(this.locatorPort, 50, null, -1);
    this.locatorPort = this.socket.getLocalPort();

    assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
    assertTrue(this.socket.isBound());
    assertFalse(this.socket.isClosed());

    // launch locator
    final List<String> jvmArguments = getJvmArguments();
    jvmArguments.add("-D" + DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.locatorPort);

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream())
        .inputListener(createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream())
        .inputListener(createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).build().start();

    // wait for locator to start up
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      int code = process.waitFor(); // TODO: create flavor with timeout in ProcessUtils
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      // check the status
      final LocatorState locatorState = dirLauncher.status();
      assertNotNull(locatorState);
      assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());

      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName() + ".log";
      assertFalse("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // if the following fails, then the SHORTER_TIMEOUT might be too short for slow machines
    // or this test needs to use MainLauncher in ProcessWrapper

    // validate that output contained BindException 
    this.errorCollector.checkThat(outputContainedExpectedString.get(), is(equalTo(true)));

    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) {
      // ignore
    }

    this.errorCollector.checkThat(status.getStatus(), is(equalTo(getExpectedStopStatusForNotRunning())));
  }

  @Test
  @Ignore("Need to rewrite this without using dunit.Host")
  public void testStartWithExistingPidFileFails() throws Throwable {
  }/*
    this.temporaryFolder.getRoot() = new File(getUniqueName());
    this.temporaryFolder.getRoot().mkdir();
    assertTrue(this.temporaryFolder.getRoot().isDirectory() && this.temporaryFolder.getRoot().canWrite());

    // create existing pid file
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invoke(() -> ProcessUtils.identifyPid());
    assertFalse("Remote pid shouldn't be the same as local pid " + realPid, realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);
    
    // build and start the locator
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher, 10*1000, false);
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final LocatorState locatorState = dirLauncher.status();
      assertNotNull(locatorState);
      assertIndexDetailsEquals(Status.NOT_RESPONDING, locatorState.getStatus());
      
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    try {
      final LocatorState status = dirLauncher.stop();
      final Status theStatus = status.getStatus();
      assertFalse(theStatus == Status.STARTING);
      assertFalse(theStatus == Status.ONLINE);
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartWithExistingPidFileFails
  */

  @Test
  public void testStatusUsingPid() throws Throwable {
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for locator to start
    int pid = 0;
    LocatorLauncher pidLauncher = null;
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // use launcher with pid
      pidLauncher = new Builder()
          .setPid(pid)
          .build();

      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      // validate the status
      final LocatorState actualStatus = pidLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(jvmArguments, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"), actualStatus.getJavaVersion());
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      } else {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
      }
      waitForPidToStop(pid);
      waitForFileToDelete(this.pidFile);

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Category(FlakyTest.class) // GEODE-569: BindException, random ports
  @Test
  public void testStatusUsingWorkingDirectory() throws Throwable {
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for locator to start
    int pid = 0;
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());

      // validate the status
      final LocatorState actualStatus = dirLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(jvmArguments, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"), actualStatus.getJavaVersion());
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testStatusWithEmptyPidFile() throws Exception {
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
    assertTrue(this.pidFile + " already exists", this.pidFile.createNewFile());

    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    final LocatorState actualStatus = dirLauncher.status();
    assertThat(actualStatus, is(notNullValue()));
    assertThat(actualStatus.getStatus(), is(equalTo(Status.NOT_RESPONDING)));
    assertThat(actualStatus.getPid(), is(nullValue()));
    assertThat(actualStatus.getUptime().intValue(), is(equalTo(0)));
    assertThat(actualStatus.getWorkingDirectory(), is(equalTo(this.temporaryFolder.getRoot().getCanonicalPath())));
    assertThat(actualStatus.getClasspath(), is(nullValue()));
    assertThat(actualStatus.getGemFireVersion(), is(equalTo(GemFireVersion.getGemFireVersion())));
    assertThat(actualStatus.getJavaVersion(), is(nullValue()));
    assertThat(actualStatus.getLogFile(), is(nullValue()));
    assertThat(actualStatus.getHost(), is(nullValue()));
    assertThat(actualStatus.getMemberName(), is(nullValue()));
  }

  @Test
  public void testStatusWithNoPidFile() throws Exception {
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    LocatorState locatorState = dirLauncher.status();
    assertEquals(Status.NOT_RESPONDING, locatorState.getStatus());
  }

  @Test
  public void testStatusWithStalePidFile() throws Exception {
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
    final int pid = 0;
    assertFalse(ProcessUtils.isProcessAlive(pid));
    writePid(this.pidFile, pid);

    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    final LocatorState actualStatus = dirLauncher.status();
    assertThat(actualStatus, is(notNullValue()));
    assertThat(actualStatus.getStatus(), is(equalTo(Status.NOT_RESPONDING)));
    assertThat(actualStatus.getPid(), is(nullValue()));
    assertThat(actualStatus.getUptime().intValue(), is(equalTo(0)));
    assertThat(actualStatus.getWorkingDirectory(), is(equalTo(this.temporaryFolder.getRoot().getCanonicalPath())));
    assertThat(actualStatus.getClasspath(), is(nullValue()));
    assertThat(actualStatus.getGemFireVersion(), is(equalTo(GemFireVersion.getGemFireVersion())));
    assertThat(actualStatus.getJavaVersion(), is(nullValue()));
    assertThat(actualStatus.getLogFile(), is(nullValue()));
    assertThat(actualStatus.getHost(), is(nullValue()));
    assertThat(actualStatus.getMemberName(), is(nullValue()));
  }

  @Test
  public void testStopUsingPid() throws Throwable {
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream())
        .inputListener(createLoggingListener("sysout", getUniqueName() + "#sysout")).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream())
        .inputListener(createLoggingListener("syserr", getUniqueName() + "#syserr")).build().start();

    // wait for locator to start
    int pid = 0;
    LocatorLauncher pidLauncher = null;
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // use launcher with pid
      pidLauncher = new Builder()
          .setPid(pid)
          .build();

      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      // validate the status
      final LocatorState status = pidLauncher.status();
      assertNotNull(status);
      assertEquals(Status.ONLINE, status.getStatus());
      assertEquals(pid, status.getPid().intValue());

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      } else {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
      }
      waitForPidToStop(pid);
      waitForFileToDelete(pidFile);

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Category(FlakyTest.class) // GEODE-847: random ports, BindException, forks JVM, uses ErrorCollector
  @Test
  public void testStopUsingWorkingDirectory() throws Throwable {
    final List<String> jvmArguments = getJvmArguments();

    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(LocatorLauncher.class.getName());
    command.add(LocatorLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--port=" + this.locatorPort);
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for locator to start
    int pid = 0;
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName() + ".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      // stop the locator
      assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      waitForPidToStop(pid);
      assertFalse("PID file still exists!", this.pidFile.exists());

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  /**
   * Used only by {@link LocatorLauncherRemoteIntegrationTest#testRunningLocatorOutlivesForkingProcess}
   */
  public static class LocatorLauncherForkingProcess {

    public static void main(final String... args) throws FileNotFoundException {
      File file = new File(System.getProperty("user.dir"), LocatorLauncherForkingProcess.class.getSimpleName().concat(".log"));

      LocalLogWriter logWriter = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, new PrintStream(new FileOutputStream(file, true)));

      try {
        final int port = Integer.parseInt(args[0]);

        // launch LocatorLauncher
        List<String> command = new ArrayList<String>();
        command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getAbsolutePath());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("-D" + DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT+"=0");
        command.add(LocatorLauncher.class.getName());
        command.add(LocatorLauncher.Command.START.getName());
        command.add(LocatorLauncherForkingProcess.class.getSimpleName() + "_Locator");
        command.add("--port=" + port);
        command.add("--redirect-output");

        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main command: " + command);
        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main starting...");

        Process forkedProcess = new ProcessBuilder(command).start();

        @SuppressWarnings("unused")
        ProcessStreamReader processOutReader = new ProcessStreamReader.Builder(forkedProcess).inputStream(forkedProcess.getInputStream()).build().start();
        @SuppressWarnings("unused")
        ProcessStreamReader processErrReader = new ProcessStreamReader.Builder(forkedProcess).inputStream(forkedProcess.getErrorStream()).build().start();

        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main waiting for locator to start...");

        waitForLocatorToStart(port, TIMEOUT_MILLISECONDS, 10, true);

        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main exiting...");

        System.exit(0);
      } catch (Throwable t) {
        logWriter.info(LocatorLauncherForkingProcess.class.getSimpleName() + "#main error: " + t, t);
        System.exit(-1);
      }
    }
  }
}
