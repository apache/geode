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
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.net.SocketCreatorFactory;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;
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
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetAddress;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

/**
 * Tests usage of LocatorLauncher as a local API in existing JVM.
 *
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class LocatorLauncherLocalIntegrationTest extends AbstractLocatorLauncherIntegrationTestCase {

  @Before
  public final void setUpLocatorLauncherLocalIntegrationTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.TEST_PREFIX_PROPERTY, getUniqueName()+"-");
  }

  @After
  public final void tearDownLocatorLauncherLocalIntegrationTest() throws Exception {
    disconnectFromDS();
  }

  @Test
  public void testBuilderSetProperties() throws Throwable {
    this.launcher = new Builder()
        .setForce(true)
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(DISABLE_AUTO_RECONNECT, "true")
        .set(LOG_LEVEL, "config")
        .set(MCAST_PORT, "0")
        .build();

    try {
      assertEquals(Status.ONLINE, this.launcher.start().getStatus());
      waitForLocatorToStart(this.launcher, true);
  
      final InternalLocator locator = this.launcher.getLocator();
      assertNotNull(locator);
  
      final DistributedSystem distributedSystem = locator.getDistributedSystem();
  
      assertNotNull(distributedSystem);
      assertEquals("true", distributedSystem.getProperties().getProperty(DISABLE_AUTO_RECONNECT));
      assertEquals("0", distributedSystem.getProperties().getProperty(MCAST_PORT));
      assertEquals("config", distributedSystem.getProperties().getProperty(LOG_LEVEL));
      assertEquals(getUniqueName(), distributedSystem.getProperties().getProperty(NAME));
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      assertNull(this.launcher.getLocator());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertTrue(factory.isAttachAPIFound());
  }
  
  @Test
  public void testStartCreatesPidFile() throws Throwable {
    this.launcher = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config")
        .build();

    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
      assertEquals(Status.ONLINE, this.launcher.status().getStatus());

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);

      assertEquals(Status.ONLINE, this.launcher.status().getStatus());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

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
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    try {
      waitForLocatorToStart(this.launcher);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);
      
      // validate stale control files were deleted
      assertFalse(stopRequestFile.exists());
      assertFalse(statusRequestFile.exists());
      assertFalse(statusFile.exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStartOverwritesStalePidFile() throws Throwable {
    // create existing pid file
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
    assertFalse("Integer.MAX_VALUE shouldn't be the same as local pid " + Integer.MAX_VALUE, Integer.MAX_VALUE == ProcessUtils.identifyPid());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    try {
      waitForLocatorToStart(this.launcher);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  @Ignore("Need to rewrite this without using dunit.Host")
  public void testStartUsingForceOverwritesExistingPidFile() throws Throwable {
  }
  /*
    assertTrue(getUniqueName() + " is broken if PID == Integer.MAX_VALUE", ProcessUtils.identifyPid() != Integer.MAX_VALUE);
    
    // create existing pid file
    this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invoke(() -> ProcessUtils.identifyPid());
    assertFalse(realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);

    // build and start the locator
    final Builder builder = new Builder()
        .setForce(true)
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)

    assertTrue(builder.getForce());
    this.launcher = builder.build();
    assertTrue(this.launcher.isForcing());
    this.launcher.start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertIndexDetailsEquals(getPid(), pid);
      
      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      assertIndexDetailsEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartUsingForceOverwritesExistingPidFile
  */
  
  @Test
  public void testStartWithDefaultPortInUseFails() throws Throwable {
    // Test makes no sense in this case
    if (this.locatorPort == 0) {
      return;
    }

    this.socket = SocketCreatorFactory.getClusterSSLSocketCreator().createServerSocket(this.locatorPort, 50, null, -1);
    assertTrue(this.socket.isBound());
    assertFalse(this.socket.isClosed());
    assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));

    assertNotNull(System.getProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY));
    assertEquals(this.locatorPort, Integer.valueOf(System.getProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY)).intValue());
    assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
    
    this.launcher = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config")
        .build();
    
    assertEquals(this.locatorPort, this.launcher.getPort().intValue());
    
    RuntimeException expected = null;
    try {
      this.launcher.start();
     
      // why did it not fail like it's supposed to?
      final String property = System.getProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY);
      assertNotNull(property);
      assertEquals(this.locatorPort, Integer.valueOf(property).intValue());
      assertFalse(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
      assertEquals(this.locatorPort, this.launcher.getPort().intValue());
      assertEquals(this.locatorPort, this.socket.getLocalPort());
      assertTrue(this.socket.isBound());
      assertFalse(this.socket.isClosed());
      
      fail("LocatorLauncher start should have thrown RuntimeException caused by BindException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      // BindException text varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof BindException);
      // BindException string varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.pidFile = new File (this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertFalse("Pid file should not exist: " + this.pidFile, this.pidFile.exists());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      waitForFileToDelete(this.pidFile);
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  @Ignore("Need to rewrite this without using dunit.Host")
  public void testStartWithExistingPidFileFails() throws Throwable {
  }/*
    // create existing pid file
    final int realPid = Host.getHost(0).getVM(3).invoke(() -> ProcessUtils.identifyPid());
    assertFalse("Remote pid shouldn't be the same as local pid " + realPid, realPid == ProcessUtils.identifyPid());

    this.pidFile = new File(ProcessType.LOCATOR.getPidFileName());
    writePid(this.pidFile, realPid);
    
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .set(logLevel, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    // collect and throw the FIRST failure
    Throwable failure = null;
    RuntimeException expected = null;
    
    try {
      this.launcher.start();
      fail("LocatorLauncher start should have thrown RuntimeException caused by FileAlreadyExistsException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      assertTrue(expected.getMessage(), expected.getMessage().contains("A PID file already exists and a Locator may be running in"));
      assertIndexDetailsEquals(RuntimeException.class, expected.getClass());
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof FileAlreadyExistsException);
      assertTrue(cause.getMessage().contains("Pid file already exists: "));
      assertTrue(cause.getMessage().contains("vf.gf.locator.pid for process " + realPid));
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      delete(this.pidFile);
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
  public void testStartUsingPort() throws Throwable {
    // generate one free port and then use it instead of default
    final int freeTCPPort = AvailablePortHelper.getRandomAvailableTCPPort();
    assertTrue(AvailablePort.isPortAvailable(freeTCPPort, AvailablePort.SOCKET));

    this.launcher = new Builder()
        .setMemberName(getUniqueName())
        .setPort(freeTCPPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config")
        .build();

    int pid = 0;
    try {
      // if start succeeds without throwing exception then #47778 is fixed
      this.launcher.start();
      waitForLocatorToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(pidFile.exists());
      pid = readPid(pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);

      // verify locator did not use default port
      assertTrue(AvailablePort.isPortAvailable(this.locatorPort, AvailablePort.SOCKET));
      
      final LocatorState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"" + freeTCPPort + "\" instead of " + portString, String.valueOf(freeTCPPort), portString);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStartUsingPortInUseFails() throws Throwable {
    // Test makes no sense in this case
    if (this.locatorPort == 0) {
      return;
    }

    // generate one free port and then use it instead of default
    this.socket = SocketCreatorFactory.getClusterSSLSocketCreator().createServerSocket(this.locatorPort, 50, null, -1);
    
    this.launcher = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config")
        .build();
    
    RuntimeException expected = null;
    try {
      this.launcher.start();
      fail("LocatorLauncher start should have thrown RuntimeException caused by BindException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      // BindException string varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof BindException);
      // BindException string varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.pidFile = new File (this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertFalse("Pid file should not exist: " + this.pidFile, this.pidFile.exists());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // just in case the launcher started...
    LocatorState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      waitForFileToDelete(this.pidFile);
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStatusUsingPid() throws Throwable {
    // build and start the locator
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    LocatorLauncher pidLauncher = null;
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
      
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue("Pid file " + this.pidFile.getCanonicalPath().toString() + " should exist", this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      final LocatorState actualStatus = pidLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      // getWorkingDirectory returns user.dir instead of rootFolder because test is starting Locator in this process (to move logFile and pidFile into temp dir)
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.workingDirectory + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    if (pidLauncher == null) {
      try {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
      
    } else {
      try {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
    }
  }
  
  @Test
  public void testStatusUsingWorkingDirectory() throws Throwable {
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    LocatorLauncher dirLauncher = null;
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
      
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue("Pid file " + this.pidFile.getCanonicalPath().toString() + " should exist", this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      dirLauncher = new Builder().setWorkingDirectory(this.workingDirectory).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());

      final LocatorState actualStatus = dirLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      // getWorkingDirectory returns user.dir instead of rootFolder because test is starting Locator in this process (to move logFile and pidFile into temp dir)
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.workingDirectory + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    if (dirLauncher == null) {
      try {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
      
    } else {
      try {
        assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
    }
  }
  
  @Test
  public void testStopUsingPid() throws Throwable {
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    LocatorLauncher pidLauncher = null;
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());
      
      // stop the locator
      final LocatorState locatorState = pidLauncher.stop();
      assertNotNull(locatorState);
      assertEquals(Status.STOPPED, locatorState.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.launcher.stop();
    } catch (Throwable e) {
      // ignore
    }

    try {
      // verify the PID file was deleted
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStopUsingWorkingDirectory() throws Throwable {
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setPort(this.locatorPort)
        .setRedirectOutput(true)
        .setWorkingDirectory(this.workingDirectory)
        .set(CLUSTER_CONFIGURATION_DIR, this.clusterConfigDirectory)
        .set(LOG_LEVEL, "config");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    LocatorLauncher dirLauncher = null;
    try {
      this.launcher.start();
      waitForLocatorToStart(this.launcher);
    
      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue("Pid file " + this.pidFile.getCanonicalPath().toString() + " should exist", this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      dirLauncher = new Builder().setWorkingDirectory(this.workingDirectory).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());
      
      // stop the locator
      final LocatorState locatorState = dirLauncher.stop();
      assertNotNull(locatorState);
      assertEquals(Status.STOPPED, locatorState.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.launcher.stop();
    } catch (Throwable e) {
      // ignore
    }

    try {
      // verify the PID file was deleted
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
}
