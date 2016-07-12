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

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.ServerLauncher.ServerState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.cache.AbstractCacheServer;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.net.SocketCreatorFactory;
import com.gemstone.gemfire.internal.process.*;
import com.gemstone.gemfire.test.junit.categories.FlakyTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.process.ProcessWrapper;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

/**
 * Integration tests for launching a Server in a forked process.
 *
 * @see com.gemstone.gemfire.distributed.AbstractLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder
 * @see com.gemstone.gemfire.distributed.ServerLauncher.ServerState
 * @see com.gemstone.gemfire.internal.AvailablePortHelper
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
public class ServerLauncherRemoteIntegrationTest extends AbstractServerLauncherRemoteIntegrationTestCase {
  
  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertTrue(factory.isAttachAPIFound());
  }
  
  @Test
  @Ignore("TRAC bug #52304: test is broken and needs to be reworked")
  public void testRunningServerOutlivesForkingProcess() throws Throwable {
    // launch ServerLauncherForkingProcess which then launches server
    
//    final List<String> command = new ArrayList<String>();
//    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
//    command.add("-cp");
//    command.add(System.getProperty("java.class.path"));
//    command.add(ServerLauncherDUnitTest.class.getName().concat("$").concat(ServerLauncherForkingProcess.class.getSimpleName()));
//
//    process = new ProcessBuilder(command).directory(temporaryFolder.getRoot()).start();
//    assertNotNull(process);
//    processOutReader = new ProcessStreamReader(process.getInputStream(), createListener("sysout", getUniqueName() + "#sysout")).start();
//    processErrReader = new ProcessStreamReader(process.getErrorStream(), createListener("syserr", getUniqueName() + "#syserr")).start();

    @SuppressWarnings("unused")
    File file = new File(this.temporaryFolder.getRoot(), ServerLauncherForkingProcess.class.getSimpleName().concat(".log"));
    //-logger.info("log file is " + file);
    
    final ProcessWrapper pw = new ProcessWrapper.Builder().mainClass(ServerLauncherForkingProcess.class).build();
    pw.execute(null, this.temporaryFolder.getRoot()).waitFor(true);
    //logger.info("[testRunningServerOutlivesForkingProcess] ServerLauncherForkingProcess output is:\n\n"+pw.getOutput());
    
//    // create waiting thread since waitFor does not have a timeout 
//    Thread waiting = new Thread(new Runnable() {
//      @Override
//      public void run() {
//        try {
//          assertIndexDetailsEquals(0, process.waitFor());
//        } catch (InterruptedException e) {
//          logger.error("Interrupted while waiting for process", e);
//        }
//      }
//    });

//    // start waiting thread and join to it for timeout
//    try {
//      waiting.start();
//      waiting.join(TIMEOUT_MILLISECONDS);
//      assertFalse("ServerLauncherForkingProcess took too long and caused timeout", waiting.isAlive());
//      
//    } catch (Throwable e) {
//      logger.error(e);
//      if (failure == null) {
//        failure = e;
//      }
//    } finally {
//      if (waiting.isAlive()) {
//        waiting.interrupt();
//      }
//    }

    // wait for server to start
    int pid = 0;
    final String serverName = ServerLauncherForkingProcess.class.getSimpleName()+"_server";
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart(dirLauncher);

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = serverName+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
      // validate the status
      final ServerState actualStatus = dirLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(getJvmArguments(), actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + serverName + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(serverName, actualStatus.getMemberName());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testStartCreatesPidFile() throws Throwable {
    // build and start the server
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();
    
      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
      // check the status
      final ServerState serverState = this.launcher.status();
      assertNotNull(serverState);
      assertEquals(Status.ONLINE, serverState.getStatus());

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testStartDeletesStaleControlFiles() throws Throwable {
    // create existing control files
    this.stopRequestFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getStopRequestFileName());
    this.stopRequestFile.createNewFile();
    assertTrue(this.stopRequestFile.exists());

    this.statusRequestFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getStatusRequestFileName());
    this.statusRequestFile.createNewFile();
    assertTrue(this.statusRequestFile.exists());

    this.statusFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getStatusFileName());
    this.statusFile.createNewFile();
    assertTrue(this.statusFile.exists());
    
    // build and start the server
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate stale control files were deleted
      waitForFileToDelete(this.stopRequestFile);
      waitForFileToDelete(this.statusRequestFile);
      waitForFileToDelete(this.statusFile);
      
      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Category(FlakyTest.class) // GEODE-721: random ports (setup overriding default port), TemporaryFolder
  @Test
  public void testStartOverwritesStalePidFile() throws Throwable {
    // create existing pid file
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the server
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertFalse(pid == Integer.MAX_VALUE);

      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  /**
   * Confirms fix for #47778.
   */
  @Test
  public void testStartUsingDisableDefaultServerLeavesPortFree() throws Throwable {
    assertTrue(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final List<String> jvmArguments = getJvmArguments();
    jvmArguments.add("-D" + AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.serverPort);
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // verify server did not a port
      assertTrue(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"\" instead of " + portString, "", portString);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testStartUsingDisableDefaultServerSkipsPortCheck() throws Throwable {
    // make serverPort in use
    this.socket = SocketCreatorFactory.getClusterSSLSocketCreator().createServerSocket(this.serverPort, 50, null, -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final List<String> jvmArguments = getJvmArguments();
    jvmArguments.add("-D" + AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.serverPort);
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"\" instead of " + portString, "", portString);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // verify port is still in use
    this.errorCollector.checkThat(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET), is(equalTo(false)));
  }

  @Category(FlakyTest.class) // GEODE-764: random ports, BindException, forks JVM, uses ErrorCollector
  @Test
  public void testStartUsingForceOverwritesExistingPidFile() throws Throwable {
    // create existing pid file
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
    final int otherPid = getPid();
    assertTrue("Pid " + otherPid + " should be alive", ProcessUtils.isProcessAlive(otherPid));
    writePid(this.pidFile, otherPid);

    // build and start the server
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");
    command.add("--force");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertTrue(pid != otherPid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  /**
   * Confirms fix for #47665.
   */
  @Test
  public void testStartUsingServerPortInUseFails() throws Throwable {
    // make serverPort in use
    this.socket = SocketCreatorFactory.getClusterSSLSocketCreator().createServerSocket(this.serverPort, 50, null, -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--server-port=" + this.serverPort);

    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();
    
    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).inputListener(createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).inputListener(createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).build().start();

    // wait for server to start and fail
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      int code = this.process.waitFor();
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    try {
      // check the status
      final ServerState serverState = dirLauncher.status();
      assertNotNull(serverState);
      assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
      
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // if the following fails, then the SHORTER_TIMEOUT is too short for slow machines
    // or this test needs to use MainLauncher in ProcessWrapper
    
    // validate that output contained BindException 
    this.errorCollector.checkThat(outputContainedExpectedString.get(), is(equalTo(true)));

    // just in case the launcher started...
    ServerState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  /**
   * Confirms part of fix for #47664
   */
  @Test
  public void testStartUsingServerPortOverridesCacheXml() throws Throwable {
    // generate two free ports
    final int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    
    // write out cache.xml with one port
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer().setPort(freeTCPPorts[0]);
    
    File cacheXmlFile = new File(this.temporaryFolder.getRoot(), getUniqueName()+".xml");
    final PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();
    
    // launch server and specify a different port
    final List<String> jvmArguments = getJvmArguments();
    jvmArguments.add("-D" + DistributionConfig.GEMFIRE_PREFIX + "" + CACHE_XML_FILE + "=" + cacheXmlFile.getCanonicalPath());
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--server-port=" + freeTCPPorts[1]);

    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();
    
    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).inputListener(createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).inputListener(createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).build().start();
    
    // wait for server to start up
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();
  
      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // verify server used --server-port instead of default or port in cache.xml
      assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[0], AvailablePort.SOCKET));
      assertFalse(AvailablePort.isPortAvailable(freeTCPPorts[1], AvailablePort.SOCKET));
      
      ServerState status = this.launcher.status();
      String portString = status.getPort();
      int port = Integer.valueOf(portString);
      assertEquals("Port should be " + freeTCPPorts[1] + " instead of " + port, freeTCPPorts[1], port);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
      waitForFileToDelete(this.pidFile);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  /**
   * Confirms part of fix for #47664
   */
  @Test
  public void testStartUsingServerPortUsedInsteadOfDefaultCacheXml() throws Throwable {
    // write out cache.xml with one port
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer();
    
    File cacheXmlFile = new File(this.temporaryFolder.getRoot(), getUniqueName()+".xml");
    final PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();
  
    // launch server and specify a different port
    final List<String> jvmArguments = getJvmArguments();
    jvmArguments.add("-D" + DistributionConfig.GEMFIRE_PREFIX + "" + CACHE_XML_FILE + "=" + cacheXmlFile.getCanonicalPath());
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    command.add("--server-port=" + this.serverPort);

    final String expectedString = "java.net.BindException";
    final AtomicBoolean outputContainedExpectedString = new AtomicBoolean();
    
    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).inputListener(createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).inputListener(createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).build().start();
    
    // wait for server to start up
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();
  
      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // verify server used --server-port instead of default or port in cache.xml
      assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      int port = Integer.valueOf(portString);
      assertEquals("Port should be " + this.serverPort + " instead of " + port, this.serverPort, port);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Category(FlakyTest.class) // GEODE-1135: random ports, BindException, fork JVM
  @Test
  public void testStartWithDefaultPortInUseFails() throws Throwable {
    String expectedString = "java.net.BindException";
    AtomicBoolean outputContainedExpectedString = new AtomicBoolean();

    // make serverPort in use
    this.socket = SocketCreatorFactory.getClusterSSLSocketCreator().createServerSocket(this.serverPort, 50, null, -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // launch server
    final List<String> jvmArguments = getJvmArguments();
    jvmArguments.add("-D" + AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY + "=" + this.serverPort);
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--redirect-output");
    
    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).inputListener(createExpectedListener("sysout", getUniqueName() + "#sysout", expectedString, outputContainedExpectedString)).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).inputListener(createExpectedListener("syserr", getUniqueName() + "#syserr", expectedString, outputContainedExpectedString)).build().start();
    
    // wait for server to start up
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      int code = this.process.waitFor();
      assertEquals("Expected exit code 1 but was " + code, 1, code);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    try {
      // check the status
      final ServerState serverState = dirLauncher.status();
      assertNotNull(serverState);
      assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // if the following fails, then the SHORTER_TIMEOUT might be too short for slow machines
    // or this test needs to use MainLauncher in ProcessWrapper
    
    // validate that output contained BindException 
    this.errorCollector.checkThat(outputContainedExpectedString.get(), is(equalTo(true)));

    // just in case the launcher started...
    ServerState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  @Ignore("Need to rewrite this without using dunit.Host")
  public void testStartWithExistingPidFileFails() throws Throwable {
  }/*
    this.temporaryFolder.getRoot() = new File(getUniqueName());
    this.temporaryFolder.getRoot().mkdir();
    assertTrue(this.temporaryFolder.getRoot().isDirectory() && this.temporaryFolder.getRoot().canWrite());

    // create existing pid file
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invoke(() -> ProcessUtils.identifyPid());
    assertFalse("Remote pid shouldn't be the same as local pid " + realPid, realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);
    
    // build and start the server
    final List<String> jvmArguments = getJvmArguments();
    
    final List<String> command = new ArrayList<String>();
    command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    for (String jvmArgument : jvmArguments) {
      command.add(jvmArgument);
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // collect and throw the FIRST failure
    Throwable failure = null;
    
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart(dirLauncher, 10*1000, false);
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
      
    try {
      // check the status
      final ServerState serverState = dirLauncher.status();
      assertNotNull(serverState);
      assertIndexDetailsEquals(Status.NOT_RESPONDING, serverState.getStatus());
      
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());
      
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    ServerState status = null;
    try {
      status = dirLauncher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
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

  @Category(FlakyTest.class) // GEODE-957: random ports, BindException, fork JVM
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
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for server to start
    int pid = 0;
    ServerLauncher pidLauncher = null; 
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // use launcher with pid
      pidLauncher = new Builder()
          .setPid(pid)
          .build();

      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      // validate the status
      final ServerState actualStatus = pidLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(jvmArguments, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      } else {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
      }          
      waitForPidToStop(pid);
      waitForFileToDelete(this.pidFile);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
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
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      assertNotNull(this.launcher);
      assertFalse(this.launcher.isRunning());

      // validate the status
      final ServerState actualStatus = this.launcher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath(), actualStatus.getWorkingDirectory());
      assertEquals(jvmArguments, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStatusWithEmptyPidFile() throws Exception {
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
    assertTrue(this.pidFile + " already exists", this.pidFile.createNewFile());
    
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    final ServerState actualStatus = dirLauncher.status();
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
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    ServerState serverState = dirLauncher.status();
    assertEquals(Status.NOT_RESPONDING, serverState.getStatus());
  }
  
  @Test
  public void testStatusWithStalePidFile() throws Exception {
    this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
    final int pid = 0;
    assertFalse(ProcessUtils.isProcessAlive(pid));
    writePid(this.pidFile, pid);
    
    final ServerLauncher dirLauncher = new ServerLauncher.Builder()
        .setWorkingDirectory(temporaryFolder.getRoot().getCanonicalPath())
        .build();
    final ServerState actualStatus = dirLauncher.status();
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
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).inputListener(createLoggingListener("sysout", getUniqueName() + "#sysout")).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).inputListener(createLoggingListener("syserr", getUniqueName() + "#syserr")).build().start();

    // wait for server to start
    int pid = 0;
    ServerLauncher pidLauncher = null; 
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

      // use launcher with pid
      pidLauncher = new Builder()
          .setPid(pid)
          .build();

      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      // validate the status
      final ServerState status = pidLauncher.status();
      assertNotNull(status);
      assertEquals(Status.ONLINE, status.getStatus());
      assertEquals(pid, status.getPid().intValue());

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      if (pidLauncher == null) {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      } else {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
      }          
      waitForPidToStop(pid);
      waitForFileToDelete(this.pidFile);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
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
    command.add(ServerLauncher.class.getName());
    command.add(ServerLauncher.Command.START.getName());
    command.add(getUniqueName());
    command.add("--disable-default-server");
    command.add("--redirect-output");

    this.process = new ProcessBuilder(command).directory(this.temporaryFolder.getRoot()).start();
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).build().start();

    // wait for server to start
    int pid = 0;
    this.launcher = new ServerLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForServerToStart();

      // validate the pid file and its contents
      this.pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(this.temporaryFolder.getRoot(), logFileName).exists());

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      // stop the server
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForPidToStop(pid);
      assertFalse("PID file still exists!", this.pidFile.exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Override
  protected List<String> getJvmArguments() {
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + DistributionConfig.GEMFIRE_PREFIX + ConfigurationProperties.LOG_LEVEL+"=config");
    jvmArguments.add("-D" + DistributionConfig.GEMFIRE_PREFIX + ConfigurationProperties.MCAST_PORT+"=0");
    return jvmArguments;
  }
  
  /**
   * Used only by {@link ServerLauncherRemoteIntegrationTest#testRunningServerOutlivesForkingProcess}
   */
  public static class ServerLauncherForkingProcess {

    public static void main(final String... args) throws IOException, PidUnavailableException {
      //-System.out.println("inside main");
      File file = new File(System.getProperty("user.dir"), ServerLauncherForkingProcess.class.getSimpleName().concat(".log"));
      file.createNewFile();
      LocalLogWriter logWriter = new LocalLogWriter(InternalLogWriter.ALL_LEVEL, new PrintStream(new FileOutputStream(file, true)));
      //LogWriter logWriter = new PureLogWriter(LogWriterImpl.ALL_LEVEL);
      logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main PID is " + getPid());

      try {
        // launch ServerLauncher
        final List<String> jvmArguments = null;//getJvmArguments();
        assertTrue(jvmArguments.size() == 2);
        final List<String> command = new ArrayList<String>();
        command.add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
        for (String jvmArgument : jvmArguments) {
          command.add(jvmArgument);
        }
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ServerLauncher.class.getName());
        command.add(ServerLauncher.Command.START.getName());
        command.add(ServerLauncherForkingProcess.class.getSimpleName()+"_server");
        command.add("--disable-default-server");
        command.add("--redirect-output");

        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main command: " + command);
        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main starting...");

        //-System.out.println("launching " + command);
        
        @SuppressWarnings("unused")
        Process forkedProcess = new ProcessBuilder(command).start();

//        processOutReader = new ProcessStreamReader(forkedProcess.getInputStream()).start();
//        processErrReader = new ProcessStreamReader(forkedProcess.getErrorStream()).start();

//        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main waiting for Server to start...");
//
//        File workingDir = new File(System.getProperty("user.dir"));
//        System.out.println("waiting for server to start in " + workingDir);
//        final ServerLauncher dirLauncher = new ServerLauncher.Builder()
//            .setWorkingDirectory(workingDir.getCanonicalPath())
//            .build();
//        waitForServerToStart(dirLauncher, true);

        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main exiting...");

        //-System.out.println("exiting");
        System.exit(0);
      }
      catch (Throwable t) {
        logWriter.info(ServerLauncherForkingProcess.class.getSimpleName() + "#main error: " + t, t);
        System.exit(-1);
      }
    }
  }
}
