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

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessStreamReader;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.lang.AttachAPINotFoundException;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Subclass of LocatorLauncherRemoteDUnitTest which forces the code to not find 
 * the Attach API which is in the JDK tools.jar.  As a result LocatorLauncher
 * ends up using the FileProcessController implementation.
 * 
 * @since 8.0
 */
@Category(IntegrationTest.class)
public class LocatorLauncherRemoteFileJUnitTest extends LocatorLauncherRemoteJUnitTest {
  
  @Before
  public final void setUpLocatorLauncherRemoteFileTest() throws Exception {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }
  
  @After
  public final void tearDownLocatorLauncherRemoteFileTest() throws Exception {   
  }
  
  @Override
  @Test
  /**
   * Override and assert Attach API is NOT found
   */
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertFalse(factory.isAttachAPIFound());
  }
  
  @Override
  @Test
  /**
   * Override because FileProcessController cannot request status with PID
   */
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
      final File pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(pidFile.exists());
      pid = readPid(pidFile);
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

      // status with pid only should throw AttachAPINotFoundException
      try {
        pidLauncher.status();
        fail("FileProcessController should have thrown AttachAPINotFoundException");
      } catch (AttachAPINotFoundException e) {
        // passed
      }
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the locator
    try {
      assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      waitForPidToStop(pid, true);
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Override
  @Test
  /**
   * Override because FileProcessController cannot request stop with PID
   */
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
    this.processOutReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getInputStream()).inputListener(createLoggingListener("sysout", getUniqueName() + "#sysout")).build().start();
    this.processErrReader = new ProcessStreamReader.Builder(this.process).inputStream(this.process.getErrorStream()).inputListener(createLoggingListener("syserr", getUniqueName() + "#syserr")).build().start();

    // wait for locator to start
    int pid = 0;
    File pidFile = null;
    LocatorLauncher pidLauncher = null; 
    final LocatorLauncher dirLauncher = new LocatorLauncher.Builder()
        .setWorkingDirectory(this.temporaryFolder.getRoot().getCanonicalPath())
        .build();
    try {
      waitForLocatorToStart(dirLauncher);

      // validate the pid file and its contents
      pidFile = new File(this.temporaryFolder.getRoot(), ProcessType.LOCATOR.getPidFileName());
      assertTrue(pidFile.exists());
      pid = readPid(pidFile);
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

      // stop with pid only should throw AttachAPINotFoundException
      try {
        pidLauncher.stop();
        fail("FileProcessController should have thrown AttachAPINotFoundException");
      } catch (AttachAPINotFoundException e) {
        // passed
      }

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      // stop the locator
      assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
      waitForPidToStop(pid);
      waitForFileToDelete(pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
}
