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
package org.apache.geode.internal.process;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.OSProcess;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Unit tests for ProcessLauncher.
 * 
 * @since GemFire 7.0
 */
@Category(IntegrationTest.class)
public class LocalProcessLauncherJUnitTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  private File pidFile;

  @Before
  public void setUp() throws Exception {
    this.pidFile = new File(this.temporaryFolder.getRoot(), testName.getMethodName() + ".pid");
  }

  @Test
  public void testPidAccuracy() throws PidUnavailableException {
    int pid = ProcessUtils.identifyPid();
    assertTrue(pid > 0);
    int osProcessPid = OSProcess.getId();
    if (osProcessPid > 0) {
      assertEquals(OSProcess.getId(), pid);
    } else {
      // not much to test if OSProcess native code is unusable
    }
  }
  
  @Test
  public void testPidFileIsCreated() throws Exception {
    assertFalse(pidFile.exists());
    new LocalProcessLauncher(pidFile, false);
    assertTrue(pidFile.exists());
  }
  
  @Test
  public void testPidFileContainsPid() throws Exception {
    final LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);
    assertNotNull(launcher);
    assertTrue(pidFile.exists());
    
    final FileReader fr = new FileReader(pidFile);
    final BufferedReader br = new BufferedReader(fr);
    final int pid = Integer.parseInt(br.readLine());
    br.close();
    
    assertTrue(pid > 0);
    assertEquals(launcher.getPid(), pid);
    assertEquals(ProcessUtils.identifyPid(), pid);
  }
  
  @Test
  public void testPidFileIsCleanedUp() throws Exception {
    final LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);
    assertTrue(pidFile.exists());
    launcher.close(); // TODO: launch an external JVM and then close it nicely
    assertFalse(pidFile.exists());
  }
  
  @Test
  public void testExistingPidFileThrows() throws Exception {
    assertTrue(pidFile.createNewFile());
    assertTrue(pidFile.exists());
    
    final FileWriter writer = new FileWriter(pidFile);
    // use a read pid that exists
    writer.write(String.valueOf(ProcessUtils.identifyPid()));
    writer.close();

    try {
      new LocalProcessLauncher(pidFile, false);
      fail("LocalProcessLauncher should have thrown FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      // passed
    }
  }

  @Test
  public void testStalePidFileIsReplaced() throws Exception {
    assertTrue(pidFile.createNewFile());
    assertTrue(pidFile.exists());
    
    final FileWriter writer = new FileWriter(pidFile);
    writer.write(String.valueOf(Integer.MAX_VALUE));
    writer.close();

    try {
      new LocalProcessLauncher(pidFile, false);
    } catch (FileAlreadyExistsException e) {
      fail("LocalProcessLauncher should not have thrown FileAlreadyExistsException");
    }

    final FileReader fr = new FileReader(pidFile);
    final BufferedReader br = new BufferedReader(fr);
    final int pid = Integer.parseInt(br.readLine());
    br.close();

    assertTrue(pid > 0);
    assertEquals(ProcessUtils.identifyPid(), pid);
  }

  @Test
  public void testForceReplacesExistingPidFile() throws Exception {
    assertTrue("testForceReplacesExistingPidFile is broken if PID == Integer.MAX_VALUE", 
        ProcessUtils.identifyPid() != Integer.MAX_VALUE);

    assertTrue(pidFile.createNewFile());
    assertTrue(pidFile.exists());
    
    final FileWriter writer = new FileWriter(pidFile);
    writer.write(String.valueOf(Integer.MAX_VALUE));
    writer.close();
    
    try {
      new LocalProcessLauncher(pidFile, true);
    } catch (FileAlreadyExistsException e) {
      fail("LocalProcessLauncher should not have thrown FileAlreadyExistsException");
    }

    final FileReader fr = new FileReader(pidFile);
    final BufferedReader br = new BufferedReader(fr);
    final int pid = Integer.parseInt(br.readLine());
    br.close();

    assertTrue(pid > 0);
    assertEquals(ProcessUtils.identifyPid(), pid);
  }
  
  @Test
  public void testPidUnavailableThrows() {
    final String name = "Name without PID";
    try {
      ProcessUtils.identifyPid(name);
      fail("PidUnavailableException should have been thrown for " + name);
    } catch (PidUnavailableException e) {
     // passed
    }
  }
}
