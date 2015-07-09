/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.process.LocalProcessLauncher;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;

/**
 * Multi-process tests for ProcessLauncher.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
@SuppressWarnings("serial")
public class LocalProcessLauncherDUnitTest extends DistributedTestCase {

  public LocalProcessLauncherDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    new File(getClass().getSimpleName()).mkdir();
  }
  
  @Override
  public void tearDown2() throws Exception {
  }
  
  public void testExistingPidFileThrows() throws Exception {
    final File pidFile = new File(getClass().getSimpleName() 
        + File.separator + "testExistingPidFileThrows.pid");
    final String absolutePath = pidFile.getAbsolutePath();

    assertFalse(pidFile.exists());
    new LocalProcessLauncher(pidFile, false);
    assertTrue(pidFile.exists());
    
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable("LocalProcessLauncherDUnitTest#testExistingPidFileThrows") {
      @Override
      public void run() {
        try {
          new LocalProcessLauncher(new File(absolutePath), false);
          fail("Two processes both succeeded in creating " + pidFile);
        } catch(FileAlreadyExistsException e) {
          // passed
        } catch(IllegalStateException e) {
          throw new AssertionError(e);
        } catch(IOException e) {
          throw new AssertionError(e);
        } catch(PidUnavailableException e) {
          throw new AssertionError(e);
        }
      }
    });
  }
  
  public void testForceReplacesExistingPidFile() throws Exception {
    final File pidFile = new File(getClass().getSimpleName() 
        + File.separator + "testForceReplacesExistingPidFile.pid");
    final String absolutePath = pidFile.getAbsolutePath();

    assertFalse(pidFile.exists());
    final LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);
    assertTrue(pidFile.exists());
    assertNotNull(launcher);
    final int pid = launcher.getPid();
    
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable("LocalProcessLauncherDUnitTest#testForceReplacesExistingPidFile") {
      @Override
      public void run() {
        try {
          final LocalProcessLauncher launcher = new LocalProcessLauncher(new File(absolutePath), true);
          assertNotSame(pid, launcher.getPid());
          assertFalse(pid == launcher.getPid());
          
          final FileReader fr = new FileReader(absolutePath);
          final BufferedReader br = new BufferedReader(fr);
          final int pidFromFile = Integer.parseInt(br.readLine());
          br.close();
          
          assertNotSame(pid, pidFromFile);
          assertFalse(pid == pidFromFile);
          assertEquals(launcher.getPid(), pidFromFile);
        } catch(IllegalStateException e) {
          throw new AssertionError(e);
        } catch(IOException e) {
          throw new AssertionError(e);
        } catch(FileAlreadyExistsException e) {
          throw new AssertionError(e);
        } catch(PidUnavailableException e) {
          throw new AssertionError(e);
        }
      }
    });
  }
  
  public void testPidFileReadByOtherProcess() throws Exception {
    final File pidFile = new File(getClass().getSimpleName() 
        + File.separator + "testPidFileReadByOtherProcess.pid");
    final String absolutePath = pidFile.getAbsolutePath();
    
    assertFalse(pidFile.exists());
    final LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);
    assertTrue(pidFile.exists());
    assertNotNull(launcher);
    final int pid = launcher.getPid();
    
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable("LocalProcessLauncherDUnitTest#testPidFileReadByOtherProcess") {
      @Override
      public void run() {
        try {
          final FileReader fr = new FileReader(absolutePath);
          final BufferedReader br = new BufferedReader(fr);
          final int pidFromFile = Integer.parseInt(br.readLine());
          br.close();
          assertEquals(pid, pidFromFile);
        } catch (FileNotFoundException e) {
          throw new Error(e);
        } catch (IOException e) {
          throw new Error(e);
        }
      }
    });
  }
}
