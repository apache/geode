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
package com.gemstone.gemfire.internal.process;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Test;

import com.gemstone.gemfire.internal.util.StopWatch;

/**
 * Functional tests for ProcessStreamReader.
 * 
 */
public abstract class ProcessStreamReaderTestCase {
  
  /** Sleep timeout for {@link ProcessSleeps} instead of sleeping Long.MAX_VALUE */
  protected static final int PROCESS_FAILSAFE_TIMEOUT = 10*60*1000;

  /** Additional time for launched processes to live before terminating */
  protected static final int PROCESS_TIME_TO_LIVE = 3*500;
  
  /** Timeout to wait for a forked process to start */
  protected static final int WAIT_FOR_PROCESS_TO_START_TIMEOUT = 60*1000;
  
  /** Timeout to wait for a running process to die -- this keeps timing out so I'm increasing it very large */
  protected static final int WAIT_FOR_PROCESS_TO_DIE_TIMEOUT = 5*60*1000;
  
  /** Timeout to wait for a new {@link ProcessStreamReader} to be running */
  protected static final int WAIT_FOR_READER_IS_RUNNING_TIMEOUT = 20*1000;

  /** Timeout to join to a running ProcessStreamReader thread */
  protected static final int READER_JOIN_TIMEOUT = 20*1000;
  
  /** Brief time to sleep before repeating a conditional check */
  protected static final int INTERVAL = 20;
  
  protected Process process;
  protected ProcessStreamReader stderr;
  protected ProcessStreamReader stdout;
  
  @After
  public void stopReadersAndDestroyProcess() throws Exception {
    if (this.stderr != null) {
      this.stderr.stop();
    }
    if (this.stdout != null) {
      this.stdout.stop();
    }
    if (this.process != null) {
      this.process.destroy(); // this is async and can require more than 10 seconds in Jenkins 
      /*assertEventuallyFalse("Timed out destroying process after " + WAIT_FOR_PROCESS_TO_DIE_TIMEOUT/(60*1000) + " minutes", new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return isAlive(process);
        }
      }, WAIT_FOR_PROCESS_TO_DIE_TIMEOUT, INTERVAL);*/
    }
  }
  
  @Test
  public void processLivesAfterClosingStreams() throws Exception {
    this.process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    this.process.getErrorStream().close();
    this.process.getOutputStream().close();
    this.process.getInputStream().close();
    assertIsAlive(process);
    this.process.destroy();
  }
  
  @Test
  public void processTerminatesWhenDestroyed() throws Exception {
    this.process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    assertIsAlive(this.process);
    this.process.destroy();
    assertEventuallyFalse("Timed out destroying process", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return isAlive(process);
      }
    }, WAIT_FOR_PROCESS_TO_DIE_TIMEOUT, INTERVAL);
    assertNotEquals(0, this.process.exitValue());
  }
  
  protected static void assertEventuallyTrue(final String message, final Callable<Boolean> callable, final int timeout, final int interval) throws Exception {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < timeout; done = (callable.call())) {
      Thread.sleep(interval);
    }
    assertTrue(message + " within timeout of " + timeout + " milliseconds", done);
  }
  
  protected static void assertEventuallyFalse(final String message, final Callable<Boolean> callable, final int timeout, final int interval) throws Exception {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < timeout; done = (!callable.call())) {
      Thread.sleep(interval);
    }
    assertTrue(message + " within timeout of " + timeout + " milliseconds", done);
  }
  
  protected static void assertIsAlive(final Process process) {
    assertTrue(isAlive(process));
  }
  
  protected static void assertIsNotAlive(final Process process) {
    assertFalse(isAlive(process));
  }
  
  protected static boolean isAlive(final Process process) {
    try {
      process.exitValue();
      return false;
    } catch (IllegalThreadStateException e) {
      return true;
    }
  }
  
  protected static String getJavaPath() {
    String java = "java";
//    if (SystemUtils.isWindows()) {
//      java = "javaw";
//    }
    return new File(new File(System.getProperty("java.home"), "bin"), java).getPath();
  }
  
  protected static String getClassPath() {
    return System.getProperty("java.class.path");
  }
  
  protected static String[] createCommandLine(final Class<?> clazz) {
    return createCommandLine(clazz, null);
  }
  
  protected static String[] createCommandLine(final Class<?> clazz, final String[] jvmArgsOpts) {
    List<String> commandLine = new ArrayList<>();
    
    commandLine.add(getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine.add(getClassPath());

    addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);
    
    commandLine.add("-Djava.awt.headless=true");
    commandLine.add(clazz.getName());
    
    return commandLine.toArray(new String[commandLine.size()]);
  }
  
  protected static void addJvmArgumentsAndOptions(final List<String> commandLine, final String[] jvmArgsOpts) {
    if (jvmArgsOpts != null) {
      commandLine.addAll(Arrays.asList(jvmArgsOpts));
    }
  }
  
  protected static void assertEventuallyIsRunning(final ProcessStreamReader reader) throws Exception {
    assertEventuallyTrue("Waiting for ProcessStreamReader to be running", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return reader.isRunning();
      }
    }, WAIT_FOR_READER_IS_RUNNING_TIMEOUT, INTERVAL);
  }

  protected static class ProcessSleeps {
    public static void main(String[] args) throws InterruptedException {
      Thread.sleep(PROCESS_FAILSAFE_TIMEOUT);
    }
  }

  protected static class ProcessThrowsError {
    protected static String[] LINES = new String[] { "ProcessThrowsError is starting\n", "ProcessThrowsError is sleeping\n", "ProcessThrowsError is throwing\n" };
    protected static String ERROR_MSG = "ProcessThrowsError throws Error";
    public static void main(String[] args) throws InterruptedException {
      System.err.print(LINES[0]);
      System.err.print(LINES[1]);
      Thread.sleep(PROCESS_TIME_TO_LIVE);
      System.err.print(LINES[2]);
      throw new Error(ERROR_MSG);
    }
  }

  protected static class ProcessPrintsToStdout {
    protected static String[] LINES = new String[] { "ProcessPrintsToStdout is starting\n", "ProcessPrintsToStdout is sleeping\n", "ProcessPrintsToStdout is exiting\n" };
    public static void main(String[] args) throws InterruptedException {
      System.out.print(LINES[0]);
      System.out.print(LINES[1]);
      Thread.sleep(PROCESS_TIME_TO_LIVE);
      System.out.print(LINES[2]);
    }
  }

  protected static class ProcessPrintsToStderr {
    protected static String[] LINES = new String[] { "ProcessPrintsToStdout is starting\n", "ProcessPrintsToStdout is sleeping\n", "ProcessPrintsToStdout is exiting\n" };
    public static void main(String[] args) throws InterruptedException {
      System.err.print(LINES[0]);
      System.err.print(LINES[1]);
      Thread.sleep(PROCESS_TIME_TO_LIVE);
      System.err.print(LINES[2]);
    }
  }

  protected static class ProcessPrintsToBoth {
    protected static String[] OUT_LINES = new String[] { "ProcessPrintsToBoth(out) is starting\n", "ProcessPrintsToBoth(out) is sleeping\n", "ProcessPrintsToBoth(out) is exiting\n" };
    protected static String[] ERR_LINES = new String[] { "ProcessPrintsToBoth(err) is starting\n", "ProcessPrintsToBoth(err) is sleeping\n", "ProcessPrintsToBoth(err) is exiting\n" };
    public static void main(String[] args) throws InterruptedException {
      System.out.print(OUT_LINES[0]);
      System.err.print(ERR_LINES[0]);
      System.out.print(OUT_LINES[1]);
      System.err.print(ERR_LINES[1]);
      Thread.sleep(PROCESS_TIME_TO_LIVE);
      System.out.print(OUT_LINES[2]);
      System.err.print(ERR_LINES[2]);
    }
  }
}
