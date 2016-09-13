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
import static org.junit.Assume.*;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ProcessStreamReader.InputListener;
import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Tests BlockingProcessStreamReader. Most tests are skipped on Windows due to 
 * TRAC bug #51967 which is caused by a JDK bug. The test {@link #hangsOnWindows}
 * verifies the existence of the bug.
 * 
 * @since GemFire 8.2
 */
@Category(IntegrationTest.class)
public class BlockingProcessStreamReaderJUnitTest extends ProcessStreamReaderTestCase {

  /** Timeout to confirm hang on Windows */
  private static final int HANG_TIMEOUT = 10;
  
  private ExecutorService futures;
  
  @Before
  public void createFutures() {
    this.futures = Executors.newSingleThreadExecutor();
  }
  
  @After
  public void shutdownFutures() {
    assertTrue(this.futures.shutdownNow().isEmpty());
  }
  
  @Test
  public void hangsOnWindows() throws Exception {
    assumeTrue(SystemUtils.isWindows());
    
    this.process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .build();
    
    this.stderr.start();
    this.stdout.start();
    
    assertIsAlive(this.process);

    assertEventuallyIsRunning(this.stderr);
    assertEventuallyIsRunning(this.stdout);

    Future<Boolean> future = this.futures.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws IOException {
        process.getErrorStream().close();
        process.getOutputStream().close();
        process.getInputStream().close();
        return true;
      }
    });
    
    try {
      future.get(HANG_TIMEOUT, TimeUnit.SECONDS);
      // if the following fails then perhaps we're testing with a new JRE that 
      // fixes blocking reads of process streams on windows 
      fail("future should have timedout due to hang on windows");
    } catch (TimeoutException expected) {
      // verified hang on windows which causes TRAC bug #51967
    }
    
    this.process.destroy();
  }
  
  @Test
  public void canCloseStreamsWhileProcessIsAlive() throws Exception {
    assumeFalse(SystemUtils.isWindows());
    
    this.process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .build();
    
    this.stderr.start();
    this.stdout.start();
    
    assertIsAlive(this.process);

    assertEventuallyIsRunning(this.stderr);
    assertEventuallyIsRunning(this.stdout);

    this.process.getErrorStream().close();
    this.process.getOutputStream().close();
    this.process.getInputStream().close();
    
    assertIsAlive(this.process);

    this.process.destroy();
  }

  @Test
  public void canStopReadersWhileProcessIsAlive() throws Exception {
    assumeFalse(SystemUtils.isWindows());

    this.process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .build();
    
    this.stderr.start();
    this.stdout.start();
    
    assertIsAlive(this.process);
    
    assertEventuallyIsRunning(this.stderr);
    assertEventuallyIsRunning(this.stdout);

    this.stderr.stop();
    this.stdout.stop();
    
    this.process.getErrorStream().close();
    this.process.getOutputStream().close();
    this.process.getInputStream().close();
    
    assertIsAlive(this.process);

    this.process.destroy();
  }
  
  @Test
  public void capturesStdoutWhileProcessIsAlive() throws Exception {
    assumeFalse(SystemUtils.isWindows());
    
    this.process = new ProcessBuilder(createCommandLine(ProcessPrintsToStdout.class)).start();
    
    final StringBuffer stderrBuffer = new StringBuffer();
    InputListener stderrListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stderrBuffer.append(line);
      }
    };
    
    final StringBuffer stdoutBuffer = new StringBuffer();
    InputListener stdoutListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stdoutBuffer.append(line);
      }
    };
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .inputListener(stderrListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .inputListener(stdoutListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
  
    this.stderr.start();
    this.stdout.start();
    
    assertEventuallyIsRunning(this.stderr);
    assertEventuallyIsRunning(this.stdout);
    
    // wait for process to die
    assertEventuallyFalse("Process never died", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return ProcessUtils.isProcessAlive(process);
      }
    }, WAIT_FOR_PROCESS_TO_DIE_TIMEOUT, INTERVAL);

    final int exitValue = this.process.exitValue();
    assertEquals(0, exitValue);

    this.stderr.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stderr.isRunning());
    
    this.stdout.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stdout.isRunning());
    
    //System.out.println("Stopping ProcessStreamReader");
    this.stderr.stop();
    this.stdout.stop();
    
    //System.out.println("stderr=\n" + stderrBuffer.toString());
    assertEquals("", stderrBuffer.toString());
    
    //System.out.println("stdout=\n" + stdoutBuffer.toString());
    StringBuilder sb = new StringBuilder().append(ProcessPrintsToStdout.LINES[0]).append(ProcessPrintsToStdout.LINES[1]).append(ProcessPrintsToStdout.LINES[2]);
    assertEquals(sb.toString(), stdoutBuffer.toString());
    
    //System.out.println("Closing streams");
    this.process.getErrorStream().close();
    this.process.getInputStream().close();
    
    this.process.destroy();
  }

  @Test
  public void capturesStderrWhileProcessIsAlive() throws Exception {
    assumeFalse(SystemUtils.isWindows());
    
    this.process = new ProcessBuilder(createCommandLine(ProcessPrintsToStderr.class)).start();
    
    final StringBuffer stderrBuffer = new StringBuffer();
    InputListener stderrListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stderrBuffer.append(line);
      }
    };
    
    final StringBuffer stdoutBuffer = new StringBuffer();
    InputListener stdoutListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stdoutBuffer.append(line);
      }
    };
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .inputListener(stderrListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .inputListener(stdoutListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
  
    this.stderr.start();
    this.stdout.start();
    
    // wait for process to die
    assertEventuallyFalse("Process never died", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return ProcessUtils.isProcessAlive(process);
      }
    }, WAIT_FOR_PROCESS_TO_DIE_TIMEOUT, INTERVAL);

    final int exitValue = this.process.exitValue();
    assertEquals(0, exitValue);

    this.stderr.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stderr.isRunning());
    
    this.stdout.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stdout.isRunning());
    
    //System.out.println("Stopping ProcessStreamReader");
    this.stderr.stop();
    this.stdout.stop();
    
    //System.out.println("stderr=\n" + stderrBuffer.toString());
    StringBuilder sb = new StringBuilder().append(ProcessPrintsToStderr.LINES[0]).append(ProcessPrintsToStderr.LINES[1]).append(ProcessPrintsToStderr.LINES[2]);
    assertEquals(sb.toString(), stderrBuffer.toString());
    
    //System.out.println("stdout=\n" + stdoutBuffer.toString());
    assertEquals("", stdoutBuffer.toString());
    
    //System.out.println("Closing streams");
    this.process.getErrorStream().close();
    this.process.getInputStream().close();

    this.process.destroy();
  }

  @Test
  public void capturesBothWhileProcessIsAlive() throws Exception {
    assumeFalse(SystemUtils.isWindows());
    
    this.process = new ProcessBuilder(createCommandLine(ProcessPrintsToBoth.class)).start();
    
    final StringBuffer stderrBuffer = new StringBuffer();
    InputListener stderrListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stderrBuffer.append(line);
      }
    };
    
    final StringBuffer stdoutBuffer = new StringBuffer();
    InputListener stdoutListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stdoutBuffer.append(line);
      }
    };
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .inputListener(stderrListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .inputListener(stdoutListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
  
    this.stderr.start();
    this.stdout.start();
    
    // wait for process to die
    assertEventuallyFalse("Process never died", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return ProcessUtils.isProcessAlive(process);
      }
    }, WAIT_FOR_PROCESS_TO_DIE_TIMEOUT, INTERVAL);

    final int exitValue = this.process.exitValue();
    assertEquals(0, exitValue);

    this.stderr.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stderr.isRunning());
    
    this.stdout.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stdout.isRunning());
    
    //System.out.println("Stopping ProcessStreamReader");
    this.stderr.stop();
    this.stdout.stop();
    
    //System.out.println("stderr=\n" + stderrBuffer.toString());
    StringBuilder sb = new StringBuilder().append(ProcessPrintsToBoth.ERR_LINES[0]).append(ProcessPrintsToBoth.ERR_LINES[1]).append(ProcessPrintsToBoth.ERR_LINES[2]);
    assertEquals(sb.toString(), stderrBuffer.toString());
    
    //System.out.println("stdout=\n" + stdoutBuffer.toString());
    sb = new StringBuilder().append(ProcessPrintsToBoth.OUT_LINES[0]).append(ProcessPrintsToBoth.OUT_LINES[1]).append(ProcessPrintsToBoth.OUT_LINES[2]);
    assertEquals(sb.toString(), stdoutBuffer.toString());
    
    //System.out.println("Closing streams");
    this.process.getErrorStream().close();
    this.process.getInputStream().close();

    this.process.destroy();
  }

  @Test
  public void capturesStderrWhenProcessFailsDuringStart() throws Exception {
    assumeFalse(SystemUtils.isWindows());
    
    this.process = new ProcessBuilder(createCommandLine(ProcessThrowsError.class)).start();
    
    final StringBuffer stderrBuffer = new StringBuffer();
    InputListener stderrListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stderrBuffer.append(line);
      }
    };
    
    final StringBuffer stdoutBuffer = new StringBuffer();
    InputListener stdoutListener = new InputListener() {
      @Override
      public void notifyInputLine(String line) {
        stdoutBuffer.append(line);
      }
    };
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .inputListener(stderrListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .inputListener(stdoutListener)
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
  
    this.stderr.start();
    this.stdout.start();
    
    // wait for process to die
    assertEventuallyFalse("Process never died", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return ProcessUtils.isProcessAlive(process);
      }
    }, WAIT_FOR_PROCESS_TO_DIE_TIMEOUT, INTERVAL);
    
    final int exitValue = this.process.exitValue();
    assertNotEquals(0, exitValue);

    this.stderr.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stderr.isRunning());
    
    this.stdout.join(READER_JOIN_TIMEOUT);
    assertFalse(this.stdout.isRunning());
    
    //System.out.println("Stopping ProcessStreamReader");
    this.stderr.stop();
    this.stdout.stop();
    
    //System.out.println("stderr=\n" + stderrBuffer.toString());
    assertTrue(stderrBuffer.toString() + " does not contain " + ProcessThrowsError.ERROR_MSG, stderrBuffer.toString().contains(ProcessThrowsError.ERROR_MSG));

    //System.out.println("stdout=\n" + stdoutBuffer.toString());
    
    //System.out.println("Closing streams");
    this.process.getErrorStream().close();
    this.process.getInputStream().close();

    this.process.destroy();
  }
}
