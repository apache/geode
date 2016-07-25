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

import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.process.ProcessStreamReader.InputListener;
import com.gemstone.gemfire.internal.process.ProcessStreamReader.ReadingMode;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests NonBlockingProcessStreamReader which was introduced to fix TRAC bug #51967.
 * 
 * None of the tests should be skipped or hang on Windows.
 * 
 * @since GemFire 8.2
 */
@Category(IntegrationTest.class)
public class NonBlockingProcessStreamReaderJUnitTest extends ProcessStreamReaderTestCase {

  @Test
  public void canCloseStreamsWhileProcessIsAlive() throws Exception {
    this.process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
    
    this.stderr.start();
    this.stdout.start();
    
    assertIsAlive(this.process);

    assertEventuallyIsRunning(this.stderr);
    assertEventuallyIsRunning(this.stdout);

    this.process.getErrorStream().close();
    this.process.getOutputStream().close();
    this.process.getInputStream().close();
    
    this.stderr.stop();
    this.stdout.stop();
    
    assertIsAlive(this.process);

    this.process.destroy();
  }

  @Test
  public void canStopReadersWhileProcessIsAlive() throws Exception {
    this.process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    
    this.stderr = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getErrorStream())
      .readingMode(ReadingMode.NON_BLOCKING)
      .build();
    
    this.stdout = new ProcessStreamReader.Builder(this.process)
      .inputStream(this.process.getInputStream())
      .readingMode(ReadingMode.NON_BLOCKING)
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
