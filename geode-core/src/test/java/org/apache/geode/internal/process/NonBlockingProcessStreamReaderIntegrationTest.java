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
package org.apache.geode.internal.process;

import static org.apache.geode.internal.process.ProcessStreamReader.ReadingMode.NON_BLOCKING;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.process.ProcessStreamReader.Builder;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Functional integration tests for NonBlockingProcessStreamReader which was introduced to fix TRAC
 * #51967: "GFSH start hangs on Windows"
 *
 * @see BlockingProcessStreamReaderIntegrationTest
 * @see BlockingProcessStreamReaderWindowsTest
 *
 * @since GemFire 8.2
 */
@Category(IntegrationTest.class)
public class NonBlockingProcessStreamReaderIntegrationTest
    extends BaseProcessStreamReaderIntegrationTest {

  private StringBuffer stdoutBuffer;
  private StringBuffer stderrBuffer;

  @Before
  public void before() {
    stdoutBuffer = new StringBuffer();
    stderrBuffer = new StringBuffer();
  }

  /**
   * This test hangs on Windows if the implementation is blocking instead of non-blocking. Geode
   * will always use the non-blocking implementation on Windows. If someone accidentally changes
   * this, then the probably the first thing you'll notice is this test hanging.
   */
  @Test
  public void canCloseStreamsWhileProcessIsAlive() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream()).readingMode(NON_BLOCKING)
        .build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream()).readingMode(NON_BLOCKING)
        .build().start();

    assertThat(process.isAlive()).isTrue();

    await().until(() -> assertThat(stdout.isRunning()).isTrue());
    await().until(() -> assertThat(stderr.isRunning()).isTrue());

    // act
    process.getOutputStream().close();
    process.getErrorStream().close();
    process.getInputStream().close();

    // assert
    assertThat(process.isAlive()).isTrue();
  }

  @Test
  public void canStopReadersWhileProcessIsAlive() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream()).readingMode(NON_BLOCKING)
        .build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream()).readingMode(NON_BLOCKING)
        .build().start();

    assertThat(process.isAlive()).isTrue();

    await().until(() -> assertThat(stdout.isRunning()).isTrue());
    await().until(() -> assertThat(stderr.isRunning()).isTrue());

    // act
    stdout.stop();
    stderr.stop();

    // assert
    assertThat(process.isAlive()).isTrue();
  }

  @Test
  public void capturesStdoutWhileProcessIsAlive() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessPrintsToStdout.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream())
        .inputListener(stdoutBuffer::append).readingMode(NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(NON_BLOCKING).build().start();

    // act
    await().until(() -> assertThat(isProcessAlive(process)).isFalse());

    // assert
    assertThat(process.exitValue()).isEqualTo(0);
    assertThat(stdout.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
    assertThat(stderr.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();

    assertThat(stdoutBuffer.toString()).isEqualTo(ProcessPrintsToStdout.STDOUT);
    assertThat(stderrBuffer.toString()).isEqualTo(ProcessPrintsToStdout.STDERR);
  }

  @Test
  public void capturesStderrWhileProcessIsAlive() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessPrintsToStderr.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream())
        .inputListener(stdoutBuffer::append).readingMode(NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(NON_BLOCKING).build().start();

    // act
    await().until(() -> assertThat(isProcessAlive(process)).isFalse());

    // assert
    assertThat(process.exitValue()).isEqualTo(0);
    assertThat(stdout.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
    assertThat(stderr.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();

    assertThat(stdoutBuffer.toString()).isEqualTo(ProcessPrintsToStderr.STDOUT);
    assertThat(stderrBuffer.toString()).isEqualTo(ProcessPrintsToStderr.STDERR);
  }

  @Test
  public void capturesBothWhileProcessIsAlive() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessPrintsToBoth.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream())
        .inputListener(stdoutBuffer::append).readingMode(NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(NON_BLOCKING).build().start();

    // act
    await().until(() -> assertThat(isProcessAlive(process)).isFalse());

    // assert
    assertThat(process.exitValue()).isEqualTo(0);
    assertThat(stdout.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
    assertThat(stderr.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();

    assertThat(stdoutBuffer.toString()).isEqualTo(ProcessPrintsToBoth.STDOUT);
    assertThat(stderrBuffer.toString()).isEqualTo(ProcessPrintsToBoth.STDERR);
  }

  @Test
  public void capturesStderrWhenProcessFailsDuringStart() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessThrowsError.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream())
        .inputListener(stdoutBuffer::append).readingMode(NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(NON_BLOCKING).build().start();

    // act
    await().until(() -> assertThat(isProcessAlive(process)).isFalse());

    // assert
    assertThat(process.exitValue()).isEqualTo(1);
    assertThat(stdout.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
    assertThat(stderr.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();

    assertThat(stdoutBuffer.toString()).isEqualTo(ProcessThrowsError.STDOUT);
    assertThat(stderrBuffer.toString()).contains(ProcessThrowsError.ERROR_MSG);
  }
}
