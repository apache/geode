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

import static org.apache.geode.internal.lang.SystemUtils.isWindows;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.process.ProcessStreamReader.Builder;
import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Functional integration tests for BlockingProcessStreamReader. All tests are skipped on Windows
 * due to TRAC #51967: "GFSH start hangs on Windows"
 *
 * @see BlockingProcessStreamReaderWindowsTest
 * @see NonBlockingProcessStreamReaderIntegrationTest
 *
 * @since GemFire 8.2
 */
@Category(IntegrationTest.class)
public class BlockingProcessStreamReaderIntegrationTest
    extends BaseProcessStreamReaderIntegrationTest {

  private StringBuffer stdoutBuffer; // needs to be thread-safe
  private StringBuffer stderrBuffer; // needs to be thread-safe

  @Before
  public void before() {
    assumeFalse(isWindows());

    stdoutBuffer = new StringBuffer();
    stderrBuffer = new StringBuffer();
  }

  @Test
  public void canCloseStreams() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream()).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream()).build().start();

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
  public void canStopReaders() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessSleeps.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream()).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream()).build().start();

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
  public void capturesStdout() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessPrintsToStdout.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream())
        .inputListener(stdoutBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();

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
  public void capturesStderr() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessPrintsToStderr.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream())
        .inputListener(stdoutBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();

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
  public void capturesStdoutAndStderr() throws Exception {
    // arrange
    process = new ProcessBuilder(createCommandLine(ProcessPrintsToBoth.class)).start();
    stdout = new Builder(process).inputStream(process.getInputStream())
        .inputListener(stdoutBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();

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
        .inputListener(stdoutBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();
    stderr = new Builder(process).inputStream(process.getErrorStream())
        .inputListener(stderrBuffer::append).readingMode(ReadingMode.NON_BLOCKING).build().start();

    // act
    await().until(() -> assertThat(isProcessAlive(process)).isFalse());

    // assert
    assertThat(process.exitValue()).isNotEqualTo(0);
    assertThat(stdout.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
    assertThat(stderr.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();

    assertThat(stdoutBuffer.toString()).isEqualTo(ProcessThrowsError.STDOUT);
    assertThat(stderrBuffer.toString()).contains(ProcessThrowsError.ERROR_MSG);
  }
}
