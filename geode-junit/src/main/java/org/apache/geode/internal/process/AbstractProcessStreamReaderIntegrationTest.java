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

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;

import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.internal.util.StopWatch;

/**
 * Abstract base class for functional integration testing of {@link ProcessStreamReader}.
 */
public abstract class AbstractProcessStreamReaderIntegrationTest {

  /** Timeout to join to a running ProcessStreamReader thread */
  private static final int READER_JOIN_TIMEOUT_MILLIS = 2 * 60 * 1000;

  /** Sleep timeout for {@link ProcessSleeps} instead of sleeping Long.MAX_VALUE */
  private static final int PROCESS_FAIL_SAFE_TIMEOUT_MILLIS = 8 * 60 * 1000;

  /** Additional time for launched processes to live before terminating */
  private static final int PROCESS_TIME_TO_LIVE_MILLIS = 3 * 500;

  protected Process process;
  protected ProcessStreamReader stderr;
  protected ProcessStreamReader stdout;

  private StringBuffer stdoutBuffer;
  private StringBuffer stderrBuffer;

  @Before
  public void setUpAbstractProcessStreamReaderIntegrationTest() {
    stdoutBuffer = new StringBuffer();
    stderrBuffer = new StringBuffer();
  }

  @After
  public void afterProcessStreamReaderTestCase() throws Exception {
    if (stderr != null) {
      stderr.stop();
    }
    if (stdout != null) {
      stdout.stop();
    }
    if (process != null) {
      try {
        process.getErrorStream().close();
        process.getInputStream().close();
        process.getOutputStream().close();
      } finally {
        // this is async and can require more than 10 seconds on slower machines
        process.destroy();
      }
    }
  }

  protected abstract ReadingMode getReadingMode();

  protected void assertThatProcessAndReadersStopped() throws InterruptedException {
    assertThatProcessAndReadersStoppedWithExitValue(0);
  }

  protected void assertThatProcessAndReadersStoppedWithExitValue(final int exitValue)
      throws InterruptedException {
    assertThat(process.exitValue()).isEqualTo(exitValue);
    assertThat(stdout.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
    assertThat(stderr.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
  }

  protected void assertThatProcessAndReadersDied() throws InterruptedException {
    assertThat(process.isAlive()).isFalse();
    assertThat(stdout.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
    assertThat(stderr.join(READER_JOIN_TIMEOUT_MILLIS).isRunning()).isFalse();
  }

  protected void assertThatProcessIsAlive(final Process process) {
    assertThat(process.isAlive()).isTrue();
  }

  protected void assertThatStdErrContains(final String value) {
    assertThat(stderrBuffer.toString()).contains(value);
  }

  protected void assertThatStdErrContainsExactly(final String value) {
    assertThat(stderrBuffer.toString()).isEqualTo(value);
  }

  protected void assertThatStdOutContainsExactly(final String value) {
    assertThat(stdoutBuffer.toString()).isEqualTo(value);
  }

  protected void givenRunningProcessWithStreamReaders(final Class<?> mainClass) {
    givenStartedProcess(mainClass);

    assertThat(process.isAlive()).isTrue();

    await().untilAsserted(() -> assertThat(stdout.isRunning()).isTrue());
    await().untilAsserted(() -> assertThat(stderr.isRunning()).isTrue());
  }

  private void givenStartedProcess(final Class<?> mainClass) {
    try {
      process = new ProcessBuilder(createCommandLine(mainClass)).start();
      stdout = buildProcessStreamReader(process.getInputStream(), getReadingMode());
      stderr = buildProcessStreamReader(process.getErrorStream(), getReadingMode());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected void givenStartedProcessWithStreamListeners(final Class<?> mainClass) {
    try {
      process = new ProcessBuilder(createCommandLine(mainClass)).start();
      stdout = buildProcessStreamReader(process.getInputStream(), getReadingMode(), stdoutBuffer);
      stderr = buildProcessStreamReader(process.getErrorStream(), getReadingMode(), stderrBuffer);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected static String[] createCommandLine(final Class<?> clazz) {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine.add(getClassPath());
    commandLine.add("-D" + "java.awt.headless=true");
    commandLine.add(clazz.getName());

    return commandLine.toArray(new String[commandLine.size()]);
  }

  protected void waitUntilProcessStops() {
    await().untilAsserted(() -> assertThat(isProcessAlive(process)).isFalse());
  }

  protected void waitUntilProcessStops(final long timeout, final TimeUnit unit) {
    await()
        .untilAsserted(() -> assertThat(isProcessAlive(process)).isFalse());
  }

  private ProcessStreamReader buildProcessStreamReader(final InputStream stream,
      final ReadingMode mode) {
    return new ProcessStreamReader.Builder(process).inputStream(stream).readingMode(mode).build()
        .start();
  }

  private ProcessStreamReader buildProcessStreamReader(final InputStream stream,
      final ReadingMode mode, final StringBuffer buffer) {
    ProcessStreamReader.Builder builder =
        new ProcessStreamReader.Builder(process).inputStream(stream).readingMode(mode);
    if (buffer != null) {
      builder.inputListener(buffer::append);
    }
    return builder.build().start();
  }

  private static String getClassPath() {
    return System.getProperty("java.class.path");
  }

  private static String getJavaPath() {
    String java = "java";
    return new File(new File(System.getProperty("java.home"), "bin"), java).getPath();
  }

  private static void sleepAtMost(final int duration) throws InterruptedException {
    StopWatch stopWatch = new StopWatch(true);
    while (stopWatch.elapsedTimeMillis() < duration) {
      Thread.sleep(1000);
    }
  }

  /**
   * Class with main that sleeps until destroyed.
   */
  protected static class ProcessSleeps {
    public static void main(final String... args) throws InterruptedException {
      sleepAtMost(PROCESS_FAIL_SAFE_TIMEOUT_MILLIS);
    }
  }

  /**
   * Class with main that throws Error.
   */
  protected static class ProcessThrowsError {
    private static final String[] LINES =
        new String[] {"ProcessThrowsError is starting" + LINE_SEPARATOR,
            "ProcessThrowsError is sleeping" + LINE_SEPARATOR,
            "ProcessThrowsError is throwing" + LINE_SEPARATOR};

    protected static final String STDOUT = "";

    protected static final String ERROR_MSG = "ProcessThrowsError throws Error";

    public static void main(final String... args) throws InterruptedException {
      System.err.print(LINES[0]);
      System.err.print(LINES[1]);
      sleepAtMost(PROCESS_TIME_TO_LIVE_MILLIS);
      System.err.print(LINES[2]);
      throw new Error(ERROR_MSG);
    }
  }

  /**
   * Class with main that prints to stdout and sleeps.
   */
  protected static class ProcessPrintsToStdout {
    private static final String[] LINES =
        new String[] {"ProcessPrintsToStdout is starting" + LINE_SEPARATOR,
            "ProcessPrintsToStdout is sleeping" + LINE_SEPARATOR,
            "ProcessPrintsToStdout is exiting" + LINE_SEPARATOR};

    protected static final String STDOUT =
        new StringBuilder().append(LINES[0]).append(LINES[1]).append(LINES[2]).toString();

    protected static final String STDERR = "";

    public static void main(final String... args) throws InterruptedException {
      System.out.print(LINES[0]);
      System.out.print(LINES[1]);
      sleepAtMost(PROCESS_TIME_TO_LIVE_MILLIS);
      System.out.print(LINES[2]);
    }
  }

  /**
   * Class with main that prints to stderr and sleeps.
   */
  protected static class ProcessPrintsToStderr {
    private static final String[] LINES =
        new String[] {"ProcessPrintsToStdout is starting" + LINE_SEPARATOR,
            "ProcessPrintsToStdout is sleeping" + LINE_SEPARATOR,
            "ProcessPrintsToStdout is exiting" + LINE_SEPARATOR};

    protected static final String STDOUT = "";

    protected static final String STDERR =
        new StringBuilder().append(LINES[0]).append(LINES[1]).append(LINES[2]).toString();

    public static void main(final String... args) throws InterruptedException {
      System.err.print(LINES[0]);
      System.err.print(LINES[1]);
      sleepAtMost(PROCESS_TIME_TO_LIVE_MILLIS);
      System.err.print(LINES[2]);
    }
  }

  /**
   * Class with main that prints to both stdout and stderr and sleeps.
   */
  protected static class ProcessPrintsToBoth {
    private static final String[] OUT_LINES =
        new String[] {"ProcessPrintsToBoth(out) is starting" + LINE_SEPARATOR,
            "ProcessPrintsToBoth(out) is sleeping" + LINE_SEPARATOR,
            "ProcessPrintsToBoth(out) is exiting" + LINE_SEPARATOR};

    private static final String[] ERR_LINES =
        new String[] {"ProcessPrintsToBoth(err) is starting" + LINE_SEPARATOR,
            "ProcessPrintsToBoth(err) is sleeping" + LINE_SEPARATOR,
            "ProcessPrintsToBoth(err) is exiting" + LINE_SEPARATOR};

    protected static final String STDOUT = new StringBuilder().append(OUT_LINES[0])
        .append(OUT_LINES[1]).append(OUT_LINES[2]).toString();

    protected static final String STDERR = new StringBuilder().append(ERR_LINES[0])
        .append(ERR_LINES[1]).append(ERR_LINES[2]).toString();

    public static void main(final String... args) throws InterruptedException {
      System.out.print(OUT_LINES[0]);
      System.err.print(ERR_LINES[0]);
      System.out.print(OUT_LINES[1]);
      System.err.print(ERR_LINES[1]);
      sleepAtMost(PROCESS_TIME_TO_LIVE_MILLIS);
      System.out.print(OUT_LINES[2]);
      System.err.print(ERR_LINES[2]);
    }
  }
}
