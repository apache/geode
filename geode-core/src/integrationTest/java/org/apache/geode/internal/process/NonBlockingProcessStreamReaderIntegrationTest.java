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

import org.junit.Test;

import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;

/**
 * Functional integration tests for NonBlockingProcessStreamReader which was introduced to fix TRAC
 * #51967: "GFSH start hangs on Windows"
 *
 * @see BlockingProcessStreamReaderIntegrationTest
 * @see BlockingProcessStreamReaderWindowsTest
 *
 * @since GemFire 8.2
 */
public class NonBlockingProcessStreamReaderIntegrationTest
    extends BaseProcessStreamReaderIntegrationTest {

  /**
   * This test hangs on Windows if the implementation is blocking instead of non-blocking. Geode
   * will always use the non-blocking implementation on Windows. If someone accidentally changes
   * this, then the probably the first thing you'll notice is this test hanging.
   */
  @Test
  public void canCloseStreamsWhileProcessIsAlive() throws Exception {
    // arrange
    givenRunningProcessWithStreamReaders(ProcessSleeps.class);

    // act
    process.getOutputStream().close();
    process.getErrorStream().close();
    process.getInputStream().close();

    // assert
    assertThatProcessIsAlive(process);
  }

  @Test
  public void canStopReadersWhileProcessIsAlive() throws Exception {
    // arrange
    givenRunningProcessWithStreamReaders(ProcessSleeps.class);

    // act
    stdout.stop();
    stderr.stop();

    // assert
    assertThatProcessIsAlive(process);
  }

  @Test
  public void capturesStdoutWhileProcessIsAlive() throws Exception {
    // arrange
    givenStartedProcessWithStreamListeners(ProcessPrintsToStdout.class);

    // act
    waitUntilProcessStops();

    // assert
    assertThatProcessAndReadersStopped();
    assertThatStdOutContainsExactly(ProcessPrintsToStdout.STDOUT);
    assertThatStdErrContainsExactly(ProcessPrintsToStdout.STDERR);
  }

  @Test
  public void capturesStderrWhileProcessIsAlive() throws Exception {
    // arrange
    givenStartedProcessWithStreamListeners(ProcessPrintsToStderr.class);

    // act
    waitUntilProcessStops();

    // assert
    assertThatProcessAndReadersStopped();
    assertThatStdOutContainsExactly(ProcessPrintsToStderr.STDOUT);
    assertThatStdErrContainsExactly(ProcessPrintsToStderr.STDERR);
  }

  @Test
  public void capturesBothWhileProcessIsAlive() throws Exception {
    // arrange
    givenStartedProcessWithStreamListeners(ProcessPrintsToBoth.class);

    // act
    waitUntilProcessStops();

    // assert
    assertThatProcessAndReadersStopped();
    assertThatStdOutContainsExactly(ProcessPrintsToBoth.STDOUT);
    assertThatStdErrContainsExactly(ProcessPrintsToBoth.STDERR);
  }

  @Override
  protected ReadingMode getReadingMode() {
    return NON_BLOCKING;
  }
}
