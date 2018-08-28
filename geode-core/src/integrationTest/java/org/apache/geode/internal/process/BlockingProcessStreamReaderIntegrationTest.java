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
import static org.apache.geode.internal.process.ProcessStreamReader.ReadingMode.BLOCKING;
import static org.junit.Assume.assumeFalse;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;

/**
 * Functional integration tests for BlockingProcessStreamReader. All tests are skipped on Windows
 * due to TRAC #51967: "GFSH start hangs on Windows"
 *
 * @see BlockingProcessStreamReaderWindowsTest
 * @see NonBlockingProcessStreamReaderIntegrationTest
 *
 * @since GemFire 8.2
 */
public class BlockingProcessStreamReaderIntegrationTest
    extends BaseProcessStreamReaderIntegrationTest {

  @Before
  public void setUp() throws Exception {
    assumeFalse(isWindows());
  }

  @Test
  public void canCloseStreams() throws Exception {
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
  public void canStopReaders() throws Exception {
    // arrange
    givenRunningProcessWithStreamReaders(ProcessSleeps.class);

    // act
    stdout.stop();
    stderr.stop();

    // assert
    assertThatProcessIsAlive(process);
  }

  @Test
  public void capturesStdout() throws Exception {
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
  public void capturesStderr() throws Exception {
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
  public void capturesStdoutAndStderr() throws Exception {
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
    return BLOCKING;
  }
}
