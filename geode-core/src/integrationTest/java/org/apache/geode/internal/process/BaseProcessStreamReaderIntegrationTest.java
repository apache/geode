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

import static java.util.concurrent.TimeUnit.MINUTES;

import org.junit.Test;


/**
 * Integration tests that should be executed under both {@link ProcessStreamReader.ReadingMode}s.
 */
public abstract class BaseProcessStreamReaderIntegrationTest
    extends AbstractProcessStreamReaderIntegrationTest {

  @Test
  public void processLivesAfterClosingStreams() throws Exception {
    // arrange
    givenRunningProcessWithStreamReaders(ProcessSleeps.class);

    // act
    process.getErrorStream().close();
    process.getOutputStream().close();
    process.getInputStream().close();

    // assert
    assertThatProcessIsAlive(process);
  }

  @Test
  public void processTerminatesWhenDestroyed() throws Exception {
    // arrange
    givenRunningProcessWithStreamReaders(ProcessSleeps.class);

    // act
    process.destroyForcibly(); // results in SIGTERM which usually has an exit code of 143

    // assert
    waitUntilProcessStops(10, MINUTES);
    assertThatProcessAndReadersDied();
  }

  @Test
  public void capturesStderrWhenProcessFailsDuringStart() throws Exception {
    // arrange
    givenStartedProcessWithStreamListeners(ProcessThrowsError.class);

    // act
    waitUntilProcessStops();

    // assert
    assertThatProcessAndReadersStoppedWithExitValue(1);
    assertThatStdOutContainsExactly(ProcessThrowsError.STDOUT);
    assertThatStdErrContains(ProcessThrowsError.ERROR_MSG);
  }
}
