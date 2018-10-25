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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.process.ControlFileWatchdog.ControlRequestHandler;
import org.apache.geode.internal.process.io.EmptyFileWriter;

/**
 * Functional integration tests for {@link ControlFileWatchdog}.
 */
public class ControlFileWatchdogIntegrationTest {

  private static final int TEN_MINUTES_MILLIS = 10 * 60 * 1000;

  private File directory;
  private String requestFileName;
  private File requestFile;
  private ControlRequestHandler requestHandler;
  private boolean stopAfterRequest;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    directory = temporaryFolder.getRoot();
    requestFileName = "myFile";
    requestFile = new File(directory, requestFileName);
    requestHandler = mock(ControlRequestHandler.class);
    stopAfterRequest = false;
  }

  @Test
  public void isAlive_returnsFalse_beforeStart() throws Exception {
    // arrange
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);

    // act: nothing

    // assert
    await().untilAsserted(() -> assertThat(watchdog.isAlive()).isFalse());
  }

  @Test
  public void isAlive_returnsTrue_afterStart() throws Exception {
    // arrange
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);

    // act
    watchdog.start();

    // assert
    await().untilAsserted(() -> assertThat(watchdog.isAlive()).isTrue());
  }

  @Test
  public void isAlive_returnsFalse_afterStop() throws Exception {
    // arrange
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);
    watchdog.start();

    // act
    watchdog.stop();

    // assert
    await().untilAsserted(() -> assertThat(watchdog.isAlive()).isFalse());
  }

  @Test
  public void nullFileName_throwsIllegalArgumentException() throws Exception {
    // arrange
    requestFileName = null;

    // act/assert
    assertThatThrownBy(
        () -> new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest))
            .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void nullDirectory_throwsIllegalArgumentException() throws Exception {
    // arrange
    directory = null;

    // act/assert
    assertThatThrownBy(
        () -> new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest))
            .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void nullRequestHandler_throwsIllegalArgumentException() throws Exception {
    // arrange
    requestHandler = null;

    // act/assert
    assertThatThrownBy(
        () -> new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest))
            .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void invokesRequestHandler_afterFileCreation() throws Exception {
    // arrange
    requestHandler = mock(ControlRequestHandler.class);
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);
    watchdog.start();

    // act
    File file = new EmptyFileWriter(requestFile).createNewFile();

    // assert
    verify(requestHandler, timeout(TEN_MINUTES_MILLIS)).handleRequest();
    await().untilAsserted(() -> assertThat(file).doesNotExist());
  }

  @Test
  public void deletesFile_afterInvokingRequestHandler() throws Exception {
    // arrange
    requestHandler = mock(ControlRequestHandler.class);
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);
    watchdog.start();

    // act
    File file = new EmptyFileWriter(requestFile).createNewFile();

    // assert
    verify(requestHandler, timeout(TEN_MINUTES_MILLIS)).handleRequest();
    await().untilAsserted(() -> assertThat(file).doesNotExist());
  }

  @Test
  public void doesNotInvokeRequestHandler_whileFileDoesNotExist() throws Exception {
    // arrange
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);

    // act
    watchdog.start();

    // assert
    verifyZeroInteractions(requestHandler); // would be prefer to wait some time
  }

  @Test
  public void nothingHappens_beforeStart() throws Exception {
    // arrange
    requestHandler = mock(ControlRequestHandler.class);
    new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);

    // act
    File file = new EmptyFileWriter(requestFile).createNewFile();

    // assert
    verifyZeroInteractions(requestHandler); // would be prefer to wait some time
    assertThat(file).exists();
  }

  @Test
  public void stops_afterInvokingRequestHandler_whenStopAfterRequest() throws Exception {
    // arrange
    requestHandler = mock(ControlRequestHandler.class);
    stopAfterRequest = true;
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);
    watchdog.start();

    // act
    File file = new EmptyFileWriter(requestFile).createNewFile();

    // assert
    verify(requestHandler, timeout(TEN_MINUTES_MILLIS)).handleRequest();
    await().untilAsserted(() -> assertThat(watchdog.isAlive()).isFalse());
    await().untilAsserted(() -> assertThat(file).doesNotExist());
  }

  @Test
  public void doesNotStop_afterInvokingRequestHandler_whenNotStopAfterRequest() throws Exception {
    // arrange
    requestHandler = mock(ControlRequestHandler.class);
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);
    watchdog.start();

    // act
    File file = new EmptyFileWriter(requestFile).createNewFile();

    // assert
    verify(requestHandler, timeout(TEN_MINUTES_MILLIS)).handleRequest();
    await().untilAsserted(() -> assertThat(watchdog.isAlive()).isTrue());
    await().untilAsserted(() -> assertThat(file).doesNotExist());
  }

  @Test
  public void toStringIsUsefulForDebugging() throws Exception {
    // arrange
    ControlFileWatchdog watchdog =
        new ControlFileWatchdog(directory, requestFileName, requestHandler, stopAfterRequest);

    // act/assert
    assertThat(watchdog.toString()).isNotEmpty().contains("directory=").contains("file=")
        .contains("alive=").contains("stopAfterRequest=");
  }
}
