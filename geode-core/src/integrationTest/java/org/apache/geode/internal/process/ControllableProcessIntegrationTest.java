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

import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.AbstractLauncher.ServiceState;
import org.apache.geode.internal.process.ControlFileWatchdog.ControlRequestHandler;
import org.apache.geode.internal.process.io.EmptyFileWriter;

public class ControllableProcessIntegrationTest {

  private LocalProcessLauncher localProcessLauncher;
  private ControlFileWatchdog stopRequestFileWatchdog;
  private ControlFileWatchdog statusRequestFileWatchdog;
  private ProcessType processType;
  private File directory;
  private File pidFile;
  private int pid;
  private File statusRequestFile;
  private File stopRequestFile;
  private File statusFile;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    processType = ProcessType.LOCATOR;
    directory = temporaryFolder.getRoot();
    pidFile = new File(directory, processType.getPidFileName());
    pid = identifyPid();
    statusRequestFile = new File(directory, processType.getStatusRequestFileName());
    stopRequestFile = new File(directory, processType.getStopRequestFileName());
    statusFile = new File(directory, processType.getStatusFileName());
    statusRequestFileWatchdog = new ControlFileWatchdog(directory,
        processType.getStatusRequestFileName(), mock(ControlRequestHandler.class), false);
    stopRequestFileWatchdog = new ControlFileWatchdog(directory,
        processType.getStopRequestFileName(), mock(ControlRequestHandler.class), false);
  }

  @Test
  public void getDirectoryExists() throws Exception {
    // arrange
    localProcessLauncher = new LocalProcessLauncher(pidFile, false);

    // act
    ControllableProcess controllable = new ControllableProcess(directory, processType,
        localProcessLauncher, stopRequestFileWatchdog, statusRequestFileWatchdog);

    // assert
    assertThat(controllable.getDirectory()).isEqualTo(directory);
  }

  @Test
  public void creationDeletesStatusRequestFileInDirectory() throws Exception {
    // arrange
    localProcessLauncher = new LocalProcessLauncher(pidFile, false);
    File file = new EmptyFileWriter(statusRequestFile).createNewFile();

    // act
    new ControllableProcess(directory, processType, localProcessLauncher, stopRequestFileWatchdog,
        statusRequestFileWatchdog);

    // assert
    assertThat(file).doesNotExist();
  }

  @Test
  public void creationDeletesStatusResponseFileInDirectory() throws Exception {
    // arrange
    localProcessLauncher = new LocalProcessLauncher(pidFile, false);
    File file = new EmptyFileWriter(statusFile).createNewFile();

    // act
    new ControllableProcess(directory, processType, localProcessLauncher, stopRequestFileWatchdog,
        statusRequestFileWatchdog);

    // assert
    assertThat(file).doesNotExist();
  }

  @Test
  public void creationDeletesStopRequestFileInDirectory() throws Exception {
    // arrange
    localProcessLauncher = new LocalProcessLauncher(pidFile, false);
    File file = new EmptyFileWriter(stopRequestFile).createNewFile();

    // act
    new ControllableProcess(directory, processType, localProcessLauncher, stopRequestFileWatchdog,
        statusRequestFileWatchdog);

    // assert
    assertThat(file).doesNotExist();
  }

  @Test
  public void getPidReturnsPid() throws Exception {
    // arrange
    localProcessLauncher = new LocalProcessLauncher(pidFile, false);

    // act
    ControllableProcess controllable = new ControllableProcess(directory, processType,
        localProcessLauncher, stopRequestFileWatchdog, statusRequestFileWatchdog);

    // assert
    assertThat(controllable.getPid()).isEqualTo(pid);
  }

  @Test
  public void getPidFileExists() throws Exception {
    // arrange
    localProcessLauncher = new LocalProcessLauncher(pidFile, false);

    // act
    ControllableProcess controllable = new ControllableProcess(directory, processType,
        localProcessLauncher, stopRequestFileWatchdog, statusRequestFileWatchdog);

    // assert
    assertThat(controllable.getPidFile()).exists().hasContent(String.valueOf(pid));
  }

  @Test
  public void stopsBothControlFileWatchdogs() throws Exception {
    // arrange
    ControlFileWatchdog stopRequestFileWatchdog = new ControlFileWatchdog(directory,
        "stopRequestFile", mock(ControlRequestHandler.class), false);
    ControlFileWatchdog statusRequestFileWatchdog = new ControlFileWatchdog(directory,
        "statusRequestFile", mock(ControlRequestHandler.class), false);

    stopRequestFileWatchdog = spy(stopRequestFileWatchdog);
    statusRequestFileWatchdog = spy(statusRequestFileWatchdog);

    localProcessLauncher = new LocalProcessLauncher(pidFile, false);

    ControllableProcess controllable = new ControllableProcess(directory, processType,
        localProcessLauncher, stopRequestFileWatchdog, statusRequestFileWatchdog);

    // act
    controllable.stop();

    // assert
    verify(stopRequestFileWatchdog).stop();
    verify(statusRequestFileWatchdog).stop();
  }

  @Test
  public void statusRequestFileIsDeletedAndStatusFileIsCreated() throws Exception {
    // arrange
    File statusRequestFile = new File(directory, processType.getStatusRequestFileName());
    File statusFile = new File(directory, processType.getStatusFileName());

    ServiceState mockServiceState = mock(ServiceState.class);
    when(mockServiceState.toJson()).thenReturn("json");
    ControlNotificationHandler mockHandler = mock(ControlNotificationHandler.class);
    when(mockHandler.handleStatus()).thenReturn(mockServiceState);
    new ControllableProcess(mockHandler, directory, processType, false);

    // act
    boolean created = statusRequestFile.createNewFile();

    // assert
    assertThat(created).isTrue();
    await().untilAsserted(() -> assertThat(statusRequestFile).doesNotExist());
    assertThat(statusFile).exists();
  }
}
