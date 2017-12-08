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

import static com.googlecode.catchexception.CatchException.caughtException;
import static com.googlecode.catchexception.CatchException.verifyException;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.internal.process.io.EmptyFileWriter;
import org.apache.geode.internal.process.io.IntegerFileWriter;
import org.apache.geode.internal.process.io.StringFileWriter;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for {@link FileProcessController}.
 *
 * <p>
 * This test shows one of the more appropriate uses of ErrorCollector Rule -- catching any failed
 * assertions in another thread that isn't the main JUnit thread.
 */
@Category(IntegrationTest.class)
public class FileProcessControllerIntegrationTest {

  private static final String STATUS_JSON = generateStatusJson();

  private final AtomicReference<String> statusRef = new AtomicReference<>();

  private File pidFile;
  private File statusFile;
  private File statusRequestFile;
  private File stopRequestFile;
  private int pid;
  private FileControllerParameters params;
  private ExecutorService executor;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    ProcessType processType = ProcessType.LOCATOR;
    File directory = temporaryFolder.getRoot();
    pidFile = new File(directory, processType.getPidFileName());
    statusFile = new File(directory, processType.getStatusFileName());
    statusRequestFile = new File(directory, processType.getStatusRequestFileName());
    stopRequestFile = new File(directory, processType.getStopRequestFileName());
    pid = ProcessUtils.identifyPid();

    params = mock(FileControllerParameters.class);
    when(params.getPidFile()).thenReturn(pidFile);
    when(params.getProcessId()).thenReturn(pid);
    when(params.getProcessType()).thenReturn(processType);
    when(params.getDirectory()).thenReturn(temporaryFolder.getRoot());

    executor = Executors.newSingleThreadExecutor();
  }

  @After
  public void tearDown() throws Exception {
    assertThat(executor.shutdownNow()).isEmpty();
  }

  @Test
  public void statusShouldAwaitTimeoutWhileFileIsEmpty() throws Exception {
    // given: FileProcessController with empty pidFile
    FileProcessController controller = new FileProcessController(params, pid, 10, MILLISECONDS);

    // when:
    verifyException(controller).status();

    // then: we expect TimeoutException to be thrown
    assertThat((Exception) caughtException()).isInstanceOf(TimeoutException.class)
        .hasMessageContaining("Timed out waiting for process to create").hasNoCause();
  }

  @Test
  public void statusShouldReturnJsonFromStatusFile() throws Exception {
    // given: FileProcessController with pidFile containing real pid
    new IntegerFileWriter(pidFile).writeToFile(pid);
    FileProcessController controller = new FileProcessController(params, pid, 2, MINUTES);

    // when: status is called in one thread
    executor.execute(() -> {
      try {
        statusRef.set(controller.status());
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    });

    // and: json is written to the status file
    new StringFileWriter(statusFile).writeToFile(STATUS_JSON);

    // then: returned status should be the json in the file
    await().until(() -> assertThat(statusRef.get()).isEqualTo(STATUS_JSON));
  }

  /**
   * This is a new test written for GEODE-3413: "Overhaul launcher tests and process tests."
   * Unfortunately, it hangs so I have filed GEODE-3278. This test should be used to fix and verify
   * that bug.
   */
  @Ignore("GEODE-3278: Empty status file causes status server and status locator to hang")
  @Test
  public void emptyStatusFileCausesStatusToHang() throws Exception {
    // given: FileProcessController with pidFile containing real pid
    new IntegerFileWriter(pidFile).writeToFile(pid);
    FileProcessController controller = new FileProcessController(params, pid, 2, MINUTES);

    // when: status is called in one thread
    executor.execute(() -> {
      try {
        statusRef.set(controller.status());
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    });

    // and: json is written to the status file
    new EmptyFileWriter(statusFile).createNewFile();

    // then: returned status should be the json in the file
    await().until(() -> assertThat(statusRef.get()).isEqualTo(STATUS_JSON));
  }

  @Test
  public void stopCreatesStopRequestFile() throws Exception {
    // arrange
    FileProcessController controller = new FileProcessController(params, pid);
    assertThat(stopRequestFile).doesNotExist();

    // act
    controller.stop();

    // assert
    assertThat(stopRequestFile).exists();
  }

  @Test
  public void stop_withStopRequestFileExists_doesNotFail() throws Exception {
    // arrange
    FileProcessController controller = new FileProcessController(params, pid);
    assertThat(stopRequestFile.createNewFile()).isTrue();

    // act
    controller.stop();

    // assert
    assertThat(stopRequestFile).exists();
  }

  @Test
  @Category(FlakyTest.class) // GEODE-3778
  public void status_withStatusRequestFileExists_doesNotFail() throws Exception {
    // arrange
    FileProcessController controller = new FileProcessController(params, pid);
    assertThat(statusRequestFile.createNewFile()).isTrue();

    // act
    executor.execute(() -> {
      try {
        statusRef.set(controller.status());
      } catch (Exception e) {
        errorCollector.addError(e);
      }
    });

    new StringFileWriter(statusFile).writeToFile(STATUS_JSON);

    // assert
    await().until(() -> assertThat(statusRequestFile).exists());
  }

  @Test
  public void statusCreatesStatusRequestFile() throws Exception {
    // arrange
    new IntegerFileWriter(pidFile).writeToFile(pid);
    FileProcessController controller = new FileProcessController(params, pid, 2, MINUTES);

    // act
    executor.execute(() -> {
      try {
        assertThatThrownBy(() -> statusRef.set(controller.status()))
            .isInstanceOf(InterruptedException.class);
      } catch (Throwable t) {
        errorCollector.addError(t);
      }
    });

    // assert
    await().until(() -> assertThat(statusRequestFile).exists());
  }

  private ConditionFactory await() {
    return Awaitility.await().atMost(2, MINUTES);
  }

  private static String generateStatusJson() {
    Builder builder = new Builder();
    LocatorLauncher defaultLauncher = builder.build();
    Status status = Status.ONLINE;
    LocatorState locatorState = new LocatorState(defaultLauncher, status);
    return locatorState.toJson();
  }
}
