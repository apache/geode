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

import static com.googlecode.catchexception.CatchException.*;
import static com.jayway.awaitility.Awaitility.*;
import static java.util.concurrent.TimeUnit.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import com.gemstone.gemfire.distributed.LocatorLauncher;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for FileProcessController.
 */
@Category(IntegrationTest.class)
public class FileProcessControllerIntegrationJUnitTest {

  private ProcessType processType;
  private ExecutorService executor;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  
  @Rule
  public TestName testName = new TestName();
  
  @Before
  public void setUp() throws Exception {
    this.processType = ProcessType.LOCATOR;
  }
  
  @After
  public void tearDown() throws Exception {
    if (this.executor != null) {
      this.executor.shutdownNow();
    }
  }
  
  @Test
  public void statusShouldAwaitTimeoutWhileFileIsEmpty() throws Exception {
    // given: FileProcessController with empty pidFile
    int pid = ProcessUtils.identifyPid();
    File emptyPidFile = this.temporaryFolder.newFile(this.processType.getPidFileName());
    FileControllerParameters params = mock(FileControllerParameters.class);
    when(params.getPidFile()).thenReturn(emptyPidFile);
    when(params.getProcessId()).thenReturn(pid);
    when(params.getProcessType()).thenReturn(this.processType);
    when(params.getWorkingDirectory()).thenReturn(this.temporaryFolder.getRoot());
    
    FileProcessController controller = new FileProcessController(params, 1, 10, MILLISECONDS);
    
    // when
    verifyException(controller).status();

    // then: we expect TimeoutException to be thrown
    assertThat((Exception)caughtException())
            .isInstanceOf(TimeoutException.class)
            .hasMessageContaining("Timed out waiting for process to create")
            .hasNoCause();
  }
  
  @Test
  public void statusShouldReturnJsonFromStatusFile() throws Exception {
    // given: FileProcessController with pidFile containing real pid
    int pid = ProcessUtils.identifyPid();
    File pidFile = this.temporaryFolder.newFile(this.processType.getPidFileName());
    writeToFile(pidFile, String.valueOf(pid));
    
    FileControllerParameters params = mock(FileControllerParameters.class);
    when(params.getPidFile()).thenReturn(pidFile);
    when(params.getProcessId()).thenReturn(pid);
    when(params.getProcessType()).thenReturn(this.processType);
    when(params.getWorkingDirectory()).thenReturn(this.temporaryFolder.getRoot());
    
    FileProcessController controller = new FileProcessController(params, pid, 1, MINUTES);
    
    // when: status is called in one thread and json is written to the file
    AtomicReference<String> status = new AtomicReference<String>();
    AtomicReference<Exception> exception = new AtomicReference<Exception>();
    this.executor = Executors.newSingleThreadExecutor();
    this.executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          status.set(controller.status());
        } catch (Exception e) {
          exception.set(e);
        }
      }
    });
    
    // write status
    String statusJson = generateStatusJson();
    File statusFile = this.temporaryFolder.newFile(this.processType.getStatusFileName());
    writeToFile(statusFile, statusJson);
    
    // then: returned status should be the json in the file
    assertThat(exception.get()).isNull();
    with().pollInterval(10, MILLISECONDS).await().atMost(2, MINUTES).untilAtomic(status, equalTo(statusJson));
    assertThat(status.get()).isEqualTo(statusJson);
    System.out.println(statusJson);
  }

  private static void writeToFile(final File file, final String value) throws IOException {
    final FileWriter writer = new FileWriter(file);
    writer.write(value);
    writer.flush();
    writer.close();
  }
  
  private static String generateStatusJson() {
    Builder builder = new Builder();
    LocatorLauncher defaultLauncher = builder.build();
    Status status = Status.ONLINE;
    LocatorState locatorState = new LocatorState(defaultLauncher, status);
    return locatorState.toJson();
  }
}
