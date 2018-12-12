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

import static org.apache.geode.internal.process.LocalProcessLauncher.PROPERTY_IGNORE_IS_PID_ALIVE;
import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.process.io.EmptyFileWriter;
import org.apache.geode.internal.process.lang.AvailablePid;
import org.apache.geode.test.junit.Retry;
import org.apache.geode.test.junit.rules.RetryRule;

/**
 * Functional integration tests for {@link LocalProcessLauncher}.
 *
 * <p>
 * Tests involving fakePid use {@link RetryRule} because the fakePid may become used by a real
 * process before the test executes.
 */
public class LocalProcessLauncherIntegrationTest {

  private static final int PREFERRED_FAKE_PID = 42;

  private File pidFile;
  private int actualPid;
  private int fakePid;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    pidFile = new File(temporaryFolder.getRoot(), "my.pid");
    actualPid = identifyPid();
    fakePid = new AvailablePid().findAvailablePid(PREFERRED_FAKE_PID);

    assertThat(pidFile).doesNotExist();
  }

  @Test
  public void createsPidFile() throws Exception {
    // act
    new LocalProcessLauncher(pidFile, false);

    // assert
    assertThat(pidFile).exists().hasContent(String.valueOf(actualPid));
  }

  @Test
  @Retry(3)
  public void existingPidFileThrowsFileAlreadyExistsException() throws Exception {
    // arrange
    System.setProperty(PROPERTY_IGNORE_IS_PID_ALIVE, "true");
    FileUtils.writeStringToFile(pidFile, String.valueOf(fakePid), Charset.defaultCharset());

    // act/assert
    assertThatThrownBy(() -> new LocalProcessLauncher(pidFile, false))
        .isInstanceOf(FileAlreadyExistsException.class);
  }

  @Test
  public void overwritesPidFileIfForce() throws Exception {
    // arrange
    FileUtils.writeStringToFile(pidFile, String.valueOf(actualPid), Charset.defaultCharset());

    // act
    new LocalProcessLauncher(pidFile, true);

    // assert
    assertThat(pidFile).exists().hasContent(String.valueOf(actualPid));
  }

  @Test
  @Retry(3)
  public void overwritesPidFileIfOtherPidIsNotAlive() throws Exception {
    // arrange
    FileUtils.writeStringToFile(pidFile, String.valueOf(fakePid), Charset.defaultCharset());

    // act
    new LocalProcessLauncher(pidFile, false);

    // assert
    assertThat(pidFile).exists().hasContent(String.valueOf(actualPid));
  }

  @Test
  public void overwritesEmptyPidFile() throws Exception {
    // arrange
    new EmptyFileWriter(pidFile).createNewFile();

    // act
    new LocalProcessLauncher(pidFile, false);

    // assert
    assertThat(pidFile).exists().hasContent(String.valueOf(actualPid));
  }

  @Test
  public void getPidReturnsActualPid() throws Exception {
    // arrange
    LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);

    // act/assert
    assertThat(launcher.getPid()).isEqualTo(actualPid);
  }

  @Test
  public void getPidFileReturnsPidFile() throws Exception {
    // arrange
    LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);

    // act/assert
    assertThat(launcher.getPidFile()).isEqualTo(pidFile);
  }

  @Test
  public void closeDeletesPidFile() throws Exception {
    // arrange
    LocalProcessLauncher launcher = new LocalProcessLauncher(pidFile, false);
    assertThat(pidFile).exists();

    // act
    launcher.close();

    // assert
    assertThat(pidFile).doesNotExist();
  }
}
