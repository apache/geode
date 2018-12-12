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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.process.lang.AvailablePid;

/**
 * Functional integration tests for {@link PidFile}.
 *
 * @since GemFire 8.2
 */
public class PidFileIntegrationTest {

  private File directory;
  private File pidFile;
  private String pidFileName;
  private int pid;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    directory = temporaryFolder.getRoot();
    pidFile = new File(directory, "pid.txt");
    pidFileName = pidFile.getName();
    pid = identifyPid();
  }

  @Test
  public void readsIntFromFile() throws Exception {
    // arrange
    String value = "42";
    FileUtils.writeStringToFile(pidFile, value, Charset.defaultCharset());

    // act
    int readValue = new PidFile(pidFile).readPid();

    // assert
    assertThat(readValue).isEqualTo(Integer.parseInt(value));
  }

  @Test
  public void readingEmptyFileThrowsIllegalArgumentException() throws Exception {
    // arrange
    FileUtils.writeStringToFile(pidFile, "", Charset.defaultCharset());

    // act/assert
    assertThatThrownBy(() -> new PidFile(pidFile).readPid())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void readingFileWithNonIntegerThrowsIllegalArgumentException() throws Exception {
    // arrange
    String value = "forty two";
    FileUtils.writeStringToFile(pidFile, value, Charset.defaultCharset());

    // act/assert
    assertThatThrownBy(() -> new PidFile(pidFile).readPid())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid pid '" + value + "' found");
  }

  @Test
  public void readingFileWithNegativeIntegerThrowsIllegalArgumentException() throws Exception {
    // arrange
    String value = "-42";
    FileUtils.writeStringToFile(pidFile, value, Charset.defaultCharset());

    // act/assert
    assertThatThrownBy(() -> new PidFile(pidFile).readPid())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid pid '" + value + "' found");
  }

  @Test
  public void readingNullFileThrowsNullPointerException() throws Exception {
    // arrange
    pidFile = null;

    // act/assert
    assertThatThrownBy(() -> new PidFile(pidFile).readPid())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void findsCorrectFileByName() throws Exception {
    // arrange
    FileUtils.writeStringToFile(pidFile, String.valueOf(pid), Charset.defaultCharset());
    int[] pids = new AvailablePid().findAvailablePids(4);
    for (int i = 1; i <= pids.length; i++) {
      FileUtils.writeStringToFile(
          new File(directory, "pid" + i + ".txt"), String.valueOf(pids[i - 1]),
          Charset.defaultCharset());
    }
    assertThat(directory.listFiles()).hasSize(pids.length + 1);

    // act
    PidFile namedPidFile = new PidFile(directory, pidFile.getName());

    // assert
    assertThat(namedPidFile.getFile()).hasContent(String.valueOf(pid));
    assertThat(namedPidFile.readPid()).isEqualTo(pid);
  }

  @Test
  public void missingFileInEmptyDirectoryThrowsFileNotFoundException() throws Exception {
    // arrange
    assertThat(pidFile).doesNotExist();

    // act/assert
    assertThatThrownBy(() -> new PidFile(directory, pidFileName).readPid())
        .isInstanceOf(FileNotFoundException.class).hasMessage(
            "Unable to find PID file '" + pidFileName + "' in directory '" + directory + "'");
  }

  @Test
  public void fileForDirectoryThrowsIllegalArgumentException() throws Exception {
    // arrange
    File directoryIsFile = temporaryFolder.newFile("my.file");

    // act/assert
    assertThatThrownBy(() -> new PidFile(directoryIsFile, pidFileName).readPid())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Nonexistent directory '" + directoryIsFile + "' specified");
  }

  @Test
  public void missingFileThrowsFileNotFoundException() throws Exception {
    // act/assert
    assertThatThrownBy(() -> new PidFile(pidFile).readPid())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Nonexistent file '" + pidFile + "' specified");
  }

  @Test
  public void missingFileInFullDirectoryThrowsFileNotFoundException() throws Exception {
    // arrange
    int[] pids = new AvailablePid().findAvailablePids(4);
    for (int i = 1; i <= pids.length; i++) {
      FileUtils.writeStringToFile(
          new File(directory, "pid" + i + ".txt"), String.valueOf(pids[i - 1]),
          Charset.defaultCharset());
    }
    assertThat(directory.listFiles()).hasSameSizeAs(pids);

    // act/assert
    assertThatThrownBy(() -> new PidFile(directory, pidFileName).readPid())
        .isInstanceOf(FileNotFoundException.class).hasMessage(
            "Unable to find PID file '" + pidFileName + "' in directory '" + directory + "'");
  }
}
