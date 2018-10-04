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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.util.StopWatch;

/**
 * Functional integration tests for {@link NativeProcessUtils}.
 */
public class NativeProcessUtilsIntegrationTest {

  /** Max sleep timeout for {@link ProcessSleeps} */
  private static final int PROCESS_TIMEOUT_MILLIS = 10 * 60 * 1000;

  private static final String FILE_NAME = "pid.txt";

  private NativeProcessUtils nativeProcessUtils;
  private Process process;
  private File pidFile;
  private int pid;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    File directory = temporaryFolder.getRoot();

    List<String> command = new ArrayList<>();
    command
        .add(new File(new File(System.getProperty("java.home"), "bin"), "java").getCanonicalPath());
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(ProcessSleeps.class.getName());

    process = new ProcessBuilder(command).directory(directory).start();
    assertThat(process.isAlive()).isTrue();

    pidFile = new File(directory, FILE_NAME);
    await().untilAsserted(() -> assertThat(pidFile).exists());

    pid = new PidFile(pidFile).readPid();
    assertThat(pid).isGreaterThan(0);

    nativeProcessUtils = new NativeProcessUtils();
  }

  @After
  public void after() throws Exception {
    process.destroyForcibly();
  }

  @Test
  public void killProcessKillsOtherProcess() throws Exception {
    // act
    nativeProcessUtils.killProcess(pid);

    // assert
    await().untilAsserted(() -> assertThat(process.isAlive()).isFalse());
  }

  @Test
  public void isProcessAliveReturnsTrueForLiveProcess() throws Exception {
    // act/assert
    assertThat(nativeProcessUtils.isProcessAlive(pid)).isTrue();
  }

  @Test
  public void isProcessAliveReturnsFalseForDeadProcess() throws Exception {
    // arrange
    process.destroyForcibly();

    // act/assert
    await().untilAsserted(() -> assertThat(process.isAlive()).isFalse());
    assertThat(nativeProcessUtils.isProcessAlive(pid)).isFalse();
  }

  /**
   * Class with main that uses LocalProcessLauncher to create a PidFile and then sleeps.
   */
  protected static class ProcessSleeps {
    public static void main(final String... args) throws Exception {
      new LocalProcessLauncher(new File(FILE_NAME), false);
      StopWatch stopWatch = new StopWatch(true);
      while (stopWatch.elapsedTimeMillis() < PROCESS_TIMEOUT_MILLIS) {
        Thread.sleep(1000);
      }
    }
  }
}
