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
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.process.io.IntegerFileReader;
import org.apache.geode.test.dunit.DistributedTestCase;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Two-process functional tests for {@link LocalProcessLauncher}.
 *
 * @since GemFire 7.0
 */

public class LocalProcessLauncherDistributedTest extends DistributedTestCase {

  private int pid;
  private File pidFile;
  private VM otherVM;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void before() throws Exception {
    pid = identifyPid();
    pidFile = new File(temporaryFolder.getRoot(), testName.getMethodName() + ".pid");
    otherVM = getHost(0).getVM(0);
  }

  @Test
  public void existingPidFileThrowsFileAlreadyExistsException() throws Exception {
    // arrange
    otherVM.invoke(this::createPidFile);
    int firstPid = new IntegerFileReader(pidFile).readFromFile();

    // act/assert
    assertThatThrownBy(() -> new LocalProcessLauncher(pidFile, false))
        .isInstanceOf(FileAlreadyExistsException.class);
    assertThat(new IntegerFileReader(pidFile).readFromFile()).isEqualTo(firstPid);
  }

  @Test
  public void forceReplacesExistingPidFile() throws Exception {
    // arrange
    otherVM.invoke(this::createPidFile);
    int firstPid = new IntegerFileReader(pidFile).readFromFile();

    // act
    new LocalProcessLauncher(pidFile, true);

    // assert
    int secondPid = new IntegerFileReader(pidFile).readFromFile();
    assertThat(secondPid).isNotEqualTo(firstPid).isEqualTo(pid);
  }

  private void createPidFile()
      throws FileAlreadyExistsException, IOException, PidUnavailableException {
    new LocalProcessLauncher(pidFile, false);
  }
}
