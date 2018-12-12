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
import static org.apache.geode.internal.process.ProcessUtils.isProcessAlive;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

/**
 * Functional integration tests for {@link ProcessControllerFactory}.
 */
public class ProcessControllerFactoryIntegrationTest {

  private ProcessControllerFactory factory;
  private ProcessControllerParameters parameters;
  private File directory;
  private File pidFile;
  private String pidFileName;
  private int pid;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    factory = new ProcessControllerFactory();
    parameters = mock(ProcessControllerParameters.class);
    directory = temporaryFolder.getRoot();
    pidFile = new File(directory, "pid.txt");
    pidFileName = pidFile.getName();
    pid = identifyPid();

    assertThat(pidFile).doesNotExist();
    assertThat(isProcessAlive(pid)).isTrue();
  }

  @Test
  public void createProcessController_withoutPidFile_throwsFileNotFoundException()
      throws Exception {
    // act/assert
    assertThatThrownBy(() -> factory.createProcessController(parameters, directory, pidFileName))
        .isInstanceOf(FileNotFoundException.class);
  }

  @Test
  public void createProcessController_returnsMBeanProcessController() throws Exception {
    // arrange
    FileUtils.writeStringToFile(pidFile, String.valueOf(pid), Charset.defaultCharset());

    // act
    ProcessController controller =
        factory.createProcessController(parameters, directory, pidFileName);

    // assert
    assertThat(controller).isInstanceOf(MBeanOrFileProcessController.class);
  }

  @Test
  public void createProcessController_withoutAttachAPI_returnsFileProcessController()
      throws Exception {
    // arrange
    FileUtils.writeStringToFile(pidFile, String.valueOf(pid), Charset.defaultCharset());
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
    factory = new ProcessControllerFactory();

    // act
    ProcessController controller =
        factory.createProcessController(parameters, directory, pidFileName);

    // assert
    assertThat(controller).isInstanceOf(FileProcessController.class);
  }

  @Test
  public void createProcessController_nullParameters_throwsNullPointerException() throws Exception {
    // arrange
    parameters = null;

    // act/assert
    assertThatThrownBy(() -> factory.createProcessController(parameters, directory, pidFileName))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createProcessController_nullDirectory_throwsNullPointerException() throws Exception {
    // arrange
    directory = null;

    // act/assert
    assertThatThrownBy(() -> factory.createProcessController(parameters, directory, pidFileName))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void createProcessController_nullPidFileName_throwsNullPointerException()
      throws Exception {
    // arrange
    pidFileName = null;

    // act/assert
    assertThatThrownBy(() -> factory.createProcessController(parameters, directory, pidFileName))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
