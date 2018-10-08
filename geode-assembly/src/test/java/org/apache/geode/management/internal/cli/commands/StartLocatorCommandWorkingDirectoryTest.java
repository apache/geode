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
package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StartLocatorCommandWorkingDirectoryTest {

  private String memberName;
  private String workingDirectory;
  private StartLocatorCommand startLocatorCommand;
  private ArgumentCaptor<String> workingDirectoryCaptor;

  @Before
  public void setUp() throws Exception {
    memberName = "locator1";
    startLocatorCommand = spy(new StartLocatorCommand());

    doReturn(null).when(startLocatorCommand).doStartLocator(
        anyString(), isNull(), isNull(), anyBoolean(),
        isNull(), isNull(), isNull(), anyBoolean(),
        isNull(), isNull(), isNull(), anyInt(),
        anyInt(), anyString(), isNull(), isNull(),
        isNull(), isNull(), isNull(), anyBoolean(),
        anyBoolean(), anyBoolean(), isNull(), anyInt(),
        isNull(), anyBoolean());

    workingDirectoryCaptor = ArgumentCaptor.forClass(String.class);
  }

  @Test
  public void startLocatorWithRelativeWorkingDirectory() throws Exception {
    workingDirectory = "locator1Directory";

    startLocatorCommand.startLocator(memberName, null, null, false,
        null, null, null, false,
        null, null, null, 0,
        0, workingDirectory, null, null,
        null, null, null, false,
        false, false, null, 0,
        null, false);

    verifyDoStartLocatorInvoked();

    assertThat(workingDirectoryCaptor.getValue())
        .isEqualTo(new File(workingDirectory).getAbsolutePath());
  }

  @Test
  public void startLocatorWithNullWorkingDirectory() throws Exception {
    workingDirectory = null;

    startLocatorCommand.startLocator(memberName, null, null, false,
        null, null, null, false,
        null, null, null, 0,
        0, workingDirectory, null, null,
        null, null, null, false,
        false, false, null, 0,
        null, false);

    verifyDoStartLocatorInvoked();

    assertThat(workingDirectoryCaptor.getValue()).isEqualTo(new File(memberName).getAbsolutePath());
  }

  @Test
  public void startLocatorWithEmptyWorkingDirectory() throws Exception {
    workingDirectory = "";

    startLocatorCommand.startLocator(memberName, null, null, false,
        null, null, null, false,
        null, null, null, 0,
        0, workingDirectory, null, null,
        null, null, null, false,
        false, false, null, 0,
        null, false);

    verifyDoStartLocatorInvoked();

    assertThat(workingDirectoryCaptor.getValue()).isEqualTo(new File(memberName).getAbsolutePath());
  }

  @Test
  public void startLocatorWithDotWorkingDirectory() throws Exception {
    workingDirectory = ".";

    startLocatorCommand.startLocator(memberName, null, null, false,
        null, null, null, false,
        null, null, null, 0,
        0, workingDirectory, null, null,
        null, null, null, false,
        false, false, null, 0,
        null, false);

    verifyDoStartLocatorInvoked();

    assertThat(workingDirectoryCaptor.getValue())
        .isEqualTo(StartMemberUtils.resolveWorkingDir(new File("."), new File(memberName)));
  }

  @Test
  public void startLocatorWithAbsoluteWorkingDirectory() throws Exception {
    workingDirectory = new File(System.getProperty("user.dir")).getAbsolutePath();

    startLocatorCommand.startLocator(memberName, null, null, false,
        null, null, null, false,
        null, null, null, 0,
        0, workingDirectory, null, null,
        null, null, null, false,
        false, false, null, 0,
        null, false);

    verifyDoStartLocatorInvoked();

    assertThat(workingDirectoryCaptor.getValue()).isEqualTo(workingDirectory);
  }

  private void verifyDoStartLocatorInvoked()
      throws Exception {
    verify(startLocatorCommand).doStartLocator(anyString(), isNull(), isNull(), anyBoolean(),
        isNull(), isNull(), isNull(), anyBoolean(), isNull(),
        isNull(), isNull(), anyInt(), anyInt(), workingDirectoryCaptor.capture(),
        isNull(), isNull(), isNull(), isNull(), isNull(),
        anyBoolean(), anyBoolean(), anyBoolean(), isNull(), anyInt(), isNull(),
        anyBoolean());
  }
}
