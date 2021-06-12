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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.management.internal.cli.domain.StackTracesPerMember;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class ExportStackTraceCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ExportStackTraceCommand command;

  private final DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
          .withZone(ZoneId.systemDefault());

  @Before
  public void before() {
    command = spy(ExportStackTraceCommand.class);
  }

  @Test
  public void noMemberFound() {
    doReturn(Collections.emptySet()).when(command).findMembersIncludingLocators(any(), any());
    gfsh.executeAndAssertThat(command, "export stack-traces").statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void abortIfFileExists() throws IOException {
    File file = temporaryFolder.newFile("stackTrace.txt");
    gfsh.executeAndAssertThat(command,
        "export stack-traces --abort-if-file-exists --file=" + file.getAbsolutePath())
        .statusIsError().containsOutput("already present");

    // try again without the flag, the command should continue after the check
    doReturn(Collections.emptySet()).when(command).findMembersIncludingLocators(any(), any());
    gfsh.executeAndAssertThat(command, "export stack-traces --file=" + file.getAbsolutePath())
        .statusIsError().containsOutput("No Members Found");
  }

  @Test
  public void getHeaderMessageWithTimestamp() throws IOException {
    Instant time = Instant.now();
    StackTracesPerMember stackTracePerMember =
        new StackTracesPerMember("server", time,
            OSProcess.zipStacks());
    String headerMessage = command.getHeaderMessage(stackTracePerMember);

    assertThat(headerMessage).isEqualTo("server at " + formatter.format(time));
  }

  @Test
  public void getHeaderMessageWithoutTimestamp() throws IOException {
    StackTracesPerMember stackTracePerMember =
        new StackTracesPerMember("server", null,
            OSProcess.zipStacks());
    String headerMessage = command.getHeaderMessage(stackTracePerMember);

    assertThat(headerMessage).isEqualTo("server");
  }
}
