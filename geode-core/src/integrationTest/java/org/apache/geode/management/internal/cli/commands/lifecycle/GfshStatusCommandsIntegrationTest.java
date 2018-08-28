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
package org.apache.geode.management.internal.cli.commands.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class GfshStatusCommandsIntegrationTest {

  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();


  @Before
  public void connect() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void statusLocatorWithBadPortReportsNotResponding() throws Exception {
    CommandResult result = gfsh.executeCommand("status locator --host=localhost --port="
        + String.valueOf(locator.getLocator().getPort() - 1));
    assertThat(result.getMessageFromContent()).contains("not responding");
  }

  @Test
  public void statusLocatorWithActivePortReportsOnline() throws Exception {
    CommandResult result = gfsh.executeCommand(
        "status locator --host=localhost --port=" + String.valueOf(locator.getLocator().getPort()));
    assertThat(result.getMessageFromContent()).contains("is currently online");
  }

  @Test
  public void statusServerWithWithNoOptions() throws Exception {
    File serverDir = new File(temporaryFolder.getRoot(), "serverDir");
    serverDir.mkdirs();
    CommandResult result = gfsh.executeCommand("status server");
    assertThat(result.getMessageFromContent()).contains("not responding");
  }

  @Test
  public void statusServerWithInvalidDirReturnsMeangingfulMessage() throws Exception {
    File serverDir = new File(temporaryFolder.getRoot(), "serverDir");
    serverDir.mkdirs();
    CommandResult result = gfsh.executeCommand("status server --dir=" + serverDir.toString());
    assertThat(result.getMessageFromContent()).contains("not responding");
  }
}
