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
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.DistributionLocator;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class GfshStatusCommandsIntegrationTest {
  private static final String LOCATOR_NAME = "locator1";

  @Rule
  public LocatorStarterRule locator =
      new LocatorStarterRule().withJMXManager().withName(LOCATOR_NAME).withAutoStart();


  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void connect() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void statusLocatorWithBadPort_statusNotResponding() throws Exception {
    String wrongPort = String.valueOf(locator.getLocator().getPort() - 1);
    CommandResult result =
        gfsh.executeCommand("status locator --host=localhost --port=" + wrongPort);
    assertThat(result.getContent().getString("message")).doesNotContain("null");
    assertThat(result.getContent().getString("message")).contains("not responding");
    assertThat(result.getContent().getString("message")).contains(wrongPort);
  }

  @Test
  public void statusLocatorDefault_LocatorOnNonDefaultPort_statusNotResponding() throws Exception {
    CommandResult result = gfsh.executeCommand("status locator --host=localhost");
    assertThat(result.getContent().getString("message")).doesNotContain("null");
    assertThat(result.getContent().getString("message")).contains("not responding");
    assertThat(result.getContent().getString("message"))
        .contains(String.valueOf(DistributionLocator.DEFAULT_LOCATOR_PORT));
  }

  @Test
  public void statusLocatorWithActivePort_statusOnline() throws Exception {
    CommandResult result = gfsh.executeCommand(
        "status locator --host=localhost --port=" + String.valueOf(locator.getLocator().getPort()));
    assertThat(result.getContent().getString("message")).contains("is currently online");
  }

  @Test
  public void statusServerNoServer_statusNotResponding() throws Exception {
    CommandResult result = gfsh.executeCommand("status server");
    assertThat(result.getContent().getString("message")).doesNotContain("null");
    assertThat(result.getContent().getString("message")).contains("not responding");
  }

  @Test
  public void statusServerWithEmptyDir_statusNotResponding() throws Exception {
    File serverDir = new File(temporaryFolder.getRoot(), "serverDir");
    serverDir.mkdirs();
    CommandResult result = gfsh.executeCommand("status server --dir=" + serverDir.toString());
    assertThat(result.getContent().getString("message")).doesNotContain("null");
    assertThat(result.getContent().getString("message")).contains("not responding");
  }
}
