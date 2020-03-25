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

import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(GfshTest.class)
public class GfshCommandIntegrationTest {

  private static final String LOCATOR_NAME = "locator";

  private int locatorPort;
  private LocatorLauncher locatorLauncher;

  @Rule
  public GfshCommandRule gfshCommandRule = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    Path locatorFolder = temporaryFolder.newFolder(LOCATOR_NAME).toPath().toAbsolutePath();
    locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();

    locatorLauncher = new LocatorLauncher.Builder()
        .setMemberName(LOCATOR_NAME)
        .setWorkingDirectory(locatorFolder.toString())
        .setPort(locatorPort)
        .set("jmx-manager-port", String.valueOf(AvailablePortHelper.getRandomAvailableTCPPort()))
        .build();
    locatorLauncher.start();
  }

  @After
  public void tearDown() {
    locatorLauncher.stop();
  }

  @Test
  public void invalidCommandWhenNotConnected() {
    gfshCommandRule.executeAndAssertThat("abc").statusIsError()
        .containsOutput("Command 'abc' not found");
  }

  @Test
  public void invalidCommandWhenConnected() throws Exception {
    gfshCommandRule.connectAndVerify(locatorPort, GfshCommandRule.PortType.locator);
    gfshCommandRule.executeAndAssertThat("abc").statusIsError()
        .containsOutput("Command 'abc' not found");
  }
}
