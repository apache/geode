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

import static org.apache.geode.test.dunit.rules.GfshShellConnectionRule.*;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorStarterBuilder;
import org.apache.geode.test.dunit.rules.LocalLocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ExportLogsIntegrationTest {

  @ClassRule
  public static LocalLocatorStarterRule locator = new LocatorStarterBuilder().buildInThisVM();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  protected void connect() throws Exception {
    gfsh.connectAndVerify(locator.getLocatorPort(), PortType.locator);
  }

  @Test
  public void testInvalidMember() throws Exception {
    connect();
    gfsh.executeCommand("export logs --member=member1,member2");
    assertThat(gfsh.getGfshOutput()).contains("No Members Found");
  }

  @Test
  public void testNothingToExport() throws Exception {
    connect();
    gfsh.executeCommand("export logs --stats-only");
    assertThat(gfsh.getGfshOutput()).contains("No files to be exported.");
  }
}
