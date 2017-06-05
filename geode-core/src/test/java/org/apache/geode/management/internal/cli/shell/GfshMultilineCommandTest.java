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
package org.apache.geode.management.internal.cli.shell;

import static org.apache.geode.management.internal.cli.i18n.CliStrings.LIST_MEMBER;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.NO_MEMBERS_FOUND_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(IntegrationTest.class)
public class GfshMultilineCommandTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withJMXManager().withAutoStart();


  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Test
  public void testMultiLineCommand() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    // Execute a command
    CommandStringBuilder csb = new CommandStringBuilder(LIST_MEMBER);
    csb.addOption(GROUP, "nogroup");
    gfsh.executeAndVerifyCommand(csb.getCommandString());
    assertThat(gfsh.getGfshOutput().trim()).isEqualTo(NO_MEMBERS_FOUND_MESSAGE);

    // Now execute same command with a new Continuation on new line
    csb = new CommandStringBuilder(LIST_MEMBER).addNewLine().addOption(GROUP, "nogroup");
    gfsh.executeAndVerifyCommand(csb.getCommandString());
    assertThat(gfsh.getGfshOutput().trim()).isEqualTo(NO_MEMBERS_FOUND_MESSAGE);
  }

}
