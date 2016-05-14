/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.shell.GfshConfig;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;
import java.util.Properties;

import static com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase.commandResultToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class HelpCommandsJUnitTest extends JUnit4DistributedTestCase {

  private int jmxPort;

  private Gfsh gfsh;

  @Before
  public void setup() throws Exception {
    jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
    localProps.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    localProps.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(jmxPort));
    getSystem(localProps);

    gfsh = Gfsh.getInstance(false, new String[0], new GfshConfig());
  }

  @After
  public void teardown() {
    disconnectAllFromDS();

    gfsh.executeCommand("disconnect");
  }

  @Ignore("Disconnect command doesn't appear to be working")
  @Test
  public void testOfflineHelp() throws Exception {
    Properties helpProps = new Properties();
    helpProps.load(HelpCommandsJUnitTest.class.getResourceAsStream("golden-help-offline.properties"));

    CommandManager cm = CommandManager.getInstance();
    for (Map.Entry<String, CommandTarget> e : cm.getCommands().entrySet()) {
      // Mock commands may have been produced in the VM by other tests
      // 'quit' is an alias for 'exit' and doesn't produce help
      if (e.getKey().contains("mock") || e.getKey().contains("quit")) {
        continue;
      }

      CommandResult cr = (CommandResult) gfsh.executeCommand("help " + e.getKey()).getResult();
      String gfshResult = commandResultToString(cr);

      String goldParam = e.getKey().replace(" ", "-") + ".help";
      String goldResult = helpProps.getProperty(goldParam);
      assertNotNull("No golden text for: " + goldParam, goldResult);
      assertEquals(goldResult.trim(), gfshResult.trim());

      helpProps.remove(goldParam);
    }

    // No help should remain unchecked
    assertEquals(0, helpProps.size());
  }

  @Test
  public void testOnlineHelp() throws Exception {
    Properties helpProps = new Properties();
    helpProps.load(HelpCommandsJUnitTest.class.getResourceAsStream("golden-help-online.properties"));

    gfsh.executeCommand("connect --jmx-manager=localhost[" + jmxPort + "]");

    CommandManager cm = CommandManager.getInstance();
    for (Map.Entry<String, CommandTarget> e : cm.getCommands().entrySet()) {
      // Mock commands may have been produced in the VM by other tests
      // 'quit' is an alias for 'exit' and doesn't produce help
      if (e.getKey().contains("mock") || e.getKey().contains("quit")) {
        continue;
      }

      CommandResult cr = (CommandResult) gfsh.executeCommand("help " + e.getKey()).getResult();
      String gfshResult = commandResultToString(cr);

      String goldParam = e.getKey().replace(" ", "-") + ".help";
      String goldResult = helpProps.getProperty(goldParam);
      assertNotNull("No golden text for: " + goldParam, goldResult);

      String[] lines = gfshResult.split("\n");
      gfshResult = String.join("\n", lines[0], lines[1], lines[2], lines[3]);

      assertEquals(goldResult.trim(), gfshResult.trim());

      helpProps.remove(goldParam);
    }

    // No help should remain unchecked
    assertEquals(0, helpProps.size());
  }
}
