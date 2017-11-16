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
package org.apache.geode.management.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * CommandManagerTest - Includes tests to check the CommandManager functions
 */
@Category(UnitTest.class)
public class CommandManagerJUnitTest {
  private CommandManager commandManager;

  @Before
  public void before() {
    commandManager = new CommandManager();
  }

  /**
   * tests loadCommands()
   */
  @Test
  public void testCommandManagerLoadCommands() throws Exception {
    assertNotNull(commandManager);
    assertThat(commandManager.getCommandMarkers().size()).isGreaterThan(0);
    assertThat(commandManager.getConverters().size()).isGreaterThan(0);
  }

  /**
   * tests commandManagerInstance method
   */
  @Test
  public void testCommandManagerInstance() throws Exception {
    assertNotNull(commandManager);
  }

  @Test
  public void testCommandManagerLoadPluginCommands() throws Exception {
    assertNotNull(commandManager);

    // see META-INF/services/org.springframework.shell.core.CommandMarker service loader file.
    assertTrue("Should find listed plugin.",
        commandManager.getHelper().getCommands().contains("mock plugin command"));
    assertThat(
        commandManager.getCommandMarkers().stream().anyMatch(c -> c instanceof MockPluginCommand));
  }


  public static class MockPluginCommand implements CommandMarker {
    @CliCommand(value = "mock plugin command")
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public Result mockPluginCommand() {
      return null;
    }
  }
}
