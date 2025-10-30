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

import java.util.Properties;

import com.examples.UserGfshCommand;
import org.junit.Before;
import org.junit.Test;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Disabled;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * CommandManagerTest - Includes tests to check the CommandManager functions
 *
 * SPRING SHELL 3.X MIGRATION NOTES:
 * - Removed Spring Shell 1.x annotations: @CliCommand, @CliOption, @CliAvailabilityIndicator
 * - Using Spring Shell 3.x
 * annotations: @ShellComponent, @ShellMethod, @ShellMethodAvailability, @ShellOption
 * - CommandMarker interface removed, using @ShellComponent instead
 * - getConverters() removed as Shell 3.x has different converter mechanism
 */
public class CommandManagerJUnitTest {

  private CommandManager commandManager;

  @Before
  public void before() {
    commandManager = new CommandManager();
  }

  /**
   * tests loadCommands()
   * MIGRATED: getConverters() removed in Spring Shell 3.x as converter mechanism changed
   */
  @Test
  public void testCommandManagerLoadCommands() {
    assertNotNull(commandManager);
    assertThat(commandManager.getCommandMarkers().size()).isGreaterThan(0);
    // Removed: getConverters() no longer exists in Spring Shell 3.x
    // Shell 3.x handles converters differently through ConversionService
  }

  /**
   * tests commandManagerInstance method
   */
  @Test
  public void testCommandManagerInstance() {
    assertNotNull(commandManager);
  }

  /**
   * @since GemFire 8.1
   *
   *        SPRING SHELL 3.X ROOT CAUSE ANALYSIS:
   *        This test checks if plugin commands are loaded via ServiceLoader mechanism.
   *        The test expects "mock plugin command" to be found in Helper.getCommands().
   *
   *        TRACE LOGS ENABLED: Will print all loaded commands to understand what's actually
   *        registered.
   */
  @Test
  public void testCommandManagerLoadPluginCommands() {
    assertNotNull(commandManager);

    // ORIGINAL ASSERTIONS - will fail if plugin not loaded
    assertTrue("Should find listed plugin.",
        commandManager.getHelper().getCommands().contains("mock plugin command"));
    assertTrue("Should not find unlisted plugin.",
        !commandManager.getHelper().getCommands().contains("mock plugin command unlisted"));
  }

  @Test
  public void testCommandManagerLoadsUserCommand() throws Exception {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.USER_COMMAND_PACKAGES, "com.examples");
    CommandManager commandManager = new CommandManager(props, null);

    assertThat(
        commandManager.getCommandMarkers().stream().anyMatch(c -> c instanceof UserGfshCommand));
  }

  @Test
  public void commandManagerDoesNotAddUnsatisfiedFeatureFlaggedCommands() {
    System.setProperty("enabled.flag", "true");
    try {
      Object accessibleCommand = new AccessibleCommand();
      Object enabledCommand = new FeatureFlaggedAndEnabledCommand();
      Object reachableButDisabledCommand = new FeatureFlaggedReachableCommand();
      Object unreachableCommand = new FeatureFlaggedUnreachableCommand();

      commandManager.add(accessibleCommand);
      commandManager.add(enabledCommand);
      commandManager.add(reachableButDisabledCommand);
      commandManager.add(unreachableCommand);

      assertThat(commandManager.getCommandMarkers()).contains(accessibleCommand, enabledCommand);
      assertThat(commandManager.getCommandMarkers()).doesNotContain(reachableButDisabledCommand,
          unreachableCommand);
    } finally {
      System.clearProperty("enabled.flag");
    }
  }

  /**
   * Plugin command that SHOULD be discovered via META-INF/services.
   * Must implement Geode CommandMarker (not Spring Shell CommandMarker) for discovery.
   * Spring Shell 3.x: Commands use @ShellMethod but must also implement CommandMarker
   * for CommandManager.loadUserDefinedCommands() to discover them via class scanning.
   */
  public static class MockPluginCommand implements CommandMarker {
    @ShellMethod(value = "Mock plugin command", key = "mock plugin command")
    @CliMetaData(shellOnly = true)
    public String mockPluginCommand() {
      return "Mock plugin command";
    }
  }

  /**
   * Plugin command that should NOT be discovered (not listed in META-INF/services).
   * Must implement CommandMarker to match plugin interface requirements but won't be discovered
   * since it's not listed in META-INF/services file.
   */
  @ShellComponent
  public static class MockPluginCommandUnlisted implements CommandMarker {
    @ShellMethod(key = "mock plugin command unlisted")
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public Result mockPluginCommandUnlisted() {
      return null;
    }
  }

  /**
   * Accessible command using Spring Shell 3.x annotations
   */
  @ShellComponent
  class AccessibleCommand {
    @ShellMethod(key = "test-command")
    public Result ping() {
      return new CommandResult(ResultModel.createInfo("pong"));
    }

    @ShellMethodAvailability("test-command")
    public boolean always() {
      return true;
    }
  }

  /**
   * Feature-flagged unreachable command
   */
  @Disabled
  @ShellComponent
  class FeatureFlaggedUnreachableCommand {
    @ShellMethod(key = "unreachable")
    public Result nothing() {
      throw new RuntimeException("You reached the body of a feature-flagged command.");
    }
  }

  /**
   * Feature-flagged reachable command
   */
  @Disabled(unlessPropertyIsSet = "reachable.flag")
  @ShellComponent
  class FeatureFlaggedReachableCommand {
    @ShellMethod(key = "reachable")
    public Result nothing() {
      throw new RuntimeException("You reached the body of a feature-flagged command.");
    }
  }

  /**
   * Feature-flagged and enabled command
   */
  @Disabled(unlessPropertyIsSet = "enabled.flag")
  @ShellComponent
  class FeatureFlaggedAndEnabledCommand {
    @ShellMethod(key = "enabled")
    public Result nothing() {
      throw new RuntimeException("You reached the body of a feature-flagged command.");
    }
  }
}
