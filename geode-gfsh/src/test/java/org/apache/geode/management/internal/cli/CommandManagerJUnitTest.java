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
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

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
 */
public class CommandManagerJUnitTest {

  private static final String COMMAND1_NAME = "command1";
  private static final String COMMAND1_NAME_ALIAS = "command1_alias";
  private static final String COMMAND2_NAME = "c2";

  private static final String COMMAND1_HELP = "help for " + COMMAND1_NAME;
  // ARGUMENTS
  private static final String ARGUMENT1_NAME = "argument1";
  private static final String ARGUMENT1_HELP = "help for argument1";
  private static final String ARGUMENT1_CONTEXT = "context for argument 1";
  private static final Completion[] ARGUMENT1_COMPLETIONS =
      {new Completion("arg1"), new Completion("arg1alt")};
  private static final String ARGUMENT2_NAME = "argument2";
  private static final String ARGUMENT2_CONTEXT = "context for argument 2";
  private static final String ARGUMENT2_HELP = "help for argument2";
  private static final String ARGUMENT2_UNSPECIFIED_DEFAULT_VALUE =
      "{unspecified default value for argument2}";
  private static final Completion[] ARGUMENT2_COMPLETIONS =
      {new Completion("arg2"), new Completion("arg2alt")};

  // OPTIONS
  private static final String OPTION1_NAME = "option1";
  private static final String OPTION1_SYNONYM = "opt1";
  private static final String OPTION1_HELP = "help for option1";
  private static final String OPTION1_CONTEXT = "context for option1";
  private static final String OPTION1_SPECIFIED_DEFAULT_VALUE =
      "{specified default value for option1}";
  private static final Completion[] OPTION1_COMPLETIONS =
      {new Completion("option1"), new Completion("option1Alternate")};
  private static final String OPTION2_NAME = "option2";
  private static final String OPTION2_HELP = "help for option2";
  private static final String OPTION2_CONTEXT = "context for option2";
  private static final String OPTION2_SPECIFIED_DEFAULT_VALUE =
      "{specified default value for option2}";
  private static final String OPTION3_NAME = "option3";
  private static final String OPTION3_SYNONYM = "opt3";
  private static final String OPTION3_HELP = "help for option3";
  private static final String OPTION3_CONTEXT = "context for option3";
  private static final String OPTION3_SPECIFIED_DEFAULT_VALUE =
      "{specified default value for option3}";
  private static final String OPTION3_UNSPECIFIED_DEFAULT_VALUE =
      "{unspecified default value for option3}";

  private CommandManager commandManager;

  @Before
  public void before() {
    commandManager = new CommandManager();
  }

  /**
   * tests loadCommands()
   */
  @Test
  public void testCommandManagerLoadCommands() {
    assertNotNull(commandManager);
    assertThat(commandManager.getCommandMarkers().size()).isGreaterThan(0);
    assertThat(commandManager.getConverters().size()).isGreaterThan(0);
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
   */
  @Test
  public void testCommandManagerLoadPluginCommands() {
    assertNotNull(commandManager);

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
      CommandMarker accessibleCommand = new AccessibleCommand();
      CommandMarker enabledCommand = new FeatureFlaggedAndEnabledCommand();
      CommandMarker reachableButDisabledCommand = new FeatureFlaggedReachableCommand();
      CommandMarker unreachableCommand = new FeatureFlaggedUnreachableCommand();

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
   * class that represents dummy commands
   */
  public static class Commands implements CommandMarker {

    @CliCommand(value = {COMMAND1_NAME, COMMAND1_NAME_ALIAS}, help = COMMAND1_HELP)
    @CliMetaData(shellOnly = true, relatedTopic = {"relatedTopicOfCommand1"})
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static String command1(
        @CliOption(key = ARGUMENT1_NAME, optionContext = ARGUMENT1_CONTEXT, help = ARGUMENT1_HELP,
            mandatory = true) String argument1,
        @CliOption(key = ARGUMENT2_NAME, optionContext = ARGUMENT2_CONTEXT, help = ARGUMENT2_HELP,
            unspecifiedDefaultValue = ARGUMENT2_UNSPECIFIED_DEFAULT_VALUE) String argument2,
        @CliOption(key = {OPTION1_NAME, OPTION1_SYNONYM}, help = OPTION1_HELP, mandatory = true,
            optionContext = OPTION1_CONTEXT,
            specifiedDefaultValue = OPTION1_SPECIFIED_DEFAULT_VALUE) String option1,
        @CliOption(key = {OPTION2_NAME}, help = OPTION2_HELP, optionContext = OPTION2_CONTEXT,
            specifiedDefaultValue = OPTION2_SPECIFIED_DEFAULT_VALUE) String option2,
        @CliOption(key = {OPTION3_NAME, OPTION3_SYNONYM}, help = OPTION3_HELP,
            optionContext = OPTION3_CONTEXT,
            unspecifiedDefaultValue = OPTION3_UNSPECIFIED_DEFAULT_VALUE,
            specifiedDefaultValue = OPTION3_SPECIFIED_DEFAULT_VALUE) String option3) {
      return null;
    }

    @CliCommand(value = {COMMAND2_NAME})
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static String command2() {
      return null;
    }

    @CliCommand(value = {"testParamConcat"})
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static Result testParamConcat(@CliOption(key = {"string"}) String string,
        @CliOption(key = {"stringArray"}) String[] stringArray,
        @CliOption(key = {"integer"}) Integer integer,
        @CliOption(key = {"colonArray"}) String[] colonArray) {
      return null;
    }

    @CliCommand(value = {"testMultiWordArg"})
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public static Result testMultiWordArg(@CliOption(key = "arg1") String arg1,
        @CliOption(key = "arg2") String arg2) {
      return null;
    }

    @CliAvailabilityIndicator({COMMAND1_NAME})
    public boolean isAvailable() {
      return true; // always available on server
    }
  }

  public static class MockPluginCommand implements CommandMarker {
    @CliCommand(value = "mock plugin command")
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public Result mockPluginCommand() {
      return null;
    }
  }

  public static class MockPluginCommandUnlisted implements CommandMarker {
    @CliCommand(value = "mock plugin command unlisted")
    @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
    public Result mockPluginCommandUnlisted() {
      return null;
    }
  }


  class AccessibleCommand implements CommandMarker {
    @CliCommand(value = "test-command")
    public Result ping() {
      return new CommandResult(ResultModel.createInfo("pong"));
    }

    @CliAvailabilityIndicator("test-command")
    public boolean always() {
      return true;
    }
  }

  @Disabled
  class FeatureFlaggedUnreachableCommand implements CommandMarker {
    @CliCommand(value = "unreachable")
    public Result nothing() {
      throw new RuntimeException("You reached the body of a feature-flagged command.");
    }
  }

  @Disabled(unlessPropertyIsSet = "reachable.flag")
  class FeatureFlaggedReachableCommand implements CommandMarker {
    @CliCommand(value = "reachable")
    public Result nothing() {
      throw new RuntimeException("You reached the body of a feature-flagged command.");
    }
  }

  @Disabled(unlessPropertyIsSet = "enabled.flag")
  class FeatureFlaggedAndEnabledCommand implements CommandMarker {
    @CliCommand(value = "reachable")
    public Result nothing() {
      throw new RuntimeException("You reached the body of a feature-flagged command.");
    }
  }

}
