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
package org.apache.geode.management.internal.cli.completion;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.CompletionContext;
import org.apache.geode.management.internal.cli.help.Helper;

/**
 * Provides completion for help command's command parameter.
 *
 * ROOT CAUSE #12 (Help commands): In Spring Shell 2.x, HelpConverter provided command name
 * completions for the help command. During Spring Shell 3.x migration, this was never
 * migrated to the CompletionProvider pattern.
 *
 * REASONING: The "help" command takes a "command" parameter that should complete to
 * available command names. Unlike hint's topic parameter which completes to topics,
 * help completes to actual command names.
 *
 * Test expectations (from GfshParserAutoCompletionIntegrationTest):
 * - "help start" → 8 candidates like "help start gateway-receiver"
 * - "help st" → 18 candidates like "help start gateway-receiver"
 *
 * The completion should append matching command names to "help " prefix.
 *
 * @since Spring Shell 3.x migration - fixing missing converter
 */
public class HelpCommandCompletionProvider implements ValueCompletionProvider {

  @Override
  public boolean supports(Class<?> targetType) {
    // This provider handles String parameters for help command
    return String.class.equals(targetType);
  }

  @Override
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    List<Completion> completions = new ArrayList<>();

    // ROOT CAUSE #12: Only provide command completions for the "command" parameter of "help"
    // command.
    // Check the option name from the context - should be "command" (CliStrings.HELP__COMMAND).
    // Also check command name to ensure we're in help command context.
    String optionName = context.getOptionName();
    String commandName = context.getCommandName();

    if (optionName == null || !optionName.equals("command")) {
      return completions; // Not the command parameter, return empty
    }

    if (commandName == null || !commandName.equals("help")) {
      return completions; // Not the help command, return empty
    }

    // Get CommandManager from context (passed by GfshParser)
    Object cmdMgr = context.getCommandManager();
    if (cmdMgr == null || !(cmdMgr instanceof CommandManager)) {
      return completions; // No CommandManager, can't get commands
    }

    CommandManager commandManager = (CommandManager) cmdMgr;
    Helper helper = commandManager.getHelper();
    if (helper == null) {
      return completions;
    }

    // Get all command names
    Set<String> allCommands = helper.getCommands();
    // Filter commands based on partial input
    // MATCHING LOGIC: If partialValue is empty/null, return all commands
    // Otherwise return commands that start with partialValue
    String partial = (partialValue == null) ? "" : partialValue;

    for (String cmd : allCommands) {
      if (partial.isEmpty() || cmd.startsWith(partial)) {
        completions.add(new Completion(cmd));
      }
    }

    return completions;
  }
}
