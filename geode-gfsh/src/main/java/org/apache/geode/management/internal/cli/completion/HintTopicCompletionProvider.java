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
 * Provides completion for hint command topic names.
 *
 * ROOT CAUSE #10: In Spring Shell 2.x, HintTopicConverter provided topic name completions
 * by calling helper.getTopicNames(). During Spring Shell 3.x migration, this converter
 * was removed and never migrated to the CompletionProvider pattern, causing all hint
 * completion tests to fail with 0 candidates.
 *
 * REASONING: The "hint" command takes an optional "topic" parameter that should complete
 * to available command topics (e.g., "client", "data", "deploy", etc.). These topic names
 * are maintained by Helper.getTopicNames() and represent categories of commands.
 *
 * Test expectations (from GfshParserAutoCompletionIntegrationTest):
 * - "hint" → >10 candidates, first = "hint client"
 * - "hint " → >10 candidates, first = "hint client"
 * - "hint d" → 3 candidates, first = "hint data"
 * - "hint data" → 1 candidate = "hint data"
 *
 * Matching logic (from original HintTopicConverter):
 * 1. Exact case match first: "Data" matches "Data" but not "data"
 * 2. Case-insensitive fallback: "d" matches "data", "Data", "deploy"
 * 3. Preserve user's case in completion: "d" + "ata" = "data", "D" + "ata" = "Data"
 *
 * @since Spring Shell 3.x migration - fixing missing converter
 */
public class HintTopicCompletionProvider implements ValueCompletionProvider {

  @Override
  public boolean supports(Class<?> targetType) {
    // This provider handles String parameters for hint topics
    return String.class.equals(targetType);
  }

  @Override
  public List<Completion> getCompletions(Class<?> targetType, String partialValue,
      CompletionContext context) {
    List<Completion> completions = new ArrayList<>();

    // ROOT CAUSE #10: Only provide topic completions for the "topic" parameter of "hint" command.
    // Check the option name from the context - should be "topic" (CliStrings.HINT__TOPICNAME).
    // Also check command name to ensure we're in hint command context.
    String optionName = context.getOptionName();
    String commandName = context.getCommandName();

    if (optionName == null || !optionName.equals("topic")) {
      return completions; // Not the topic parameter, return empty
    }

    if (commandName == null || !commandName.equals("hint")) {
      return completions; // Not the hint command, return empty
    }

    // Get CommandManager from context (passed by GfshParser)
    Object cmdMgr = context.getCommandManager();
    if (cmdMgr == null || !(cmdMgr instanceof CommandManager)) {
      return completions; // No CommandManager, can't get topics
    }

    CommandManager commandManager = (CommandManager) cmdMgr;
    Helper helper = commandManager.getHelper();
    if (helper == null) {
      return completions;
    }

    // Get all topic names
    Set<String> topicNames = helper.getTopicNames();
    // Filter topics based on partial input
    // MATCHING LOGIC (from original HintTopicConverter):
    // 1. If partialValue is empty/null, return all topics
    // 2. Try exact case match first
    // 3. Fall back to case-insensitive match, preserving user's case in result
    String partial = (partialValue == null) ? "" : partialValue;

    for (String topicName : topicNames) {
      if (partial.isEmpty()) {
        // No filter, add all topics
        completions.add(new Completion(topicName));
      } else if (topicName.startsWith(partial)) {
        // Exact case match - use topic name as-is
        completions.add(new Completion(topicName));
      } else if (topicName.toLowerCase().startsWith(partial.toLowerCase())) {
        // Case-insensitive match - preserve user's case for typed part
        // e.g., user typed "D", topic is "data" → completion is "Data" (D + ata)
        String completionStr = partial + topicName.substring(partial.length());
        completions.add(new Completion(completionStr));
      }
    }

    return completions;
  }
}
