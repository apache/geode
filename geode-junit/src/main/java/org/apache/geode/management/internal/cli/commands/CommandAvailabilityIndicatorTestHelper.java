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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.CommandManager;


/**
 * Test helper for verifying command availability indicators.
 * Updated for Spring Shell 3.x - uses ShellMethod instead of CliCommand.
 */
public class CommandAvailabilityIndicatorTestHelper {

  /**
   * Asserts that all online commands (non-shell-only commands) have availability indicators
   * defined.
   * This ensures commands properly declare when they can be executed.
   *
   * In Spring Shell 3.x, availability is controlled by ShellMethodAvailability annotation.
   * Shell-only commands (those that run locally in gfsh) don't need availability indicators
   * since they don't depend on cluster connection state.
   */
  public static void assertOnlineCommandsHasAvailabilityIndicator(CommandManager manager) {
    List<String> commandsWithoutAvailabilityIndicator = new ArrayList<>();

    // Get all registered command markers
    for (Object commandMarker : manager.getCommandMarkers()) {
      // Check each method for ShellMethod annotation
      for (Method method : commandMarker.getClass().getMethods()) {
        CliMetaData cliMetaData = method.getAnnotation(CliMetaData.class);

        // Skip if this is a shell-only command (doesn't need availability indicator)
        if (cliMetaData != null && cliMetaData.shellOnly()) {
          continue;
        }

        // Get the command name from Spring Shell's ShellMethod annotation
        org.springframework.shell.standard.ShellMethod shellMethod =
            method.getAnnotation(org.springframework.shell.standard.ShellMethod.class);

        if (shellMethod != null) {
          String commandName = shellMethod.key()[0]; // Primary command name

          // Check if this online command has an availability indicator
          if (!manager.getHelper().hasAvailabilityIndicator(commandName)) {
            commandsWithoutAvailabilityIndicator.add(commandName);
          }
        }
      }
    }

    assertThat(commandsWithoutAvailabilityIndicator)
        .as("All online commands must have availability indicators defined")
        .isEmpty();
  }
}
