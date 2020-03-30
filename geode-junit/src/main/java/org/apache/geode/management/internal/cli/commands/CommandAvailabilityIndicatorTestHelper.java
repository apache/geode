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
import java.util.List;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.CommandManager;


public class CommandAvailabilityIndicatorTestHelper {

  public static void assertOnlineCommandsHasAvailabilityIndicator(CommandManager manager) {
    List<CommandMarker> commandMarkers = manager.getCommandMarkers();
    for (CommandMarker commandMarker : commandMarkers) {
      // ignore all the other commands beside GfshCommand
      if (!GfshCommand.class.isAssignableFrom(commandMarker.getClass())) {
        continue;
      }

      for (Method method : commandMarker.getClass().getMethods()) {
        CliCommand cliCommand = method.getAnnotation(CliCommand.class);
        if (cliCommand == null) {
          // the method is not a command method
          continue;
        }

        CliMetaData cliMetaData = method.getAnnotation(CliMetaData.class);
        // all the online commands have availability indicator defined in the commandManager
        if (cliMetaData == null || !cliMetaData.shellOnly()) {
          assertThat(manager.getHelper().hasAvailabilityIndicator(cliCommand.value()[0]))
              .describedAs(cliCommand.value()[0] + " in " + commandMarker.getClass()
                  + " has no availability indicator defined. "
                  + "Please add the command in the CommandAvailabilityIndicator")
              .isTrue();
        }
      }
    }
  }
}
