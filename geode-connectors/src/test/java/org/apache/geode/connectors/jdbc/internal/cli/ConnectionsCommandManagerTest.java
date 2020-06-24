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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.shell.core.CommandMarker;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.commands.VersionCommand;
import org.apache.geode.management.internal.util.ClasspathScanLoadHelper;
import org.apache.geode.services.module.internal.impl.ServiceLoaderModuleService;
import org.apache.geode.services.result.ModuleServiceResult;

/**
 * CommandManagerTest - Includes tests to check the CommandManager functions
 */
public class ConnectionsCommandManagerTest {

  private CommandManager commandManager;

  @Before
  public void before() {
    commandManager = new CommandManager(new ServiceLoaderModuleService(LogService.getLogger()));
  }

  /**
   * tests loadCommands()
   */
  @Test
  public void testCommandManagerLoadCommands() {
    Set<String> packagesToScan = new HashSet<>();
    packagesToScan.add(GfshCommand.class.getPackage().getName());
    packagesToScan.add(VersionCommand.class.getPackage().getName());

    Set<Class<?>> foundClasses;

    ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(packagesToScan);
    // geode's commands
    foundClasses = scanner.scanPackagesForClassesImplementing(CommandMarker.class,
        GfshCommand.class.getPackage().getName(),
        VersionCommand.class.getPackage().getName());

    ModuleServiceResult<Set<CommandMarker>> serviceLoadResult =
        new ServiceLoaderModuleService(LogService.getLogger())
            .loadService(CommandMarker.class);

    if (serviceLoadResult.isSuccessful()) {
      serviceLoadResult.getMessage()
          .forEach(CommandMarker -> foundClasses.add(CommandMarker.getClass()));
    }

    Set<Class<?>> expectedClasses = new HashSet<>();

    for (CommandMarker commandMarker : commandManager.getCommandMarkers()) {
      expectedClasses.add(commandMarker.getClass());
    }

    assertThat(expectedClasses).isEqualTo(foundClasses);
  }
}
