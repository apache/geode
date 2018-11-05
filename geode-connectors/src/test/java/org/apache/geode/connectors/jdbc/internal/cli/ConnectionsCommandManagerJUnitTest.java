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
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.springframework.shell.core.CommandMarker;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.commands.InternalGfshCommand;
import org.apache.geode.management.internal.cli.util.ClasspathScanLoadHelper;

/**
 * CommandManagerTest - Includes tests to check the CommandManager functions
 */
public class ConnectionsCommandManagerJUnitTest {

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
    Set<String> packagesToScan = new HashSet<>();
    packagesToScan.add(GfshCommand.class.getPackage().getName());
    packagesToScan.add(InternalGfshCommand.class.getPackage().getName());

    ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(packagesToScan);
    ServiceLoader<CommandMarker> loader =
        ServiceLoader.load(CommandMarker.class, ClassPathLoader.getLatest().asClassLoader());
    loader.reload();
    Iterator<CommandMarker> iterator = loader.iterator();

    Set<Class<?>> foundClasses;

    // geode's commands
    foundClasses = scanner.scanPackagesForClassesImplementing(CommandMarker.class,
        GfshCommand.class.getPackage().getName(),
        InternalGfshCommand.class.getPackage().getName());

    while (iterator.hasNext()) {
      foundClasses.add(iterator.next().getClass());
    }

    Set<Class<?>> expectedClasses = new HashSet<>();

    for (CommandMarker commandMarker : commandManager.getCommandMarkers()) {
      expectedClasses.add(commandMarker.getClass());
    }

    assertThat(expectedClasses).isEqualTo(foundClasses);
  }
}
