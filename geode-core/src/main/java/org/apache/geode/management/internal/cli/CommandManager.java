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


import static java.util.stream.Collectors.toSet;

import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;
import org.springframework.shell.commands.ConsoleCommands;
import org.springframework.shell.commands.ExitCommands;
import org.springframework.shell.commands.HelpCommands;
import org.springframework.shell.converters.EnumConverter;
import org.springframework.shell.converters.SimpleFileConverter;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Converter;

import org.apache.geode.management.internal.cli.help.Helper;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.ClasspathScanLoadHelper;

/**
 * this only takes care of loading all available command markers and converters from the application
 *
 * @since GemFire 7.0
 */
public class CommandManager {
  // Skip some of the Converters from Spring Shell for our customization
  private static final Set<Class> EXCLUDED_CLASSES = ImmutableSet.of(SimpleFileConverter.class,
      EnumConverter.class, ExitCommands.class, HelpCommands.class, ConsoleCommands.class);

  private final LogWrapper logWrapper = LogWrapper.getInstance();
  private final Set<CommandMarker> commandMarkers;
  private final Set<Converter> converters;
  private final Helper helper;

  public CommandManager() {
    helper = new Helper();
    converters = loadConverters();
    commandMarkers = loadCommandMarkers();
  }

  private Set<Converter> loadConverters() {
    Set<Converter> converters = instantiateAllClassesImplementing(Converter.class);
    raiseExceptionIfEmpty(converters, "converters");

    converters.forEach(this::setContextIfCommandManagerAware);
    return converters;
  }

  private Set<CommandMarker> loadCommandMarkers() {
    Set<CommandMarker> commandMarkers = instantiateAllClassesImplementing(CommandMarker.class);
    raiseExceptionIfEmpty(commandMarkers, "commandMarkers");

    commandMarkers.forEach(this::setContextIfCommandManagerAware);
    commandMarkers.forEach(helper::registerCommand);
    return commandMarkers;
  }

  private <T> Set<T> instantiateAllClassesImplementing(Class<T> implementedInterface) {
    Set<Class<? extends T>> classes =
        ClasspathScanLoadHelper.scanClasspathForClassesImplementing(implementedInterface);

    Predicate<Class<? extends T>> classIsNotExcluded = aClass -> !EXCLUDED_CLASSES.contains(aClass);

    return classes.stream().filter(classIsNotExcluded).map(this::instantiateClass)
        .filter(Objects::nonNull).collect(toSet());
  }

  private <T> T instantiateClass(Class<T> classToInstantiate) {
    try {
      return classToInstantiate.newInstance();
    } catch (Exception e) {
      logWrapper.warning("Could not load command or converter from: " + classToInstantiate, e);
    }
    return null;
  }

  private void setContextIfCommandManagerAware(Object commandOrConverter) {
    if (CommandManagerAware.class.isAssignableFrom(commandOrConverter.getClass())) {
      ((CommandManagerAware) commandOrConverter).setCommandManager(this);
    }
  }

  private static void raiseExceptionIfEmpty(Set<?> foundClasses, String classType)
      throws IllegalStateException {
    if (foundClasses == null || foundClasses.isEmpty()) {
      throw new IllegalStateException("No " + classType + " were loaded. Check logs for errors.");
    }
  }

  public String obtainHelp(String buffer) {
    int terminalWidth = -1;
    Gfsh gfsh = Gfsh.getCurrentInstance();
    if (gfsh != null) {
      terminalWidth = gfsh.getTerminalWidth();
    }
    return helper.getHelp(buffer, terminalWidth);
  }

  public String obtainHint(String topic) {
    return helper.getHint(topic);
  }

  public Set<Converter> getConverters() {
    return converters;
  }

  public Set<CommandMarker> getCommandMarkers() {
    return commandMarkers;
  }

  public Helper getHelper() {
    return helper;
  }
}
