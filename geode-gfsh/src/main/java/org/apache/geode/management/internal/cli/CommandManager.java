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

import static org.apache.geode.distributed.ConfigurationProperties.USER_COMMAND_PACKAGES;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.springframework.shell.converters.EnumConverter;
import org.springframework.shell.converters.SimpleFileConverter;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Disabled;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.commands.VersionCommand;
import org.apache.geode.management.internal.cli.help.Helper;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.util.ClasspathScanLoadHelper;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 *
 * this only takes care of loading all available command markers and converters from the application
 *
 * @since GemFire 7.0
 */
public class CommandManager {

  private static final String USER_CMD_PACKAGES_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + USER_COMMAND_PACKAGES;
  private static final String USER_CMD_PACKAGES_ENV_VARIABLE = "GEMFIRE_USER_COMMAND_PACKAGES";

  private final Helper helper = new Helper();

  private final List<Converter<?>> converters = new ArrayList<>();
  private final List<CommandMarker> commandMarkers = new ArrayList<>();

  private Properties cacheProperties;
  private LogWrapper logWrapper;
  private InternalCache cache;

  /**
   * this constructor is used from Gfsh VM. We are getting the user-command-package from system
   * environment. used by Gfsh.
   */
  public CommandManager(ModuleService moduleService) {
    this(null, null, moduleService);
  }

  /**
   * this is used when getting the instance in a cache server. We are getting the
   * user-command-package from distribution properties. used by OnlineCommandProcessor.
   */
  public CommandManager(final Properties cacheProperties, InternalCache cache,
      ModuleService moduleService) {
    if (cacheProperties != null) {
      this.cacheProperties = cacheProperties;
    }
    this.cache = cache;
    logWrapper = LogWrapper.getInstance(cache);
    loadCommands(moduleService);
  }

  private static void raiseExceptionIfEmpty(Set<Class<?>> foundClasses, String errorFor)
      throws IllegalStateException {
    if (foundClasses == null || foundClasses.isEmpty()) {
      throw new IllegalStateException(
          "Required " + errorFor + " classes were not loaded. Check logs for errors.");
    }
  }

  private Set<String> getUserCommandPackages() {
    final Set<String> userCommandPackages = new HashSet<>();

    List<String> userCommandSources = new ArrayList<>();
    // Find by packages specified by the system property
    if (System.getProperty(USER_CMD_PACKAGES_PROPERTY) != null) {
      userCommandSources.add(System.getProperty(USER_CMD_PACKAGES_PROPERTY));
    }

    // Find by packages specified by the environment variable
    if (System.getenv().containsKey(USER_CMD_PACKAGES_ENV_VARIABLE)) {
      userCommandSources.add(System.getenv().get(USER_CMD_PACKAGES_ENV_VARIABLE));
    }

    // Find by packages specified in the distribution config
    if (this.cacheProperties != null) {
      String cacheUserCmdPackages =
          this.cacheProperties.getProperty(ConfigurationProperties.USER_COMMAND_PACKAGES);
      if (cacheUserCmdPackages != null && !cacheUserCmdPackages.isEmpty()) {
        userCommandSources.add(cacheUserCmdPackages);
      }
    }

    for (String source : userCommandSources) {
      userCommandPackages.addAll(Arrays.asList(source.split(",")));
    }

    return userCommandPackages;
  }

  private void loadUserCommands(ClasspathScanLoadHelper scanner, Set<String> restrictedToPackages) {
    if (restrictedToPackages.size() == 0) {
      return;
    }

    // Load commands found in all of the packages
    try {
      Set<Class<?>> foundClasses =
          scanner.scanPackagesForClassesImplementing(CommandMarker.class,
              restrictedToPackages.toArray(new String[] {}));
      for (Class<?> klass : foundClasses) {
        try {
          CommandMarker commandMarker = (CommandMarker) klass.newInstance();
          add(commandMarker);
        } catch (Exception e) {
          logWrapper.warning("Could not load User Commands from: " + klass + " due to "
              + e.getLocalizedMessage()); // continue
        }
      }
      raiseExceptionIfEmpty(foundClasses, "User Command");
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Loads commands via {@link ModuleService}.
   *
   * @since GemFire 8.1
   */
  private void loadPluginCommands(ModuleService moduleService) {
    ServiceResult<Set<CommandMarker>> serviceLoadResult =
        moduleService.loadService(CommandMarker.class);

    serviceLoadResult.ifSuccessful(commandMarkers -> {
      commandMarkers.forEach(commandMarker -> {
        try {
          add(commandMarker);
        } catch (Throwable t) {
          logWrapper.warning("Could not load plugin command: " + t.getMessage());
        }
      });
    });
    serviceLoadResult.ifFailure(errorMessage -> logWrapper
        .severe(String.format("Could not load plugin commands in the latest classLoader. %s",
            errorMessage)));
  }

  private void loadCommands(ModuleService moduleService) {
    Set<String> userCommandPackages = getUserCommandPackages();
    Set<String> packagesToScan = new HashSet<>(userCommandPackages);
    packagesToScan.add("org.apache.geode.management.internal.cli.converters");
    packagesToScan.add("org.springframework.shell.converters");
    packagesToScan.add(GfshCommand.class.getPackage().getName());
    packagesToScan.add(VersionCommand.class.getPackage().getName());

    // Create one scanner to be used everywhere
    try (ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(packagesToScan)) {
      loadUserCommands(scanner, userCommandPackages);
      loadPluginCommands(moduleService);
      loadGeodeCommands(scanner);
      loadConverters(scanner);
    }
  }

  private void loadConverters(ClasspathScanLoadHelper scanner) {
    Set<Class<?>> foundClasses;
    // Converters
    try {
      foundClasses = scanner.scanPackagesForClassesImplementing(Converter.class,
          "org.apache.geode.management.internal.cli.converters");
      for (Class<?> klass : foundClasses) {
        try {
          Converter<?> object = (Converter<?>) klass.newInstance();
          add(object);

        } catch (Exception e) {
          logWrapper.warning(
              "Could not load Converter from: " + klass + " due to " + e.getLocalizedMessage()); // continue
        }
      }
      raiseExceptionIfEmpty(foundClasses, "Converters");

      // Spring shell's converters
      foundClasses = scanner.scanPackagesForClassesImplementing(Converter.class,
          "org.springframework.shell.converters");
      for (Class<?> klass : foundClasses) {
        if (!SHL_CONVERTERS_TOSKIP.contains(klass)) {
          try {
            add((Converter<?>) klass.newInstance());
          } catch (Exception e) {
            logWrapper.warning(
                "Could not load Converter from: " + klass + " due to " + e.getLocalizedMessage()); // continue
          }
        }
      }
      raiseExceptionIfEmpty(foundClasses, "Basic Converters");
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
    }
  }

  private void loadGeodeCommands(ClasspathScanLoadHelper scanner) {
    // CommandMarkers
    Set<Class<?>> foundClasses;
    try {
      // geode's commands
      foundClasses = scanner.scanPackagesForClassesImplementing(CommandMarker.class,
          GfshCommand.class.getPackage().getName(),
          VersionCommand.class.getPackage().getName());

      for (Class<?> klass : foundClasses) {
        try {
          CommandMarker commandMarker = (CommandMarker) klass.newInstance();

          add(commandMarker);
        } catch (Exception e) {
          logWrapper.warning(
              "Could not load Command from: " + klass + " due to " + e.getLocalizedMessage()); // continue
        }
      }
      raiseExceptionIfEmpty(foundClasses, "Commands");

      // do not add Spring shell's commands for now. When we add it, we need to tell the parser that
      // these are offline commands.
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
    }
  }

  /** Skip some of the Converters from Spring Shell for our customization */
  @Immutable
  private static final List<Class> SHL_CONVERTERS_TOSKIP =
      Collections.unmodifiableList(Arrays.asList(
          // skip springs SimpleFileConverter to use our own FilePathConverter
          SimpleFileConverter.class,
          // skip spring's EnumConverter to use our own EnumConverter
          EnumConverter.class));

  public List<Converter<?>> getConverters() {
    return converters;
  }

  public List<CommandMarker> getCommandMarkers() {
    return commandMarkers;
  }

  /**
   * Method to add new Converter
   */
  private void add(Converter<?> converter) {
    if (CommandManagerAware.class.isAssignableFrom(converter.getClass())) {
      ((CommandManagerAware) converter).setCommandManager(this);
    }
    converters.add(converter);
  }

  /**
   * Method to add new Commands to the parser
   */
  void add(CommandMarker commandMarker) {
    Disabled classDisabled = commandMarker.getClass().getAnnotation(Disabled.class);
    if (classDisabled != null && (classDisabled.unlessPropertyIsSet().isEmpty()
        || System.getProperty(classDisabled.unlessPropertyIsSet()) == null)) {
      return;
    }

    // inject the cache into the commands
    if (GfshCommand.class.isAssignableFrom(commandMarker.getClass())) {
      ((GfshCommand) commandMarker).setCache(cache);
    }

    // inject the commandManager into the commands
    if (CommandManagerAware.class.isAssignableFrom(commandMarker.getClass())) {
      ((CommandManagerAware) commandMarker).setCommandManager(this);
    }
    commandMarkers.add(commandMarker);
    for (Method method : commandMarker.getClass().getMethods()) {
      CliCommand cliCommand = method.getAnnotation(CliCommand.class);
      CliAvailabilityIndicator availability = method.getAnnotation(CliAvailabilityIndicator.class);
      if (cliCommand == null && availability == null) {
        continue;
      }

      if (cliCommand != null) {
        helper.addCommand(cliCommand, method);
      }

      if (availability != null) {
        helper.addAvailabilityIndicator(availability, new MethodTarget(method, commandMarker));
      }
    }
  }

  public Helper getHelper() {
    return helper;
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

}
