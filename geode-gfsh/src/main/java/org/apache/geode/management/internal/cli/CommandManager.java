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
import java.util.ServiceLoader;
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
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.management.cli.Disabled;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.help.Helper;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.util.ClasspathScanLoadHelper;
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
  private static final String SPRING_CONVERTER_PACKAGE = "org.springframework.shell.converters";

  /** Skip some of the Converters from Spring Shell for our customization */
  @Immutable
  private static final List<Class<?>> SPRING_CONVERTERS_TO_SKIP =
      Collections.unmodifiableList(Arrays.asList(
          // skip springs SimpleFileConverter to use our own FilePathConverter
          SimpleFileConverter.class,
          // skip spring's EnumConverter to use our own EnumConverter
          EnumConverter.class));

  private final Helper helper = new Helper();

  private final List<Converter<?>> converters = new ArrayList<>();
  private final List<CommandMarker> commandMarkers = new ArrayList<>();

  private final Properties cacheProperties = new Properties();
  private final LogWrapper logWrapper;
  private final InternalCache cache;

  /**
   * this constructor is used from Gfsh VM. We are getting the user-command-package from system
   * environment. used by Gfsh.
   */
  public CommandManager() {
    this(null, null);
  }

  /**
   * this is used when getting the instance in a cache server. We are getting the
   * user-command-package from distribution properties. used by OnlineCommandProcessor.
   */
  public CommandManager(final Properties newCacheProperties, InternalCache cache) {
    if (newCacheProperties != null) {
      this.cacheProperties.putAll(newCacheProperties);
    }
    this.cache = cache;
    logWrapper = LogWrapper.getInstance(cache);
    loadCommands();
    loadConverters();
  }

  private static void raiseExceptionIfEmpty(Set<?> foundClasses, String errorFor)
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
    String cacheUserCmdPackages =
        this.cacheProperties.getProperty(ConfigurationProperties.USER_COMMAND_PACKAGES, "");
    if (!cacheUserCmdPackages.isEmpty()) {
      userCommandSources.add(cacheUserCmdPackages);
    }

    for (String source : userCommandSources) {
      userCommandPackages.addAll(Arrays.asList(source.split(",")));
    }

    return userCommandPackages;
  }

  private void loadUserDefinedCommands() {
    String[] userCommandPackages = getUserCommandPackages().toArray(new String[] {});

    if (userCommandPackages.length == 0) {
      return;
    }

    // Load commands found in all of the packages
    try (ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(userCommandPackages)) {
      Set<Class<?>> foundClasses = scanner.scanPackagesForClassesImplementing(CommandMarker.class,
          userCommandPackages);
      for (Class<?> klass : foundClasses) {
        try {
          add((CommandMarker) klass.newInstance());
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
   * Loads commands via {@link ServiceLoader} from {@link ClassPathLoader}.
   *
   * @since GemFire 8.1
   */
  private void loadGeodeCommands() {
    ServiceLoader<CommandMarker> commandMarkers =
        ServiceLoader.load(CommandMarker.class, ClassPathLoader.getLatest().asClassLoader());

    boolean loadedAtLeastOneCommand = false;
    for (CommandMarker commandMarker : commandMarkers) {
      add(commandMarker);
      loadedAtLeastOneCommand = true;
    }
    if (!loadedAtLeastOneCommand) {
      throw new IllegalStateException(
          "Required Command classes were not loaded. Check logs for errors.");
    }
  }

  private void loadConverters() {
    loadGeodeDefinedConverters();
    loadSpringDefinedConverters();
  }

  private void loadCommands() {
    loadGeodeCommands();
    loadUserDefinedCommands();
  }

  private void loadSpringDefinedConverters() {
    try (ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(SPRING_CONVERTER_PACKAGE)) {
      // Spring shell's converters
      Set<Class<?>> foundClasses = scanner.scanPackagesForClassesImplementing(Converter.class,
          SPRING_CONVERTER_PACKAGE);
      for (Class<?> klass : foundClasses) {
        if (!SPRING_CONVERTERS_TO_SKIP.contains(klass)) {
          try {
            add((Converter<?>) klass.newInstance());
          } catch (Exception e) {
            logWrapper.warning(
                "Could not load Converter from: " + klass + " due to " + e.getLocalizedMessage()); // continue
          }
        }
      }
      raiseExceptionIfEmpty(foundClasses, "Spring Converter");
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
    }
  }

  private void loadGeodeDefinedConverters() {
    ServiceLoader<Converter> converters =
        ServiceLoader.load(Converter.class, ClassPathLoader.getLatestAsClassLoader());

    boolean loadedAtLeastOneConverter = false;
    for (Converter<?> converter : converters) {
      add(converter);
      loadedAtLeastOneConverter = true;
    }
    if (!loadedAtLeastOneConverter) {
      throw new IllegalStateException(
          "Required Converter classes were not loaded. Check logs for errors.");
    }
  }

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
