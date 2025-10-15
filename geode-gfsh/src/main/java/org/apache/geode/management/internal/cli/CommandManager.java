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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.InternalCache;
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

  // WHY COMMENTED: Spring Shell 3.x uses different converter mechanism
  // Shell 1.x: Used SPRING_CONVERTER_PACKAGE to load Spring's converters
  // Shell 3.x: Converters registered differently, no longer need package scanning
  // private static final String SPRING_CONVERTER_PACKAGE = "org.springframework.shell.converters";

  // WHY COMMENTED: Spring Shell 3.x converters handled differently
  // Shell 1.x: Had to skip Spring's SimpleFileConverter and EnumConverter to use custom ones
  // Shell 3.x: Converter registration mechanism changed, no longer need skip list
  // @Immutable
  // private static final List<Class<?>> SPRING_CONVERTERS_TO_SKIP =
  // Collections.unmodifiableList(Arrays.asList(
  // SimpleFileConverter.class, // skip springs SimpleFileConverter to use our own FilePathConverter
  // EnumConverter.class)); // skip spring's EnumConverter to use our own EnumConverter

  private final Helper helper = new Helper();

  // WHY KEPT: Converters list retained for potential future Shell 3.x converter support
  // Currently unused as Shell 3.x converter mechanism is different from Shell 1.x
  private final List<Object> converters = new ArrayList<>();
  private final List<Object> commandMarkers = new ArrayList<>();

  private final Properties cacheProperties = new Properties();
  private final LogWrapper logWrapper;
  private final InternalCache cache;

  /**
   * this constructor is used from Gfsh VM. We are getting the user-command-package from system
   * environment. used by Gfsh.
   */
  public CommandManager() {
    // Initialize fields (cache will be null for client-side usage)
    this.cache = null;
    this.logWrapper = LogWrapper.getInstance(null);

    // Load commands and converters (same as server-side)
    loadCommands();
    loadConverters();
  }

  /**
   * this is used when getting the instance in a cache server. We are getting the
   * user-command-package from distribution properties. used by OnlineCommandProcessor.
   */
  public CommandManager(final Properties newCacheProperties, InternalCache cache) {
    if (newCacheProperties != null) {
      cacheProperties.putAll(newCacheProperties);
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
        cacheProperties.getProperty(ConfigurationProperties.USER_COMMAND_PACKAGES, "");
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
      Set<Class<?>> foundClasses =
          scanner.scanPackagesForClassesImplementing(CommandMarker.class, userCommandPackages);
      for (Class<?> klass : foundClasses) {
        try {
          // WHY CAST: Explicit cast to CommandMarker kept for code clarity, though add() accepts
          // Object
          // This makes the intent clear that user commands must implement CommandMarker interface
          add((CommandMarker) klass.getDeclaredConstructor().newInstance());
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
   * Loads commands by scanning for classes implementing {@link CommandMarker}.
   * In Spring Shell 3.x, commands still implement CommandMarker for discovery purposes,
   * though they use @ShellComponent and @ShellMethod for command registration.
   *
   * @since GemFire 8.1
   */
  private void loadGeodeCommands() {
    // Define packages containing Geode command classes
    String[] commandPackages = {
        "org.apache.geode.management.internal.cli.commands",
        "org.apache.geode.management.cli",
        "org.apache.geode.cache.wan.internal.cli.commands", // WAN commands
        "org.apache.geode.connectors.jdbc.internal.cli", // JDBC connector commands
        "org.apache.geode.cache.lucene.internal.cli.commands" // Lucene commands
    };

    try (ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(commandPackages)) {
      Set<Class<?>> foundClasses = new HashSet<>();

      // Spring Shell 3.x: Find classes annotated with @ShellComponent
      Set<Class<?>> shellComponentClasses = scanner.scanClasspathForAnnotation(
          org.springframework.shell.standard.ShellComponent.class,
          commandPackages);

      // For each class with @ShellComponent, also find non-abstract subclasses.
      Set<Class<?>> allShellComponentClasses = new HashSet<>();
      for (Class<?> klass : shellComponentClasses) {
        // Find all concrete subclasses recursively
        Set<Class<?>> concreteSubclasses =
            scanner.scanPackagesForSubclassesOf(klass, commandPackages);

        if (!concreteSubclasses.isEmpty()) {
          allShellComponentClasses.addAll(concreteSubclasses);
        } else {
          if (!java.lang.reflect.Modifier.isAbstract(klass.getModifiers())) {
            allShellComponentClasses.add(klass);
          }
        }
      }
      foundClasses.addAll(allShellComponentClasses);

      // Also check for classes implementing CommandMarker (for backwards compatibility)
      Set<Class<?>> commandMarkerClasses = scanner.scanPackagesForClassesImplementing(
          CommandMarker.class,
          commandPackages);
      foundClasses.addAll(commandMarkerClasses);

      boolean loadedAtLeastOneCommand = false;
      for (Class<?> klass : foundClasses) {
        try {
          Object commandInstance = klass.getDeclaredConstructor().newInstance();
          add(commandInstance);
          loadedAtLeastOneCommand = true;
        } catch (Exception e) {
          logWrapper.warning("Could not load Command from: " + klass + " due to "
              + e.getLocalizedMessage());
          e.printStackTrace();
        }
      }

      if (!loadedAtLeastOneCommand) {
        throw new IllegalStateException(
            "Required Command classes were not loaded. Check logs for errors.");
      }
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
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
    // WHY DISABLED: Spring Shell 3.x converter loading mechanism differs from Shell 1.x
    // Shell 1.x: Used Converter interface, scanned SPRING_CONVERTER_PACKAGE for implementations
    // Shell 3.x: Uses different converter registration approach, needs migration
    // Once Shell 3.x converter approach is finalized, this should be re-implemented
    /*
     * try (ClasspathScanLoadHelper scanner = new ClasspathScanLoadHelper(SPRING_CONVERTER_PACKAGE))
     * {
     * Set<Class<?>> foundClasses =
     * scanner.scanPackagesForClassesImplementing(Converter.class, SPRING_CONVERTER_PACKAGE);
     * for (Class<?> klass : foundClasses) {
     * if (!SPRING_CONVERTERS_TO_SKIP.contains(klass)) {
     * try {
     * add((Converter<?>) klass.newInstance());
     * } catch (Exception e) {
     * logWrapper.warning(
     * "Could not load Converter from: " + klass + " due to " + e.getLocalizedMessage());
     * }
     * }
     * }
     * raiseExceptionIfEmpty(foundClasses, "Spring Converter");
     * } catch (IllegalStateException e) {
     * logWrapper.warning(e.getMessage(), e);
     * throw e;
     * }
     */
  }

  private void loadGeodeDefinedConverters() {
    // WHY DISABLED: Spring Shell 3.x Converter interface changed from Shell 1.x
    // Shell 1.x: Used ServiceLoader to load Converter implementations
    // Shell 3.x: Converter mechanism redesigned, pending migration strategy
    /*
     * ServiceLoader<Converter> converters =
     * ServiceLoader.load(Converter.class, ClassPathLoader.getLatestAsClassLoader());
     *
     * boolean loadedAtLeastOneConverter = false;
     * for (Converter<?> converter : converters) {
     * add(converter);
     * loadedAtLeastOneConverter = true;
     * }
     * if (!loadedAtLeastOneConverter) {
     * throw new IllegalStateException(
     * "Required Converter classes were not loaded. Check logs for errors.");
     * }
     */
  }

  // WHY COMMENTED: Spring Shell 3.x migration - Shell 1.x Converter API removed
  // Shell 1.x: getConverters() returned List<Converter<?>> for parser to access converters
  // Shell 3.x: Converter mechanism changed, this method no longer applicable
  // public List<Converter<?>> getConverters() {
  // return converters;
  // }

  /**
   * Returns the list of command markers (command instances) registered with this manager.
   * Used by GfshParser to find command instances when building parse results.
   */
  public List<Object> getCommandMarkers() {
    return commandMarkers;
  }

  /**
   * WHY COMMENTED: Spring Shell 3.x migration - Shell 1.x Converter API removed
   * Shell 1.x: add(Converter<?>) method registered converters with CommandManagerAware injection
   * Shell 3.x: Converter registration mechanism changed, method no longer needed
   */
  // private void add(Converter<?> converter) {
  // if (CommandManagerAware.class.isAssignableFrom(converter.getClass())) {
  // ((CommandManagerAware) converter).setCommandManager(this);
  // }
  // converters.add(converter);
  // }

  /**
   * Method to add new Commands to the parser
   * Made public to support test command registration in GfshParserRule
   */
  public void add(Object commandMarker) {
    Disabled classDisabled = commandMarker.getClass().getAnnotation(Disabled.class);
    if (classDisabled != null && (classDisabled.unlessPropertyIsSet().isEmpty()
        || System.getProperty(classDisabled.unlessPropertyIsSet()) == null)) {
      return;
    }

    // inject the cache into the commands (only if not already set, to support test mocking)
    if (GfshCommand.class.isAssignableFrom(commandMarker.getClass())) {
      GfshCommand gfshCommand = (GfshCommand) commandMarker;
      if (!gfshCommand.hasCacheSet()) {
        gfshCommand.setCache(cache);
      }
    }

    // inject the commandManager into the commands
    if (CommandManagerAware.class.isAssignableFrom(commandMarker.getClass())) {
      ((CommandManagerAware) commandMarker).setCommandManager(this);
    }
    commandMarkers.add(commandMarker);

    for (Method method : commandMarker.getClass().getMethods()) {
      ShellMethod shellMethod = method.getAnnotation(ShellMethod.class);
      ShellMethodAvailability availability = method.getAnnotation(ShellMethodAvailability.class);
      if (shellMethod == null && availability == null) {
        continue;
      }

      if (shellMethod != null) {
        helper.addCommand(shellMethod, method);
      }

      if (availability != null) {
        // Spring Shell 3.x: Pass method and target separately instead of MethodTarget
        helper.addAvailabilityIndicator(availability, method, commandMarker);
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
