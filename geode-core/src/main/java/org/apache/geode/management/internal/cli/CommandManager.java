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
import java.util.Iterator;
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

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.Disabled;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.commands.InternalGfshCommand;
import org.apache.geode.management.internal.cli.help.Helper;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.ClasspathScanLoadHelper;

/**
 *
 * this only takes care of loading all available command markers and converters from the application
 *
 * @since GemFire 7.0
 */
public class CommandManager {

  public static final String USER_CMD_PACKAGES_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + USER_COMMAND_PACKAGES;
  public static final String USER_CMD_PACKAGES_ENV_VARIABLE = "GEMFIRE_USER_COMMAND_PACKAGES";

  private final Helper helper = new Helper();

  private final List<Converter<?>> converters = new ArrayList<Converter<?>>();
  private final List<CommandMarker> commandMarkers = new ArrayList<>();

  private Properties cacheProperties;
  private LogWrapper logWrapper;
  private InternalCache cache;

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
  public CommandManager(final Properties cacheProperties, InternalCache cache) {
    if (cacheProperties != null) {
      this.cacheProperties = cacheProperties;
    }
    this.cache = cache;
    logWrapper = LogWrapper.getInstance(cache);
    loadCommands();
  }

  private static void raiseExceptionIfEmpty(Set<Class<?>> foundClasses, String errorFor)
      throws IllegalStateException {
    if (foundClasses == null || foundClasses.isEmpty()) {
      throw new IllegalStateException(
          "Required " + errorFor + " classes were not loaded. Check logs for errors.");
    }
  }

  private void loadUserCommands() {
    final Set<String> userCommandPackages = new HashSet<String>();

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
      Arrays.stream(source.split(",")).forEach(userCommandPackages::add);
    }

    // Load commands found in all of the packages
    for (String userCommandPackage : userCommandPackages) {
      try {
        Set<Class<?>> foundClasses = ClasspathScanLoadHelper.scanPackagesForClassesImplementing(
            CommandMarker.class, userCommandPackage, GfshCommand.class.getPackage().getName());
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
  }

  /**
   * Loads commands via {@link ServiceLoader} from {@link ClassPathLoader}.
   *
   * @since GemFire 8.1
   */
  private void loadPluginCommands() {
    ServiceLoader<CommandMarker> loader =
        ServiceLoader.load(CommandMarker.class, ClassPathLoader.getLatest().asClassLoader());
    Iterator<CommandMarker> iterator = loader.iterator();
    try {
      while (iterator.hasNext()) {
        try {
          add(iterator.next());
        } catch (Throwable t) {
          logWrapper.warning("Could not load plugin command: " + t.getMessage());
        }
      }
    } catch (Throwable th) {
      logWrapper.severe("Could not load plugin commands in the latest classLoader.", th);
    }
  }

  private void loadCommands() {
    loadUserCommands();

    loadPluginCommands();
    loadGeodeCommands();
    loadConverters();
  }

  private void loadConverters() {
    Set<Class<?>> foundClasses;
    // Converters
    try {
      foundClasses = ClasspathScanLoadHelper.scanPackagesForClassesImplementing(Converter.class,
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
      foundClasses = ClasspathScanLoadHelper.scanPackagesForClassesImplementing(Converter.class,
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

  private void loadGeodeCommands() {
    // CommandMarkers
    Set<Class<?>> foundClasses;
    try {
      // geode's commands
      foundClasses = ClasspathScanLoadHelper.scanPackagesForClassesImplementing(CommandMarker.class,
          GfshCommand.class.getPackage().getName(),
          InternalGfshCommand.class.getPackage().getName());

      for (Class<?> klass : foundClasses) {
        try {
          add((CommandMarker) klass.newInstance());
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
  private static List<Class> SHL_CONVERTERS_TOSKIP = new ArrayList();
  static {
    // skip springs SimpleFileConverter to use our own FilePathConverter
    SHL_CONVERTERS_TOSKIP.add(SimpleFileConverter.class);
    // skip spring's EnumConverter to use our own EnumConverter
    SHL_CONVERTERS_TOSKIP.add(EnumConverter.class);
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
  void add(Converter<?> converter) {
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
