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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.management.internal.cli.shell.MXBeanProvider.getDistributedSystemMXBean;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.management.MalformedObjectNameException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.process.ProcessLauncherContext;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;

/**
 * Encapsulates methods used by StartServerCommand and StartLocatorCommand and their associated
 * tests.
 *
 * @see StartLocatorCommand
 * @see StartServerCommand
 */
public class StartMemberUtils {
  public static final String GEODE_HOME = System.getenv("GEODE_HOME");

  private static final String JAVA_HOME = System.getProperty("java.home");
  static final int CMS_INITIAL_OCCUPANCY_FRACTION = 60;
  private static final ThreePhraseGenerator nameGenerator = new ThreePhraseGenerator();

  static final String EXTENSIONS_PATHNAME = IOUtils.appendToPath(GEODE_HOME, "extensions");
  static final String CORE_DEPENDENCIES_JAR_PATHNAME =
      IOUtils.appendToPath(GEODE_HOME, "lib", "geode-dependencies.jar");
  static final String GEODE_JAR_PATHNAME =
      IOUtils.appendToPath(GEODE_HOME, "lib", GemFireVersion.getGemFireJarFileName());
  static final long PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS = 5 * 1000;
  static final int INVALID_PID = -1;

  static ThreePhraseGenerator getNameGenerator() {
    return nameGenerator;
  }

  static void setPropertyIfNotNull(Properties properties, String key, Object value) {
    if (key != null && value != null) {
      properties.setProperty(key, value.toString());
    }
  }

  static String resolveWorkingDir(File userSpecifiedDir, File memberNameDir) {
    File workingDir =
        (userSpecifiedDir == null || userSpecifiedDir.equals(new File(""))) ? memberNameDir
            : userSpecifiedDir;
    String workingDirPath = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(workingDir);
    if (!workingDir.exists()) {
      if (!workingDir.mkdirs()) {
        throw new IllegalStateException(String.format(
            "Could not create directory %s. Please verify directory path or user permissions.",
            workingDirPath));
      }
    }
    return workingDirPath;
  }

  static void addGemFirePropertyFile(final List<String> commandLine,
      final File gemfirePropertiesFile) {
    if (gemfirePropertiesFile != null) {
      commandLine.add("-DgemfirePropertyFile=" + gemfirePropertiesFile.getAbsolutePath());
    }
  }

  static void addGemFireSecurityPropertyFile(final List<String> commandLine,
      final File gemfireSecurityPropertiesFile) {
    if (gemfireSecurityPropertiesFile != null) {
      commandLine
          .add("-DgemfireSecurityPropertyFile=" + gemfireSecurityPropertiesFile.getAbsolutePath());
    }
  }

  static void addGemFireSystemProperties(final List<String> commandLine,
      final Properties gemfireProperties) {
    for (final Object property : gemfireProperties.keySet()) {
      final String propertyName = property.toString();
      final String propertyValue = gemfireProperties.getProperty(propertyName);
      if (StringUtils.isNotBlank(propertyValue)) {
        commandLine.add(
            "-D" + DistributionConfig.GEMFIRE_PREFIX + "" + propertyName + "=" + propertyValue);
      }
    }
  }

  static void addJvmArgumentsAndOptions(final List<String> commandLine,
      final String[] jvmArgsOpts) {
    if (jvmArgsOpts != null) {
      commandLine.addAll(Arrays.asList(jvmArgsOpts));
    }
  }

  static void addInitialHeap(final List<String> commandLine, final String initialHeap) {
    if (StringUtils.isNotBlank(initialHeap)) {
      commandLine.add("-Xms" + initialHeap);
    }
  }

  static void addMaxHeap(final List<String> commandLine, final String maxHeap) {
    if (StringUtils.isNotBlank(maxHeap)) {
      commandLine.add("-Xmx" + maxHeap);

      String collectorKey = "-XX:+UseConcMarkSweepGC";
      if (!commandLine.contains(collectorKey)) {
        commandLine.add(collectorKey);
      }

      String occupancyFractionKey = "-XX:CMSInitiatingOccupancyFraction=";
      if (commandLine.stream().noneMatch(s -> s.contains(occupancyFractionKey))) {
        commandLine.add(occupancyFractionKey + CMS_INITIAL_OCCUPANCY_FRACTION);
      }
    }
  }

  static void addCurrentLocators(InternalGfshCommand gfshCommand, final List<String> commandLine,
      final Properties gemfireProperties) throws MalformedObjectNameException {
    if (StringUtils.isBlank(gemfireProperties.getProperty(LOCATORS))) {
      String currentLocators = getCurrentLocators(gfshCommand);
      if (StringUtils.isNotBlank(currentLocators)) {
        commandLine.add("-D".concat(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX)
            .concat(LOCATORS).concat("=").concat(currentLocators));
      }
    }
  }

  private static String getCurrentLocators(InternalGfshCommand gfshCommand)
      throws MalformedObjectNameException {
    String delimitedLocators = "";
    try {
      if (gfshCommand.isConnectedAndReady()) {
        final DistributedSystemMXBean dsMBeanProxy = getDistributedSystemMXBean();
        if (dsMBeanProxy != null) {
          final String[] locators = dsMBeanProxy.listLocators();
          if (locators != null && locators.length > 0) {
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < locators.length; i++) {
              if (i > 0) {
                sb.append(",");
              }
              sb.append(locators[i]);
            }
            delimitedLocators = sb.toString();
          }
        }
      }
    } catch (IOException e) { // thrown by getDistributedSystemMXBean
      // leave delimitedLocators = ""
      gfshCommand.getGfsh().logWarning("DistributedSystemMXBean is unavailable\n", e);
    }
    return delimitedLocators;
  }

  public static int readPid(final File pidFile) {
    assert pidFile != null : "The file from which to read the process ID (pid) cannot be null!";
    if (pidFile.isFile()) {
      BufferedReader fileReader = null;
      try {
        fileReader = new BufferedReader(new FileReader(pidFile));
        return Integer.parseInt(fileReader.readLine());
      } catch (IOException | NumberFormatException ignore) {
      } finally {
        IOUtils.close(fileReader);
      }
    }
    return INVALID_PID;
  }

  static String getJavaPath() {
    return new File(new File(JAVA_HOME, "bin"), "java").getPath();
  }

  static String getSystemClasspath() {
    return System.getProperty("java.class.path");
  }

  static String toClasspath(final boolean includeSystemClasspath, String[] jarFilePathnames,
      String... userClasspaths) {
    // gemfire jar must absolutely be the first JAR file on the CLASSPATH!!!
    StringBuilder classpath = new StringBuilder(getGemFireJarPath());

    userClasspaths = (userClasspaths != null ? userClasspaths : ArrayUtils.EMPTY_STRING_ARRAY);

    // Then, include user-specified classes on CLASSPATH to enable the user to override GemFire JAR
    // dependencies
    // with application-specific versions; this logic/block corresponds to classes/jar-files
    // specified with the
    // --classpath option to the 'start locator' and 'start server commands'; also this will
    // override any
    // System CLASSPATH environment variable setting, which is consistent with the Java platform
    // behavior...
    for (String userClasspath : userClasspaths) {
      if (StringUtils.isNotBlank(userClasspath)) {
        classpath.append((classpath.length() == 0) ? StringUtils.EMPTY : File.pathSeparator);
        classpath.append(userClasspath);
      }
    }

    // Now, include any System-specified CLASSPATH environment variable setting...
    if (includeSystemClasspath) {
      classpath.append(File.pathSeparator);
      classpath.append(getSystemClasspath());
    }

    jarFilePathnames =
        (jarFilePathnames != null ? jarFilePathnames : ArrayUtils.EMPTY_STRING_ARRAY);

    // And finally, include all GemFire dependencies on the CLASSPATH...
    for (String jarFilePathname : jarFilePathnames) {
      if (StringUtils.isNotBlank(jarFilePathname)) {
        classpath.append((classpath.length() == 0) ? StringUtils.EMPTY : File.pathSeparator);
        classpath.append(jarFilePathname);
      }
    }
    return classpath.toString();
  }

  static String getGemFireJarPath() {
    String classpath = getSystemClasspath();
    String gemfireJarPath = GEODE_JAR_PATHNAME;
    for (String classpathElement : classpath.split(File.pathSeparator)) {
      if (classpathElement.endsWith("geode-core-" + GemFireVersion.getGemFireVersion() + ".jar")) {
        gemfireJarPath = classpathElement;
        break;
      }
    }
    return gemfireJarPath;
  }
}
