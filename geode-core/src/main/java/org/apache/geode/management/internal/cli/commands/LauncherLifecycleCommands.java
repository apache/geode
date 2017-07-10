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

import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.distributed.ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATOR_WAIT_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.LOCK_MEMORY;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_PROTOCOL;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.LOCATOR_TERM_NAME;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.START_SERVER__PASSWORD;
import static org.apache.geode.management.internal.cli.shell.MXBeanProvider.getDistributedSystemMXBean;
import static org.apache.geode.management.internal.cli.util.HostUtils.getLocatorId;

import org.apache.commons.lang.ArrayUtils;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.ServerLauncher.ServerState;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ClusterConfigurationNotAvailableException;
import org.apache.geode.internal.process.ProcessLauncherContext;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessStreamReader.InputListener;
import org.apache.geode.internal.process.ProcessStreamReader.ReadingMode;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.signal.SignalEvent;
import org.apache.geode.internal.process.signal.SignalListener;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.domain.ConnectToLocatorResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.JmxOperationInvoker;
import org.apache.geode.management.internal.cli.util.CauseFinder;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.ConnectionEndpoint;
import org.apache.geode.management.internal.cli.util.HostUtils;
import org.apache.geode.management.internal.cli.util.ThreePhraseGenerator;
import org.apache.geode.management.internal.configuration.utils.ClusterConfigurationStatusRetriever;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.AuthenticationFailedException;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.management.MalformedObjectNameException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

/**
 * The LauncherLifecycleCommands class encapsulates all GemFire launcher commands for GemFire tools
 * (like starting GemFire Monitor (GFMon) and Visual Statistics Display (VSD)) as well external
 * tools (like jconsole).
 * <p>
 *
 * @see org.apache.geode.distributed.LocatorLauncher
 * @see org.apache.geode.distributed.ServerLauncher
 * @see GfshCommand
 * @see org.apache.geode.management.internal.cli.shell.Gfsh
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class LauncherLifecycleCommands implements GfshCommand {
  private static final String SERVER_TERM_NAME = "Server";

  private static final long PROCESS_STREAM_READER_JOIN_TIMEOUT_MILLIS = 30 * 1000;
  private static final long PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS = 5 * 1000;
  private static final long WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS = 30 * 1000;
  private static final long WAITING_FOR_PID_FILE_TO_CONTAIN_PID_TIMEOUT_MILLIS = 2 * 1000;

  protected static final int CMS_INITIAL_OCCUPANCY_FRACTION = 60;
  protected static final int INVALID_PID = -1;
  protected static final int MINIMUM_HEAP_FREE_RATIO = 10;

  protected static final String GEODE_HOME = System.getenv("GEODE_HOME");
  protected static final String JAVA_HOME = System.getProperty("java.home");

  // MUST CHANGE THIS TO REGEX SINCE VERSION CHANGES IN JAR NAME
  protected static final String GEODE_JAR_PATHNAME =
      IOUtils.appendToPath(GEODE_HOME, "lib", GemFireVersion.getGemFireJarFileName());

  protected static final String CORE_DEPENDENCIES_JAR_PATHNAME =
      IOUtils.appendToPath(GEODE_HOME, "lib", "geode-dependencies.jar");

  private final ThreePhraseGenerator nameGenerator;

  public LauncherLifecycleCommands() {
    nameGenerator = new ThreePhraseGenerator();
  }

  @CliCommand(value = CliStrings.START_LOCATOR, help = CliStrings.START_LOCATOR__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public Result startLocator(
      @CliOption(key = CliStrings.START_LOCATOR__MEMBER_NAME,
          help = CliStrings.START_LOCATOR__MEMBER_NAME__HELP) String memberName,
      @CliOption(key = CliStrings.START_LOCATOR__BIND_ADDRESS,
          help = CliStrings.START_LOCATOR__BIND_ADDRESS__HELP) final String bindAddress,
      @CliOption(key = CliStrings.START_LOCATOR__CLASSPATH,
          help = CliStrings.START_LOCATOR__CLASSPATH__HELP) final String classpath,
      @CliOption(key = CliStrings.START_LOCATOR__FORCE, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__FORCE__HELP) final Boolean force,
      @CliOption(key = CliStrings.GROUP, optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.START_LOCATOR__GROUP__HELP) final String group,
      @CliOption(key = CliStrings.START_LOCATOR__HOSTNAME_FOR_CLIENTS,
          help = CliStrings.START_LOCATOR__HOSTNAME_FOR_CLIENTS__HELP) final String hostnameForClients,
      @CliOption(key = ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS,
          help = CliStrings.START_LOCATOR__JMX_MANAGER_HOSTNAME_FOR_CLIENTS__HELP) final String jmxManagerHostnameForClients,
      @CliOption(key = CliStrings.START_LOCATOR__INCLUDE_SYSTEM_CLASSPATH,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CliStrings.START_LOCATOR__INCLUDE_SYSTEM_CLASSPATH__HELP) final Boolean includeSystemClasspath,
      @CliOption(key = CliStrings.START_LOCATOR__LOCATORS,
          optionContext = ConverterHint.LOCATOR_DISCOVERY_CONFIG,
          help = CliStrings.START_LOCATOR__LOCATORS__HELP) final String locators,
      @CliOption(key = CliStrings.START_LOCATOR__LOG_LEVEL, optionContext = ConverterHint.LOG_LEVEL,
          help = CliStrings.START_LOCATOR__LOG_LEVEL__HELP) final String logLevel,
      @CliOption(key = CliStrings.START_LOCATOR__MCAST_ADDRESS,
          help = CliStrings.START_LOCATOR__MCAST_ADDRESS__HELP) final String mcastBindAddress,
      @CliOption(key = CliStrings.START_LOCATOR__MCAST_PORT,
          help = CliStrings.START_LOCATOR__MCAST_PORT__HELP) final Integer mcastPort,
      @CliOption(key = CliStrings.START_LOCATOR__PORT,
          help = CliStrings.START_LOCATOR__PORT__HELP) final Integer port,
      @CliOption(key = CliStrings.START_LOCATOR__DIR,
          help = CliStrings.START_LOCATOR__DIR__HELP) String workingDirectory,
      @CliOption(key = CliStrings.START_LOCATOR__PROPERTIES,
          optionContext = ConverterHint.FILE_PATH,
          help = CliStrings.START_LOCATOR__PROPERTIES__HELP) String gemfirePropertiesPathname,
      @CliOption(key = CliStrings.START_LOCATOR__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATH,
          help = CliStrings.START_LOCATOR__SECURITY_PROPERTIES__HELP) String gemfireSecurityPropertiesPathname,
      @CliOption(key = CliStrings.START_LOCATOR__INITIALHEAP,
          help = CliStrings.START_LOCATOR__INITIALHEAP__HELP) final String initialHeap,
      @CliOption(key = CliStrings.START_LOCATOR__MAXHEAP,
          help = CliStrings.START_LOCATOR__MAXHEAP__HELP) final String maxHeap,
      @CliOption(key = CliStrings.START_LOCATOR__J, optionContext = GfshParser.J_OPTION_CONTEXT,
          help = CliStrings.START_LOCATOR__J__HELP) final String[] jvmArgsOpts,
      @CliOption(key = CliStrings.START_LOCATOR__CONNECT, unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__CONNECT__HELP) final boolean connect,
      @CliOption(key = CliStrings.START_LOCATOR__ENABLE__SHARED__CONFIGURATION,
          unspecifiedDefaultValue = "true", specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__ENABLE__SHARED__CONFIGURATION__HELP) final boolean enableSharedConfiguration,
      @CliOption(key = CliStrings.START_LOCATOR__LOAD__SHARED_CONFIGURATION__FROM__FILESYSTEM,
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_LOCATOR__LOAD__SHARED_CONFIGURATION__FROM__FILESYSTEM__HELP) final boolean loadSharedConfigurationFromDirectory,
      @CliOption(key = CliStrings.START_LOCATOR__CLUSTER__CONFIG__DIR, unspecifiedDefaultValue = "",
          help = CliStrings.START_LOCATOR__CLUSTER__CONFIG__DIR__HELP) final String clusterConfigDir,
      @CliOption(key = CliStrings.START_LOCATOR__HTTP_SERVICE_PORT,
          help = CliStrings.START_LOCATOR__HTTP_SERVICE_PORT__HELP) final Integer httpServicePort,
      @CliOption(key = CliStrings.START_LOCATOR__HTTP_SERVICE_BIND_ADDRESS,
          help = CliStrings.START_LOCATOR__HTTP_SERVICE_BIND_ADDRESS__HELP) final String httpServiceBindAddress) {
    try {
      if (StringUtils.isBlank(memberName)) {
        // when the user doesn't give us a name, we make one up!
        memberName = nameGenerator.generate('-');
      }

      workingDirectory = resolveWorkingDir(workingDirectory, memberName);

      gemfirePropertiesPathname = CliUtil.resolvePathname(gemfirePropertiesPathname);

      if (StringUtils.isNotBlank(gemfirePropertiesPathname)
          && !IOUtils.isExistingPathname(gemfirePropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, StringUtils.EMPTY,
                gemfirePropertiesPathname));
      }

      gemfireSecurityPropertiesPathname =
          CliUtil.resolvePathname(gemfireSecurityPropertiesPathname);

      if (StringUtils.isNotBlank(gemfireSecurityPropertiesPathname)
          && !IOUtils.isExistingPathname(gemfireSecurityPropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "Security ",
                gemfireSecurityPropertiesPathname));
      }

      File locatorPidFile = new File(workingDirectory, ProcessType.LOCATOR.getPidFileName());

      final int oldPid = readPid(locatorPidFile);

      Properties gemfireProperties = new Properties();

      setPropertyIfNotNull(gemfireProperties, GROUPS, group);
      setPropertyIfNotNull(gemfireProperties, LOCATORS, locators);
      setPropertyIfNotNull(gemfireProperties, LOG_LEVEL, logLevel);
      setPropertyIfNotNull(gemfireProperties, MCAST_ADDRESS, mcastBindAddress);
      setPropertyIfNotNull(gemfireProperties, MCAST_PORT, mcastPort);
      setPropertyIfNotNull(gemfireProperties, ENABLE_CLUSTER_CONFIGURATION,
          enableSharedConfiguration);
      setPropertyIfNotNull(gemfireProperties, LOAD_CLUSTER_CONFIGURATION_FROM_DIR,
          loadSharedConfigurationFromDirectory);
      setPropertyIfNotNull(gemfireProperties, CLUSTER_CONFIGURATION_DIR, clusterConfigDir);
      setPropertyIfNotNull(gemfireProperties, HTTP_SERVICE_PORT, httpServicePort);
      setPropertyIfNotNull(gemfireProperties, HTTP_SERVICE_BIND_ADDRESS, httpServiceBindAddress);
      setPropertyIfNotNull(gemfireProperties, JMX_MANAGER_HOSTNAME_FOR_CLIENTS,
          jmxManagerHostnameForClients);


      // read the OSProcess enable redirect system property here -- TODO: replace with new GFSH
      // argument
      final boolean redirectOutput =
          Boolean.getBoolean(OSProcess.ENABLE_OUTPUT_REDIRECTION_PROPERTY);
      LocatorLauncher.Builder locatorLauncherBuilder =
          new LocatorLauncher.Builder().setBindAddress(bindAddress).setForce(force).setPort(port)
              .setRedirectOutput(redirectOutput).setWorkingDirectory(workingDirectory);
      if (hostnameForClients != null) {
        locatorLauncherBuilder.setHostnameForClients(hostnameForClients);
      }
      if (memberName != null) {
        locatorLauncherBuilder.setMemberName(memberName);
      }
      LocatorLauncher locatorLauncher = locatorLauncherBuilder.build();

      String[] locatorCommandLine = createStartLocatorCommandLine(locatorLauncher,
          gemfirePropertiesPathname, gemfireSecurityPropertiesPathname, gemfireProperties,
          classpath, includeSystemClasspath, jvmArgsOpts, initialHeap, maxHeap);

      final Process locatorProcess = new ProcessBuilder(locatorCommandLine)
          .directory(new File(locatorLauncher.getWorkingDirectory())).start();

      locatorProcess.getInputStream().close();
      locatorProcess.getOutputStream().close();

      // fix TRAC bug #51967 by using NON_BLOCKING on Windows
      final ReadingMode readingMode =
          SystemUtils.isWindows() ? ReadingMode.NON_BLOCKING : ReadingMode.BLOCKING;

      final StringBuffer message = new StringBuffer(); // need thread-safe StringBuffer
      InputListener inputListener = new InputListener() {
        @Override
        public void notifyInputLine(String line) {
          message.append(line);
          if (readingMode == ReadingMode.BLOCKING) {
            message.append(StringUtils.LINE_SEPARATOR);
          }
        }
      };

      ProcessStreamReader stderrReader = new ProcessStreamReader.Builder(locatorProcess)
          .inputStream(locatorProcess.getErrorStream()).inputListener(inputListener)
          .readingMode(readingMode).continueReadingMillis(2 * 1000).build().start();

      LocatorState locatorState;

      String previousLocatorStatusMessage = null;

      LauncherSignalListener locatorSignalListener = new LauncherSignalListener();

      final boolean registeredLocatorSignalListener =
          getGfsh().getSignalHandler().registerListener(locatorSignalListener);

      try {
        getGfsh().logInfo(String.format(CliStrings.START_LOCATOR__RUN_MESSAGE,
            IOUtils.tryGetCanonicalPathElseGetAbsolutePath(
                new File(locatorLauncher.getWorkingDirectory()))),
            null);

        locatorState = LocatorState.fromDirectory(workingDirectory, memberName);
        do {
          if (locatorProcess.isAlive()) {
            Gfsh.print(".");

            synchronized (this) {
              TimeUnit.MILLISECONDS.timedWait(this, 500);
            }

            locatorState = LocatorState.fromDirectory(workingDirectory, memberName);

            String currentLocatorStatusMessage = locatorState.getStatusMessage();

            if (locatorState.isStartingOrNotResponding()
                && !(StringUtils.isBlank(currentLocatorStatusMessage)
                    || currentLocatorStatusMessage.equalsIgnoreCase(previousLocatorStatusMessage)
                    || currentLocatorStatusMessage.trim().toLowerCase().equals("null"))) {
              Gfsh.println();
              Gfsh.println(currentLocatorStatusMessage);
              previousLocatorStatusMessage = currentLocatorStatusMessage;
            }
          } else {
            final int exitValue = locatorProcess.exitValue();

            return ResultBuilder.createShellClientErrorResult(
                String.format(CliStrings.START_LOCATOR__PROCESS_TERMINATED_ABNORMALLY_ERROR_MESSAGE,
                    exitValue, locatorLauncher.getWorkingDirectory(), message.toString()));
          }
        } while (!(registeredLocatorSignalListener && locatorSignalListener.isSignaled())
            && locatorState.isStartingOrNotResponding());
      } finally {
        stderrReader.stopAsync(PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS); // stop will close
                                                                                 // ErrorStream
        getGfsh().getSignalHandler().unregisterListener(locatorSignalListener);
      }

      Gfsh.println();

      final boolean asyncStart =
          (registeredLocatorSignalListener && locatorSignalListener.isSignaled()
              && ServerState.isStartingNotRespondingOrNull(locatorState));

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();

      if (asyncStart) {
        infoResultData
            .addLine(String.format(CliStrings.ASYNC_PROCESS_LAUNCH_MESSAGE, LOCATOR_TERM_NAME));
      } else {
        infoResultData.addLine(locatorState.toString());

        String locatorHostName;
        InetAddress bindAddr = locatorLauncher.getBindAddress();
        if (bindAddr != null) {
          locatorHostName = bindAddr.getCanonicalHostName();
        } else {
          locatorHostName = StringUtils.defaultIfBlank(locatorLauncher.getHostnameForClients(),
              HostUtils.getLocalHost());
        }

        int locatorPort = Integer.parseInt(locatorState.getPort());

        // AUTO-CONNECT
        // If the connect succeeds add the connected message to the result,
        // Else, ask the user to use the "connect" command to connect to the Locator.
        if (shouldAutoConnect(connect)) {
          doAutoConnect(locatorHostName, locatorPort, gemfirePropertiesPathname,
              gemfireSecurityPropertiesPathname, infoResultData);
        }

        // Report on the state of the Shared Configuration service if enabled...
        if (enableSharedConfiguration) {
          infoResultData.addLine(
              ClusterConfigurationStatusRetriever.fromLocator(locatorHostName, locatorPort));
        }
      }

      return ResultBuilder.buildResult(infoResultData);
    } catch (IllegalArgumentException e) {
      String message = e.getMessage();
      if (message != null && message.matches(
          LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toLocalizedString(".+"))) {
        message =
            CliStrings.format(CliStrings.LAUNCHERLIFECYCLECOMMANDS__MSG__FAILED_TO_START_0_REASON_1,
                LOCATOR_TERM_NAME, message);
      }
      return ResultBuilder.createUserErrorResult(message);
    } catch (IllegalStateException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      String errorMessage = String.format(CliStrings.START_LOCATOR__GENERAL_ERROR_MESSAGE,
          StringUtils.defaultIfBlank(workingDirectory, memberName), getLocatorId(bindAddress, port),
          toString(t, getGfsh().getDebug()));
      getGfsh().logToFile(errorMessage, t);
      return ResultBuilder.createShellClientErrorResult(errorMessage);
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  private void setPropertyIfNotNull(Properties properties, String key, Object value) {
    if (key != null && value != null) {
      properties.setProperty(key, value.toString());
    }
  }

  protected String[] createStartLocatorCommandLine(final LocatorLauncher launcher,
      final String gemfirePropertiesPathname, final String gemfireSecurityPropertiesPathname,
      final Properties gemfireProperties, final String userClasspath,
      final Boolean includeSystemClasspath, final String[] jvmArgsOpts, final String initialHeap,
      final String maxHeap) throws MalformedObjectNameException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine
        .add(getLocatorClasspath(Boolean.TRUE.equals(includeSystemClasspath), userClasspath));

    addCurrentLocators(commandLine, gemfireProperties);
    addGemFirePropertyFile(commandLine, gemfirePropertiesPathname);
    addGemFireSecurityPropertyFile(commandLine, gemfireSecurityPropertiesPathname);
    addGemFireSystemProperties(commandLine, gemfireProperties);
    addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);
    addInitialHeap(commandLine, initialHeap);
    addMaxHeap(commandLine, maxHeap);

    commandLine.add(
        "-D".concat(AbstractLauncher.SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY.concat("=true")));
    commandLine.add("-Djava.awt.headless=true");
    commandLine.add(
        "-Dsun.rmi.dgc.server.gcInterval".concat("=").concat(Long.toString(Long.MAX_VALUE - 1)));
    commandLine.add(LocatorLauncher.class.getName());
    commandLine.add(LocatorLauncher.Command.START.getName());

    if (StringUtils.isNotBlank(launcher.getMemberName())) {
      commandLine.add(launcher.getMemberName());
    }

    if (launcher.getBindAddress() != null) {
      commandLine.add("--bind-address=" + launcher.getBindAddress().getCanonicalHostName());
    }

    if (launcher.isDebugging() || isDebugging()) {
      commandLine.add("--debug");
    }

    if (launcher.isForcing()) {
      commandLine.add("--force");
    }

    if (StringUtils.isNotBlank(launcher.getHostnameForClients())) {
      commandLine.add("--hostname-for-clients=" + launcher.getHostnameForClients());
    }

    if (launcher.getPort() != null) {
      commandLine.add("--port=" + launcher.getPort());
    }

    if (launcher.isRedirectingOutput()) {
      commandLine.add("--redirect-output");
    }

    return commandLine.toArray(new String[commandLine.size()]);
  }

  // TODO should we connect implicitly when in non-interactive, headless mode (e.g. gfsh -e "start
  // locator ...")?
  // With execute option (-e), there could be multiple commands which might presume that a prior
  // "start locator"
  // has formed the connection.
  private boolean shouldAutoConnect(final boolean connect) {
    return (connect && !(getGfsh() == null || isConnectedAndReady()));
  }

  private boolean doAutoConnect(final String locatorHostname, final int locatorPort,
      final String gemfirePropertiesPathname, final String gemfireSecurityPropertiesPathname,
      final InfoResultData infoResultData) {
    boolean connectSuccess = false;
    boolean jmxManagerAuthEnabled = false;
    boolean jmxManagerSslEnabled = false;

    Map<String, String> configurationProperties = loadConfigurationProperties(
        gemfireSecurityPropertiesPathname, loadConfigurationProperties(gemfirePropertiesPathname));
    Map<String, String> locatorConfigurationProperties = new HashMap<>(configurationProperties);

    String responseFailureMessage = null;

    for (int attempts = 0; (attempts < 10 && !connectSuccess); attempts++) {
      try {
        ConnectToLocatorResult connectToLocatorResult =
            ShellCommands.connectToLocator(locatorHostname, locatorPort,
                ShellCommands.getConnectLocatorTimeoutInMS() / 4, locatorConfigurationProperties);

        ConnectionEndpoint memberEndpoint = connectToLocatorResult.getMemberEndpoint();

        jmxManagerSslEnabled = connectToLocatorResult.isJmxManagerSslEnabled();

        if (!jmxManagerSslEnabled) {
          configurationProperties.clear();
        }

        getGfsh().setOperationInvoker(new JmxOperationInvoker(memberEndpoint.getHost(),
            memberEndpoint.getPort(), null, null, configurationProperties, null));

        String shellAndLogMessage = CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS,
            "JMX Manager " + memberEndpoint.toString(false));

        infoResultData.addLine("\n");
        infoResultData.addLine(shellAndLogMessage);
        getGfsh().logToFile(shellAndLogMessage, null);

        connectSuccess = true;
        responseFailureMessage = null;
      } catch (IllegalStateException unexpected) {
        if (CauseFinder.indexOfCause(unexpected, ClassCastException.class, false) != -1) {
          responseFailureMessage = "The Locator might require SSL Configuration.";
        }
      } catch (SecurityException ignore) {
        getGfsh().logToFile(ignore.getMessage(), ignore);
        jmxManagerAuthEnabled = true;
        break; // no need to continue after SecurityException
      } catch (AuthenticationFailedException ignore) {
        getGfsh().logToFile(ignore.getMessage(), ignore);
        jmxManagerAuthEnabled = true;
        break; // no need to continue after AuthenticationFailedException
      } catch (SSLException ignore) {
        if (ignore instanceof SSLHandshakeException) {
          // try to connect again without SSL since the SSL handshake failed implying a plain text
          // connection...
          locatorConfigurationProperties.clear();
        } else {
          // another type of SSL error occurred (possibly a configuration issue); pass the buck...
          getGfsh().logToFile(ignore.getMessage(), ignore);
          responseFailureMessage = "Check your SSL configuration and try again.";
          break;
        }
      } catch (Exception ignore) {
        getGfsh().logToFile(ignore.getMessage(), ignore);
        responseFailureMessage = "Failed to connect; unknown cause: " + ignore.getMessage();
      }
    }

    if (!connectSuccess) {
      doOnConnectionFailure(locatorHostname, locatorPort, jmxManagerAuthEnabled,
          jmxManagerSslEnabled, infoResultData);
    }

    if (StringUtils.isNotBlank(responseFailureMessage)) {
      infoResultData.addLine("\n");
      infoResultData.addLine(responseFailureMessage);
    }

    return connectSuccess;
  }

  private void doOnConnectionFailure(final String locatorHostName, final int locatorPort,
      final boolean jmxManagerAuthEnabled, final boolean jmxManagerSslEnabled,
      final InfoResultData infoResultData) {

    infoResultData.addLine("\n");
    infoResultData.addLine(CliStrings.format(CliStrings.START_LOCATOR__USE__0__TO__CONNECT,
        new CommandStringBuilder(CliStrings.CONNECT)
            .addOption(CliStrings.CONNECT__LOCATOR, locatorHostName + "[" + locatorPort + "]")
            .toString()));

    StringBuilder message = new StringBuilder();

    if (jmxManagerAuthEnabled) {
      message.append("Authentication");
    }
    if (jmxManagerSslEnabled) {
      message.append(jmxManagerAuthEnabled ? " and " : StringUtils.EMPTY)
          .append("SSL configuration");
    }
    if (jmxManagerAuthEnabled || jmxManagerSslEnabled) {
      message.append(" required to connect to the Manager.");
      infoResultData.addLine("\n");
      infoResultData.addLine(message.toString());
    }
  }

  private Map<String, String> loadConfigurationProperties(
      final String configurationPropertiesPathname) {
    return loadConfigurationProperties(configurationPropertiesPathname, null);
  }

  private Map<String, String> loadConfigurationProperties(
      final String configurationPropertiesPathname, Map<String, String> configurationProperties) {
    configurationProperties =
        (configurationProperties != null ? configurationProperties : new HashMap<>());

    if (IOUtils.isExistingPathname(configurationPropertiesPathname)) {
      try {
        configurationProperties.putAll(ShellCommands
            .loadPropertiesFromURL(new File(configurationPropertiesPathname).toURI().toURL()));
      } catch (MalformedURLException ignore) {
        LogWrapper.getInstance()
            .warning(String.format(
                "Failed to load GemFire configuration properties from pathname (%1$s)!",
                configurationPropertiesPathname), ignore);
      }
    }

    return configurationProperties;
  }


  // TODO re-evaluate whether a MalformedObjectNameException should be thrown here; just because we
  // were not able to find
  // the "current" Locators in order to conveniently add the new member to the GemFire cluster does
  // not mean we should
  // throw an Exception!
  protected void addCurrentLocators(final List<String> commandLine,
      final Properties gemfireProperties) throws MalformedObjectNameException {
    if (StringUtils.isBlank(gemfireProperties.getProperty(LOCATORS))) {
      String currentLocators = getCurrentLocators();

      if (StringUtils.isNotBlank(currentLocators)) {
        commandLine.add("-D".concat(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX)
            .concat(LOCATORS).concat("=").concat(currentLocators));
      }
    }
  }

  protected void addGemFirePropertyFile(final List<String> commandLine,
      final String gemfirePropertiesPathname) {
    if (StringUtils.isNotBlank(gemfirePropertiesPathname)) {
      commandLine.add("-DgemfirePropertyFile=" + gemfirePropertiesPathname);
    }
  }

  protected void addGemFireSecurityPropertyFile(final List<String> commandLine,
      final String gemfireSecurityPropertiesPathname) {
    if (StringUtils.isNotBlank(gemfireSecurityPropertiesPathname)) {
      commandLine.add("-DgemfireSecurityPropertyFile=" + gemfireSecurityPropertiesPathname);
    }
  }

  protected void addGemFireSystemProperties(final List<String> commandLine,
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

  protected void addInitialHeap(final List<String> commandLine, final String initialHeap) {
    if (StringUtils.isNotBlank(initialHeap)) {
      commandLine.add("-Xms" + initialHeap);
    }
  }

  protected void addJvmArgumentsAndOptions(final List<String> commandLine,
      final String[] jvmArgsOpts) {
    if (jvmArgsOpts != null) {
      commandLine.addAll(Arrays.asList(jvmArgsOpts));
    }
  }

  // Fix for Bug #47192 - "Causing the GemFire member (JVM process) to exit on OutOfMemoryErrors"
  protected void addJvmOptionsForOutOfMemoryErrors(final List<String> commandLine) {
    if (SystemUtils.isHotSpotVM()) {
      if (SystemUtils.isWindows()) {
        // ProcessBuilder "on Windows" needs every word (space separated) to be
        // a different element in the array/list. See #47312. Need to study why!
        commandLine.add("-XX:OnOutOfMemoryError=taskkill /F /PID %p");
      } else { // All other platforms (Linux, Mac OS X, UNIX, etc)
        commandLine.add("-XX:OnOutOfMemoryError=kill -KILL %p");
      }
    } else if (SystemUtils.isJ9VM()) {
      // NOTE IBM states the following IBM J9 JVM command-line option/switch has side-effects on
      // "performance",
      // as noted in the reference documentation...
      // http://publib.boulder.ibm.com/infocenter/javasdk/v6r0/index.jsp?topic=/com.ibm.java.doc.diagnostics.60/diag/appendixes/cmdline/commands_jvm.html
      commandLine.add("-Xcheck:memory");
    } else if (SystemUtils.isJRockitVM()) {
      // NOTE the following Oracle JRockit JVM documentation was referenced to identify the
      // appropriate JVM option to
      // set when handling OutOfMemoryErrors.
      // http://docs.oracle.com/cd/E13150_01/jrockit_jvm/jrockit/jrdocs/refman/optionXX.html
      commandLine.add("-XXexitOnOutOfMemory");
    }
  }

  protected void addMaxHeap(final List<String> commandLine, final String maxHeap) {
    if (StringUtils.isNotBlank(maxHeap)) {
      commandLine.add("-Xmx" + maxHeap);
      commandLine.add("-XX:+UseConcMarkSweepGC");
      commandLine.add("-XX:CMSInitiatingOccupancyFraction=" + CMS_INITIAL_OCCUPANCY_FRACTION);
      // commandLine.add("-XX:MinHeapFreeRatio=" + MINIMUM_HEAP_FREE_RATIO);
    }
  }

  protected int readPid(final File pidFile) {
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

  @Deprecated
  protected String getClasspath(final String userClasspath) {
    String classpath = getSystemClasspath();

    if (StringUtils.isNotBlank(userClasspath)) {
      classpath += (File.pathSeparator + userClasspath);
    }

    return classpath;
  }

  protected String getLocatorClasspath(final boolean includeSystemClasspath,
      final String userClasspath) {
    return toClasspath(includeSystemClasspath, new String[] {CORE_DEPENDENCIES_JAR_PATHNAME},
        userClasspath);
  }

  protected String getServerClasspath(final boolean includeSystemClasspath,
      final String userClasspath) {
    List<String> jarFilePathnames = new ArrayList<>();

    jarFilePathnames.add(CORE_DEPENDENCIES_JAR_PATHNAME);

    return toClasspath(includeSystemClasspath,
        jarFilePathnames.toArray(new String[jarFilePathnames.size()]), userClasspath);
  }

  protected String getSystemClasspath() {
    return System.getProperty("java.class.path");
  }

  String toClasspath(final boolean includeSystemClasspath, String[] jarFilePathnames,
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

  protected String getGemFireJarPath() {
    String classpath = getSystemClasspath();
    String gemfireJarPath = GEODE_JAR_PATHNAME;

    for (String classpathElement : classpath.split(File.pathSeparator)) {
      // MUST CHANGE THIS TO REGEX SINCE VERSION CHANGES IN JAR NAME
      if (classpathElement.endsWith("gemfire-core-8.2.0.0-SNAPSHOT.jar")) {
        gemfireJarPath = classpathElement;
        break;
      }
    }

    return gemfireJarPath;
  }

  protected String getJavaPath() {
    return new File(new File(JAVA_HOME, "bin"), "java").getPath();
  }

  @CliCommand(value = CliStrings.START_SERVER, help = CliStrings.START_SERVER__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public Result startServer(
      @CliOption(key = CliStrings.START_SERVER__NAME,
          help = CliStrings.START_SERVER__NAME__HELP) String memberName,
      @CliOption(key = CliStrings.START_SERVER__ASSIGN_BUCKETS, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__ASSIGN_BUCKETS__HELP) final Boolean assignBuckets,
      @CliOption(key = CliStrings.START_SERVER__BIND_ADDRESS,
          help = CliStrings.START_SERVER__BIND_ADDRESS__HELP) final String bindAddress,
      @CliOption(key = CliStrings.START_SERVER__CACHE_XML_FILE,
          optionContext = ConverterHint.FILE_PATH,
          help = CliStrings.START_SERVER__CACHE_XML_FILE__HELP) String cacheXmlPathname,
      @CliOption(key = CliStrings.START_SERVER__CLASSPATH,
          /* optionContext = ConverterHint.FILE_PATH, // there's an issue with TAB here */
          help = CliStrings.START_SERVER__CLASSPATH__HELP) final String classpath,
      @CliOption(key = CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE,
          help = CliStrings.START_SERVER__CRITICAL__HEAP__HELP) final Float criticalHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE,
          help = CliStrings.START_SERVER__CRITICAL_OFF_HEAP__HELP) final Float criticalOffHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__DIR,
          help = CliStrings.START_SERVER__DIR__HELP) String workingDirectory,
      @CliOption(key = CliStrings.START_SERVER__DISABLE_DEFAULT_SERVER,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__DISABLE_DEFAULT_SERVER__HELP) final Boolean disableDefaultServer,
      @CliOption(key = CliStrings.START_SERVER__DISABLE_EXIT_WHEN_OUT_OF_MEMORY,
          unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__DISABLE_EXIT_WHEN_OUT_OF_MEMORY_HELP) final Boolean disableExitWhenOutOfMemory,
      @CliOption(key = CliStrings.START_SERVER__ENABLE_TIME_STATISTICS,
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__ENABLE_TIME_STATISTICS__HELP) final Boolean enableTimeStatistics,
      @CliOption(key = CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE,
          help = CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE__HELP) final Float evictionHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE,
          help = CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE__HELP) final Float evictionOffHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__FORCE, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__FORCE__HELP) final Boolean force,
      @CliOption(key = CliStrings.GROUP, optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.START_SERVER__GROUP__HELP) final String group,
      @CliOption(key = CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS,
          help = CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS__HELP) final String hostNameForClients,
      @CliOption(key = ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS,
          help = CliStrings.START_SERVER__JMX_MANAGER_HOSTNAME_FOR_CLIENTS__HELP) final String jmxManagerHostnameForClients,
      @CliOption(key = CliStrings.START_SERVER__INCLUDE_SYSTEM_CLASSPATH,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false",
          help = CliStrings.START_SERVER__INCLUDE_SYSTEM_CLASSPATH__HELP) final Boolean includeSystemClasspath,
      @CliOption(key = CliStrings.START_SERVER__INITIAL_HEAP,
          help = CliStrings.START_SERVER__INITIAL_HEAP__HELP) final String initialHeap,
      @CliOption(key = CliStrings.START_SERVER__J, optionContext = GfshParser.J_OPTION_CONTEXT,
          help = CliStrings.START_SERVER__J__HELP) final String[] jvmArgsOpts,
      @CliOption(key = CliStrings.START_SERVER__LOCATORS,
          optionContext = ConverterHint.LOCATOR_DISCOVERY_CONFIG,
          help = CliStrings.START_SERVER__LOCATORS__HELP) final String locators,
      @CliOption(key = CliStrings.START_SERVER__LOCATOR_WAIT_TIME,
          help = CliStrings.START_SERVER__LOCATOR_WAIT_TIME_HELP) final Integer locatorWaitTime,
      @CliOption(key = CliStrings.START_SERVER__LOCK_MEMORY, specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__LOCK_MEMORY__HELP) final Boolean lockMemory,
      @CliOption(key = CliStrings.START_SERVER__LOG_LEVEL, optionContext = ConverterHint.LOG_LEVEL,
          help = CliStrings.START_SERVER__LOG_LEVEL__HELP) final String logLevel,
      @CliOption(key = CliStrings.START_SERVER__MAX__CONNECTIONS,
          help = CliStrings.START_SERVER__MAX__CONNECTIONS__HELP) final Integer maxConnections,
      @CliOption(key = CliStrings.START_SERVER__MAXHEAP,
          help = CliStrings.START_SERVER__MAXHEAP__HELP) final String maxHeap,
      @CliOption(key = CliStrings.START_SERVER__MAX__MESSAGE__COUNT,
          help = CliStrings.START_SERVER__MAX__MESSAGE__COUNT__HELP) final Integer maxMessageCount,
      @CliOption(key = CliStrings.START_SERVER__MAX__THREADS,
          help = CliStrings.START_SERVER__MAX__THREADS__HELP) final Integer maxThreads,
      @CliOption(key = CliStrings.START_SERVER__MCAST_ADDRESS,
          help = CliStrings.START_SERVER__MCAST_ADDRESS__HELP) final String mcastBindAddress,
      @CliOption(key = CliStrings.START_SERVER__MCAST_PORT,
          help = CliStrings.START_SERVER__MCAST_PORT__HELP) final Integer mcastPort,
      @CliOption(key = CliStrings.START_SERVER__MEMCACHED_PORT,
          help = CliStrings.START_SERVER__MEMCACHED_PORT__HELP) final Integer memcachedPort,
      @CliOption(key = CliStrings.START_SERVER__MEMCACHED_PROTOCOL,
          help = CliStrings.START_SERVER__MEMCACHED_PROTOCOL__HELP) final String memcachedProtocol,
      @CliOption(key = CliStrings.START_SERVER__MEMCACHED_BIND_ADDRESS,
          help = CliStrings.START_SERVER__MEMCACHED_BIND_ADDRESS__HELP) final String memcachedBindAddress,
      @CliOption(key = CliStrings.START_SERVER__REDIS_PORT,
          help = CliStrings.START_SERVER__REDIS_PORT__HELP) final Integer redisPort,
      @CliOption(key = CliStrings.START_SERVER__REDIS_BIND_ADDRESS,
          help = CliStrings.START_SERVER__REDIS_BIND_ADDRESS__HELP) final String redisBindAddress,
      @CliOption(key = CliStrings.START_SERVER__REDIS_PASSWORD,
          help = CliStrings.START_SERVER__REDIS_PASSWORD__HELP) final String redisPassword,
      @CliOption(key = CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE,
          help = CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE__HELP) final Integer messageTimeToLive,
      @CliOption(key = CliStrings.START_SERVER__OFF_HEAP_MEMORY_SIZE,
          help = CliStrings.START_SERVER__OFF_HEAP_MEMORY_SIZE__HELP) final String offHeapMemorySize,
      @CliOption(key = CliStrings.START_SERVER__PROPERTIES, optionContext = ConverterHint.FILE_PATH,
          help = CliStrings.START_SERVER__PROPERTIES__HELP) String gemfirePropertiesPathname,
      @CliOption(key = CliStrings.START_SERVER__REBALANCE, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__REBALANCE__HELP) final Boolean rebalance,
      @CliOption(key = CliStrings.START_SERVER__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATH,
          help = CliStrings.START_SERVER__SECURITY_PROPERTIES__HELP) String gemfireSecurityPropertiesPathname,
      @CliOption(key = CliStrings.START_SERVER__SERVER_BIND_ADDRESS,
          unspecifiedDefaultValue = CacheServer.DEFAULT_BIND_ADDRESS,
          help = CliStrings.START_SERVER__SERVER_BIND_ADDRESS__HELP) final String serverBindAddress,
      @CliOption(key = CliStrings.START_SERVER__SERVER_PORT,
          unspecifiedDefaultValue = ("" + CacheServer.DEFAULT_PORT),
          help = CliStrings.START_SERVER__SERVER_PORT__HELP) final Integer serverPort,
      @CliOption(key = CliStrings.START_SERVER__SOCKET__BUFFER__SIZE,
          help = CliStrings.START_SERVER__SOCKET__BUFFER__SIZE__HELP) final Integer socketBufferSize,
      @CliOption(key = CliStrings.START_SERVER__SPRING_XML_LOCATION,
          help = CliStrings.START_SERVER__SPRING_XML_LOCATION_HELP) final String springXmlLocation,
      @CliOption(key = CliStrings.START_SERVER__STATISTIC_ARCHIVE_FILE,
          help = CliStrings.START_SERVER__STATISTIC_ARCHIVE_FILE__HELP) final String statisticsArchivePathname,
      @CliOption(key = CliStrings.START_SERVER__USE_CLUSTER_CONFIGURATION,
          unspecifiedDefaultValue = "true", specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__USE_CLUSTER_CONFIGURATION__HELP) final Boolean requestSharedConfiguration,
      @CliOption(key = CliStrings.START_SERVER__REST_API, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__REST_API__HELP) final Boolean startRestApi,
      @CliOption(key = CliStrings.START_SERVER__HTTP_SERVICE_PORT, unspecifiedDefaultValue = "",
          help = CliStrings.START_SERVER__HTTP_SERVICE_PORT__HELP) final String httpServicePort,
      @CliOption(key = CliStrings.START_SERVER__HTTP_SERVICE_BIND_ADDRESS,
          unspecifiedDefaultValue = "",
          help = CliStrings.START_SERVER__HTTP_SERVICE_BIND_ADDRESS__HELP) final String httpServiceBindAddress,
      @CliOption(key = CliStrings.START_SERVER__USERNAME, unspecifiedDefaultValue = "",
          help = CliStrings.START_SERVER__USERNAME__HELP) final String userName,
      @CliOption(key = START_SERVER__PASSWORD, unspecifiedDefaultValue = "",
          help = CliStrings.START_SERVER__PASSWORD__HELP) String passwordToUse)
  // NOTICE: keep the parameters in alphabetical order based on their CliStrings.START_SERVER_* text
  {
    try {
      if (StringUtils.isBlank(memberName)) {
        // when the user doesn't give us a name, we make one up!
        memberName = nameGenerator.generate('-');
      }

      // prompt for password is username is specified in the command
      if (StringUtils.isNotBlank(userName)) {
        if (StringUtils.isBlank(passwordToUse)) {
          passwordToUse = getGfsh().readPassword(START_SERVER__PASSWORD + ": ");
        }
        if (StringUtils.isBlank(passwordToUse)) {
          return ResultBuilder.createConnectionErrorResult(
              CliStrings.START_SERVER__MSG__PASSWORD_MUST_BE_SPECIFIED);
        }
      }

      workingDirectory = resolveWorkingDir(workingDirectory, memberName);

      cacheXmlPathname = CliUtil.resolvePathname(cacheXmlPathname);

      if (StringUtils.isNotBlank(cacheXmlPathname)
          && !IOUtils.isExistingPathname(cacheXmlPathname)) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.CACHE_XML_NOT_FOUND_MESSAGE, cacheXmlPathname));
      }

      gemfirePropertiesPathname = CliUtil.resolvePathname(gemfirePropertiesPathname);

      if (StringUtils.isNotBlank(gemfirePropertiesPathname)
          && !IOUtils.isExistingPathname(gemfirePropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, StringUtils.EMPTY,
                gemfirePropertiesPathname));
      }

      gemfireSecurityPropertiesPathname =
          CliUtil.resolvePathname(gemfireSecurityPropertiesPathname);

      if (StringUtils.isNotBlank(gemfireSecurityPropertiesPathname)
          && !IOUtils.isExistingPathname(gemfireSecurityPropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "Security ",
                gemfireSecurityPropertiesPathname));
      }

      File serverPidFile = new File(workingDirectory, ProcessType.SERVER.getPidFileName());

      final int oldPid = readPid(serverPidFile);

      Properties gemfireProperties = new Properties();

      setPropertyIfNotNull(gemfireProperties, BIND_ADDRESS, bindAddress);
      setPropertyIfNotNull(gemfireProperties, CACHE_XML_FILE, cacheXmlPathname);
      setPropertyIfNotNull(gemfireProperties, ENABLE_TIME_STATISTICS, enableTimeStatistics);
      setPropertyIfNotNull(gemfireProperties, GROUPS, group);
      setPropertyIfNotNull(gemfireProperties, JMX_MANAGER_HOSTNAME_FOR_CLIENTS,
          jmxManagerHostnameForClients);
      setPropertyIfNotNull(gemfireProperties, LOCATORS, locators);
      setPropertyIfNotNull(gemfireProperties, LOCATOR_WAIT_TIME, locatorWaitTime);
      setPropertyIfNotNull(gemfireProperties, LOG_LEVEL, logLevel);
      setPropertyIfNotNull(gemfireProperties, MCAST_ADDRESS, mcastBindAddress);
      setPropertyIfNotNull(gemfireProperties, MCAST_PORT, mcastPort);
      setPropertyIfNotNull(gemfireProperties, MEMCACHED_PORT, memcachedPort);
      setPropertyIfNotNull(gemfireProperties, MEMCACHED_PROTOCOL, memcachedProtocol);
      setPropertyIfNotNull(gemfireProperties, MEMCACHED_BIND_ADDRESS, memcachedBindAddress);
      setPropertyIfNotNull(gemfireProperties, REDIS_PORT, redisPort);
      setPropertyIfNotNull(gemfireProperties, REDIS_BIND_ADDRESS, redisBindAddress);
      setPropertyIfNotNull(gemfireProperties, REDIS_PASSWORD, redisPassword);
      setPropertyIfNotNull(gemfireProperties, STATISTIC_ARCHIVE_FILE, statisticsArchivePathname);
      setPropertyIfNotNull(gemfireProperties, USE_CLUSTER_CONFIGURATION,
          requestSharedConfiguration);
      setPropertyIfNotNull(gemfireProperties, LOCK_MEMORY, lockMemory);
      setPropertyIfNotNull(gemfireProperties, OFF_HEAP_MEMORY_SIZE, offHeapMemorySize);
      setPropertyIfNotNull(gemfireProperties, START_DEV_REST_API, startRestApi);
      setPropertyIfNotNull(gemfireProperties, HTTP_SERVICE_PORT, httpServicePort);
      setPropertyIfNotNull(gemfireProperties, HTTP_SERVICE_BIND_ADDRESS, httpServiceBindAddress);
      // if username is specified in the command line, it will overwrite what's set in the
      // properties file
      if (StringUtils.isNotBlank(userName)) {
        gemfireProperties.setProperty(ResourceConstants.USER_NAME, userName);
        gemfireProperties.setProperty(ResourceConstants.PASSWORD, passwordToUse);
      }


      // read the OSProcess enable redirect system property here -- TODO: replace with new GFSH
      // argument
      final boolean redirectOutput =
          Boolean.getBoolean(OSProcess.ENABLE_OUTPUT_REDIRECTION_PROPERTY);

      ServerLauncher.Builder serverLauncherBuilder = new ServerLauncher.Builder()
          .setAssignBuckets(assignBuckets).setDisableDefaultServer(disableDefaultServer)
          .setForce(force).setRebalance(rebalance).setRedirectOutput(redirectOutput)
          .setServerBindAddress(serverBindAddress).setServerPort(serverPort)
          .setSpringXmlLocation(springXmlLocation).setWorkingDirectory(workingDirectory)
          .setCriticalHeapPercentage(criticalHeapPercentage)
          .setEvictionHeapPercentage(evictionHeapPercentage)
          .setCriticalOffHeapPercentage(criticalOffHeapPercentage)
          .setEvictionOffHeapPercentage(evictionOffHeapPercentage).setMaxConnections(maxConnections)
          .setMaxMessageCount(maxMessageCount).setMaxThreads(maxThreads)
          .setMessageTimeToLive(messageTimeToLive).setSocketBufferSize(socketBufferSize);
      if (hostNameForClients != null) {
        serverLauncherBuilder.setHostNameForClients(hostNameForClients);
      }
      if (memberName != null) {
        serverLauncherBuilder.setMemberName(memberName);
      }
      ServerLauncher serverLauncher = serverLauncherBuilder.build();

      String[] serverCommandLine = createStartServerCommandLine(serverLauncher,
          gemfirePropertiesPathname, gemfireSecurityPropertiesPathname, gemfireProperties,
          classpath, includeSystemClasspath, jvmArgsOpts, disableExitWhenOutOfMemory, initialHeap,
          maxHeap);

      if (getGfsh().getDebug()) {
        getGfsh().logInfo(StringUtils.join(serverCommandLine, StringUtils.SPACE), null);
      }

      Process serverProcess = new ProcessBuilder(serverCommandLine)
          .directory(new File(serverLauncher.getWorkingDirectory())).start();

      serverProcess.getInputStream().close();
      serverProcess.getOutputStream().close();

      // fix TRAC bug #51967 by using NON_BLOCKING on Windows
      final ReadingMode readingMode =
          SystemUtils.isWindows() ? ReadingMode.NON_BLOCKING : ReadingMode.BLOCKING;

      final StringBuffer message = new StringBuffer(); // need thread-safe StringBuffer
      InputListener inputListener = new InputListener() {
        @Override
        public void notifyInputLine(String line) {
          message.append(line);
          if (readingMode == ReadingMode.BLOCKING) {
            message.append(StringUtils.LINE_SEPARATOR);
          }
        }
      };

      ProcessStreamReader stderrReader = new ProcessStreamReader.Builder(serverProcess)
          .inputStream(serverProcess.getErrorStream()).inputListener(inputListener)
          .readingMode(readingMode).continueReadingMillis(2 * 1000).build().start();

      ServerState serverState;

      String previousServerStatusMessage = null;

      LauncherSignalListener serverSignalListener = new LauncherSignalListener();

      final boolean registeredServerSignalListener =
          getGfsh().getSignalHandler().registerListener(serverSignalListener);

      try {
        getGfsh().logInfo(String.format(CliStrings.START_SERVER__RUN_MESSAGE,
            IOUtils.tryGetCanonicalPathElseGetAbsolutePath(
                new File(serverLauncher.getWorkingDirectory()))),
            null);

        serverState = ServerState.fromDirectory(workingDirectory, memberName);
        do {
          if (serverProcess.isAlive()) {
            Gfsh.print(".");

            synchronized (this) {
              TimeUnit.MILLISECONDS.timedWait(this, 500);
            }

            serverState = ServerState.fromDirectory(workingDirectory, memberName);

            String currentServerStatusMessage = serverState.getStatusMessage();

            if (serverState.isStartingOrNotResponding()
                && !(StringUtils.isBlank(currentServerStatusMessage)
                    || currentServerStatusMessage.equalsIgnoreCase(previousServerStatusMessage)
                    || currentServerStatusMessage.trim().toLowerCase().equals("null"))) {
              Gfsh.println();
              Gfsh.println(currentServerStatusMessage);
              previousServerStatusMessage = currentServerStatusMessage;
            }
          } else {
            final int exitValue = serverProcess.exitValue();

            return ResultBuilder.createShellClientErrorResult(
                String.format(CliStrings.START_SERVER__PROCESS_TERMINATED_ABNORMALLY_ERROR_MESSAGE,
                    exitValue, serverLauncher.getWorkingDirectory(), message.toString()));

          }
        } while (!(registeredServerSignalListener && serverSignalListener.isSignaled())
            && serverState.isStartingOrNotResponding());
      } finally {
        stderrReader.stopAsync(PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS); // stop will close
                                                                                 // ErrorStream
        getGfsh().getSignalHandler().unregisterListener(serverSignalListener);
      }

      Gfsh.println();

      final boolean asyncStart = ServerState.isStartingNotRespondingOrNull(serverState);

      if (asyncStart) { // async start
        Gfsh.print(String.format(CliStrings.ASYNC_PROCESS_LAUNCH_MESSAGE, SERVER_TERM_NAME));
        return ResultBuilder.createInfoResult("");
      } else {
        return ResultBuilder.createInfoResult(serverState.toString());
      }
    } catch (IllegalArgumentException e) {
      String message = e.getMessage();
      if (message != null && message.matches(
          LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toLocalizedString(".+"))) {
        message =
            CliStrings.format(CliStrings.LAUNCHERLIFECYCLECOMMANDS__MSG__FAILED_TO_START_0_REASON_1,
                SERVER_TERM_NAME, message);
      }
      return ResultBuilder.createUserErrorResult(message);
    } catch (IllegalStateException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (ClusterConfigurationNotAvailableException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(
          CliStrings.START_SERVER__GENERAL_ERROR_MESSAGE, toString(t, getGfsh().getDebug())));
    }
  }

  protected String[] createStartServerCommandLine(final ServerLauncher launcher,
      final String gemfirePropertiesPathname, final String gemfireSecurityPropertiesPathname,
      final Properties gemfireProperties, final String userClasspath,
      final Boolean includeSystemClasspath, final String[] jvmArgsOpts,
      final Boolean disableExitWhenOutOfMemory, final String initialHeap, final String maxHeap)
      throws MalformedObjectNameException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine.add(getServerClasspath(Boolean.TRUE.equals(includeSystemClasspath), userClasspath));

    addCurrentLocators(commandLine, gemfireProperties);
    addGemFirePropertyFile(commandLine, gemfirePropertiesPathname);
    addGemFireSecurityPropertyFile(commandLine, gemfireSecurityPropertiesPathname);
    addGemFireSystemProperties(commandLine, gemfireProperties);
    addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);

    // NOTE asserting not equal to true rather than equal to false handles the null case and ensures
    // the user
    // explicitly specified the command-line option in order to disable JVM memory checks.
    if (!Boolean.TRUE.equals(disableExitWhenOutOfMemory)) {
      addJvmOptionsForOutOfMemoryErrors(commandLine);
    }

    addInitialHeap(commandLine, initialHeap);
    addMaxHeap(commandLine, maxHeap);

    commandLine.add(
        "-D".concat(AbstractLauncher.SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY.concat("=true")));
    commandLine.add("-Djava.awt.headless=true");
    commandLine.add(
        "-Dsun.rmi.dgc.server.gcInterval".concat("=").concat(Long.toString(Long.MAX_VALUE - 1)));

    commandLine.add(ServerLauncher.class.getName());
    commandLine.add(ServerLauncher.Command.START.getName());

    if (StringUtils.isNotBlank(launcher.getMemberName())) {
      commandLine.add(launcher.getMemberName());
    }

    if (launcher.isAssignBuckets()) {
      commandLine.add("--assign-buckets");
    }

    if (launcher.isDebugging() || isDebugging()) {
      commandLine.add("--debug");
    }

    if (launcher.isDisableDefaultServer()) {
      commandLine.add("--disable-default-server");
    }

    if (launcher.isForcing()) {
      commandLine.add("--force");
    }

    if (launcher.isRebalancing()) {
      commandLine.add("--rebalance");
    }

    if (launcher.isRedirectingOutput()) {
      commandLine.add("--redirect-output");
    }

    if (launcher.getServerBindAddress() != null) {
      commandLine
          .add("--server-bind-address=" + launcher.getServerBindAddress().getCanonicalHostName());
    }

    if (launcher.getServerPort() != null) {
      commandLine.add("--server-port=" + launcher.getServerPort());
    }

    if (launcher.isSpringXmlLocationSpecified()) {
      commandLine.add("--spring-xml-location=".concat(launcher.getSpringXmlLocation()));
    }

    if (launcher.getCriticalHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE + "="
          + launcher.getCriticalHeapPercentage());
    }

    if (launcher.getEvictionHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE + "="
          + launcher.getEvictionHeapPercentage());
    }

    if (launcher.getCriticalOffHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE + "="
          + launcher.getCriticalOffHeapPercentage());
    }

    if (launcher.getEvictionOffHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE + "="
          + launcher.getEvictionOffHeapPercentage());
    }

    if (launcher.getMaxConnections() != null) {
      commandLine.add(
          "--" + CliStrings.START_SERVER__MAX__CONNECTIONS + "=" + launcher.getMaxConnections());
    }

    if (launcher.getMaxMessageCount() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__MAX__MESSAGE__COUNT + "="
          + launcher.getMaxMessageCount());
    }

    if (launcher.getMaxThreads() != null) {
      commandLine
          .add("--" + CliStrings.START_SERVER__MAX__THREADS + "=" + launcher.getMaxThreads());
    }

    if (launcher.getMessageTimeToLive() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE + "="
          + launcher.getMessageTimeToLive());
    }

    if (launcher.getSocketBufferSize() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__SOCKET__BUFFER__SIZE + "="
          + launcher.getSocketBufferSize());
    }

    if (launcher.getHostNameForClients() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS + "="
          + launcher.getHostNameForClients());
    }

    return commandLine.toArray(new String[commandLine.size()]);
  }

  private String getCurrentLocators() throws MalformedObjectNameException {
    String delimitedLocators = "";
    try {
      if (isConnectedAndReady()) {
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
      getGfsh().logWarning("DistributedSystemMXBean is unavailable\n", e);
    }
    return delimitedLocators;
  }

  @Deprecated
  protected File readIntoTempFile(final String classpathResourceLocation) throws IOException {
    String resourceName = classpathResourceLocation
        .substring(classpathResourceLocation.lastIndexOf(File.separator) + 1);
    File resourceFile = new File(System.getProperty("java.io.tmpdir"), resourceName);

    if (!resourceFile.exists() && resourceFile.createNewFile()) {
      BufferedReader resourceReader = new BufferedReader(new InputStreamReader(
          ClassLoader.getSystemClassLoader().getResourceAsStream(classpathResourceLocation)));

      BufferedWriter resourceFileWriter = new BufferedWriter(new FileWriter(resourceFile, false));

      try {
        for (String line = resourceReader.readLine(); line != null; line =
            resourceReader.readLine()) {
          resourceFileWriter.write(line);
          resourceFileWriter.write(StringUtils.LINE_SEPARATOR);
        }

        resourceFileWriter.flush();
      } finally {
        IOUtils.close(resourceReader);
        IOUtils.close(resourceFileWriter);
      }
    }

    resourceFile.deleteOnExit();

    return resourceFile;
  }

  protected static class LauncherSignalListener implements SignalListener {

    private volatile boolean signaled = false;

    public boolean isSignaled() {
      return signaled;
    }

    public void handle(final SignalEvent event) {
      // System.err.printf("Gfsh LauncherSignalListener Received Signal '%1$s' (%2$d)...%n",
      // event.getSignal().getName(), event.getSignal().getNumber());
      this.signaled = true;
    }
  }

  protected String resolveWorkingDir(String userSpecifiedDir, String memberName) {
    File workingDir =
        (userSpecifiedDir == null) ? new File(memberName) : new File(userSpecifiedDir);

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
}
