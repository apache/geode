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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.MalformedObjectNameException;
import javax.net.ssl.SSLException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.domain.ConnectToLocatorResult;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.JmxOperationInvoker;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.ConnectionEndpoint;
import org.apache.geode.management.internal.configuration.utils.ClusterConfigurationStatusRetriever;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.HostUtils;
import org.apache.geode.security.AuthenticationFailedException;

public class StartLocatorCommand extends OfflineGfshCommand {
  @CliCommand(value = CliStrings.START_LOCATOR, help = CliStrings.START_LOCATOR__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public ResultModel startLocator(
      @CliOption(key = CliStrings.START_LOCATOR__MEMBER_NAME,
          help = CliStrings.START_LOCATOR__MEMBER_NAME__HELP) String memberName,
      @CliOption(key = CliStrings.START_LOCATOR__BIND_ADDRESS,
          help = CliStrings.START_LOCATOR__BIND_ADDRESS__HELP) final String bindAddress,
      @CliOption(key = CliStrings.START_LOCATOR__CLASSPATH,
          help = CliStrings.START_LOCATOR__CLASSPATH__HELP) final String classpath,
      @CliOption(key = CliStrings.START_LOCATOR__FORCE, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__FORCE__HELP) final Boolean force,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
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
      @CliOption(key = CliStrings.START_LOCATOR__PROPERTIES, optionContext = ConverterHint.FILE,
          help = CliStrings.START_LOCATOR__PROPERTIES__HELP) File gemfirePropertiesFile,
      @CliOption(key = CliStrings.START_LOCATOR__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE,
          help = CliStrings.START_LOCATOR__SECURITY_PROPERTIES__HELP) File gemfireSecurityPropertiesFile,
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
          help = CliStrings.START_LOCATOR__HTTP_SERVICE_BIND_ADDRESS__HELP) final String httpServiceBindAddress,
      @CliOption(key = CliStrings.START_LOCATOR__REDIRECT_OUTPUT, unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__REDIRECT_OUTPUT__HELP) final Boolean redirectOutput)
      throws Exception {
    if (StringUtils.isBlank(memberName)) {
      // when the user doesn't give us a name, we make one up!
      memberName = StartMemberUtils.getNameGenerator().generate('-');
    }

    workingDirectory = StartMemberUtils.resolveWorkingDir(
        workingDirectory == null ? null : new File(workingDirectory), new File(memberName));

    return doStartLocator(memberName, bindAddress, classpath, force, group, hostnameForClients,
        jmxManagerHostnameForClients, includeSystemClasspath, locators, logLevel, mcastBindAddress,
        mcastPort, port, workingDirectory, gemfirePropertiesFile, gemfireSecurityPropertiesFile,
        initialHeap, maxHeap, jvmArgsOpts, connect, enableSharedConfiguration,
        loadSharedConfigurationFromDirectory, clusterConfigDir, httpServicePort,
        httpServiceBindAddress, redirectOutput);

  }

  ResultModel doStartLocator(
      String memberName,
      String bindAddress,
      String classpath,
      Boolean force,
      String group,
      String hostnameForClients,
      String jmxManagerHostnameForClients,
      Boolean includeSystemClasspath,
      String locators,
      String logLevel,
      String mcastBindAddress,
      Integer mcastPort,
      Integer port,
      String workingDirectory,
      File gemfirePropertiesFile,
      File gemfireSecurityPropertiesFile,
      String initialHeap,
      String maxHeap,
      String[] jvmArgsOpts,
      boolean connect,
      boolean enableSharedConfiguration,
      boolean loadSharedConfigurationFromDirectory,
      String clusterConfigDir,
      Integer httpServicePort,
      String httpServiceBindAddress,
      Boolean redirectOutput)
      throws MalformedObjectNameException, IOException, InterruptedException,
      ClassNotFoundException {
    if (gemfirePropertiesFile != null && !gemfirePropertiesFile.exists()) {
      return ResultModel.createError(
          CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, StringUtils.EMPTY,
              gemfirePropertiesFile.getAbsolutePath()));
    }

    if (gemfireSecurityPropertiesFile != null && !gemfireSecurityPropertiesFile.exists()) {
      return ResultModel.createError(
          CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE, "Security ",
              gemfireSecurityPropertiesFile.getAbsolutePath()));
    }

    Properties gemfireProperties = new Properties();

    StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.GROUPS, group);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.LOCATORS,
        locators);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.LOG_LEVEL,
        logLevel);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.MCAST_ADDRESS,
        mcastBindAddress);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.MCAST_PORT,
        mcastPort);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
        ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION, enableSharedConfiguration);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
        ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR,
        loadSharedConfigurationFromDirectory);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
        ConfigurationProperties.CLUSTER_CONFIGURATION_DIR, clusterConfigDir);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
        ConfigurationProperties.HTTP_SERVICE_PORT, httpServicePort);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
        ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS, httpServiceBindAddress);
    StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
        ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, jmxManagerHostnameForClients);

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
        gemfirePropertiesFile, gemfireSecurityPropertiesFile, gemfireProperties, classpath,
        includeSystemClasspath, jvmArgsOpts, initialHeap, maxHeap);

    final Process locatorProcess =
        getProcess(locatorLauncher.getWorkingDirectory(), locatorCommandLine);

    locatorProcess.getInputStream().close();
    locatorProcess.getOutputStream().close();

    // fix TRAC bug #51967 by using NON_BLOCKING on Windows
    final ProcessStreamReader.ReadingMode readingMode = SystemUtils.isWindows()
        ? ProcessStreamReader.ReadingMode.NON_BLOCKING : ProcessStreamReader.ReadingMode.BLOCKING;

    final StringBuffer message = new StringBuffer(); // need thread-safe StringBuffer
    ProcessStreamReader.InputListener inputListener = line -> {
      message.append(line);
      if (readingMode == ProcessStreamReader.ReadingMode.BLOCKING) {
        message.append(SystemUtils.getLineSeparator());
      }
    };

    ProcessStreamReader stderrReader = new ProcessStreamReader.Builder(locatorProcess)
        .inputStream(locatorProcess.getErrorStream()).inputListener(inputListener)
        .readingMode(readingMode).continueReadingMillis(2 * 1000).build().start();

    LocatorLauncher.LocatorState locatorState;

    String previousLocatorStatusMessage = null;

    LauncherSignalListener locatorSignalListener = new LauncherSignalListener();

    final boolean registeredLocatorSignalListener =
        getGfsh().getSignalHandler().registerListener(locatorSignalListener);

    try {
      getGfsh().logInfo(String.format(CliStrings.START_LOCATOR__RUN_MESSAGE, IOUtils
          .tryGetCanonicalPathElseGetAbsolutePath(new File(locatorLauncher.getWorkingDirectory()))),
          null);

      do {
        if (locatorProcess.isAlive()) {
          Gfsh.print(".");

          synchronized (this) {
            TimeUnit.MILLISECONDS.timedWait(this, 500);
          }

          locatorState = LocatorLauncher.LocatorState.fromDirectory(workingDirectory, memberName);

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

          return ResultModel.createError(
              String.format(CliStrings.START_LOCATOR__PROCESS_TERMINATED_ABNORMALLY_ERROR_MESSAGE,
                  exitValue, locatorLauncher.getWorkingDirectory(), message.toString()));
        }
      } while (!(registeredLocatorSignalListener && locatorSignalListener.isSignaled())
          && locatorState.isStartingOrNotResponding());
    } finally {
      // stop will close
      stderrReader.stopAsync(StartMemberUtils.PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS);

      // ErrorStream
      getGfsh().getSignalHandler().unregisterListener(locatorSignalListener);
    }

    Gfsh.println();

    final boolean asyncStart =
        (registeredLocatorSignalListener && locatorSignalListener.isSignaled()
            && ServerLauncher.ServerState.isStartingNotRespondingOrNull(locatorState));

    ResultModel result = new ResultModel();
    InfoResultModel infoResult = result.addInfo();

    if (loadSharedConfigurationFromDirectory) {
      infoResult.addLine("Warning: Option --load-cluster-config-from-dir is deprecated, use '"
          + CliStrings.IMPORT_SHARED_CONFIG
          + "' command instead to import any existing configuration.\n");
    }

    if (asyncStart) {
      infoResult.addLine(
          String.format(CliStrings.ASYNC_PROCESS_LAUNCH_MESSAGE, CliStrings.LOCATOR_TERM_NAME));
      return result;
    }

    infoResult.addLine(locatorState.toString());
    String locatorHostName;
    InetAddress bindAddr = locatorLauncher.getBindAddress();
    if (bindAddr != null) {
      locatorHostName = bindAddr.getCanonicalHostName();
    } else {
      locatorHostName = StringUtils.defaultIfBlank(locatorLauncher.getHostnameForClients(),
          HostUtils.getLocalHost());
    }

    int locatorPort = Integer.parseInt(locatorState.getPort());


    ConnectCommand connectCommand = new ConnectCommand();
    Properties configProperties = connectCommand.resolveSslProperties(getGfsh(), false,
        gemfirePropertiesFile, gemfireSecurityPropertiesFile);

    // AUTO-CONNECT
    // If the connect succeeds add the connected message to the result,
    // Else, ask the user to use the "connect" command to connect to the Locator.
    if (shouldAutoConnect(connect)) {
      boolean connected =
          doAutoConnect(locatorHostName, locatorPort, configProperties, infoResult);

      // Report on the state of the Shared Configuration service if enabled...
      if (enableSharedConfiguration && connected) {
        infoResult.addLine(ClusterConfigurationStatusRetriever.fromLocator(locatorHostName,
            locatorPort, configProperties));
      }
    }

    return result;
  }

  Process getProcess(String workingDir, String[] locatorCommandLine)
      throws IOException {
    return new ProcessBuilder(locatorCommandLine)
        .directory(new File(workingDir)).start();
  }

  // TODO should we connect implicitly when in non-interactive, headless mode (e.g. gfsh -e "start
  // locator ...")?
  // With execute option (-e), there could be multiple commands which might presume that a prior
  // "start locator" has formed the connection.
  private boolean shouldAutoConnect(final boolean connect) {
    return (connect && !isConnectedAndReady());
  }

  private boolean doAutoConnect(final String locatorHostname, final int locatorPort,
      final Properties configurationProperties, final InfoResultModel infoResult) {
    boolean connectSuccess = false;
    boolean jmxManagerAuthEnabled = false;
    boolean jmxManagerSslEnabled = false;

    String responseFailureMessage = null;

    for (int attempts = 0; (attempts < 10 && !connectSuccess); attempts++) {
      try {
        ConnectToLocatorResult connectToLocatorResult =
            ConnectCommand.connectToLocator(locatorHostname, locatorPort,
                ConnectCommand.CONNECT_LOCATOR_TIMEOUT_MS / 4, configurationProperties);

        ConnectionEndpoint memberEndpoint = connectToLocatorResult.getMemberEndpoint();

        jmxManagerSslEnabled = connectToLocatorResult.isJmxManagerSslEnabled();

        getGfsh().setOperationInvoker(new JmxOperationInvoker(memberEndpoint.getHost(),
            memberEndpoint.getPort(), configurationProperties));

        String shellAndLogMessage = CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS,
            "JMX Manager " + memberEndpoint.toString(false));

        infoResult.addLine("\n");
        infoResult.addLine(shellAndLogMessage);
        getGfsh().logToFile(shellAndLogMessage, null);

        connectSuccess = true;
        responseFailureMessage = null;
      } catch (SecurityException | AuthenticationFailedException e) {
        getGfsh().logToFile(e.getMessage(), e);
        jmxManagerAuthEnabled = true;
        break; // no need to continue after SecurityException
      } // no need to continue after AuthenticationFailedException
      catch (SSLException e) {
        // another type of SSL error occurred (possibly a configuration issue); pass the buck...
        getGfsh().logToFile(e.getMessage(), e);
        responseFailureMessage = "Check your SSL configuration and try again.";
      } catch (Exception e) {
        getGfsh().logToFile(e.getMessage(), e);
        responseFailureMessage = "Failed to connect; unknown cause: " + e.getMessage();
      }
    }

    if (!connectSuccess) {
      doOnConnectionFailure(locatorHostname, locatorPort, jmxManagerAuthEnabled,
          jmxManagerSslEnabled, infoResult);
    }

    if (StringUtils.isNotBlank(responseFailureMessage)) {
      infoResult.addLine("\n");
      infoResult.addLine(responseFailureMessage);
    }
    return connectSuccess;
  }

  private void doOnConnectionFailure(final String locatorHostName, final int locatorPort,
      final boolean jmxManagerAuthEnabled, final boolean jmxManagerSslEnabled,
      final InfoResultModel infoResult) {
    infoResult.addLine("\n");
    CommandStringBuilder commandUsage = new CommandStringBuilder(CliStrings.CONNECT)
        .addOption(CliStrings.CONNECT__LOCATOR, locatorHostName + "[" + locatorPort + "]");

    StringBuilder message = new StringBuilder();

    if (jmxManagerAuthEnabled) {
      commandUsage.addOption(CliStrings.CONNECT__USERNAME).addOption(CliStrings.CONNECT__PASSWORD);
      message.append("Authentication");
    }
    if (jmxManagerSslEnabled) {
      message.append(jmxManagerAuthEnabled ? " and " : StringUtils.EMPTY)
          .append("SSL configuration");
    }
    infoResult.addLine(CliStrings.format(
        CliStrings.START_LOCATOR__USE__0__TO__CONNECT_WITH_SECURITY, commandUsage.toString()));
    if (jmxManagerAuthEnabled || jmxManagerSslEnabled) {
      message.append(" required to connect to the Manager.");
      infoResult.addLine("\n");
      infoResult.addLine(message.toString());
    }
  }

  @SuppressWarnings("deprecation")
  String[] createStartLocatorCommandLine(final LocatorLauncher launcher,
      final File gemfirePropertiesFile, final File gemfireSecurityPropertiesFile,
      final Properties gemfireProperties, final String userClasspath,
      final Boolean includeSystemClasspath, final String[] jvmArgsOpts, final String initialHeap,
      final String maxHeap) throws MalformedObjectNameException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(StartMemberUtils.getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine
        .add(getLocatorClasspath(Boolean.TRUE.equals(includeSystemClasspath), userClasspath));

    StartMemberUtils.addCurrentLocators(this, commandLine, gemfireProperties);
    StartMemberUtils.addGemFirePropertyFile(commandLine, gemfirePropertiesFile);
    StartMemberUtils.addGemFireSecurityPropertyFile(commandLine, gemfireSecurityPropertiesFile);
    StartMemberUtils.addGemFireSystemProperties(commandLine, gemfireProperties);
    StartMemberUtils.addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);
    StartMemberUtils.addInitialHeap(commandLine, initialHeap);
    StartMemberUtils.addMaxHeap(commandLine, maxHeap);

    commandLine.add(
        "-D".concat(AbstractLauncher.SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY.concat("=true")));
    commandLine.add("-Djava.awt.headless=true");
    commandLine.add(
        "-Dsun.rmi.dgc.server.gcInterval".concat("=").concat(Long.toString(Long.MAX_VALUE - 1)));
    if (launcher.isRedirectingOutput()) {
      commandLine
          .add("-D".concat(OSProcess.DISABLE_REDIRECTION_CONFIGURATION_PROPERTY).concat("=true"));
    }
    commandLine.add(LocatorLauncher.class.getName());
    commandLine.add(LocatorLauncher.Command.START.getName());

    if (StringUtils.isNotBlank(launcher.getMemberName())) {
      commandLine.add(launcher.getMemberName());
    }

    if (launcher.getBindAddress() != null) {
      commandLine.add("--bind-address=" + launcher.getBindAddress().getHostAddress());
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

    return commandLine.toArray(new String[] {});
  }

  String getLocatorClasspath(final boolean includeSystemClasspath, final String userClasspath) {
    List<String> jarFilePathnames = new ArrayList<>();
    jarFilePathnames.add(StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);
    // include all extension dependencies on the CLASSPATH...
    for (String extensionsJarPathname : getExtensionsJars()) {
      if (org.apache.commons.lang3.StringUtils.isNotBlank(extensionsJarPathname)) {
        jarFilePathnames.add(extensionsJarPathname);
      }
    }

    return StartMemberUtils.toClasspath(includeSystemClasspath,
        jarFilePathnames.toArray(new String[] {}), userClasspath);
  }

  private String[] getExtensionsJars() {
    File extensionsDirectory = new File(StartMemberUtils.EXTENSIONS_PATHNAME);
    File[] extensionsJars = extensionsDirectory.listFiles();

    if (extensionsJars != null) {
      // assume `extensions` directory does not contain any subdirectories. It only contains jars.
      return Arrays.stream(extensionsJars).filter(File::isFile).map(
          file -> IOUtils.appendToPath(StartMemberUtils.GEODE_HOME, "extensions", file.getName()))
          .toArray(String[]::new);
    } else {
      return ArrayUtils.EMPTY_STRING_ARRAY;
    }
  }
}
