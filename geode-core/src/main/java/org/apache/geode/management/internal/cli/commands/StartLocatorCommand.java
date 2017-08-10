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
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.MalformedObjectNameException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.util.IOUtils;
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
import org.apache.geode.management.internal.configuration.utils.ClusterConfigurationStatusRetriever;
import org.apache.geode.security.AuthenticationFailedException;

public class StartLocatorCommand implements GfshCommand {
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
        memberName = StartMemberUtils.getNameGenerator().generate('-');
      }

      workingDirectory = StartMemberUtils.resolveWorkingDir(workingDirectory, memberName);

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

      final int oldPid = StartMemberUtils.readPid(locatorPidFile);

      Properties gemfireProperties = new Properties();

      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.GROUPS,
          group);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.LOCATORS,
          locators);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.LOG_LEVEL,
          logLevel);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.MCAST_ADDRESS, mcastBindAddress);
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

      // read the OSProcess enable redirect system property here
      // TODO: replace with new GFSH argument
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
      final ProcessStreamReader.ReadingMode readingMode = SystemUtils.isWindows()
          ? ProcessStreamReader.ReadingMode.NON_BLOCKING : ProcessStreamReader.ReadingMode.BLOCKING;

      final StringBuffer message = new StringBuffer(); // need thread-safe StringBuffer
      ProcessStreamReader.InputListener inputListener = line -> {
        message.append(line);
        if (readingMode == ProcessStreamReader.ReadingMode.BLOCKING) {
          message.append(StringUtils.LINE_SEPARATOR);
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
        getGfsh().logInfo(String.format(CliStrings.START_LOCATOR__RUN_MESSAGE,
            IOUtils.tryGetCanonicalPathElseGetAbsolutePath(
                new File(locatorLauncher.getWorkingDirectory()))),
            null);

        locatorState = LocatorLauncher.LocatorState.fromDirectory(workingDirectory, memberName);
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

            return ResultBuilder.createShellClientErrorResult(
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

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();

      if (asyncStart) {
        infoResultData.addLine(
            String.format(CliStrings.ASYNC_PROCESS_LAUNCH_MESSAGE, CliStrings.LOCATOR_TERM_NAME));
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
                CliStrings.LOCATOR_TERM_NAME, message);
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
          StringUtils.defaultIfBlank(workingDirectory, memberName),
          HostUtils.getLocatorId(bindAddress, port), this.toString(t, getGfsh().getDebug()));
      getGfsh().logToFile(errorMessage, t);
      return ResultBuilder.createShellClientErrorResult(errorMessage);
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  // TODO should we connect implicitly when in non-interactive, headless mode (e.g. gfsh -e "start
  // locator ...")?
  // With execute option (-e), there could be multiple commands which might presume that a prior
  // "start locator" has formed the connection.
  private boolean shouldAutoConnect(final boolean connect) {
    return (connect && !(getGfsh() == null || isConnectedAndReady()));
  }

  private void doAutoConnect(final String locatorHostname, final int locatorPort,
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
            ConnectCommand.connectToLocator(locatorHostname, locatorPort,
                ConnectCommand.CONNECT_LOCATOR_TIMEOUT_MS / 4, locatorConfigurationProperties);

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

  String[] createStartLocatorCommandLine(final LocatorLauncher launcher,
      final String gemfirePropertiesPathname, final String gemfireSecurityPropertiesPathname,
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
    StartMemberUtils.addGemFirePropertyFile(commandLine, gemfirePropertiesPathname);
    StartMemberUtils.addGemFireSecurityPropertyFile(commandLine, gemfireSecurityPropertiesPathname);
    StartMemberUtils.addGemFireSystemProperties(commandLine, gemfireProperties);
    StartMemberUtils.addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);
    StartMemberUtils.addInitialHeap(commandLine, initialHeap);
    StartMemberUtils.addMaxHeap(commandLine, maxHeap);

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

  String getLocatorClasspath(final boolean includeSystemClasspath, final String userClasspath) {
    return StartMemberUtils.toClasspath(includeSystemClasspath,
        new String[] {StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME}, userClasspath);
  }
}
