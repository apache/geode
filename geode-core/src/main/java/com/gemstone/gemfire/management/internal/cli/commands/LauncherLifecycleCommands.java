/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.AbstractLauncher;
import com.gemstone.gemfire.distributed.AbstractLauncher.ServiceState;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.distributed.ServerLauncher;
import com.gemstone.gemfire.distributed.ServerLauncher.ServerState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.DistributionLocator;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberPattern;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.ClassUtils;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.internal.process.ClusterConfigurationNotAvailableException;
import com.gemstone.gemfire.internal.process.ProcessLauncherContext;
import com.gemstone.gemfire.internal.process.ProcessStreamReader;
import com.gemstone.gemfire.internal.process.ProcessStreamReader.InputListener;
import com.gemstone.gemfire.internal.process.ProcessStreamReader.ReadingMode;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.internal.process.signal.SignalEvent;
import com.gemstone.gemfire.internal.process.signal.SignalListener;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.lang.AttachAPINotFoundException;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.converters.ConnectionEndpointConverter;
import com.gemstone.gemfire.management.internal.cli.domain.ConnectToLocatorResult;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.InfoResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;
import com.gemstone.gemfire.management.internal.cli.shell.JmxOperationInvoker;
import com.gemstone.gemfire.management.internal.cli.shell.OperationInvoker;
import com.gemstone.gemfire.management.internal.cli.util.CauseFinder;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.management.internal.cli.util.ConnectionEndpoint;
import com.gemstone.gemfire.management.internal.cli.util.JConsoleNotFoundException;
import com.gemstone.gemfire.management.internal.cli.util.VisualVmNotFoundException;
import com.gemstone.gemfire.management.internal.configuration.domain.SharedConfigurationStatus;
import com.gemstone.gemfire.management.internal.configuration.messages.SharedConfigurationStatusRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.SharedConfigurationStatusResponse;
import com.gemstone.gemfire.security.AuthenticationFailedException;

/**
 * The LauncherLifecycleCommands class encapsulates all GemFire launcher commands for GemFire tools (like starting
 * GemFire Monitor (GFMon) and Visual Statistics Display (VSD)) as well external tools (like jconsole).
 * <p>
 * @see com.gemstone.gemfire.distributed.LocatorLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher
 * @see com.gemstone.gemfire.management.internal.cli.commands.AbstractCommandsSupport
 * @see com.gemstone.gemfire.management.internal.cli.shell.Gfsh
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class LauncherLifecycleCommands extends AbstractCommandsSupport {

  private static final String LOCATOR_TERM_NAME = "Locator";
  private static final String SERVER_TERM_NAME = "Server";

  private static final long PROCESS_STREAM_READER_JOIN_TIMEOUT_MILLIS = 30 * 1000;
  private static final long PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS = 5 * 1000;
  private static final long WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS = 30 * 1000;
  private static final long WAITING_FOR_PID_FILE_TO_CONTAIN_PID_TIMEOUT_MILLIS = 2 * 1000;

  protected static final int CMS_INITIAL_OCCUPANCY_FRACTION = 60;
  protected static final int DEFAULT_PROCESS_OUTPUT_WAIT_TIME_MILLISECONDS = 5000;
  protected static final int INVALID_PID = -1;
  protected static final int MINIMUM_HEAP_FREE_RATIO = 10;
  protected static final int NUM_ATTEMPTS_FOR_SHARED_CONFIGURATION_STATUS = 3;

  protected static final AtomicReference<Boolean> ATTACH_API_AVAILABLE = new AtomicReference<>(null);

  protected static final String ATTACH_API_CLASS_NAME = "com.sun.tools.attach.AttachNotSupportedException";
  protected static final String GEMFIRE_HOME = System.getenv("GEMFIRE");
  protected static final String JAVA_HOME = System.getProperty("java.home");
  protected static final String LOCALHOST = "localhost";

  // MUST CHANGE THIS TO REGEX SINCE VERSION CHANGES IN JAR NAME
  protected static final String GEMFIRE_JAR_PATHNAME = IOUtils.appendToPath(GEMFIRE_HOME, "lib", GemFireVersion.getGemFireJarFileName());

  protected static final String CORE_DEPENDENCIES_JAR_PATHNAME =
      IOUtils.appendToPath(GEMFIRE_HOME, "lib", "geode-dependencies.jar");

  protected static boolean isAttachApiAvailable() {
    if (ATTACH_API_AVAILABLE.get() == null) {
      try {
        ClassUtils.forName(ATTACH_API_CLASS_NAME, new AttachAPINotFoundException());
        ATTACH_API_AVAILABLE.set(Boolean.TRUE);
      } catch (AttachAPINotFoundException ignore) {
        ATTACH_API_AVAILABLE.set(Boolean.FALSE);
      }
    }

    return ATTACH_API_AVAILABLE.get();
  }

  @CliCommand(value = CliStrings.START_LOCATOR, help = CliStrings.START_LOCATOR__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LIFECYCLE })
  public Result startLocator(
      @CliOption(key = CliStrings.START_LOCATOR__MEMBER_NAME,
          mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__MEMBER_NAME__HELP)
      final String memberName,
      @CliOption(key = CliStrings.START_LOCATOR__BIND_ADDRESS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__BIND_ADDRESS__HELP)
      final String bindAddress,
      @CliOption(key = CliStrings.START_LOCATOR__CLASSPATH,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__CLASSPATH__HELP)
      final String classpath,
      @CliOption(key = CliStrings.START_LOCATOR__FORCE,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__FORCE__HELP)
      final Boolean force,
      @CliOption(key = CliStrings.START_LOCATOR__GROUP,
          optionContext = ConverterHint.MEMBERGROUP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__GROUP__HELP)
      final String group,
      @CliOption(key = CliStrings.START_LOCATOR__HOSTNAME_FOR_CLIENTS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__HOSTNAME_FOR_CLIENTS__HELP)
      final String hostnameForClients,
      @CliOption(key = CliStrings.START_LOCATOR__INCLUDE_SYSTEM_CLASSPATH,
          specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_LOCATOR__INCLUDE_SYSTEM_CLASSPATH__HELP)
      final Boolean includeSystemClasspath,
      @CliOption(key = CliStrings.START_LOCATOR__LOCATORS,
          optionContext = ConverterHint.LOCATOR_DISCOVERY_CONFIG,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__LOCATORS__HELP)
      final String locators,
      @CliOption(key = CliStrings.START_LOCATOR__LOG_LEVEL,
          optionContext = ConverterHint.LOG_LEVEL,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__LOG_LEVEL__HELP)
      final String logLevel,
      @CliOption(key = CliStrings.START_LOCATOR__MCAST_ADDRESS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__MCAST_ADDRESS__HELP)
      final String mcastBindAddress,
      @CliOption(key = CliStrings.START_LOCATOR__MCAST_PORT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__MCAST_PORT__HELP)
      final Integer mcastPort,
      @CliOption(key = CliStrings.START_LOCATOR__PORT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__PORT__HELP)
      final Integer port,
      @CliOption(key = CliStrings.START_LOCATOR__DIR,
          optionContext = ConverterHint.DIR_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__DIR__HELP)
          String workingDirectory,
      @CliOption(key = CliStrings.START_LOCATOR__PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__PROPERTIES__HELP)
          String gemfirePropertiesPathname,
      @CliOption(key = CliStrings.START_LOCATOR__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__SECURITY_PROPERTIES__HELP)
          String gemfireSecurityPropertiesPathname,
      @CliOption(key = CliStrings.START_LOCATOR__INITIALHEAP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__INITIALHEAP__HELP)
      final String initialHeap,
      @CliOption(key = CliStrings.START_LOCATOR__MAXHEAP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__MAXHEAP__HELP)
      final String maxHeap,
      @CliOption(key = CliStrings.START_LOCATOR__J,
          optionContext = ConverterHint.STRING_LIST,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_LOCATOR__J__HELP)
      @CliMetaData(valueSeparator = ",")
      final String[] jvmArgsOpts,
      @CliOption(key = CliStrings.START_LOCATOR__CONNECT,
          unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__CONNECT__HELP)
      final boolean connect,
      @CliOption(key = CliStrings.START_LOCATOR__ENABLE__SHARED__CONFIGURATION,
          unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true",
          help = CliStrings.START_LOCATOR__ENABLE__SHARED__CONFIGURATION__HELP)
      final boolean enableSharedConfiguration,
      @CliOption(key = CliStrings.START_LOCATOR__LOAD__SHARED_CONFIGURATION__FROM__FILESYSTEM,
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_LOCATOR__LOAD__SHARED_CONFIGURATION__FROM__FILESYSTEM__HELP)
      final boolean loadSharedConfigurationFromDirectory,
      @CliOption(key = CliStrings.START_LOCATOR__CLUSTER__CONFIG__DIR,
          unspecifiedDefaultValue = "",
          help = CliStrings.START_LOCATOR__CLUSTER__CONFIG__DIR__HELP)
      final String clusterConfigDir
  ) {
    try {
      if (workingDirectory == null) {
        // attempt to use or make sub-directory using memberName...
        File locatorWorkingDirectory = new File(memberName);

        if (!(locatorWorkingDirectory.exists() || locatorWorkingDirectory.mkdir())) {
          throw new IllegalStateException(CliStrings.format(CliStrings.START_LOCATOR__MSG__COULD_NOT_CREATE_DIRECTORY_0_VERIFY_PERMISSIONS,
              locatorWorkingDirectory.getAbsolutePath()));
        }

        workingDirectory = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(locatorWorkingDirectory);
      }

      gemfirePropertiesPathname = CliUtil.resolvePathname(gemfirePropertiesPathname);

      if (!StringUtils.isBlank(gemfirePropertiesPathname) && !IOUtils.isExistingPathname(gemfirePropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
            StringUtils.EMPTY_STRING, gemfirePropertiesPathname));
      }

      gemfireSecurityPropertiesPathname = CliUtil.resolvePathname(gemfireSecurityPropertiesPathname);

      if (!StringUtils.isBlank(gemfireSecurityPropertiesPathname) && !IOUtils.isExistingPathname(gemfireSecurityPropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
            "Security ", gemfireSecurityPropertiesPathname));
      }

      File locatorPidFile = new File(workingDirectory, ProcessType.LOCATOR.getPidFileName());

      final int oldPid = readPid(locatorPidFile);

      Properties gemfireProperties = new Properties();

      gemfireProperties.setProperty(GROUPS, StringUtils.valueOf(group, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(LOCATORS, StringUtils.valueOf(locators, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(LOG_LEVEL, StringUtils.valueOf(logLevel, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(MCAST_ADDRESS, StringUtils.valueOf(mcastBindAddress, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(MCAST_PORT, StringUtils.valueOf(mcastPort, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(ENABLE_CLUSTER_CONFIGURATION, StringUtils.valueOf(enableSharedConfiguration, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(LOAD_CLUSTER_CONFIGURATION_FROM_DIR,
          StringUtils.valueOf(loadSharedConfigurationFromDirectory, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(CLUSTER_CONFIGURATION_DIR, StringUtils.valueOf(clusterConfigDir, StringUtils.EMPTY_STRING));

      // read the OSProcess enable redirect system property here -- TODO: replace with new GFSH argument
      final boolean redirectOutput = Boolean.getBoolean(OSProcess.ENABLE_OUTPUT_REDIRECTION_PROPERTY);
      LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
          .setBindAddress(bindAddress)
          .setForce(force)
          .setHostnameForClients(hostnameForClients)
          .setMemberName(memberName)
          .setPort(port)
          .setRedirectOutput(redirectOutput)
          .setWorkingDirectory(workingDirectory)
          .build();

      String[] locatorCommandLine = createStartLocatorCommandLine(locatorLauncher, gemfirePropertiesPathname,
          gemfireSecurityPropertiesPathname, gemfireProperties, classpath, includeSystemClasspath, jvmArgsOpts, initialHeap, maxHeap);

      //getGfsh().logInfo(StringUtils.concat(locatorCommandLine, " "), null);

      final Process locatorProcess = new ProcessBuilder(locatorCommandLine)
          .directory(new File(locatorLauncher.getWorkingDirectory()))
          .start();

      locatorProcess.getInputStream().close();
      locatorProcess.getOutputStream().close();

      // fix TRAC bug #51967 by using NON_BLOCKING on Windows
      final ReadingMode readingMode = SystemUtils.isWindows() ? ReadingMode.NON_BLOCKING : ReadingMode.BLOCKING;

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
          .inputStream(locatorProcess.getErrorStream())
          .inputListener(inputListener)
          .readingMode(readingMode)
          .continueReadingMillis(2 * 1000)
          .build()
          .start();

      LocatorState locatorState;

      String previousLocatorStatusMessage = null;

      LauncherSignalListener locatorSignalListener = new LauncherSignalListener();

      final boolean registeredLocatorSignalListener = getGfsh().getSignalHandler().registerListener(locatorSignalListener);

      try {
        getGfsh().logInfo(String.format(CliStrings.START_LOCATOR__RUN_MESSAGE,
            IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(locatorLauncher.getWorkingDirectory()))), null);

        do {
          try {
            final int exitValue = locatorProcess.exitValue();

            stderrReader.join(PROCESS_STREAM_READER_JOIN_TIMEOUT_MILLIS); // was Long.MAX_VALUE

            //Gfsh.println(message);

            return ResultBuilder.createShellClientErrorResult(String.format(
                CliStrings.START_LOCATOR__PROCESS_TERMINATED_ABNORMALLY_ERROR_MESSAGE,
                exitValue, locatorLauncher.getWorkingDirectory(), message.toString()));
          } catch (IllegalThreadStateException ignore) {
            // the IllegalThreadStateException is expected; it means the Locator's process has not terminated,
            // and basically should not
            Gfsh.print(".");

            synchronized (this) {
              TimeUnit.MILLISECONDS.timedWait(this, 500);
            }

            locatorState = (ProcessUtils.isAttachApiAvailable() ? locatorStatus(locatorPidFile, oldPid, memberName)
                : locatorStatus(workingDirectory, memberName));

            String currentLocatorStatusMessage = locatorState.getStatusMessage();

            if (isStartingOrNotResponding(locatorState.getStatus())
                && !(StringUtils.isBlank(currentLocatorStatusMessage)
                || currentLocatorStatusMessage.equalsIgnoreCase(previousLocatorStatusMessage))) {
              Gfsh.println();
              Gfsh.println(currentLocatorStatusMessage);
              previousLocatorStatusMessage = currentLocatorStatusMessage;
            }
          }
        }
        while (!(registeredLocatorSignalListener && locatorSignalListener.isSignaled())
            && isStartingOrNotResponding(locatorState.getStatus()));
      } finally {
        stderrReader.stopAsync(PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS); // stop will close ErrorStream
        getGfsh().getSignalHandler().unregisterListener(locatorSignalListener);
      }

      Gfsh.println();

      final boolean asyncStart = (registeredLocatorSignalListener && locatorSignalListener.isSignaled()
          && isStartingNotRespondingOrNull(locatorState));

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();

      if (asyncStart) {
        infoResultData.addLine(String.format(CliStrings.ASYNC_PROCESS_LAUNCH_MESSAGE, LOCATOR_TERM_NAME));
      } else {
        infoResultData.addLine(locatorState.toString());

        String locatorHostName;
        InetAddress bindAddr = locatorLauncher.getBindAddress();
        if (bindAddr != null){
          locatorHostName = bindAddr.getCanonicalHostName();
        } else {
          locatorHostName = StringUtils.defaultIfBlank(locatorLauncher.getHostnameForClients(), getLocalHost());
        }

        int locatorPort = Integer.parseInt(locatorState.getPort());

        // AUTO-CONNECT
        // If the connect succeeds add the connected message to the result,
        // Else, ask the user to use the "connect" command to connect to the Locator.
        if (shouldAutoConnect(connect)) {
          doAutoConnect(locatorHostName, locatorPort, gemfirePropertiesPathname, gemfireSecurityPropertiesPathname,
              infoResultData);
        }

        // Report on the state of the Shared Configuration service if enabled...
        if (enableSharedConfiguration) {
          infoResultData.addLine(getSharedConfigurationStatusFromLocator(locatorHostName, locatorPort));
        }
      }

      return ResultBuilder.buildResult(infoResultData);
    } catch (IllegalArgumentException e) {
      String message = e.getMessage();
      if (message != null && message.matches(LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toLocalizedString(".+"))) {
        message = CliStrings.format(CliStrings.LAUNCHERLIFECYCLECOMMANDS__MSG__FAILED_TO_START_0_REASON_1, LOCATOR_TERM_NAME, message);
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

  protected String[] createStartLocatorCommandLine(final LocatorLauncher launcher,
      final String gemfirePropertiesPathname,
      final String gemfireSecurityPropertiesPathname,
      final Properties gemfireProperties,
      final String userClasspath,
      final Boolean includeSystemClasspath,
      final String[] jvmArgsOpts,
      final String initialHeap,
      final String maxHeap)
      throws MalformedObjectNameException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine.add(getLocatorClasspath(Boolean.TRUE.equals(includeSystemClasspath), userClasspath));

    addCurrentLocators(commandLine, gemfireProperties);
    addGemFirePropertyFile(commandLine, gemfirePropertiesPathname);
    addGemFireSecurityPropertyFile(commandLine, gemfireSecurityPropertiesPathname);
    addGemFireSystemProperties(commandLine, gemfireProperties);
    addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);
    addInitialHeap(commandLine, initialHeap);
    addMaxHeap(commandLine, maxHeap);

    commandLine.add("-D".concat(AbstractLauncher.SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY.concat("=true")));
    commandLine.add("-Djava.awt.headless=true");
    commandLine.add("-Dsun.rmi.dgc.server.gcInterval".concat("=").concat(Long.toString(Long.MAX_VALUE - 1)));
    commandLine.add(LocatorLauncher.class.getName());
    commandLine.add(LocatorLauncher.Command.START.getName());

    if (!StringUtils.isBlank(launcher.getMemberName())) {
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

    if (!StringUtils.isBlank(launcher.getHostnameForClients())) {
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

  // TODO should we connect implicitly when in non-interactive, headless mode (e.g. gfsh -e "start locator ...")?
  // With execute option (-e), there could be multiple commands which might presume that a prior "start locator"
  // has formed the connection.
  private boolean shouldAutoConnect(final boolean connect) {
    return (connect && !(getGfsh() == null || isConnectedAndReady()));
  }

  private boolean doAutoConnect(final String locatorHostname,
      final int locatorPort,
      final String gemfirePropertiesPathname,
      final String gemfireSecurityPropertiesPathname,
      final InfoResultData infoResultData) {
    boolean connectSuccess = false;
    boolean jmxManagerAuthEnabled = false;
    boolean jmxManagerSslEnabled = false;

    Map<String, String> configurationProperties = loadConfigurationProperties(gemfireSecurityPropertiesPathname,
        loadConfigurationProperties(gemfirePropertiesPathname));
    Map<String, String> locatorConfigurationProperties = new HashMap<>(configurationProperties);

    String responseFailureMessage = null;

    for (int attempts = 0; (attempts < 10 && !connectSuccess); attempts++) {
      try {
        ConnectToLocatorResult connectToLocatorResult = ShellCommands.connectToLocator(locatorHostname, locatorPort,
            ShellCommands.getConnectLocatorTimeoutInMS() / 4, locatorConfigurationProperties);

        ConnectionEndpoint memberEndpoint = connectToLocatorResult.getMemberEndpoint();

        jmxManagerSslEnabled = connectToLocatorResult.isJmxManagerSslEnabled();

        if (!jmxManagerSslEnabled) {
          configurationProperties.clear();
        }

        getGfsh().setOperationInvoker(new JmxOperationInvoker(memberEndpoint.getHost(), memberEndpoint.getPort(),
            null, null, configurationProperties, null));

        String shellAndLogMessage = CliStrings.format(CliStrings.CONNECT__MSG__SUCCESS, "JMX Manager " + memberEndpoint.toString(false));

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
      }
      catch (AuthenticationFailedException ignore) {
        getGfsh().logToFile(ignore.getMessage(), ignore);
        jmxManagerAuthEnabled = true;
        break; // no need to continue after AuthenticationFailedException
      }
      catch (SSLException ignore) {
        if (ignore instanceof SSLHandshakeException) {
          // try to connect again without SSL since the SSL handshake failed implying a plain text connection...
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
      doOnConnectionFailure(locatorHostname, locatorPort, jmxManagerAuthEnabled, jmxManagerSslEnabled, infoResultData);
    }

    if (!StringUtils.isBlank(responseFailureMessage)) {
      infoResultData.addLine("\n");
      infoResultData.addLine(responseFailureMessage);
    }

    return connectSuccess;
  }

  private void doOnConnectionFailure(final String locatorHostName,
      final int locatorPort,
      final boolean jmxManagerAuthEnabled,
      final boolean jmxManagerSslEnabled,
      final InfoResultData infoResultData) {

    infoResultData.addLine("\n");
    infoResultData.addLine(CliStrings.format(CliStrings.START_LOCATOR__USE__0__TO__CONNECT,
        new CommandStringBuilder(CliStrings.CONNECT).addOption(CliStrings.CONNECT__LOCATOR,
            locatorHostName + "[" + locatorPort + "]").toString()));

    StringBuilder message = new StringBuilder();

    if (jmxManagerAuthEnabled) {
      message.append("Authentication");
    }
    if (jmxManagerSslEnabled) {
      message.append(jmxManagerAuthEnabled ? " and " : StringUtils.EMPTY_STRING).append("SSL configuration");
    }
    if (jmxManagerAuthEnabled || jmxManagerSslEnabled) {
      message.append(" required to connect to the Manager.");
      infoResultData.addLine("\n");
      infoResultData.addLine(message.toString());
    }
  }

  private Map<String, String> loadConfigurationProperties(final String configurationPropertiesPathname) {
    return loadConfigurationProperties(configurationPropertiesPathname, null);
  }

  private Map<String, String> loadConfigurationProperties(final String configurationPropertiesPathname,
      Map<String, String> configurationProperties) {
    configurationProperties = (configurationProperties != null ? configurationProperties
        : new HashMap<String, String>());

    if (IOUtils.isExistingPathname(configurationPropertiesPathname)) {
      try {
        configurationProperties.putAll(ShellCommands.loadPropertiesFromURL(
            new File(configurationPropertiesPathname).toURI().toURL()));
      } catch (MalformedURLException ignore) {
        LogWrapper.getInstance().warning(String.format(
            "Failed to load GemFire configuration properties from pathname (%1$s)!",
            configurationPropertiesPathname), ignore);
      }
    }

    return configurationProperties;
  }

  private String getSharedConfigurationStatusFromLocatorState(LocatorState locatorState) throws ClassNotFoundException, IOException {
    return getSharedConfigurationStatusFromLocator(locatorState.getHost(), Integer.parseInt(locatorState.getPort()));
  }

  private String getSharedConfigurationStatusFromLocator(String locatorHostName, int locatorPort) throws ClassNotFoundException, IOException {
    final StringBuilder buffer = new StringBuilder();

    try {
      final InetAddress networkAddress = InetAddress.getByName(locatorHostName);

      SharedConfigurationStatusResponse statusResponse = (SharedConfigurationStatusResponse)
          TcpClient.requestToServer(networkAddress, locatorPort, new SharedConfigurationStatusRequest(), 10000, true);

      for (int i = 0; i < NUM_ATTEMPTS_FOR_SHARED_CONFIGURATION_STATUS; i++) {
        if (statusResponse.getStatus().equals(SharedConfigurationStatus.STARTED) || statusResponse.getStatus().equals(SharedConfigurationStatus.NOT_STARTED)) {
          statusResponse = (SharedConfigurationStatusResponse) TcpClient
              .requestToServer(networkAddress, locatorPort, new SharedConfigurationStatusRequest(), 10000, true);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            //Swallow the exception
          }
        } else {
          break;
        }
      }

      switch (statusResponse.getStatus()) {
      case RUNNING:
        buffer.append("\nCluster configuration service is up and running.");
        break;
      case STOPPED:
        buffer.append("\nCluster configuration service failed to start , please check the log file for errors.");
        break;
      case WAITING:
        buffer.append("\nCluster configuration service is waiting for other locators with newer shared configuration data.");
        Set<PersistentMemberPattern> pmpSet = statusResponse.getOtherLocatorInformation();
        if (!pmpSet.isEmpty()) {
          buffer.append("\nThis locator might have stale cluster configuration data.");
          buffer.append("\nFollowing locators contain potentially newer cluster configuration data");

          for (PersistentMemberPattern pmp : pmpSet) {
            buffer.append("\nHost : ").append(pmp.getHost());
            buffer.append("\nDirectory : ").append(pmp.getDirectory());
          }
        } else {
          buffer.append("\nPlease check the log file for errors");
        }
        break;
      case UNDETERMINED:
        buffer.append("\nUnable to determine the status of shared configuration service, please check the log file");
        break;
      case NOT_STARTED:
        buffer.append("\nCluster configuration service has not been started yet");
        break;
      case STARTED:
        buffer.append("\nCluster configuration service has been started, but its not running yet");
        break;
      }
    } catch (Exception e) {
      // TODO fix this once Trac Bug #50513 gets fixed
      // NOTE this ClassCastException occurs if the a plain text TCP/IP connection is used to connect to a Locator
      // configured with SSL.
      getGfsh().logToFile(String.format("Failed to get the status of the Shared Configuration Service running on Locator (%1$s[%2$d])!",
          locatorHostName, locatorPort), e);
    }

    return buffer.toString();
  }

  @CliCommand(value = CliStrings.STATUS_LOCATOR, help = CliStrings.STATUS_LOCATOR__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LIFECYCLE })
  public Result statusLocator(@CliOption(key = CliStrings.STATUS_LOCATOR__MEMBER,
      optionContext = ConverterHint.LOCATOR_MEMBER_IDNAME,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.STATUS_LOCATOR__MEMBER__HELP)
  final String member,
      @CliOption(key = CliStrings.STATUS_LOCATOR__HOST,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STATUS_LOCATOR__HOST__HELP)
      final String locatorHost,
      @CliOption(key = CliStrings.STATUS_LOCATOR__PORT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STATUS_LOCATOR__PORT__HELP)
      final Integer locatorPort,
      @CliOption(key = CliStrings.STATUS_LOCATOR__PID,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STATUS_LOCATOR__PID__HELP)
      final Integer pid,
      @CliOption(key = CliStrings.STATUS_LOCATOR__DIR,
          optionContext = ConverterHint.DIR_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STATUS_LOCATOR__DIR__HELP)
      final String workingDirectory) {
    try {
      if (!StringUtils.isBlank(member)) {
        if (isConnectedAndReady()) {
          final MemberMXBean locatorProxy = getMemberMXBean(member);

          if (locatorProxy != null) {
            LocatorState state = LocatorState.fromJson(locatorProxy.status());
            return createStatusLocatorResult(state);
          } else {
            return ResultBuilder.createUserErrorResult(CliStrings.format(
                CliStrings.STATUS_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE, member));
          }
        } else {
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.STATUS_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, LOCATOR_TERM_NAME));
        }
      } else {
        final LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
            .setCommand(LocatorLauncher.Command.STATUS)
            .setBindAddress(locatorHost)
            .setDebug(isDebugging())
            .setPid(pid)
            .setPort(locatorPort)
            .setWorkingDirectory(workingDirectory)
            .build();

        final LocatorState state = locatorLauncher.status();
        return createStatusLocatorResult(state);
      }
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.STATUS_LOCATOR__GENERAL_ERROR_MESSAGE,
          getLocatorId(locatorHost, locatorPort), StringUtils.defaultIfBlank(workingDirectory, SystemUtils.CURRENT_DIRECTORY),
          toString(t, getGfsh().getDebug())));
    }
  }

  @CliCommand(value = CliStrings.STOP_LOCATOR, help = CliStrings.STOP_LOCATOR__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_LOCATOR, CliStrings.TOPIC_GEODE_LIFECYCLE })
  public Result stopLocator(@CliOption(key = CliStrings.STOP_LOCATOR__MEMBER,
      optionContext = ConverterHint.LOCATOR_MEMBER_IDNAME,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.STOP_LOCATOR__MEMBER__HELP)
  final String member,
      @CliOption(key = CliStrings.STOP_LOCATOR__PID,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STOP_LOCATOR__PID__HELP)
      final Integer pid,
      @CliOption(key = CliStrings.STOP_LOCATOR__DIR,
          optionContext = ConverterHint.DIR_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STOP_LOCATOR__DIR__HELP)
      final String workingDirectory) {
    LocatorState locatorState;

    try {
      if (!StringUtils.isBlank(member)) {
        if (isConnectedAndReady()) {
          final MemberMXBean locatorProxy = getMemberMXBean(member);

          if (locatorProxy != null) {
            if (!locatorProxy.isLocator()) {
              throw new IllegalStateException(CliStrings.format(CliStrings.STOP_LOCATOR__NOT_LOCATOR_ERROR_MESSAGE, member));
            }

            if (locatorProxy.isServer()) {
              throw new IllegalStateException(CliStrings.format(CliStrings.STOP_LOCATOR__LOCATOR_IS_CACHE_SERVER_ERROR_MESSAGE, member));
            }

            locatorState = LocatorState.fromJson(locatorProxy.status());
            locatorProxy.shutDownMember();
          } else {
            return ResultBuilder.createUserErrorResult(CliStrings.format(
                CliStrings.STOP_LOCATOR__NO_LOCATOR_FOUND_FOR_MEMBER_ERROR_MESSAGE, member));
          }
        } else {
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, LOCATOR_TERM_NAME));
        }
      } else {
        final LocatorLauncher locatorLauncher = new LocatorLauncher.Builder()
            .setCommand(LocatorLauncher.Command.STOP)
            .setDebug(isDebugging())
            .setPid(pid)
            .setWorkingDirectory(workingDirectory)
            .build();

        locatorState = locatorLauncher.status();
        locatorLauncher.stop();
      }

      if (Status.ONLINE.equals(locatorState.getStatus())) {
        getGfsh().logInfo(String.format(CliStrings.STOP_LOCATOR__STOPPING_LOCATOR_MESSAGE,
            locatorState.getWorkingDirectory(), locatorState.getServiceLocation(), locatorState.getMemberName(),
            locatorState.getPid(), locatorState.getLogFile()), null);

        StopWatch stopWatch = new StopWatch(true);
        while (isVmWithProcessIdRunning(locatorState.getPid())) {
          Gfsh.print(".");
          if (stopWatch.elapsedTimeMillis() > WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS) {
            break;
          }
          synchronized (this) {
            TimeUnit.MILLISECONDS.timedWait(this, 500);
          }
        }

        return ResultBuilder.createInfoResult(StringUtils.EMPTY_STRING);
      } else {
        return ResultBuilder.createUserErrorResult(locatorState.toString());
      }
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.STOP_LOCATOR__GENERAL_ERROR_MESSAGE,
          toString(t, getGfsh().getDebug())));
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  // TODO re-evaluate whether a MalformedObjectNameException should be thrown here; just because we were not able to find
  // the "current" Locators in order to conveniently add the new member to the GemFire cluster does not mean we should
  // throw an Exception!
  protected void addCurrentLocators(final List<String> commandLine, final Properties gemfireProperties) throws MalformedObjectNameException {
    if (StringUtils.isBlank(gemfireProperties.getProperty(LOCATORS))) {
      String currentLocators = getCurrentLocators();

      if (!StringUtils.isBlank(currentLocators)) {
        commandLine.add("-D".concat(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX)
            .concat(LOCATORS).concat("=").concat(currentLocators));
      }
    }
  }

  protected Result createStatusLocatorResult(final LocatorState state) throws NumberFormatException, IOException, ClassNotFoundException {
    InfoResultData infoResultData = ResultBuilder.createInfoResultData();
    infoResultData.addLine(state.toString());
    infoResultData.addLine(getSharedConfigurationStatusFromLocatorState(state));
    return ResultBuilder.buildResult(infoResultData);
  }

  protected void addGemFirePropertyFile(final List<String> commandLine, final String gemfirePropertiesPathname) {
    if (!StringUtils.isBlank(gemfirePropertiesPathname)) {
      commandLine.add("-DgemfirePropertyFile=" + gemfirePropertiesPathname);
    }
  }

  protected void addGemFireSecurityPropertyFile(final List<String> commandLine, final String gemfireSecurityPropertiesPathname) {
    if (!StringUtils.isBlank(gemfireSecurityPropertiesPathname)) {
      commandLine.add("-DgemfireSecurityPropertyFile=" + gemfireSecurityPropertiesPathname);
    }
  }

  protected void addGemFireSystemProperties(final List<String> commandLine, final Properties gemfireProperties) {
    for (final Object property : gemfireProperties.keySet()) {
      final String propertyName = property.toString();
      final String propertyValue = gemfireProperties.getProperty(propertyName);
      if (!StringUtils.isBlank(propertyValue)) {
        commandLine.add("-D" + DistributionConfig.GEMFIRE_PREFIX + "" + propertyName + "=" + propertyValue);
      }
    }
  }

  protected void addInitialHeap(final List<String> commandLine, final String initialHeap) {
    if (!StringUtils.isBlank(initialHeap)) {
      commandLine.add("-Xms" + initialHeap);
    }
  }

  protected void addJvmArgumentsAndOptions(final List<String> commandLine, final String[] jvmArgsOpts) {
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
      // NOTE IBM states the following IBM J9 JVM command-line option/switch has side-effects on "performance",
      // as noted in the reference documentation...
      // http://publib.boulder.ibm.com/infocenter/javasdk/v6r0/index.jsp?topic=/com.ibm.java.doc.diagnostics.60/diag/appendixes/cmdline/commands_jvm.html
      commandLine.add("-Xcheck:memory");
    } else if (SystemUtils.isJRockitVM()) {
      // NOTE the following Oracle JRockit JVM documentation was referenced to identify the appropriate JVM option to
      // set when handling OutOfMemoryErrors.
      // http://docs.oracle.com/cd/E13150_01/jrockit_jvm/jrockit/jrdocs/refman/optionXX.html
      commandLine.add("-XXexitOnOutOfMemory");
    }
  }

  protected void addMaxHeap(final List<String> commandLine, final String maxHeap) {
    if (!StringUtils.isBlank(maxHeap)) {
      commandLine.add("-Xmx" + maxHeap);
      commandLine.add("-XX:+UseConcMarkSweepGC");
      commandLine.add("-XX:CMSInitiatingOccupancyFraction=" + CMS_INITIAL_OCCUPANCY_FRACTION);
      //commandLine.add("-XX:MinHeapFreeRatio=" + MINIMUM_HEAP_FREE_RATIO);
    }
  }

  protected LocatorState locatorStatus(final File locatorPidFile, final int oldPid, final String memberName) {
    final int newPid = readPid(locatorPidFile);

    if (newPid != INVALID_PID && newPid != oldPid) {
      LocatorState locatorState = new LocatorLauncher.Builder().setPid(newPid).build().status();

      if (ObjectUtils.equals(locatorState.getMemberName(), memberName)) {
        return locatorState;
      }
    }

    return new LocatorState(new LocatorLauncher.Builder().build(), Status.NOT_RESPONDING);
  }

  protected LocatorState locatorStatus(final String workingDirectory, final String memberName) {
    LocatorState locatorState = new LocatorLauncher.Builder().setWorkingDirectory(workingDirectory).build().status();

    if (ObjectUtils.equals(locatorState.getMemberName(), memberName)) {
      return locatorState;
    }

    return new LocatorState(new LocatorLauncher.Builder().build(), Status.NOT_RESPONDING);
  }

  protected String readErrorStream(final Process process) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
    StringBuilder message = new StringBuilder();

    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
      message.append(line);
      message.append(StringUtils.LINE_SEPARATOR);
    }

    IOUtils.close(reader);

    return message.toString();
  }

  protected int readPid(final File pidFile) {
    assert pidFile != null : "The file from which to read the process ID (pid) cannot be null!";

    if (pidFile.isFile()) {
      BufferedReader fileReader = null;
      try {
        fileReader = new BufferedReader(new FileReader(pidFile));
        return Integer.parseInt(fileReader.readLine());
      } catch (IOException ignore) {
      } catch (NumberFormatException ignore) {
      } finally {
        IOUtils.close(fileReader);
      }
    }

    return INVALID_PID;
  }

  protected ServerState serverStatus(final File serverPidFile, final int oldPid, final String memberName) {
    final int newPid = readPid(serverPidFile);

    if (newPid != INVALID_PID && newPid != oldPid) {
      ServerState serverState = new ServerLauncher.Builder().setPid(newPid).setDisableDefaultServer(true)
          .build().status();

      if (ObjectUtils.equals(serverState.getMemberName(), memberName)) {
        return serverState;
      }
    }

    return new ServerState(new ServerLauncher.Builder().build(), Status.NOT_RESPONDING);
  }

  protected ServerState serverStatus(final String workingDirectory, final String memberName) {
    ServerState serverState = new ServerLauncher.Builder().setWorkingDirectory(workingDirectory)
        .setDisableDefaultServer(true).build().status();

    if (ObjectUtils.equals(serverState.getMemberName(), memberName)) {
      return serverState;
    }

    return new ServerState(new ServerLauncher.Builder().build(), Status.NOT_RESPONDING);
  }

  @Deprecated
  protected String getClasspath(final String userClasspath) {
    String classpath = getSystemClasspath();

    if (!StringUtils.isBlank(userClasspath)) {
      classpath += (File.pathSeparator + userClasspath);
    }

    return classpath;
  }

  protected String getLocatorClasspath(final boolean includeSystemClasspath, final String userClasspath) {
    return toClasspath(includeSystemClasspath, new String[] { CORE_DEPENDENCIES_JAR_PATHNAME }, userClasspath);
  }

  protected String getServerClasspath(final boolean includeSystemClasspath,
      final String userClasspath) {
    List<String> jarFilePathnames = new ArrayList<>();

    jarFilePathnames.add(CORE_DEPENDENCIES_JAR_PATHNAME);

    return toClasspath(includeSystemClasspath, jarFilePathnames.toArray(new String[jarFilePathnames.size()]),
        userClasspath);
  }

  protected String getSystemClasspath() {
    return System.getProperty("java.class.path");
  }

  String toClasspath(final boolean includeSystemClasspath, String[] jarFilePathnames, String... userClasspaths) {
    // gemfire jar must absolutely be the first JAR file on the CLASSPATH!!!
    String classpath = getGemFireJarPath();

    userClasspaths = (userClasspaths != null ? userClasspaths : StringUtils.EMPTY_STRING_ARRAY);

    // Then, include user-specified classes on CLASSPATH to enable the user to override GemFire JAR dependencies
    // with application-specific versions; this logic/block corresponds to classes/jar-files specified with the
    // --classpath option to the 'start locator' and 'start server commands'; also this will override any
    // System CLASSPATH environment variable setting, which is consistent with the Java platform behavior...
    for (String userClasspath : userClasspaths) {
      if (!StringUtils.isBlank(userClasspath)) {
        classpath += (classpath.isEmpty() ? StringUtils.EMPTY_STRING : File.pathSeparator);
        classpath += userClasspath;
      }
    }

    // Now, include any System-specified CLASSPATH environment variable setting...
    if (includeSystemClasspath) {
      classpath += File.pathSeparator;
      classpath += getSystemClasspath();
    }

    jarFilePathnames = (jarFilePathnames != null ? jarFilePathnames : StringUtils.EMPTY_STRING_ARRAY);

    // And finally, include all GemFire dependencies on the CLASSPATH...
    for (String jarFilePathname : jarFilePathnames) {
      if (!StringUtils.isBlank(jarFilePathname)) {
        classpath += (classpath.isEmpty() ? StringUtils.EMPTY_STRING : File.pathSeparator);
        classpath += jarFilePathname;
      }
    }

    return classpath;
  }

  protected String getGemFireJarPath() {
    String classpath = getSystemClasspath();
    String gemfireJarPath = GEMFIRE_JAR_PATHNAME;

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

  @Deprecated
  protected String getToolsJarPath() throws AttachAPINotFoundException {
    String toolsJarPathname = null;

    if (!SystemUtils.isMacOSX()) {
      toolsJarPathname = IOUtils.appendToPath(JAVA_HOME, "lib", "tools.jar");

      if (!IOUtils.isExistingPathname(toolsJarPathname)) {
        // perhaps the java.home System property refers to the JRE ($JAVA_HOME/jre)...
        String JDK_HOME = new File(JAVA_HOME).getParentFile().getPath();
        toolsJarPathname = IOUtils.appendToPath(JDK_HOME, "lib", "tools.jar");
      }

      try {
        IOUtils.verifyPathnameExists(toolsJarPathname);
      } catch (IOException e) {
        throw new AttachAPINotFoundException(getAttachAPINotFoundMessage());
      }
    }

    return toolsJarPathname;
  }

  // TODO refactor the following method into a common base class or utility class
  protected String getLocalHost() {
    try {
      return SocketCreator.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException ignore) {
      return LOCALHOST;
    }
  }

  protected String getAttachAPINotFoundMessage() {
    return CliStrings.format(CliStrings.ATTACH_API_IN_0_NOT_FOUND_ERROR_MESSAGE,
        ((SystemUtils.isMacOSX() && SystemUtils.isAppleJVM()) ? "classes.jar" : "tools.jar"));
  }

  protected String getLocatorId(final String host, final Integer port) {
    final String locatorHost = (host != null ? host : getLocalHost());
    final String locatorPort = StringUtils.valueOf(port, String.valueOf(DistributionLocator.DEFAULT_LOCATOR_PORT));
    return locatorHost.concat("[").concat(locatorPort).concat("]");
  }

  /**
   * Gets a proxy to the DistributedSystemMXBean from the GemFire Manager's MBeanServer, or null if unable to find
   * the DistributedSystemMXBean.
   * </p>
   *
   * @return a proxy to the DistributedSystemMXBean from the GemFire Manager's MBeanServer, or null if unable to find
   * the DistributedSystemMXBean.
   */
  protected DistributedSystemMXBean getDistributedSystemMXBean() throws IOException, MalformedObjectNameException {
    assertState(isConnectedAndReady(), "Gfsh must be connected in order to get proxy to a GemFire DistributedSystemMXBean.");
    return getGfsh().getOperationInvoker().getDistributedSystemMXBean();
  }

  /**
   * Gets a proxy to the MemberMXBean for the GemFire member specified by member name or ID from the GemFire Manager's
   * MBeanServer.
   * </p>
   *
   * @param member a String indicating the GemFire member's name or ID.
   * @return a proxy to the MemberMXBean having the specified GemFire member's name or ID from the GemFire Manager's
   * MBeanServer, or null if no GemFire member could be found with the specified member name or ID.
   * @see #getMemberMXBean(String, String)
   */
  protected MemberMXBean getMemberMXBean(final String member) throws IOException {
    return getMemberMXBean(null, member);
  }

  protected MemberMXBean getMemberMXBean(final String serviceName, final String member) throws IOException {
    assertState(isConnectedAndReady(), "Gfsh must be connected in order to get proxy to a GemFire Member MBean.");

    MemberMXBean memberBean = null;

    try {
      String objectNamePattern = ManagementConstants.OBJECTNAME__PREFIX;

      objectNamePattern += (StringUtils.isBlank(serviceName) ? StringUtils.EMPTY_STRING
          : "service=" + serviceName + StringUtils.COMMA_DELIMITER);
      objectNamePattern += "type=Member,*";

      // NOTE throws a MalformedObjectNameException, however, this should not happen since the ObjectName is constructed
      // here in a conforming pattern
      final ObjectName objectName = ObjectName.getInstance(objectNamePattern);

      final QueryExp query = Query.or(
          Query.eq(Query.attr("Name"), Query.value(member)),
          Query.eq(Query.attr("Id"), Query.value(member))
      );

      final Set<ObjectName> memberObjectNames = getGfsh().getOperationInvoker().queryNames(objectName, query);

      if (!memberObjectNames.isEmpty()) {
        memberBean = getGfsh().getOperationInvoker().getMBeanProxy(memberObjectNames.iterator().next(), MemberMXBean.class);
      }
    } catch (MalformedObjectNameException e) {
      getGfsh().logSevere(e.getMessage(), e);
    }

    return memberBean;
  }

  protected String getServerId(final String host, final Integer port) {
    String serverHost = (host != null ? host : getLocalHost());
    String serverPort = StringUtils.valueOf(port, String.valueOf(CacheServer.DEFAULT_PORT));
    return serverHost.concat("[").concat(serverPort).concat("]");
  }

  protected boolean isStartingNotRespondingOrNull(final ServiceState serviceState) {
    return (serviceState == null || isStartingOrNotResponding(serviceState.getStatus()));
  }

  protected boolean isStartingOrNotResponding(final Status processStatus) {
    return (Status.NOT_RESPONDING.equals(processStatus) || Status.STARTING.equals(processStatus));
  }

  protected boolean isVmWithProcessIdRunning(final Integer pid) {
    // note: this will use JNA if available or Attach if available or return false if neither is available
    return ProcessUtils.isProcessAlive(pid);
  }

  @CliCommand(value = CliStrings.START_SERVER, help = CliStrings.START_SERVER__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_LIFECYCLE })
  public Result startServer(@CliOption(key = CliStrings.START_SERVER__ASSIGN_BUCKETS,
      unspecifiedDefaultValue = "false",
      specifiedDefaultValue = "true",
      help = CliStrings.START_SERVER__ASSIGN_BUCKETS__HELP)
  final Boolean assignBuckets,
      @CliOption(key = CliStrings.START_SERVER__BIND_ADDRESS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__BIND_ADDRESS__HELP)
      final String bindAddress,
      @CliOption(key = CliStrings.START_SERVER__CACHE_XML_FILE,
          optionContext = ConverterHint.FILE_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__CACHE_XML_FILE__HELP)
          String cacheXmlPathname,
      @CliOption(key = CliStrings.START_SERVER__CLASSPATH,
                                      /*optionContext = ConverterHint.FILE_PATHSTRING, // there's an issue with TAB here*/
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__CLASSPATH__HELP)
      final String classpath,
      @CliOption(key = CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__CRITICAL__HEAP__HELP)
      final Float criticalHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__CRITICAL_OFF_HEAP__HELP)
      final Float criticalOffHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__DIR,
          optionContext = ConverterHint.DIR_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__DIR__HELP)
          String workingDirectory,
      @CliOption(key = CliStrings.START_SERVER__DISABLE_DEFAULT_SERVER,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__DISABLE_DEFAULT_SERVER__HELP)
      final Boolean disableDefaultServer,
      @CliOption(key = CliStrings.START_SERVER__DISABLE_EXIT_WHEN_OUT_OF_MEMORY,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__DISABLE_EXIT_WHEN_OUT_OF_MEMORY_HELP)
      final Boolean disableExitWhenOutOfMemory,
      @CliOption(key = CliStrings.START_SERVER__ENABLE_TIME_STATISTICS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__ENABLE_TIME_STATISTICS__HELP)
      final Boolean enableTimeStatistics,
      @CliOption(key = CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE__HELP)
      final Float evictionHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE__HELP)
      final Float evictionOffHeapPercentage,
      @CliOption(key = CliStrings.START_SERVER__FORCE,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__FORCE__HELP)
      final Boolean force,
      @CliOption(key = CliStrings.START_SERVER__GROUP,
          optionContext = ConverterHint.MEMBERGROUP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__GROUP__HELP)
      final String group,
      @CliOption(key = CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS__HELP)
      final String hostNameForClients,
      @CliOption(key = CliStrings.START_SERVER__INCLUDE_SYSTEM_CLASSPATH,
          specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_SERVER__INCLUDE_SYSTEM_CLASSPATH__HELP)
      final Boolean includeSystemClasspath,
      @CliOption(key = CliStrings.START_SERVER__INITIAL_HEAP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__INITIAL_HEAP__HELP)
      final String initialHeap,
      @CliOption(key = CliStrings.START_SERVER__J,
          optionContext = ConverterHint.STRING_LIST,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__J__HELP)
      @CliMetaData(valueSeparator = ",")
      final String[] jvmArgsOpts,
      @CliOption(key = CliStrings.START_SERVER__LOCATORS,
          optionContext = ConverterHint.LOCATOR_DISCOVERY_CONFIG,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__LOCATORS__HELP)
      final String locators,
      @CliOption(key = CliStrings.START_SERVER__LOCATOR_WAIT_TIME,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__LOCATOR_WAIT_TIME_HELP)
      final Integer locatorWaitTime,
      @CliOption(key = CliStrings.START_SERVER__LOCK_MEMORY,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__LOCK_MEMORY__HELP)
      final Boolean lockMemory,
      @CliOption(key = CliStrings.START_SERVER__LOG_LEVEL,
          optionContext = ConverterHint.LOG_LEVEL,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__LOG_LEVEL__HELP)
      final String logLevel,
      @CliOption(key = CliStrings.START_SERVER__MAX__CONNECTIONS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MAX__CONNECTIONS__HELP)
      final Integer maxConnections,
      @CliOption(key = CliStrings.START_SERVER__MAXHEAP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MAXHEAP__HELP)
      final String maxHeap,
      @CliOption(key = CliStrings.START_SERVER__MAX__MESSAGE__COUNT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MAX__MESSAGE__COUNT__HELP)
      final Integer maxMessageCount,
      @CliOption(key = CliStrings.START_SERVER__MAX__THREADS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MAX__THREADS__HELP)
      final Integer maxThreads,
      @CliOption(key = CliStrings.START_SERVER__MCAST_ADDRESS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MCAST_ADDRESS__HELP)
      final String mcastBindAddress,
      @CliOption(key = CliStrings.START_SERVER__MCAST_PORT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MCAST_PORT__HELP)
      final Integer mcastPort,
      @CliOption(key = CliStrings.START_SERVER__MEMCACHED_PORT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MEMCACHED_PORT__HELP)
      final Integer memcachedPort,
      @CliOption(key = CliStrings.START_SERVER__MEMCACHED_PROTOCOL,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MEMCACHED_PROTOCOL__HELP)
      final String memcachedProtocol,
      @CliOption(key = CliStrings.START_SERVER__MEMCACHED_BIND_ADDRESS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MEMCACHED_BIND_ADDRESS__HELP)
      final String memcachedBindAddress,
      @CliOption(key = CliStrings.START_SERVER__REDIS_PORT,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__REDIS_PORT__HELP)
      final Integer redisPort,
      @CliOption(key = CliStrings.START_SERVER__REDIS_BIND_ADDRESS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__REDIS_BIND_ADDRESS__HELP)
      final String redisBindAddress,
      @CliOption(key = CliStrings.START_SERVER__REDIS_PASSWORD,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__REDIS_PASSWORD__HELP)
      final String redisPassword,
      @CliOption(key = CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE__HELP)
      final Integer messageTimeToLive,
      @CliOption(key = CliStrings.START_SERVER__NAME,
          mandatory = true,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__NAME__HELP)
      final String memberName,
      @CliOption(key = CliStrings.START_SERVER__OFF_HEAP_MEMORY_SIZE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__OFF_HEAP_MEMORY_SIZE__HELP)
      final String offHeapMemorySize,
      @CliOption(key = CliStrings.START_SERVER__PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__PROPERTIES__HELP)
          String gemfirePropertiesPathname,
      @CliOption(key = CliStrings.START_SERVER__REBALANCE,
          unspecifiedDefaultValue = "false",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__REBALANCE__HELP)
      final Boolean rebalance,
      @CliOption(key = CliStrings.START_SERVER__SECURITY_PROPERTIES,
          optionContext = ConverterHint.FILE_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__SECURITY_PROPERTIES__HELP)
          String gemfireSecurityPropertiesPathname,
      @CliOption(key = CliStrings.START_SERVER__SERVER_BIND_ADDRESS,
          unspecifiedDefaultValue = CacheServer.DEFAULT_BIND_ADDRESS,
          help = CliStrings.START_SERVER__SERVER_BIND_ADDRESS__HELP)
      final String serverBindAddress,
      @CliOption(key = CliStrings.START_SERVER__SERVER_PORT,
          unspecifiedDefaultValue = ("" + CacheServer.DEFAULT_PORT),
          help = CliStrings.START_SERVER__SERVER_PORT__HELP)
      final Integer serverPort,
      @CliOption(key = CliStrings.START_SERVER__SOCKET__BUFFER__SIZE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__SOCKET__BUFFER__SIZE__HELP)
      final Integer socketBufferSize,
      @CliOption(key = CliStrings.START_SERVER__SPRING_XML_LOCATION,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__SPRING_XML_LOCATION_HELP)
      final String springXmlLocation,
      @CliOption(key = CliStrings.START_SERVER__STATISTIC_ARCHIVE_FILE,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_SERVER__STATISTIC_ARCHIVE_FILE__HELP)
      final String statisticsArchivePathname,
      @CliOption(key = CliStrings.START_SERVER__USE_CLUSTER_CONFIGURATION,
          unspecifiedDefaultValue = "true",
          specifiedDefaultValue = "true",
          help = CliStrings.START_SERVER__USE_CLUSTER_CONFIGURATION__HELP)
      final Boolean requestSharedConfiguration)
  // NOTICE: keep the parameters in alphabetical order based on their CliStrings.START_SERVER_* text
  {

    try {
      if (workingDirectory == null) {
        // attempt to use or make sub-directory using memberName...
        File serverWorkingDirectory = new File(memberName);

        if (!(serverWorkingDirectory.exists() || serverWorkingDirectory.mkdir())) {
          throw new IllegalStateException(CliStrings.format(CliStrings.START_SERVER__MSG__COULD_NOT_CREATE_DIRECTORY_0_VERIFY_PERMISSIONS,
              serverWorkingDirectory.getAbsolutePath()));
        }

        workingDirectory = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(serverWorkingDirectory);
      }

      cacheXmlPathname = CliUtil.resolvePathname(cacheXmlPathname);

      if (!StringUtils.isBlank(cacheXmlPathname) && !IOUtils.isExistingPathname(cacheXmlPathname)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.CACHE_XML_NOT_FOUND_MESSAGE, cacheXmlPathname));
      }

      gemfirePropertiesPathname = CliUtil.resolvePathname(gemfirePropertiesPathname);

      if (!StringUtils.isBlank(gemfirePropertiesPathname) && !IOUtils.isExistingPathname(gemfirePropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
            StringUtils.EMPTY_STRING, gemfirePropertiesPathname));
      }

      gemfireSecurityPropertiesPathname = CliUtil.resolvePathname(gemfireSecurityPropertiesPathname);

      if (!StringUtils.isBlank(gemfireSecurityPropertiesPathname) && !IOUtils.isExistingPathname(gemfireSecurityPropertiesPathname)) {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.GEODE_0_PROPERTIES_1_NOT_FOUND_MESSAGE,
            "Security ", gemfireSecurityPropertiesPathname));
      }

      File serverPidFile = new File(workingDirectory, ProcessType.SERVER.getPidFileName());

      final int oldPid = readPid(serverPidFile);

      Properties gemfireProperties = new Properties();

      gemfireProperties.setProperty(BIND_ADDRESS, StringUtils.valueOf(bindAddress, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(CACHE_XML_FILE, StringUtils.valueOf(cacheXmlPathname, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(ENABLE_TIME_STATISTICS, StringUtils.valueOf(enableTimeStatistics, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(GROUPS, StringUtils.valueOf(group, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(LOCATORS, StringUtils.valueOf(locators, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(LOCATOR_WAIT_TIME, StringUtils.valueOf(locatorWaitTime, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(LOG_LEVEL, StringUtils.valueOf(logLevel, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(MCAST_ADDRESS, StringUtils.valueOf(mcastBindAddress, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(MCAST_PORT, StringUtils.valueOf(mcastPort, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(MEMCACHED_PORT, StringUtils.valueOf(memcachedPort, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(MEMCACHED_PROTOCOL, StringUtils.valueOf(memcachedProtocol, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(MEMCACHED_BIND_ADDRESS, StringUtils.valueOf(memcachedBindAddress, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(REDIS_PORT, StringUtils.valueOf(redisPort, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(REDIS_BIND_ADDRESS, StringUtils.valueOf(redisBindAddress, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(REDIS_PASSWORD, StringUtils.valueOf(redisPassword, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(STATISTIC_ARCHIVE_FILE, StringUtils.valueOf(statisticsArchivePathname, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(USE_CLUSTER_CONFIGURATION, StringUtils.valueOf(requestSharedConfiguration, Boolean.TRUE.toString()));
      gemfireProperties.setProperty(LOCK_MEMORY, StringUtils.valueOf(lockMemory, StringUtils.EMPTY_STRING));
      gemfireProperties.setProperty(OFF_HEAP_MEMORY_SIZE, StringUtils.valueOf(offHeapMemorySize, StringUtils.EMPTY_STRING));

      // read the OSProcess enable redirect system property here -- TODO: replace with new GFSH argument
      final boolean redirectOutput = Boolean.getBoolean(OSProcess.ENABLE_OUTPUT_REDIRECTION_PROPERTY);

      ServerLauncher serverLauncher = new ServerLauncher.Builder()
          .setAssignBuckets(assignBuckets)
          .setDisableDefaultServer(disableDefaultServer)
          .setForce(force)
          .setMemberName(memberName)
          .setRebalance(rebalance)
          .setRedirectOutput(redirectOutput)
          .setServerBindAddress(serverBindAddress)
          .setServerPort(serverPort)
          .setSpringXmlLocation(springXmlLocation)
          .setWorkingDirectory(workingDirectory)
          .setCriticalHeapPercentage(criticalHeapPercentage)
          .setEvictionHeapPercentage(evictionHeapPercentage)
          .setCriticalOffHeapPercentage(criticalOffHeapPercentage)
          .setEvictionOffHeapPercentage(evictionOffHeapPercentage)
          .setMaxConnections(maxConnections)
          .setMaxMessageCount(maxMessageCount)
          .setMaxThreads(maxThreads)
          .setMessageTimeToLive(messageTimeToLive)
          .setSocketBufferSize(socketBufferSize)
          .setHostNameForClients(hostNameForClients)
          .build();

      String[] serverCommandLine = createStartServerCommandLine(serverLauncher, gemfirePropertiesPathname,
          gemfireSecurityPropertiesPathname, gemfireProperties, classpath, includeSystemClasspath, jvmArgsOpts,
          disableExitWhenOutOfMemory, initialHeap, maxHeap);

      if (getGfsh().getDebug()) {
        getGfsh().logInfo(StringUtils.concat(serverCommandLine, " "), null);
      }

      Process serverProcess = new ProcessBuilder(serverCommandLine)
          .directory(new File(serverLauncher.getWorkingDirectory()))
          .start();

      serverProcess.getInputStream().close();
      serverProcess.getOutputStream().close();

      // fix TRAC bug #51967 by using NON_BLOCKING on Windows
      final ReadingMode readingMode = SystemUtils.isWindows() ? ReadingMode.NON_BLOCKING : ReadingMode.BLOCKING;

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
          .inputStream(serverProcess.getErrorStream())
          .inputListener(inputListener)
          .readingMode(readingMode)
          .continueReadingMillis(2 * 1000)
          .build()
          .start();

      ServerState serverState;

      String previousServerStatusMessage = null;

      LauncherSignalListener serverSignalListener = new LauncherSignalListener();

      final boolean registeredServerSignalListener = getGfsh().getSignalHandler().registerListener(serverSignalListener);

      try {
        getGfsh().logInfo(String.format(CliStrings.START_SERVER__RUN_MESSAGE,
            IOUtils.tryGetCanonicalPathElseGetAbsolutePath(new File(serverLauncher.getWorkingDirectory()))), null);

        do {
          try {
            final int exitValue = serverProcess.exitValue();

            stderrReader.join(PROCESS_STREAM_READER_JOIN_TIMEOUT_MILLIS); // was Long.MAX_VALUE

            //Gfsh.println(message);

            return ResultBuilder.createShellClientErrorResult(String.format(
                CliStrings.START_SERVER__PROCESS_TERMINATED_ABNORMALLY_ERROR_MESSAGE,
                exitValue, serverLauncher.getWorkingDirectory(), message.toString()));
          } catch (IllegalThreadStateException ignore) {
            // the IllegalThreadStateException is expected; it means the Server's process has not terminated,
            // and should not
            Gfsh.print(".");

            synchronized (this) {
              TimeUnit.MILLISECONDS.timedWait(this, 500);
            }

            serverState = (ProcessUtils.isAttachApiAvailable() ? serverStatus(serverPidFile, oldPid, memberName)
                : serverStatus(workingDirectory, memberName));

            String currentServerStatusMessage = serverState.getStatusMessage();

            if (isStartingOrNotResponding(serverState.getStatus())
                && !(StringUtils.isBlank(currentServerStatusMessage)
                || currentServerStatusMessage.equalsIgnoreCase(previousServerStatusMessage))) {
              Gfsh.println();
              Gfsh.println(currentServerStatusMessage);
              previousServerStatusMessage = currentServerStatusMessage;
            }
          }
        }
        while (!(registeredServerSignalListener && serverSignalListener.isSignaled())
            && isStartingOrNotResponding(serverState.getStatus()));
      } finally {
        stderrReader.stopAsync(PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS); // stop will close ErrorStream
        getGfsh().getSignalHandler().unregisterListener(serverSignalListener);
      }

      Gfsh.println();

      final boolean asyncStart = isStartingNotRespondingOrNull(serverState);

      if (asyncStart) { // async start
        Gfsh.print(String.format(CliStrings.ASYNC_PROCESS_LAUNCH_MESSAGE, SERVER_TERM_NAME));
        return ResultBuilder.createInfoResult("");
      } else {
        return ResultBuilder.createInfoResult(serverState.toString());
      }
    } catch (IllegalArgumentException e) {
      String message = e.getMessage();
      if (message != null && message.matches(LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toLocalizedString(".+"))) {
        message = CliStrings.format(CliStrings.LAUNCHERLIFECYCLECOMMANDS__MSG__FAILED_TO_START_0_REASON_1, SERVER_TERM_NAME, message);
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
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.START_SERVER__GENERAL_ERROR_MESSAGE,
          toString(t, getGfsh().getDebug())));
    }
  }

  protected String[] createStartServerCommandLine(final ServerLauncher launcher,
      final String gemfirePropertiesPathname,
      final String gemfireSecurityPropertiesPathname,
      final Properties gemfireProperties,
      final String userClasspath,
      final Boolean includeSystemClasspath,
      final String[] jvmArgsOpts,
      final Boolean disableExitWhenOutOfMemory,
      final String initialHeap,
      final String maxHeap)
      throws MalformedObjectNameException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine.add(getServerClasspath(Boolean.TRUE.equals(includeSystemClasspath),
        userClasspath));

    addCurrentLocators(commandLine, gemfireProperties);
    addGemFirePropertyFile(commandLine, gemfirePropertiesPathname);
    addGemFireSecurityPropertyFile(commandLine, gemfireSecurityPropertiesPathname);
    addGemFireSystemProperties(commandLine, gemfireProperties);
    addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);

    // NOTE asserting not equal to true rather than equal to false handles the null case and ensures the user
    // explicitly specified the command-line option in order to disable JVM memory checks.
    if (!Boolean.TRUE.equals(disableExitWhenOutOfMemory)) {
      addJvmOptionsForOutOfMemoryErrors(commandLine);
    }

    addInitialHeap(commandLine, initialHeap);
    addMaxHeap(commandLine, maxHeap);

    commandLine.add("-D".concat(AbstractLauncher.SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY.concat("=true")));
    commandLine.add("-Djava.awt.headless=true");
    commandLine.add("-Dsun.rmi.dgc.server.gcInterval".concat("=").concat(Long.toString(Long.MAX_VALUE - 1)));

    commandLine.add(ServerLauncher.class.getName());
    commandLine.add(ServerLauncher.Command.START.getName());

    if (!StringUtils.isBlank(launcher.getMemberName())) {
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
      commandLine.add("--server-bind-address=" + launcher.getServerBindAddress().getCanonicalHostName());
    }

    if (launcher.getServerPort() != null) {
      commandLine.add("--server-port=" + launcher.getServerPort());
    }

    if (launcher.isSpringXmlLocationSpecified()) {
      commandLine.add("--spring-xml-location=".concat(launcher.getSpringXmlLocation()));
    }

    if (launcher.getCriticalHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE + "=" + launcher.getCriticalHeapPercentage());
    }

    if (launcher.getEvictionHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE + "=" + launcher.getEvictionHeapPercentage());
    }

    if (launcher.getCriticalOffHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE + "=" + launcher.getCriticalOffHeapPercentage());
    }

    if (launcher.getEvictionOffHeapPercentage() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE + "=" + launcher.getEvictionOffHeapPercentage());
    }

    if (launcher.getMaxConnections() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__MAX__CONNECTIONS + "=" + launcher.getMaxConnections());
    }

    if (launcher.getMaxMessageCount() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__MAX__MESSAGE__COUNT + "=" + launcher.getMaxMessageCount());
    }

    if (launcher.getMaxThreads() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__MAX__THREADS + "=" + launcher.getMaxThreads());
    }

    if (launcher.getMessageTimeToLive() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE + "=" + launcher.getMessageTimeToLive());
    }

    if (launcher.getSocketBufferSize() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__SOCKET__BUFFER__SIZE + "=" + launcher.getSocketBufferSize());
    }

    if (launcher.getHostNameForClients() != null) {
      commandLine.add("--" + CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS + "=" + launcher.getHostNameForClients());
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

  @CliCommand(value = CliStrings.STATUS_SERVER, help = CliStrings.STATUS_SERVER__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_LIFECYCLE })
  public Result statusServer(@CliOption(key = CliStrings.STATUS_SERVER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.STATUS_SERVER__MEMBER__HELP)
  final String member,
      @CliOption(key = CliStrings.STATUS_SERVER__PID,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STATUS_SERVER__PID__HELP)
      final Integer pid,
      @CliOption(key = CliStrings.STATUS_SERVER__DIR,
          optionContext = ConverterHint.DIR_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STATUS_SERVER__DIR__HELP)
      final String workingDirectory) {
    try {
      if (!StringUtils.isBlank(member)) {
        if (isConnectedAndReady()) {
          final MemberMXBean serverProxy = getMemberMXBean(member);

          if (serverProxy != null) {
            return ResultBuilder.createInfoResult(ServerState.fromJson(serverProxy.status()).toString());
          } else {
            return ResultBuilder.createUserErrorResult(CliStrings.format(
                CliStrings.STATUS_SERVER__NO_SERVER_FOUND_FOR_MEMBER_ERROR_MESSAGE, member));
          }
        } else {
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.STATUS_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Cache Server"));
        }
      } else {
        final ServerLauncher serverLauncher = new ServerLauncher.Builder()
            .setCommand(ServerLauncher.Command.STATUS)
            .setDebug(isDebugging())
            // NOTE since we do not know whether the "CacheServer" was enabled or not on the GemFire server when it was started,
            // set the disableDefaultServer property in the ServerLauncher.Builder to default status to the MemberMBean
            // TODO fix this hack! (how, the 'start server' loop needs it)
            .setDisableDefaultServer(true)
            .setMemberName(member)
            .setPid(pid)
            .setWorkingDirectory(workingDirectory)
            .build();

        final ServerState status = serverLauncher.status();

        return ResultBuilder.createInfoResult(status.toString());
      }
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.STATUS_SERVER__GENERAL_ERROR_MESSAGE,
          toString(t, getGfsh().getDebug())));
    }
  }

  @CliCommand(value = CliStrings.STOP_SERVER, help = CliStrings.STOP_SERVER__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_SERVER, CliStrings.TOPIC_GEODE_LIFECYCLE })
  public Result stopServer(@CliOption(key = CliStrings.STOP_SERVER__MEMBER,
      optionContext = ConverterHint.MEMBERIDNAME,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.STOP_SERVER__MEMBER__HELP)
  final String member,
      @CliOption(key = CliStrings.STOP_SERVER__PID,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STOP_SERVER__PID__HELP)
      final Integer pid,
      @CliOption(key = CliStrings.STOP_SERVER__DIR,
          optionContext = ConverterHint.DIR_PATHSTRING,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.STOP_SERVER__DIR__HELP)
      final String workingDirectory) {
    ServerState serverState;

    try {
      if (!StringUtils.isBlank(member)) {
        if (isConnectedAndReady()) {
          final MemberMXBean serverProxy = getMemberMXBean(member);

          if (serverProxy != null) {
            if (!serverProxy.isServer()) {
              throw new IllegalStateException(CliStrings.format(CliStrings.STOP_SERVER__MEMBER_IS_NOT_SERVER_ERROR_MESSAGE, member));
            }

            serverState = ServerState.fromJson(serverProxy.status());
            serverProxy.shutDownMember();
          } else {
            return ResultBuilder.createUserErrorResult(CliStrings.format(
                CliStrings.STOP_SERVER__NO_SERVER_FOUND_FOR_MEMBER_ERROR_MESSAGE, member));
          }
        } else {
          return ResultBuilder.createUserErrorResult(CliStrings.format(
              CliStrings.STOP_SERVICE__GFSH_NOT_CONNECTED_ERROR_MESSAGE, "Cache Server"));
        }
      } else {
        final ServerLauncher serverLauncher = new ServerLauncher.Builder()
            .setCommand(ServerLauncher.Command.STOP)
            .setDebug(isDebugging())
            .setMemberName(member)
            .setPid(pid)
            .setWorkingDirectory(workingDirectory)
            .build();

        serverState = serverLauncher.status();
        serverLauncher.stop();
      }

      if (Status.ONLINE.equals(serverState.getStatus())) {
        getGfsh().logInfo(String.format(CliStrings.STOP_SERVER__STOPPING_SERVER_MESSAGE,
            serverState.getWorkingDirectory(), serverState.getServiceLocation(), serverState.getMemberName(),
            serverState.getPid(), serverState.getLogFile()), null);

        StopWatch stopWatch = new StopWatch(true);
        while (isVmWithProcessIdRunning(serverState.getPid())) {
          Gfsh.print(".");
          if (stopWatch.elapsedTimeMillis() > WAITING_FOR_STOP_TO_MAKE_PID_GO_AWAY_TIMEOUT_MILLIS) {
            break;
          }
          synchronized (this) {
            TimeUnit.MILLISECONDS.timedWait(this, 500);
          }
        }

        return ResultBuilder.createInfoResult(StringUtils.EMPTY_STRING);
      } else {
        return ResultBuilder.createUserErrorResult(serverState.toString());
      }
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.STOP_SERVER__GENERAL_ERROR_MESSAGE,
          toString(t, getGfsh().getDebug())));
    } finally {
      Gfsh.redirectInternalJavaLoggers();
    }
  }

  //@CliCommand(value=CliStrings.START_MANAGER, help=CliStrings.START_MANAGER__HELP)
  //@CliMetaData(shellOnly=true, relatedTopic = {CliStrings.TOPIC_GEODE_MANAGER, CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_LIFECYCLE})
  public Result startManager(@CliOption(key = CliStrings.START_MANAGER__MEMBERNAME,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.START_MANAGER__MEMBERNAME__HELP)
      String memberName,
      @CliOption(key = CliStrings.START_MANAGER__DIR,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_MANAGER__DIR__HELP)
          String dir,
      @CliOption(key = CliStrings.START_MANAGER__PORT,
          unspecifiedDefaultValue = "1099",
          help = CliStrings.START_MANAGER__PORT__HELP)
          int cacheServerPort,
      @CliOption(key = CliStrings.START_MANAGER__BIND_ADDRESS,
          unspecifiedDefaultValue = "localhost",
          help = CliStrings.START_MANAGER__BIND_ADDRESS__HELP)
          String cacheServerHost,
      @CliOption(key = CliStrings.START_MANAGER__CLASSPATH,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_MANAGER__CLASSPATH__HELP)
          String classpath,
      @CliOption(key = CliStrings.START_MANAGER__MAXHEAP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_MANAGER__MAXHEAP__HELP)
          String maxHeap,
      @CliOption(key = CliStrings.START_MANAGER__INITIALHEAP,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_MANAGER__INITIALHEAP__HELP)
          String initialHeap,
      @CliOption(key = CliStrings.START_MANAGER__J,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_MANAGER__J__HELP)
          Map<String, String> systepProps,
      @CliOption(key = CliStrings.START_MANAGER__GEODEPROPS,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_MANAGER__GEODEPROPS__HELP)
          Map<String, String> gemfireProps) {
    return ResultBuilder.createInfoResult("Not-implemented");
  }

  @CliCommand(value = CliStrings.START_JCONSOLE, help = CliStrings.START_JCONSOLE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_MANAGER, CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M })
  public Result startJConsole(@CliOption(key = CliStrings.START_JCONSOLE__INTERVAL,
      unspecifiedDefaultValue = "4",
      help = CliStrings.START_JCONSOLE__INTERVAL__HELP)
  final int interval,
      @CliOption(key = CliStrings.START_JCONSOLE__NOTILE,
          specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_JCONSOLE__NOTILE__HELP)
      final boolean notile,
      @CliOption(key = CliStrings.START_JCONSOLE__PLUGINPATH,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_JCONSOLE__PLUGINPATH__HELP)
      final String pluginpath,
      @CliOption(key = CliStrings.START_JCONSOLE__VERSION,
          specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false",
          help = CliStrings.START_JCONSOLE__VERSION__HELP)
      final boolean version,
      @CliOption(key = CliStrings.START_JCONSOLE__J,
          optionContext = ConverterHint.STRING_LIST,
          unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
          help = CliStrings.START_JCONSOLE__J__HELP)
      @CliMetaData(valueSeparator = ",")
      final List<String> jvmArgs) {
    try {
      String[] jconsoleCommandLine = createJConsoleCommandLine(null, interval, notile, pluginpath, version, jvmArgs);

      if (isDebugging()) {
        getGfsh().printAsInfo(String.format("JConsole command-line ($1%s)", Arrays.toString(jconsoleCommandLine)));
      }

      Process jconsoleProcess = Runtime.getRuntime().exec(jconsoleCommandLine);

      StringBuilder message = new StringBuilder();

      if (version) {
        jconsoleProcess.waitFor();

        BufferedReader reader = new BufferedReader(new InputStreamReader(jconsoleProcess.getErrorStream()));

        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          message.append(line);
          message.append(StringUtils.LINE_SEPARATOR);
        }

        IOUtils.close(reader);
      } else {
        getGfsh().printAsInfo(CliStrings.START_JCONSOLE__RUN);

        String jconsoleProcessOutput = waitAndCaptureProcessStandardErrorStream(jconsoleProcess);

        if (!StringUtils.isBlank(jconsoleProcessOutput)) {
          message.append(StringUtils.LINE_SEPARATOR);
          message.append(jconsoleProcessOutput);
        }
      }

      return ResultBuilder.createInfoResult(message.toString());
    } catch (GemFireException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IOException e) {
      return ResultBuilder.createShellClientErrorResult(CliStrings.START_JCONSOLE__IO_EXCEPTION_MESSAGE);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.START_JCONSOLE__CATCH_ALL_ERROR_MESSAGE,
          toString(t, false)));
    }
  }

  protected String[] createJConsoleCommandLine(final String member,
      final int interval,
      final boolean notile,
      final String pluginpath,
      final boolean version,
      final List<String> jvmArgs) {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getJConsolePathname());

    if (version) {
      commandLine.add("-version");
    } else {
      commandLine.add("-interval=" + interval);

      if (notile) {
        commandLine.add("-notile");
      }

      if (!StringUtils.isBlank(pluginpath)) {
        commandLine.add("-pluginpath " + pluginpath);
      }

      if (jvmArgs != null) {
        for (final String arg : jvmArgs) {
          commandLine.add("-J" + arg);
        }
      }

      String jmxServiceUrl = getJmxServiceUrlAsString(member);

      if (!StringUtils.isBlank(jmxServiceUrl)) {
        commandLine.add(jmxServiceUrl);
      }
    }

    return commandLine.toArray(new String[commandLine.size()]);
  }

  protected String getJConsolePathname() {
    return getJdkToolPathname("jconsole" + getExecutableSuffix(),
        new JConsoleNotFoundException(CliStrings.START_JCONSOLE__NOT_FOUND_ERROR_MESSAGE));
  }

  protected String getJdkToolPathname(final String jdkToolExecutableName, final GemFireException throwable) {
    assertNotNull(jdkToolExecutableName, "The JDK tool executable name cannot be null!");
    assertNotNull(throwable, "The GemFireException cannot be null!");

    Stack<String> pathnames = new Stack<>();

    pathnames.push(jdkToolExecutableName);
    pathnames.push(IOUtils.appendToPath(System.getenv("JAVA_HOME"), "..", "bin", jdkToolExecutableName));
    pathnames.push(IOUtils.appendToPath(System.getenv("JAVA_HOME"), "bin", jdkToolExecutableName));
    pathnames.push(IOUtils.appendToPath(JAVA_HOME, "..", "bin", jdkToolExecutableName));
    pathnames.push(IOUtils.appendToPath(JAVA_HOME, "bin", jdkToolExecutableName));

    return getJdkToolPathname(pathnames, throwable);
  }

  protected String getJdkToolPathname(final Stack<String> pathnames, final GemFireException throwable) {
    assertNotNull(pathnames, "The JDK tool executable pathnames cannot be null!");
    assertNotNull(throwable, "The GemFireException cannot be null!");

    try {
      // assume 'java.home' JVM System property refers to the JDK installation directory.  note, however, that the
      // 'java.home' JVM System property usually refers to the JRE used to launch this application
      return IOUtils.verifyPathnameExists(pathnames.pop());
    } catch (EmptyStackException ignore) {
      throw throwable;
    } catch (FileNotFoundException ignore) {
      return getJdkToolPathname(pathnames, throwable);
    }
  }

  protected static String getExecutableSuffix() {
    return SystemUtils.isWindows() ? ".exe" : StringUtils.EMPTY_STRING;
  }

  protected String getJmxServiceUrlAsString(final String member) {
    if (!StringUtils.isBlank(member)) {
      ConnectionEndpointConverter converter = new ConnectionEndpointConverter();

      try {
        ConnectionEndpoint connectionEndpoint = converter.convertFromText(member, ConnectionEndpoint.class, null);

        return StringUtils.concat("service:jmx:rmi://", connectionEndpoint.getHost(), ":", connectionEndpoint.getPort(),
            "/jndi/rmi://", connectionEndpoint.getHost(), ":", connectionEndpoint.getPort(), "/jmxrmi");
      } catch (Exception e) {
        throw new IllegalArgumentException(CliStrings.START_JCONSOLE__CONNECT_BY_MEMBER_NAME_ID_ERROR_MESSAGE);
      }
    } else {
      if (isConnectedAndReady() && (getGfsh().getOperationInvoker() instanceof JmxOperationInvoker)) {
        JmxOperationInvoker jmxOperationInvoker = (JmxOperationInvoker) getGfsh().getOperationInvoker();

        return ObjectUtils.toString(jmxOperationInvoker.getJmxServiceUrl());
      }
    }

    return null;
  }

  @CliCommand(value = CliStrings.START_JVISUALVM, help = CliStrings.START_JVISUALVM__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_MANAGER, CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M })
  public Result startJVisualVM(@CliOption(key = CliStrings.START_JCONSOLE__J,
      optionContext = ConverterHint.STRING_LIST,
      unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE,
      help = CliStrings.START_JCONSOLE__J__HELP)
  @CliMetaData(valueSeparator = ",")
  final List<String> jvmArgs) {
    try {
      String[] jvisualvmCommandLine = createJVisualVMCommandLine(jvmArgs);

      if (isDebugging()) {
        getGfsh().printAsInfo(String.format("JVisualVM command-line (%1$s)", Arrays.toString(jvisualvmCommandLine)));
      }

      Process jvisualvmProcess = Runtime.getRuntime().exec(jvisualvmCommandLine);

      getGfsh().printAsInfo(CliStrings.START_JVISUALVM__RUN);

      String jvisualvmProcessOutput = waitAndCaptureProcessStandardErrorStream(jvisualvmProcess);

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();

      if (!StringUtils.isBlank(jvisualvmProcessOutput)) {
        infoResultData.addLine(StringUtils.LINE_SEPARATOR);
        infoResultData.addLine(jvisualvmProcessOutput);
      }

      return ResultBuilder.buildResult(infoResultData);
    } catch (GemFireException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.START_JVISUALVM__ERROR_MESSAGE,
          toString(t, false)));
    }
  }

  protected String[] createJVisualVMCommandLine(final List<String> jvmArgs) {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getJVisualVMPathname());

    if (jvmArgs != null) {
      for (final String arg : jvmArgs) {
        commandLine.add("-J" + arg);
      }
    }

    return commandLine.toArray(new String[commandLine.size()]);
  }

  protected String getJVisualVMPathname() {
    if (SystemUtils.isMacOSX()) {
      try {
        return IOUtils.verifyPathnameExists("/System/Library/Java/Support/VisualVM.bundle/Contents/Home/bin/jvisualvm");
      } catch (FileNotFoundException e) {
        throw new VisualVmNotFoundException(CliStrings.START_JVISUALVM__NOT_FOUND_ERROR_MESSAGE, e);
      }
    } else { // Linux, Solaris, Windows, etc...
      try {
        return getJdkToolPathname("jvisualvm" + getExecutableSuffix(),
            new VisualVmNotFoundException(CliStrings.START_JVISUALVM__NOT_FOUND_ERROR_MESSAGE));
      } catch (VisualVmNotFoundException e) {
        if (!SystemUtils.isJavaVersionAtLeast("1.6")) {
          throw new VisualVmNotFoundException(CliStrings.START_JVISUALVM__EXPECTED_JDK_VERSION_ERROR_MESSAGE);
        }

        throw e;
      }
    }
  }

  @CliCommand(value = CliStrings.START_PULSE, help = CliStrings.START_PULSE__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_MANAGER, CliStrings.TOPIC_GEODE_JMX, CliStrings.TOPIC_GEODE_M_AND_M })
  // TODO change url parameter type to URL when I figure out the Converter logic in Gfsh
  public Result startPulse(@CliOption(key = CliStrings.START_PULSE__URL,
      unspecifiedDefaultValue = "http://localhost:7070/pulse",
      help = CliStrings.START_PULSE__URL__HELP)
  final String url) {
    try {
      if (!StringUtils.isBlank(url)) {
        browse(URI.create(url));
        return ResultBuilder.createInfoResult(CliStrings.START_PULSE__RUN);
      } else {
        if (isConnectedAndReady()) {
          OperationInvoker operationInvoker = getGfsh().getOperationInvoker();

          ObjectName managerObjectName = (ObjectName) operationInvoker.getAttribute(
              ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN, "ManagerObjectName");

          String pulseURL = (String) operationInvoker.getAttribute(managerObjectName.toString(), "PulseURL");

          if (!StringUtils.isBlank(pulseURL)) {
            browse(URI.create(pulseURL));
            return ResultBuilder.createInfoResult(CliStrings.START_PULSE__RUN + " with URL: " + pulseURL);
          } else {
            String pulseMessage = (String) operationInvoker.getAttribute(managerObjectName.toString(), "StatusMessage");
            return (!StringUtils.isBlank(pulseMessage) ? ResultBuilder.createGemFireErrorResult(pulseMessage)
                : ResultBuilder.createGemFireErrorResult(CliStrings.START_PULSE__URL__NOTFOUND));
          }
        } else {
          return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.GFSH_MUST_BE_CONNECTED_FOR_LAUNCHING_0,
              "GemFire Pulse"));
        }
      }
    } catch (GemFireException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (Exception e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.START_PULSE__ERROR,
          toString(t, false)));
    }
  }

  private void browse(URI uri) throws IOException {
    assertState(Desktop.isDesktopSupported(), String.format(CliStrings.DESKSTOP_APP_RUN_ERROR_MESSAGE,
        System.getProperty("os.name")));
    Desktop.getDesktop().browse(uri);
  }

  @Deprecated
  protected File readIntoTempFile(final String classpathResourceLocation) throws IOException {
    String resourceName = classpathResourceLocation.substring(classpathResourceLocation.lastIndexOf(File.separator) + 1);
    File resourceFile = new File(System.getProperty("java.io.tmpdir"), resourceName);

    if (!resourceFile.exists() && resourceFile.createNewFile()) {
      BufferedReader resourceReader = new BufferedReader(new InputStreamReader(
          ClassLoader.getSystemClassLoader().getResourceAsStream(classpathResourceLocation)));

      BufferedWriter resourceFileWriter = new BufferedWriter(new FileWriter(resourceFile, false));

      try {
        for (String line = resourceReader.readLine(); line != null; line = resourceReader.readLine()) {
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

  @CliCommand(value = CliStrings.START_VSD, help = CliStrings.START_VSD__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_M_AND_M, CliStrings.TOPIC_GEODE_STATISTICS })
  public Result startVsd(@CliOption(key = CliStrings.START_VSD__FILE, help = CliStrings.START_VSD__FILE__HELP)
  final String[] statisticsArchiveFilePathnames) {
    try {
      String gemfireHome = System.getenv("GEMFIRE");

      assertState(!StringUtils.isBlank(gemfireHome), CliStrings.GEODE_HOME_NOT_FOUND_ERROR_MESSAGE);

      assertState(IOUtils.isExistingPathname(getPathToVsd()), String.format(CliStrings.START_VSD__NOT_FOUND_ERROR_MESSAGE,
          gemfireHome));

      String[] vsdCommandLine = createdVsdCommandLine(statisticsArchiveFilePathnames);

      if (isDebugging()) {
        getGfsh().printAsInfo(String.format("GemFire VSD command-line (%1$s)", Arrays.toString(vsdCommandLine)));
      }

      Process vsdProcess = Runtime.getRuntime().exec(vsdCommandLine);

      getGfsh().printAsInfo(CliStrings.START_VSD__RUN);

      String vsdProcessOutput = waitAndCaptureProcessStandardErrorStream(vsdProcess);

      InfoResultData infoResultData = ResultBuilder.createInfoResultData();

      if (!StringUtils.isBlank(vsdProcessOutput)) {
        infoResultData.addLine(StringUtils.LINE_SEPARATOR);
        infoResultData.addLine(vsdProcessOutput);
      }

      return ResultBuilder.buildResult(infoResultData);
    } catch (GemFireException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (FileNotFoundException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createShellClientErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(String.format(CliStrings.START_VSD__ERROR_MESSAGE,
          toString(t, false)));
    }
  }

  protected String[] createdVsdCommandLine(final String[] statisticsArchiveFilePathnames) throws FileNotFoundException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getPathToVsd());
    commandLine.addAll(processStatisticsArchiveFiles(statisticsArchiveFilePathnames));

    return commandLine.toArray(new String[commandLine.size()]);
  }

  protected String getPathToVsd() {
    String vsdPathname = IOUtils.appendToPath(System.getenv("GEMFIRE"), "tools", "vsd", "bin", "vsd");

    if (SystemUtils.isWindows()) {
      vsdPathname += ".bat";
    }

    return vsdPathname;
  }

  protected Set<String> processStatisticsArchiveFiles(final String[] statisticsArchiveFilePathnames) throws FileNotFoundException {
    Set<String> statisticsArchiveFiles = new TreeSet<>();

    if (statisticsArchiveFilePathnames != null) {
      for (String pathname : statisticsArchiveFilePathnames) {
        File path = new File(pathname);

        if (path.exists()) {
          if (path.isFile()) {
            if (StatisticsArchiveFileFilter.INSTANCE.accept(path)) {
              statisticsArchiveFiles.add(pathname);
            } else {
              throw new IllegalArgumentException("A Statistics Archive File must end with a .gfs file extension.");
            }
          } else { // the File (path) is a directory
            processStatisticsArchiveFiles(path, statisticsArchiveFiles);
          }
        } else {
          throw new FileNotFoundException(String.format(
              "The pathname (%1$s) does not exist.  Please check the path and try again.",
              path.getAbsolutePath()));
        }
      }
    }

    return statisticsArchiveFiles;
  }

  @SuppressWarnings("null")
  protected void processStatisticsArchiveFiles(final File path, final Set<String> statisticsArchiveFiles) {
    if (path != null && path.isDirectory()) {
      for (File file : path.listFiles(StatisticsArchiveFileAndDirectoryFilter.INSTANCE)) {
        if (file.isDirectory()) {
          processStatisticsArchiveFiles(file, statisticsArchiveFiles);
        } else if (StatisticsArchiveFileFilter.INSTANCE.accept(file)) {
          statisticsArchiveFiles.add(file.getAbsolutePath());
        }
      }
    }
  }

  // NOTE as of 8.0, this command is no more!
  //@CliCommand(value=CliStrings.START_DATABROWSER, help=CliStrings.START_DATABROWSER__HELP)
  @CliMetaData(shellOnly = true, relatedTopic = { CliStrings.TOPIC_GEODE_M_AND_M })
  public Result startDataBrowser() {
    try {
      String gemfireHome = System.getenv("GEMFIRE");

      assertState(!StringUtils.isBlank(gemfireHome), CliStrings.GEODE_HOME_NOT_FOUND_ERROR_MESSAGE);

      if (isConnectedAndReady() && (getGfsh().getOperationInvoker() instanceof JmxOperationInvoker)) {
        String dataBrowserPath = getPathToDataBrowser();

        assertState(IOUtils.isExistingPathname(dataBrowserPath), String.format(
            CliStrings.START_DATABROWSER__NOT_FOUND_ERROR_MESSAGE, gemfireHome));

        JmxOperationInvoker operationInvoker = (JmxOperationInvoker) getGfsh().getOperationInvoker();

        String dataBrowserCommandLine = String.format("%1$s %2$s %3$d", getPathToDataBrowser(),
            operationInvoker.getManagerHost(), operationInvoker.getManagerPort());

        if (isDebugging()) {
          getGfsh().printAsInfo(String.format("GemFire DataBrowser command-line (%1$s)", dataBrowserCommandLine));
        }

        Process dataBrowserProcess = Runtime.getRuntime().exec(dataBrowserCommandLine);

        getGfsh().printAsInfo(CliStrings.START_DATABROWSER__RUN);

        String dataBrowserProcessOutput = waitAndCaptureProcessStandardOutputStream(dataBrowserProcess);

        InfoResultData infoResultData = ResultBuilder.createInfoResultData();

        if (!StringUtils.isBlank(dataBrowserProcessOutput)) {
          infoResultData.addLine(StringUtils.LINE_SEPARATOR);
          infoResultData.addLine(dataBrowserProcessOutput);
        }

        return ResultBuilder.buildResult(infoResultData);
      } else {
        return ResultBuilder.createUserErrorResult(CliStrings.format(CliStrings.GFSH_MUST_BE_CONNECTED_VIA_JMX_FOR_LAUNCHING_0,
            "GemFire DataBrowser"));
      }
    } catch (IllegalArgumentException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (IllegalStateException e) {
      return ResultBuilder.createUserErrorResult(e.getMessage());
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      return ResultBuilder.createShellClientErrorResult(t.getMessage());
    }
  }

  protected String getPathToDataBrowser() {
    String dataBrowserPathName = IOUtils.appendToPath(GEMFIRE_HOME, "tools", "DataBrowser", "bin", "databrowser");

    if (SystemUtils.isWindows()) {
      dataBrowserPathName += ".bat";
    }

    return dataBrowserPathName;
  }

  protected String waitAndCaptureProcessStandardOutputStream(final Process process) {
    return waitAndCaptureProcessStandardOutputStream(process, DEFAULT_PROCESS_OUTPUT_WAIT_TIME_MILLISECONDS);
  }

  protected String waitAndCaptureProcessStandardOutputStream(final Process process, final long waitTimeMilliseconds) {
    return waitAndCaptureProcessStream(process, process.getInputStream(), waitTimeMilliseconds);
  }

  protected String waitAndCaptureProcessStandardErrorStream(final Process process) {
    return waitAndCaptureProcessStandardErrorStream(process, DEFAULT_PROCESS_OUTPUT_WAIT_TIME_MILLISECONDS);
  }

  protected String waitAndCaptureProcessStandardErrorStream(final Process process, final long waitTimeMilliseconds) {
    return waitAndCaptureProcessStream(process, process.getErrorStream(), waitTimeMilliseconds);
  }

  private String waitAndCaptureProcessStream(final Process process, final InputStream processInputStream, long waitTimeMilliseconds) {
    final StringBuffer buffer = new StringBuffer();

    InputListener inputListener = new InputListener() {
      @Override
      public void notifyInputLine(final String line) {
        buffer.append(line);
        buffer.append(StringUtils.LINE_SEPARATOR);
      }
    };

    ProcessStreamReader reader = new ProcessStreamReader.Builder(process)
        .inputStream(processInputStream)
        .inputListener(inputListener)
        .build();

    try {
      reader.start();

      final long endTime = (System.currentTimeMillis() + waitTimeMilliseconds);

      while (System.currentTimeMillis() < endTime) {
        try {
          reader.join(waitTimeMilliseconds);
        } catch (InterruptedException ignore) {
        }
      }
    } finally {
      reader.stop();
    }

    return buffer.toString();
  }

  @CliAvailabilityIndicator({ CliStrings.START_LOCATOR, CliStrings.STOP_LOCATOR, CliStrings.STATUS_LOCATOR,
      CliStrings.START_SERVER, CliStrings.STOP_SERVER, CliStrings.STATUS_SERVER,
      CliStrings.START_MANAGER, CliStrings.START_PULSE, CliStrings.START_VSD, CliStrings.START_DATABROWSER })
  public boolean launcherCommandsAvailable() {
    return true;
  }

  protected static final class LauncherSignalListener implements SignalListener {

    private volatile boolean signaled = false;

    public boolean isSignaled() {
      return signaled;
    }

    public void handle(final SignalEvent event) {
      //System.err.printf("Gfsh LauncherSignalListener Received Signal '%1$s' (%2$d)...%n",
      //  event.getSignal().getName(), event.getSignal().getNumber());
      this.signaled = true;
    }
  }

  protected static class StatisticsArchiveFileFilter implements FileFilter {

    protected static final StatisticsArchiveFileFilter INSTANCE = new StatisticsArchiveFileFilter();

    public boolean accept(final File pathname) {
      return (pathname.isFile() && pathname.getAbsolutePath().endsWith(".gfs"));
    }
  }

  protected static class StatisticsArchiveFileAndDirectoryFilter extends StatisticsArchiveFileFilter {

    protected static final StatisticsArchiveFileAndDirectoryFilter INSTANCE = new StatisticsArchiveFileAndDirectoryFilter();

    @Override
    public boolean accept(final File pathname) {
      return (pathname.isDirectory() || super.accept(pathname));
    }
  }

}
