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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.MalformedObjectNameException;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.AbstractLauncher;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.ClusterConfigurationNotAvailableException;
import org.apache.geode.internal.process.ProcessStreamReader;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.security.ResourceConstants;

public class StartServerCommand implements GfshCommand {
  private static final String SERVER_TERM_NAME = "Server";

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
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
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
      @CliOption(key = CliStrings.START_SERVER__PASSWORD, unspecifiedDefaultValue = "",
          help = CliStrings.START_SERVER__PASSWORD__HELP) String passwordToUse)
  // NOTICE: keep the parameters in alphabetical order based on their CliStrings.START_SERVER_* text
  {
    try {
      if (StringUtils.isBlank(memberName)) {
        // when the user doesn't give us a name, we make one up!
        memberName = StartMemberUtils.getNameGenerator().generate('-');
      }

      // prompt for password is username is specified in the command
      if (StringUtils.isNotBlank(userName)) {
        if (StringUtils.isBlank(passwordToUse)) {
          passwordToUse = getGfsh().readPassword(CliStrings.START_SERVER__PASSWORD + ": ");
        }
        if (StringUtils.isBlank(passwordToUse)) {
          return ResultBuilder.createConnectionErrorResult(
              CliStrings.START_SERVER__MSG__PASSWORD_MUST_BE_SPECIFIED);
        }
      }

      workingDirectory = StartMemberUtils.resolveWorkingDir(workingDirectory, memberName);

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

      final int oldPid = StartMemberUtils.readPid(serverPidFile);

      Properties gemfireProperties = new Properties();

      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.BIND_ADDRESS,
          bindAddress);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.CACHE_XML_FILE, cacheXmlPathname);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.ENABLE_TIME_STATISTICS, enableTimeStatistics);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.GROUPS,
          group);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, jmxManagerHostnameForClients);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.LOCATORS,
          locators);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.LOCATOR_WAIT_TIME, locatorWaitTime);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.LOG_LEVEL,
          logLevel);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.MCAST_ADDRESS, mcastBindAddress);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.MCAST_PORT,
          mcastPort);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.MEMCACHED_PORT, memcachedPort);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.MEMCACHED_PROTOCOL, memcachedProtocol);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.MEMCACHED_BIND_ADDRESS, memcachedBindAddress);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.REDIS_PORT,
          redisPort);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.REDIS_BIND_ADDRESS, redisBindAddress);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.REDIS_PASSWORD, redisPassword);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.STATISTIC_ARCHIVE_FILE, statisticsArchivePathname);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.USE_CLUSTER_CONFIGURATION, requestSharedConfiguration);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties, ConfigurationProperties.LOCK_MEMORY,
          lockMemory);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.OFF_HEAP_MEMORY_SIZE, offHeapMemorySize);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.START_DEV_REST_API, startRestApi);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.HTTP_SERVICE_PORT, httpServicePort);
      StartMemberUtils.setPropertyIfNotNull(gemfireProperties,
          ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS, httpServiceBindAddress);
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
      final ProcessStreamReader.ReadingMode readingMode = SystemUtils.isWindows()
          ? ProcessStreamReader.ReadingMode.NON_BLOCKING : ProcessStreamReader.ReadingMode.BLOCKING;

      final StringBuffer message = new StringBuffer(); // need thread-safe StringBuffer
      ProcessStreamReader.InputListener inputListener = line -> {
        message.append(line);
        if (readingMode == ProcessStreamReader.ReadingMode.BLOCKING) {
          message.append(StringUtils.LINE_SEPARATOR);
        }
      };

      ProcessStreamReader stderrReader = new ProcessStreamReader.Builder(serverProcess)
          .inputStream(serverProcess.getErrorStream()).inputListener(inputListener)
          .readingMode(readingMode).continueReadingMillis(2 * 1000).build().start();

      ServerLauncher.ServerState serverState;

      String previousServerStatusMessage = null;

      LauncherSignalListener serverSignalListener = new LauncherSignalListener();

      final boolean registeredServerSignalListener =
          getGfsh().getSignalHandler().registerListener(serverSignalListener);

      try {
        getGfsh().logInfo(String.format(CliStrings.START_SERVER__RUN_MESSAGE,
            IOUtils.tryGetCanonicalPathElseGetAbsolutePath(
                new File(serverLauncher.getWorkingDirectory()))),
            null);

        serverState = ServerLauncher.ServerState.fromDirectory(workingDirectory, memberName);
        do {
          if (serverProcess.isAlive()) {
            Gfsh.print(".");

            synchronized (this) {
              TimeUnit.MILLISECONDS.timedWait(this, 500);
            }

            serverState = ServerLauncher.ServerState.fromDirectory(workingDirectory, memberName);

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
        stderrReader.stopAsync(StartMemberUtils.PROCESS_STREAM_READER_ASYNC_STOP_TIMEOUT_MILLIS); // stop
                                                                                                  // will
                                                                                                  // close
        // ErrorStream
        getGfsh().getSignalHandler().unregisterListener(serverSignalListener);
      }

      Gfsh.println();

      final boolean asyncStart =
          ServerLauncher.ServerState.isStartingNotRespondingOrNull(serverState);

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
          CliStrings.START_SERVER__GENERAL_ERROR_MESSAGE, this.toString(t, getGfsh().getDebug())));
    }
  }

  String[] createStartServerCommandLine(final ServerLauncher launcher,
      final String gemfirePropertiesPathname, final String gemfireSecurityPropertiesPathname,
      final Properties gemfireProperties, final String userClasspath,
      final Boolean includeSystemClasspath, final String[] jvmArgsOpts,
      final Boolean disableExitWhenOutOfMemory, final String initialHeap, final String maxHeap)
      throws MalformedObjectNameException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(StartMemberUtils.getJavaPath());
    commandLine.add("-server");
    commandLine.add("-classpath");
    commandLine.add(getServerClasspath(Boolean.TRUE.equals(includeSystemClasspath), userClasspath));

    StartMemberUtils.addCurrentLocators(this, commandLine, gemfireProperties);
    StartMemberUtils.addGemFirePropertyFile(commandLine, gemfirePropertiesPathname);
    StartMemberUtils.addGemFireSecurityPropertyFile(commandLine, gemfireSecurityPropertiesPathname);
    StartMemberUtils.addGemFireSystemProperties(commandLine, gemfireProperties);
    StartMemberUtils.addJvmArgumentsAndOptions(commandLine, jvmArgsOpts);

    // NOTE asserting not equal to true rather than equal to false handles the null case and ensures
    // the user
    // explicitly specified the command-line option in order to disable JVM memory checks.
    if (!Boolean.TRUE.equals(disableExitWhenOutOfMemory)) {
      addJvmOptionsForOutOfMemoryErrors(commandLine);
    }

    StartMemberUtils.addInitialHeap(commandLine, initialHeap);
    StartMemberUtils.addMaxHeap(commandLine, maxHeap);

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

  String getServerClasspath(final boolean includeSystemClasspath, final String userClasspath) {
    List<String> jarFilePathnames = new ArrayList<>();

    jarFilePathnames.add(StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);

    return StartMemberUtils.toClasspath(includeSystemClasspath,
        jarFilePathnames.toArray(new String[jarFilePathnames.size()]), userClasspath);
  }

  private void addJvmOptionsForOutOfMemoryErrors(final List<String> commandLine) {
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
}
