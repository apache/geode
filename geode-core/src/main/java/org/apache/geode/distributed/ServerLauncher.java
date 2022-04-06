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
package org.apache.geode.distributed;

import static java.lang.System.lineSeparator;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_BIND_ADDRESS;
import static org.apache.geode.internal.lang.StringUtils.wrap;
import static org.apache.geode.internal.lang.SystemUtils.CURRENT_DIRECTORY;
import static org.apache.geode.internal.process.ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX;
import static org.apache.geode.internal.util.IOUtils.tryGetCanonicalPathElseGetAbsolutePath;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DefaultServerLauncherCacheProvider;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.AbstractCacheServer;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.process.ConnectionFailedException;
import org.apache.geode.internal.process.ControlNotificationHandler;
import org.apache.geode.internal.process.ControllableProcess;
import org.apache.geode.internal.process.FileAlreadyExistsException;
import org.apache.geode.internal.process.FileControllableProcess;
import org.apache.geode.internal.process.MBeanInvocationFailedException;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessController;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.internal.process.ProcessControllerParameters;
import org.apache.geode.internal.process.ProcessLauncherContext;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.UnableToControlProcessException;
import org.apache.geode.lang.AttachAPINotFoundException;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.util.HostUtils;
import org.apache.geode.management.internal.util.JsonUtil;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * The ServerLauncher class is a launcher class with main method to start a GemFire Server (implying
 * a GemFire Cache Server process).
 *
 * @see AbstractLauncher
 * @see LocatorLauncher
 * @since GemFire 7.0
 */
@SuppressWarnings("unused")
public class ServerLauncher extends AbstractLauncher<String> {

  private static final Logger log = LogService.getLogger();

  @Immutable
  private static final Map<String, String> helpMap;

  static {
    Map<String, String> help = new HashMap<>();
    help.put("launcher",
        "A GemFire launcher used to start, stop and determine a Server's status.");
    help.put(Command.START.getName(), String.format(
        "Starts a Server running in the current working directory listening on the default port (%s) bound to all IP addresses available to the localhost.  The Server must be given a member name in the GemFire cluster.  The default server-bind-address and server-port may be overridden using the corresponding command-line options.",
        String.valueOf(getDefaultServerPort())));
    help.put(Command.STATUS.getName(),
        "Displays the status of a Server given any combination of the member name/ID, PID, or the directory in which the Server is running.");
    help.put(Command.STOP.getName(),
        "Stops a running Server given given a member name/ID, PID, or the directory in which the Server is running.");
    help.put(Command.VERSION.getName(),
        "Displays GemFire product version information.");
    help.put("assign-buckets",
        "Causes buckets to be assigned to the partitioned regions in the GemFire cache on Server start.");
    help.put("debug", "Displays verbose information during the invocation of the launcher.");
    help.put("delete-pid-file-on-stop",
        "Specifies that this Server's PID file should be deleted on stop.  The default is to not delete this Server's PID file until JVM exit if --delete-pid-file-on-stop is not specified.");
    help.put("dir",
        "Specifies the working directory where the Server is running.  Defaults to the current working directory.");
    help.put("disable-default-server",
        "Disables the addition of a default GemFire cache server.");
    help.put("force",
        "Enables any existing Server PID file to be overwritten on start.  The default is to throw an error if a PID file already exists and --force is not specified.");
    help.put("help",
        "Causes GemFire to print out information instead of performing the command. This option is supported by all commands.");
    help.put("member", "Identifies the Server by member name or ID in the GemFire cluster.");
    help.put("pid", "Indicates the OS process ID of the running Server.");
    help.put("rebalance",
        "An option to cause the GemFire cache's partitioned regions to be rebalanced on start.");
    help.put("redirect-output",
        "An option to cause the Server to redirect standard out and standard error to the GemFire log file.");
    help.put(SERVER_BIND_ADDRESS,
        "Specifies the IP address on which to bind, or on which the Server is bound, listening for client requests.  Defaults to all IP addresses available to the localhost.");
    help.put("hostname-for-clients",
        "An option to specify the hostname or IP address to send to clients so they can connect to this Server. The default is to use the IP address to which the Server is bound.");
    help.put("server-port", String.format(
        "Specifies the port on which the Server is listening for client requests. Defaults to %s.",
        String.valueOf(getDefaultServerPort())));
    helpMap = Collections.unmodifiableMap(help);
  }

  @Immutable
  private static final Map<Command, String> usageMap;

  static {
    Map<Command, String> usage = new TreeMap<>();
    usage.put(Command.START,
        "start <member-name> [--assign-buckets] [--disable-default-server] [--rebalance] [--server-bind-address=<IP-address>] [--server-port=<port>] [--force] [--debug] [--help]");
    usage.put(Command.STATUS,
        "status [--member=<member-ID/Name>] [--pid=<process-ID>] [--dir=<Server-working-directory>] [--debug] [--help]");
    usage.put(Command.STOP,
        "stop [--member=<member-ID/Name>] [--pid=<process-ID>] [--dir=<Server-working-directory>] [--debug] [--help]");
    usage.put(Command.VERSION, "version");
    usageMap = Collections.unmodifiableMap(usage);
  }

  private static final String DEFAULT_SERVER_LOG_EXT = ".log";
  private static final String DEFAULT_SERVER_LOG_NAME = "gemfire";
  private static final String SERVER_SERVICE_NAME = "Server";

  @MakeNotStatic
  private static final AtomicReference<ServerLauncher> INSTANCE = new AtomicReference<>();

  @Immutable
  private static final ServerLauncherCacheProvider DEFAULT_CACHE_PROVIDER =
      new DefaultServerLauncherCacheProvider();

  private static final Logger logger = LogService.getLogger();
  private volatile boolean debug;

  private final ControlNotificationHandler controlHandler;

  private final AtomicBoolean starting = new AtomicBoolean(false);

  private final boolean assignBuckets;
  private final boolean deletePidFileOnStop;
  private final boolean disableDefaultServer;
  private final boolean force;
  private final boolean help;
  private final boolean rebalance;
  private final boolean redirectOutput;

  private volatile Cache cache;

  private final CacheConfig cacheConfig;

  private final Command command;

  private final InetAddress serverBindAddress;

  private final Integer pid;
  private final Integer serverPort;

  private final Properties distributedSystemProperties;

  private final String memberName;
  private final String springXmlLocation;
  private final String workingDirectory;

  // NOTE in addition to debug, the other shared, mutable state
  private volatile String statusMessage;

  private final Float criticalHeapPercentage;
  private final Float evictionHeapPercentage;

  private final Float criticalOffHeapPercentage;
  private final Float evictionOffHeapPercentage;

  private final String hostNameForClients;
  private final Integer maxConnections;
  private final Integer maxMessageCount;
  private final Integer messageTimeToLive;
  private final Integer socketBufferSize;

  private final Integer maxThreads;

  private volatile ControllableProcess process;

  private final ServerControllerParameters controllerParameters;
  private final Runnable startupCompletionAction;
  private final Consumer<Throwable> startupExceptionAction;
  private final ServerLauncherCacheProvider serverLauncherCacheProvider;
  private final Supplier<ControllableProcess> controllableProcessFactory;

  /**
   * Launches a GemFire Server from the command-line configured with the given arguments.
   *
   * @param args the command-line arguments used to configure the GemFire Server at runtime.
   */
  public static void main(final String... args) {
    try {
      new Builder(args).build().run();
    } catch (AttachAPINotFoundException handled) {
      System.err.println(handled.getMessage());
    }
  }

  private static Integer getDefaultServerPort() {
    return Integer.getInteger(AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY,
        CacheServer.DEFAULT_PORT);
  }

  /**
   * Gets the instance of the ServerLauncher used to launch the GemFire Cache Server, or null if
   * this VM does not have an instance of ServerLauncher indicating no GemFire Cache Server is
   * running.
   *
   * @return the instance of ServerLauncher used to launcher a GemFire Cache Server in this VM.
   */
  public static ServerLauncher getInstance() {
    return INSTANCE.get();
  }

  /**
   * Gets the ServerState for this process or null if this process was not launched using this VM's
   * ServerLauncher reference .
   *
   * @return the ServerState for this process or null.
   */
  public static ServerState getServerState() {
    return getInstance() != null ? getInstance().status() : null;
  }

  /**
   * Private constructor used to properly construct an immutable instance of the ServerLauncher
   * using a Builder. The Builder is used to configure a ServerLauncher instance. The Builder can
   * process user input from the command-line or be used programmatically to properly construct an
   * instance of the ServerLauncher using the API.
   *
   * @param builder an instance of ServerLauncher.Builder for configuring and constructing an
   *        instance of the ServerLauncher.
   * @see ServerLauncher.Builder
   */
  private ServerLauncher(final Builder builder) {
    cache = builder.getCache();
    cacheConfig = builder.getCacheConfig();
    command = builder.getCommand();
    assignBuckets = Boolean.TRUE.equals(builder.getAssignBuckets());
    setDebug(Boolean.TRUE.equals(builder.getDebug()));
    deletePidFileOnStop = Boolean.TRUE.equals(builder.getDeletePidFileOnStop());
    disableDefaultServer = Boolean.TRUE.equals(builder.getDisableDefaultServer());
    distributedSystemProperties = builder.getDistributedSystemProperties();
    force = Boolean.TRUE.equals(builder.getForce());
    help = Boolean.TRUE.equals(builder.getHelp());
    hostNameForClients = builder.getHostNameForClients();
    memberName = builder.getMemberName();
    pid = builder.getPid();
    rebalance = Boolean.TRUE.equals(builder.getRebalance());
    redirectOutput = Boolean.TRUE.equals(builder.getRedirectOutput());
    serverBindAddress = builder.getServerBindAddress();
    serverPort = builder.getServerPort();
    springXmlLocation = builder.getSpringXmlLocation();
    workingDirectory = builder.getWorkingDirectory();
    criticalHeapPercentage = builder.getCriticalHeapPercentage();
    evictionHeapPercentage = builder.getEvictionHeapPercentage();
    criticalOffHeapPercentage = builder.getCriticalOffHeapPercentage();
    evictionOffHeapPercentage = builder.getEvictionOffHeapPercentage();
    maxConnections = builder.getMaxConnections();
    maxMessageCount = builder.getMaxMessageCount();
    maxThreads = builder.getMaxThreads();
    messageTimeToLive = builder.getMessageTimeToLive();
    socketBufferSize = builder.getSocketBufferSize();
    controllerParameters = new ServerControllerParameters();
    startupCompletionAction = builder.getStartupCompletionAction();
    startupExceptionAction = builder.getStartupExceptionAction();
    controlHandler = new ControlNotificationHandler() {
      @Override
      public void handleStop() {
        if (isStoppable()) {
          stopInProcess();
        }
      }

      @Override
      public ServiceState<?> handleStatus() {
        return statusInProcess();
      }
    };
    serverLauncherCacheProvider = builder.getServerLauncherCacheProvider();
    controllableProcessFactory = builder.getControllableProcessFactory();

    Integer serverPort =
        builder.isServerPortSetByUser() && this.serverPort != null ? this.serverPort : null;
    String serverBindAddress =
        builder.isServerBindAddressSetByUser() && this.serverBindAddress != null
            ? this.serverBindAddress.getHostAddress() : null;

    ServerLauncherParameters.INSTANCE
        .withPort(serverPort)
        .withMaxThreads(maxThreads)
        .withBindAddress(serverBindAddress)
        .withMaxConnections(maxConnections)
        .withMaxMessageCount(maxMessageCount)
        .withSocketBufferSize(socketBufferSize)
        .withMessageTimeToLive(messageTimeToLive)
        .withHostnameForClients(hostNameForClients)
        .withDisableDefaultServer(disableDefaultServer);
  }

  /**
   * Gets a reference to the {@code Cache} that was created by this {@code ServerLauncher}.
   *
   * @return a reference to the Cache
   * @see Cache
   */
  public Cache getCache() {
    if (cache != null) {
      boolean isReconnecting = cache.isReconnecting();
      if (isReconnecting) {
        Cache newCache = cache.getReconnectedCache();
        if (newCache != null) {
          cache = newCache;
        }
      }
    }
    return cache;
  }

  /**
   * Gets the CacheConfig object used to configure additional GemFire Cache components and features
   * (e.g. PDX).
   *
   * @return a CacheConfig object with additional GemFire Cache configuration meta-data used on
   *         startup to configure the Cache.
   */
  public CacheConfig getCacheConfig() {
    final CacheConfig copy = new CacheConfig();
    copy.setDeclarativeConfig(cacheConfig);
    return copy;
  }

  /**
   * Gets an identifier that uniquely identifies and represents the Server associated with this
   * launcher.
   *
   * @return a String value identifier to uniquely identify the Server and it's launcher.
   * @see #getServerBindAddressAsString()
   * @see #getServerPortAsString()
   */
  public String getId() {
    final StringBuilder buffer = new StringBuilder(ServerState.getServerBindAddressAsString(this));
    final String serverPort = ServerState.getServerPortAsString(this);

    if (isNotBlank(serverPort)) {
      buffer.append("[").append(serverPort).append("]");
    }

    return buffer.toString();
  }

  /**
   * Get the Server launcher command used to invoke the Server.
   *
   * @return the Server launcher command used to invoke the Server.
   * @see ServerLauncher.Command
   */
  public Command getCommand() {
    return command;
  }

  /**
   * Determines whether buckets should be assigned to partitioned regions in the cache upon Server
   * start.
   *
   * @return a boolean indicating if buckets should be assigned upon Server start.
   */
  public boolean isAssignBuckets() {
    return assignBuckets;
  }

  /**
   * Determines whether a default cache server will be added when the GemFire Server comes online.
   *
   * @return a boolean value indicating whether to add a default cache server.
   */
  public boolean isDisableDefaultServer() {
    return disableDefaultServer;
  }

  /**
   * Determines whether the PID file is allowed to be overwritten when the Server is started and a
   * PID file already exists in the Server's specified working directory.
   *
   * @return boolean indicating if force has been enabled.
   */
  public boolean isForcing() {
    return force;
  }

  /**
   * Determines whether this launcher will be used to display help information. If so, then none of
   * the standard Server launcher commands will be used to affect the state of the Server. A
   * launcher is said to be 'helping' if the user entered the "--help" option (switch) on the
   * command-line.
   *
   * @return a boolean value indicating if this launcher is used for displaying help information.
   * @see ServerLauncher.Command
   */
  public boolean isHelping() {
    return help;
  }

  /**
   * Determines whether a rebalance operation on the cache will occur upon starting the GemFire
   * server using this launcher.
   *
   * @return a boolean indicating if the cache will be rebalance when the GemFire server starts.
   */
  public boolean isRebalancing() {
    return rebalance;
  }

  /**
   * Determines whether this launcher will redirect output to system logs when starting a new Server
   * process.
   *
   * @return a boolean value indicating if this launcher will redirect output to system logs when
   *         starting a new Server process
   */
  public boolean isRedirectingOutput() {
    return redirectOutput;
  }

  /**
   * Gets the name of the log file used to log information about this Server.
   *
   * @return a String value indicating the name of this Server's log file.
   */
  @Override
  public String getLogFileName() {
    return defaultIfBlank(getMemberName(), DEFAULT_SERVER_LOG_NAME).concat(DEFAULT_SERVER_LOG_EXT);
  }

  /**
   * Gets the name of this member (this Server) in the GemFire distributed system as determined by
   * the 'name' GemFire property.
   *
   * @return a String indicating the name of the member (this Server) in the GemFire distributed
   *         system.
   */
  @Override
  public String getMemberName() {
    return defaultIfBlank(memberName, super.getMemberName());
  }

  /**
   * Gets the user-specified process ID (PID) of the running Server that ServerLauncher uses to
   * issue status and stop commands to the Server.
   *
   * @return an Integer value indicating the process ID (PID) of the running Server.
   */
  @Override
  public Integer getPid() {
    return pid;
  }

  /**
   * Gets the GemFire Distributed System (cluster) Properties.
   *
   * @return a Properties object containing the configuration settings for the GemFire Distributed
   *         System (cluster).
   * @see Properties
   */
  public Properties getProperties() {
    return (Properties) distributedSystemProperties.clone();
  }

  /**
   * Gets the IP address to which the Server is bound listening for and accepting cache client
   * connections. This property should not be confused with 'bindAddress' ServerLauncher property,
   * which is the port for binding the Server's ServerSocket used in distribution and messaging
   * between the peers of the GemFire distributed system.
   *
   * @return an InetAddress indicating the IP address that the Server is bound to listening for and
   *         accepting cache client connections in a client/server topology.
   */
  public InetAddress getServerBindAddress() {
    return serverBindAddress;
  }

  /**
   * Gets the host, as either hostname or IP address, on which the Server was bound and running. An
   * attempt is made to get the canonical hostname for IP address to which the Server was bound for
   * accepting client requests. If the server bind address is null or localhost is unknown, then a
   * default String value of "localhost/127.0.0.1" is returned.
   *
   * Note, this information is purely information and should not be used to re-construct state or
   * for other purposes.
   *
   * @return the hostname or IP address of the host running the Server, based on the bind-address,
   *         or 'localhost/127.0.0.1' if the bind address is null and localhost is unknown.
   * @see InetAddress
   * @see #getServerBindAddress()
   */
  public String getServerBindAddressAsString() {
    try {
      if (getServerBindAddress() != null) {
        return getServerBindAddress().getCanonicalHostName();
      }

      return LocalHostUtil.getCanonicalLocalHostName();
    } catch (UnknownHostException handled) {
      // Returning localhost/127.0.0.1 implies the serverBindAddress was null and no IP address
      // for localhost could be found
      return "localhost/127.0.0.1";
    }
  }

  /**
   * Gets the port on which the Server is listening for cache client connections. This property
   * should not be confused with the 'port' ServerLauncher property, which is used by the Server to
   * set the 'tcp-port' distribution config property and is used by the ServerSocket for peer
   * distribution and messaging.
   *
   * @return an Integer value indicating the port the Server is listening on for cache client
   *         connections in the client/server topology.
   */
  public Integer getServerPort() {
    return serverPort;
  }

  /**
   * Gets the server port on which the Server is listening for client requests represented as a
   * String value.
   *
   * @return a String representing the server port on which the Server is listening for client
   *         requests.
   * @see #getServerPort()
   */
  public String getServerPortAsString() {
    Integer v1 = getServerPort();
    return (v1 != null ? v1 : getDefaultServerPort()).toString();
  }

  /**
   * Gets the name for a GemFire Server.
   *
   * @return a String indicating the name for a GemFire Server.
   */
  @Override
  public String getServiceName() {
    return SERVER_SERVICE_NAME;
  }

  /**
   * Gets the location of the Spring XML configuration meta-data file used to bootstrap, configure
   * and initialize the GemFire Server on start.
   * <p>
   *
   * @return a String indicating the location of the Spring XML configuration file.
   * @see ServerLauncher.Builder#getSpringXmlLocation()
   */
  public String getSpringXmlLocation() {
    return springXmlLocation;
  }

  /**
   * Determines whether this GemFire Server was configured and initialized with Spring configuration
   * meta-data.
   * <p>
   *
   * @return a boolean value indicating whether this GemFire Server was configured with Spring
   *         configuration meta-data.
   */
  public boolean isSpringXmlLocationSpecified() {
    return isNotBlank(springXmlLocation);
  }

  /**
   * Gets the working directory pathname in which the Server will be run.
   *
   * @return a String value indicating the pathname of the Server's working directory.
   */
  @Override
  public String getWorkingDirectory() {
    return workingDirectory;
  }

  public Float getCriticalHeapPercentage() {
    return criticalHeapPercentage;
  }

  public Float getEvictionHeapPercentage() {
    return evictionHeapPercentage;
  }

  public Float getCriticalOffHeapPercentage() {
    return criticalOffHeapPercentage;
  }

  public Float getEvictionOffHeapPercentage() {
    return evictionOffHeapPercentage;
  }

  public String getHostNameForClients() {
    return hostNameForClients;
  }

  public Integer getMaxConnections() {
    return maxConnections;
  }

  public Integer getMaxMessageCount() {
    return maxMessageCount;
  }

  public Integer getMessageTimeToLive() {
    return messageTimeToLive;
  }

  public Integer getMaxThreads() {
    return maxThreads;
  }

  public Integer getSocketBufferSize() {
    return socketBufferSize;
  }

  private static final String TWO_NEW_LINES = lineSeparator() + lineSeparator();

  /**
   * Displays help for the specified Server launcher command to standard err. If the Server launcher
   * command is unspecified, then usage information is displayed instead.
   *
   * @param command the Server launcher command in which to display help information.
   * @see #usage()
   */
  public void help(final Command command) {
    if (Command.isUnspecified(command)) {
      usage();
    } else {
      info(wrap(helpMap.get(command.getName()), 80, ""));
      info(TWO_NEW_LINES + "usage: " + TWO_NEW_LINES);
      info(wrap("> java ... " + getClass().getName() + ' ' + usageMap.get(command), 80, "\t\t"));
      info(TWO_NEW_LINES + "options: " + TWO_NEW_LINES);

      for (final String option : command.getOptions()) {
        info(wrap("--" + option + ": " + helpMap.get(option) + lineSeparator(), 80, "\t"));
      }

      info(TWO_NEW_LINES);
    }
  }

  /**
   * Displays usage information on the proper invocation of the ServerLauncher from the command-line
   * to standard err.
   *
   * @see #help(ServerLauncher.Command)
   */
  public void usage() {
    info(wrap(helpMap.get("launcher"), 80, "\t"));
    info(TWO_NEW_LINES + "START" + TWO_NEW_LINES);
    help(Command.START);
    info("STATUS" + TWO_NEW_LINES);
    help(Command.STATUS);
    info("STOP" + TWO_NEW_LINES);
    help(Command.STOP);
  }

  /**
   * A Runnable method used to invoke the GemFire server (cache server) with the specified command.
   * From run, a user can invoke 'start', 'status', 'stop' and 'version'. Note, that 'version' is
   * also a command-line option, but can be treated as a "command" as well.
   *
   * @see Runnable
   */
  @Override
  public void run() {
    if (!isHelping()) {
      switch (getCommand()) {
        case START:
          info(start());
          waitOnServer();
          break;
        case STATUS:
          info(status());
          break;
        case STOP:
          info(stop());
          break;
        case VERSION:
          info(version());
          break;
        default:
          usage();
      }
    } else {
      help(getCommand());
    }
  }

  /**
   * Gets a File reference with the path to the PID file for the Server.
   *
   * @return a File reference to the path of the Server's PID file.
   */
  protected File getServerPidFile() {
    return new File(getWorkingDirectory(), ProcessType.SERVER.getPidFileName());
  }

  /**
   * Determines whether a GemFire Cache Server can be started with this instance of ServerLauncher.
   *
   * @return a boolean indicating whether a GemFire Cache Server can be started with this instance
   *         of ServerLauncher, which is true if the ServerLauncher has not already started a Server
   *         or a Server is not already running.
   * @see #start()
   */
  private boolean isStartable() {
    return !isRunning() && starting.compareAndSet(false, true);
  }

  /**
   * Invokes the 'start' command and operation to startup a GemFire server (a cache server). Note,
   * this method will cause the JVM to block upon server start, providing the calling Thread is a
   * non-daemon Thread.
   *
   * @see #run()
   */
  public ServerState start() {
    long startTime = System.currentTimeMillis();
    if (isStartable()) {
      INSTANCE.compareAndSet(null, this);

      try {
        process = getControllableProcess();

        if (!isDisableDefaultServer()) {
          assertPortAvailable(getServerBindAddress(), getServerPort());
        }

        SystemFailure.setExitOK(true);

        ProcessLauncherContext.set(isRedirectingOutput(), getOverriddenDefaults(),
            (String statusMessage) -> {
              debug("Callback setStatus(String) called with message (%1$s)...", statusMessage);
              this.statusMessage = statusMessage;
            });

        try {
          final Properties gemfireProperties = getDistributedSystemProperties(getProperties());
          cache = createCache(gemfireProperties);

          // Set the resource manager options
          if (criticalHeapPercentage != null) {
            cache.getResourceManager().setCriticalHeapPercentage(getCriticalHeapPercentage());
          }
          if (evictionHeapPercentage != null) {
            cache.getResourceManager().setEvictionHeapPercentage(getEvictionHeapPercentage());
          }
          if (criticalOffHeapPercentage != null) {
            cache.getResourceManager()
                .setCriticalOffHeapPercentage(getCriticalOffHeapPercentage());
          }
          if (evictionOffHeapPercentage != null) {
            cache.getResourceManager()
                .setEvictionOffHeapPercentage(getEvictionOffHeapPercentage());
          }

          cache.setIsServer(true);
          startCacheServer(cache);

          assignBuckets(cache);
          rebalance(cache);
        } finally {
          ProcessLauncherContext.remove();
        }

        awaitStartupTasks(cache, startTime);

        debug("Running Server on (%1$s) in (%2$s) as (%3$s)...", getId(), getWorkingDirectory(),
            getMember());
        running.set(true);

        return new ServerState(this, Status.ONLINE);
      } catch (AuthenticationRequiredException e) {
        failOnStart(e);
        throw new AuthenticationRequiredException(
            "user/password required. Please start your server with --user and --password. "
                + e.getMessage());
      } catch (GemFireSecurityException e) {
        failOnStart(e);
        throw new GemFireSecurityException(e.getMessage());
      } catch (IOException e) {
        failOnStart(e);
        throw new RuntimeException(
            String.format("An IO error occurred while starting a %s in %s on %s: %s",
                getServiceName(), getWorkingDirectory(), getId(), e.getMessage()),
            e);
      } catch (FileAlreadyExistsException e) {
        failOnStart(e);
        throw new RuntimeException(
            String.format("A PID file already exists and a %s may be running in %s on %s.",
                getServiceName(), getWorkingDirectory(), getId()),
            e);
      } catch (PidUnavailableException e) {
        failOnStart(e);
        throw new RuntimeException(
            String.format("The process ID could not be determined while starting %s %s in %s: %s",
                getServiceName(), getId(), getWorkingDirectory(), e.getMessage()),
            e);
      } catch (RuntimeException | Error e) {
        failOnStart(e);
        throw e;
      } catch (Exception e) {
        failOnStart(e);
        throw new RuntimeException(e);
      } finally {
        starting.set(false);
      }
    }

    throw new IllegalStateException(
        String.format("A %s is already running in %s on %s.",
            getServiceName(), getWorkingDirectory(), getId()));
  }

  Cache createCache(Properties gemfireProperties) {
    Iterable<ServerLauncherCacheProvider> loader = getServerLauncherCacheProviders();
    for (ServerLauncherCacheProvider provider : loader) {
      Cache cache = provider.createCache(gemfireProperties, this);
      if (cache != null) {
        return cache;
      }
    }

    return DEFAULT_CACHE_PROVIDER.createCache(gemfireProperties, this);
  }

  private Iterable<ServerLauncherCacheProvider> getServerLauncherCacheProviders() {
    return serverLauncherCacheProvider != null
        ? Collections.singleton(serverLauncherCacheProvider)
        : ServiceLoader.load(ServerLauncherCacheProvider.class);
  }

  /**
   * A helper method to ensure the same sequence of actions are taken when the Server fails to start
   * caused by some exception.
   *
   * @param cause the Throwable thrown during the startup operation on the Server.
   */
  private void failOnStart(final Throwable cause) {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (process != null) {
      process.stop(deletePidFileOnStop);
      process = null;
    }

    INSTANCE.compareAndSet(this, null);

    running.set(false);
  }

  /**
   * Determines whether the specified Cache has any CacheServers.
   *
   * @param cache the Cache to check for existing CacheServers.
   * @return a boolean value indicating if any CacheServers were added to the Cache.
   */
  protected boolean isServing(final Cache cache) {
    return !cache.getCacheServers().isEmpty();
  }

  /**
   * Determines whether to continue waiting and keep the GemFire non-Server data member running.
   *
   * @param cache the Cache associated with this GemFire (non-Server) data member.
   * @return a boolean value indicating whether the GemFire data member should continue running, as
   *         determined by the running flag and a connection to the distributed system (GemFire
   *         cluster).
   */
  boolean isWaiting(final Cache cache) {
    return isRunning() && (cache.getDistributedSystem().isConnected() || cache.isReconnecting());
  }

  /**
   * Causes the calling Thread to block until the GemFire Cache Server/Data Member stops.
   */
  public void waitOnServer() {
    assert getCache() != null : "The Cache Server must first be started with a call to start!";

    if (!isServing(getCache())) {
      Throwable cause = null;
      try {
        while (isWaiting(getCache())) {
          try {
            synchronized (this) {
              wait(500L);
            }
          } catch (InterruptedException handled) {
            // loop back around
          }
        }
      } catch (RuntimeException e) {
        cause = e;
        throw e;
      } finally {
        failOnStart(cause);
      }
    }
  }

  /**
   * Determines whether a default server (a cache server) should be created on startup as determined
   * by the absence of specifying the --disable-default-server command-line option (switch). In
   * addition, a default cache server is started only if no cache servers have been added to the
   * Cache by way of cache.xml.
   *
   * @param cache the reference to the Cache to check for any existing cache servers.
   * @return a boolean indicating whether a default server should be added to the Cache.
   * @see #isDisableDefaultServer()
   */
  protected boolean isDefaultServerEnabled(final Cache cache) {
    return cache.getCacheServers().isEmpty() && !isDisableDefaultServer();
  }

  /**
   * If the default server (cache server) has not been disabled and no prior cache servers were
   * added to the cache, then this method will add a cache server to the Cache and start the server
   * Thread on the specified bind address and port.
   *
   * @param cache the Cache to which the server will be added.
   * @throws IOException if the Cache server fails to start due to IO error.
   */
  private void startCacheServer(final Cache cache) throws IOException {
    if (isDefaultServerEnabled(cache)) {
      final String serverBindAddress =
          getServerBindAddress() == null ? null : getServerBindAddress().getHostAddress();
      final Integer serverPort = getServerPort();
      final CacheServer cacheServer = cache.addCacheServer();
      cacheServer.setBindAddress(serverBindAddress);
      cacheServer.setPort(serverPort);

      if (getMaxThreads() != null) {
        cacheServer.setMaxThreads(getMaxThreads());
      }

      if (getMaxConnections() != null) {
        cacheServer.setMaxConnections(getMaxConnections());
      }

      if (getMaxMessageCount() != null) {
        cacheServer.setMaximumMessageCount(getMaxMessageCount());
      }

      if (getMessageTimeToLive() != null) {
        cacheServer.setMessageTimeToLive(getMessageTimeToLive());
      }

      if (getSocketBufferSize() != null) {
        cacheServer.setSocketBufferSize(getSocketBufferSize());
      }

      if (getHostNameForClients() != null) {
        cacheServer.setHostnameForClients(getHostNameForClients());
      }

      CacheServerHelper.setIsDefaultServer(cacheServer);

      cacheServer.start();
    }
  }

  private void awaitStartupTasks(Cache cache, long startTime) {
    Runnable afterStartup = startupCompletionAction == null
        ? () -> logStartCompleted(startTime) : startupCompletionAction;

    Consumer<Throwable> exceptionAction = startupExceptionAction == null
        ? (throwable) -> logStartCompletedWithError(startTime, throwable) : startupExceptionAction;

    CompletableFuture<Void> startupTasks =
        ((InternalResourceManager) cache.getResourceManager())
            .allOfStartupTasks();

    startupTasks
        .thenRun(afterStartup)
        .exceptionally((throwable) -> {
          exceptionAction.accept(throwable);
          return null;
        })
        .join();
  }

  private void logStartCompleted(long startTime) {
    long startupDuration = System.currentTimeMillis() - startTime;
    log.info("Server {} startup completed in {} ms", memberName, startupDuration);
  }

  private void logStartCompletedWithError(long startTime, Throwable throwable) {
    long startupDuration = System.currentTimeMillis() - startTime;
    log.error("Server {} startup completed in {} ms with error: {}", memberName, startupDuration,
        throwable, throwable);
  }

  /**
   * Causes a rebalance operation to occur on the given Cache.
   *
   * @param cache the reference to the Cache to rebalance.
   * @see ResourceManager#createRebalanceFactory()
   */
  private void rebalance(final Cache cache) {
    if (isRebalancing()) {
      cache.getResourceManager().createRebalanceFactory().start();
    }
  }

  /**
   * Determines whether the user indicated that buckets should be assigned on cache server start
   * using the --assign-buckets command-line option (switch) at the command-line as well as whether
   * the option is technically allowed. The option is only allowed if the instance of the Cache is
   * the internal GemFireCacheImpl at present.
   *
   * @param cache the Cache reference to check for instance type.
   * @return a boolean indicating if bucket assignment is both enabled and allowed.
   * @see #isAssignBuckets()
   */
  private boolean isAssignBucketsAllowed(final Cache cache) {
    return isAssignBuckets() && cache instanceof GemFireCacheImpl;
  }

  /**
   * Assigns buckets to individual Partitioned Regions of the Cache.
   *
   * @param cache the Cache who's Partitioned Regions are accessed to assign buckets to.
   * @see PartitionRegionHelper#assignBucketsToPartitions(Region)
   */
  private void assignBuckets(final Cache cache) {
    if (isAssignBucketsAllowed(cache)) {
      for (PartitionedRegion region : ((InternalCache) cache).getPartitionedRegions()) {
        PartitionRegionHelper.assignBucketsToPartitions(region);
      }
    }
  }

  /**
   * Determines whether the Server is the process of starting or is already running.
   *
   * @return a boolean indicating if the Server is starting or is already running.
   */
  protected boolean isStartingOrRunning() {
    return starting.get() || isRunning();
  }

  /**
   * Invokes the 'status' command and operation to check the status of a GemFire server (a cache
   * server).
   */
  public ServerState status() {
    final ServerLauncher launcher = getInstance();
    // if this instance is running then return local status
    if (isStartingOrRunning()) {
      debug(
          "Getting status from the ServerLauncher instance that actually launched the GemFire Cache Server.%n");
      return new ServerState(this, isRunning() ? Status.ONLINE : Status.STARTING);
    }
    if (isPidInProcess() && launcher != null) {
      return launcher.statusInProcess();
    }
    if (getPid() != null) {
      debug("Getting Server status using process ID (%1$s)%n", getPid());
      return statusWithPid();
    }
    // attempt to get status using workingDirectory
    if (getWorkingDirectory() != null) {
      debug("Getting Server status using working directory (%1$s)%n", getWorkingDirectory());
      return statusWithWorkingDirectory();
    }
    debug("This ServerLauncher was not the instance used to launch the GemFire Cache Server, and "
        + "neither PID nor working directory were specified; the Server's state is unknown.%n");

    return new ServerState(this, Status.NOT_RESPONDING);
  }

  private ServerState statusInProcess() {
    if (isStartingOrRunning()) {
      debug(
          "Getting status from the ServerLauncher instance that actually launched the GemFire Cache Server.%n");
      return new ServerState(this, isRunning() ? Status.ONLINE : Status.STARTING);
    }
    return new ServerState(this, Status.NOT_RESPONDING);
  }

  private ServerState statusWithPid() {
    try {
      final ProcessController controller = new ProcessControllerFactory()
          .createProcessController(controllerParameters, getPid());
      controller.checkPidSupport();
      final String statusJson = controller.status();
      return ServerState.fromJson(statusJson);
    } catch (ConnectionFailedException handled) {
      // failed to attach to server JVM
      return createNoResponseState(handled,
          "Failed to connect to server with process id " + getPid());
    } catch (IOException | MBeanInvocationFailedException | UnableToControlProcessException
        | InterruptedException | TimeoutException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with server with process id " + getPid());
    }
  }

  private ServerState statusWithWorkingDirectory() {
    int parsedPid = 0;
    try {
      final ProcessController controller =
          new ProcessControllerFactory().createProcessController(controllerParameters,
              new File(getWorkingDirectory()), ProcessType.SERVER.getPidFileName());
      parsedPid = controller.getProcessId();

      // note: in-process request will go infinite loop unless we do the following
      if (parsedPid == identifyPid()) {
        final ServerLauncher runningLauncher = getInstance();
        if (runningLauncher != null) {
          return runningLauncher.statusInProcess();
        }
      }

      final String statusJson = controller.status();
      return ServerState.fromJson(statusJson);
    } catch (ConnectionFailedException handled) {
      // failed to attach to server JVM
      return createNoResponseState(handled,
          "Failed to connect to server with process id " + parsedPid);
    } catch (FileNotFoundException handled) {
      // could not find pid file
      return createNoResponseState(handled, "Failed to find process file "
          + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } catch (IOException | MBeanInvocationFailedException | UnableToControlProcessException
        | TimeoutException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with server with process id " + parsedPid);
    } catch (InterruptedException handled) {
      Thread.currentThread().interrupt();
      return createNoResponseState(handled,
          "Interrupted while trying to communicate with server with process id " + parsedPid);
    } catch (PidUnavailableException handled) {
      // couldn't determine pid from within server JVM
      return createNoResponseState(handled, "Failed to find usable process id within file "
          + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    }
  }

  /**
   * Determines whether the Server can be stopped in-process, such as when a Server is embedded in
   * an application and the ServerLauncher API is being used.
   *
   * @return a boolean indicating whether the Server can be stopped in-process (the application's
   *         process with an embedded Server).
   */
  private boolean isStoppable() {
    return isRunning() && getCache() != null;
  }

  /**
   * Invokes the 'stop' command and operation to stop a GemFire server (a cache server).
   */
  public ServerState stop() {
    final ServerLauncher launcher = getInstance();
    // if this instance is running then stop it
    if (isStoppable()) {
      return stopInProcess();
    }
    // if in-process but difference instance of ServerLauncher
    if (isPidInProcess() && launcher != null) {
      return launcher.stopInProcess();
    }
    // attempt to stop using pid if provided
    if (getPid() != null) {
      return stopWithPid();
    }
    // attempt to stop using workingDirectory
    if (getWorkingDirectory() != null) {
      return stopWithWorkingDirectory();
    }

    return new ServerState(this, Status.NOT_RESPONDING);
  }

  private ServerState stopInProcess() {
    if (isStoppable()) {
      if (cache.isReconnecting()) {
        cache.getDistributedSystem().stopReconnecting();
      }

      // Another case of needing to use a non-daemon thread to keep the JVM alive until a clean
      // shutdown can be performed. If not, the JVM may exit too early causing the member to be
      // seen as having crashed and not cleanly departed.
      Thread t = new LoggingThread("ServerLauncherStopper", false, this::doStopInProcess);
      t.start();

      try {
        t.join();
      } catch (InterruptedException ignore) {
        // no matter, we're shutting down...
      }

      // note: other thread may return Status.NOT_RESPONDING now
      INSTANCE.compareAndSet(this, null);
      running.set(false);
      return new ServerState(this, Status.STOPPED);
    }
    return new ServerState(this, Status.NOT_RESPONDING);
  }

  private void doStopInProcess() {
    cache.close();
    cache = null;
    if (process != null) {
      process.stop(deletePidFileOnStop);
      process = null;
    }
  }

  private ServerState stopWithPid() {
    try {
      final ProcessController controller = new ProcessControllerFactory()
          .createProcessController(controllerParameters, getPid());
      controller.checkPidSupport();
      controller.stop();
      return new ServerState(this, Status.STOPPED);
    } catch (ConnectionFailedException handled) {
      // failed to attach to server JVM
      return createNoResponseState(handled,
          "Failed to connect to server with process id " + getPid());
    } catch (IOException | MBeanInvocationFailedException
        | UnableToControlProcessException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with server with process id " + getPid());
    }
  }

  private ServerState stopWithWorkingDirectory() {
    int parsedPid = 0;
    try {
      final ProcessController controller =
          new ProcessControllerFactory().createProcessController(controllerParameters,
              new File(getWorkingDirectory()), ProcessType.SERVER.getPidFileName());
      parsedPid = controller.getProcessId();

      // NOTE in-process request will go infinite loop unless we do the following
      if (parsedPid == identifyPid()) {
        final ServerLauncher runningLauncher = getInstance();
        if (runningLauncher != null) {
          return runningLauncher.stopInProcess();
        }
      }

      controller.stop();
      return new ServerState(this, Status.STOPPED);
    } catch (ConnectionFailedException handled) {
      // failed to attach to server JVM
      return createNoResponseState(handled,
          "Failed to connect to server with process id " + parsedPid);
    } catch (FileNotFoundException handled) {
      // could not find pid file
      return createNoResponseState(handled, "Failed to find process file "
          + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } catch (IOException | MBeanInvocationFailedException
        | UnableToControlProcessException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with server with process id " + parsedPid);
    } catch (InterruptedException handled) {
      Thread.currentThread().interrupt();
      return createNoResponseState(handled,
          "Interrupted while trying to communicate with server with process id " + parsedPid);
    } catch (PidUnavailableException handled) {
      // couldn't determine pid from within server JVM
      return createNoResponseState(handled, "Failed to find usable process id within file "
          + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } catch (TimeoutException handled) {
      return createNoResponseState(handled,
          "Timed out trying to find usable process id within file "
              + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    }
  }

  private ServerState createNoResponseState(final Exception cause, final String errorMessage) {
    debug(ExceptionUtils.getStackTrace(cause) + errorMessage);
    return new ServerState(this, Status.NOT_RESPONDING, errorMessage);
  }

  private Properties getOverriddenDefaults() throws IOException {
    final Properties overriddenDefaults = new Properties();

    overriddenDefaults.setProperty(OVERRIDDEN_DEFAULTS_PREFIX.concat(LOG_FILE),
        getLogFile().getCanonicalPath());

    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith(OVERRIDDEN_DEFAULTS_PREFIX)) {
        overriddenDefaults.setProperty(key, System.getProperty(key));
      }
    }

    return overriddenDefaults;
  }

  private ControllableProcess getControllableProcess()
      throws IOException, FileAlreadyExistsException, PidUnavailableException {
    return controllableProcessFactory != null
        ? controllableProcessFactory.get()
        : new FileControllableProcess(controlHandler, new File(getWorkingDirectory()),
            ProcessType.SERVER, isForcing());
  }

  private class ServerControllerParameters implements ProcessControllerParameters {
    @Override
    public File getPidFile() {
      return getServerPidFile();
    }

    @Override
    public File getDirectory() {
      return new File(getWorkingDirectory());
    }

    @Override
    public int getProcessId() {
      return getPid();
    }

    @Override
    public ProcessType getProcessType() {
      return ProcessType.SERVER;
    }

    @Override
    public ObjectName getNamePattern() {
      try {
        return ObjectName.getInstance("GemFire:type=Member,*");
      } catch (MalformedObjectNameException | NullPointerException handled) {
        return null;
      }
    }

    @Override
    public String getPidAttribute() {
      return "ProcessId";
    }

    @Override
    public String getStopMethod() {
      return "shutDownMember";
    }

    @Override
    public String getStatusMethod() {
      return "status";
    }

    @Override
    public String[] getAttributes() {
      return new String[] {"Server"};
    }

    @Override
    public Object[] getValues() {
      return new Object[] {Boolean.TRUE};
    }
  }

  /**
   * The Builder class, modeled after the Builder creational design pattern, is used to construct a
   * properly configured and initialized instance of the ServerLauncher to control and run GemFire
   * servers (in particular, cache servers).
   */
  public static class Builder {

    @Immutable
    protected static final Command DEFAULT_COMMAND = Command.UNSPECIFIED;

    private boolean serverBindAddressSetByUser;
    private boolean serverPortSetByUser;

    private Boolean assignBuckets;
    private Boolean debug;
    private Boolean deletePidFileOnStop;
    private Boolean disableDefaultServer;
    private Boolean force;
    private Boolean help;
    private Boolean rebalance;
    private Boolean redirectOutput;

    private Cache cache;

    private final CacheConfig cacheConfig = new CacheConfig();

    private Command command;

    private InetAddress serverBindAddress;

    private Integer pid;
    private Integer serverPort;

    private final Properties distributedSystemProperties = new Properties();

    private String memberName;
    private String springXmlLocation;
    private String workingDirectory;

    private Float criticalHeapPercentage;
    private Float evictionHeapPercentage;

    private Float criticalOffHeapPercentage;
    private Float evictionOffHeapPercentage;

    private String hostNameForClients;
    private Integer loadPollInterval;
    private Integer maxConnections;
    private Integer maxMessageCount;
    private Integer messageTimeToLive;
    private Integer socketBufferSize;
    private Integer maxThreads;
    private Runnable startupCompletionAction;
    private Consumer<Throwable> startupExceptionAction;
    private ServerLauncherCacheProvider serverLauncherCacheProvider;
    private Supplier<ControllableProcess> controllableProcessFactory;

    /**
     * Default constructor used to create an instance of the Builder class for programmatical
     * access.
     */
    public Builder() {
      // nothing
    }

    /**
     * Constructor used to create and configure an instance of the Builder class with the specified
     * arguments, passed in from the command-line when launching an instance of this class from the
     * command-line using the Java launcher.
     *
     * @param args the array of arguments used to configure the Builder.
     * @see #parseArguments(String...)
     */
    public Builder(final String... args) {
      parseArguments(args != null ? args : new String[0]);
    }

    /**
     * Gets an instance of the JOptSimple OptionParser to parse the command-line arguments for
     * Server.
     *
     * @return an instance of the JOptSimple OptionParser configured with the command-line options
     *         used by the Server.
     */
    private OptionParser getParser() {
      OptionParser parser = new OptionParser(true);

      parser.accepts("assign-buckets");
      parser.accepts("debug");
      parser.accepts("dir").withRequiredArg().ofType(String.class);
      parser.accepts("disable-default-server");
      parser.accepts("force");
      parser.accepts("help");
      parser.accepts("member").withRequiredArg().ofType(String.class);
      parser.accepts("pid").withRequiredArg().ofType(Integer.class);
      parser.accepts("rebalance");
      parser.accepts("redirect-output");
      parser.accepts(SERVER_BIND_ADDRESS).withRequiredArg().ofType(String.class);
      parser.accepts("server-port").withRequiredArg().ofType(Integer.class);
      parser.accepts("spring-xml-location").withRequiredArg().ofType(String.class);
      parser.accepts("version");
      parser.accepts(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE).withRequiredArg()
          .ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE).withRequiredArg()
          .ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE).withRequiredArg()
          .ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE).withRequiredArg()
          .ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__MAX__CONNECTIONS).withRequiredArg()
          .ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__MAX__MESSAGE__COUNT).withRequiredArg()
          .ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__MAX__THREADS).withRequiredArg().ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE).withRequiredArg()
          .ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE).withRequiredArg()
          .ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS).withRequiredArg()
          .ofType(String.class);

      return parser;
    }

    /**
     * Parses the list of arguments to configure this Builder with the intent of constructing a
     * Server launcher to invoke a Cache Server. This method is called to parse the arguments
     * specified by the user on the command-line.
     *
     * @param args the array of arguments used to configure this Builder and create an instance of
     *        ServerLauncher.
     */
    void parseArguments(final String... args) {
      try {
        OptionSet options = getParser().parse(args);

        parseCommand(args);
        parseMemberName(args);

        setAssignBuckets(options.has("assign-buckets"));
        setDebug(options.has("debug"));
        setDeletePidFileOnStop(options.has("delete-pid-file-on-stop"));
        setDisableDefaultServer(options.has("disable-default-server"));
        setForce(options.has("force"));
        setHelp(options.has("help"));
        setRebalance(options.has("rebalance"));
        setRedirectOutput(options.has("redirect-output"));

        if (options.hasArgument(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE)) {
          setCriticalHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE)) {
          setEvictionHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE)) {
          setCriticalOffHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE)) {
          setEvictionOffHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__CONNECTIONS)) {
          setMaxConnections(Integer.parseInt(
              ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__CONNECTIONS))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__MESSAGE__COUNT)) {
          setMaxMessageCount(Integer.parseInt(
              ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__MESSAGE__COUNT))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE)) {
          setMessageTimeToLive(Integer.parseInt(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE)) {
          setSocketBufferSize(Integer.parseInt(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__THREADS)) {
          setMaxThreads(Integer.parseInt(
              ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__THREADS))));
        }

        if (!isHelping()) {
          if (options.has("dir")) {
            setWorkingDirectory(ObjectUtils.toString(options.valueOf("dir")));
          }

          if (options.has("pid")) {
            setPid((Integer) options.valueOf("pid"));
          }

          if (options.has(SERVER_BIND_ADDRESS)) {
            setServerBindAddress(ObjectUtils.toString(options.valueOf(SERVER_BIND_ADDRESS)));
          }

          if (options.has("server-port")) {
            setServerPort((Integer) options.valueOf("server-port"));
          }

          if (options.has("spring-xml-location")) {
            setSpringXmlLocation(ObjectUtils.toString(options.valueOf("spring-xml-location")));
          }

          if (options.has("version")) {
            setCommand(Command.VERSION);
          }
        }

        // why are these option not inside the 'if (!isHelping())' conditional block?

        if (options.hasArgument(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE)) {
          setCriticalHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE)) {
          setEvictionHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__CONNECTIONS)) {
          setMaxConnections(Integer.parseInt(
              ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__CONNECTIONS))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__MESSAGE__COUNT)) {
          setMaxMessageCount(Integer.parseInt(
              ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__MESSAGE__COUNT))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__THREADS)) {
          setMaxThreads(Integer.parseInt(
              ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__THREADS))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE)) {
          setMessageTimeToLive(Integer.parseInt(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE)) {
          setSocketBufferSize(Integer.parseInt(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS)) {
          setHostNameForClients(ObjectUtils
              .toString(options.valueOf(CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS)));
        }

      } catch (OptionException e) {
        throw new IllegalArgumentException(
            String.format("An error occurred while parsing command-line arguments for the %s: %s",
                "Server", e.getMessage()),
            e);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    /**
     * Iterates the list of arguments in search of the target Server launcher command.
     *
     * @param args an array of arguments from which to search for the Server launcher command.
     * @see ServerLauncher.Command#valueOfName(String)
     * @see #parseArguments(String...)
     */
    protected void parseCommand(final String... args) {
      if (args != null) {
        for (String arg : args) {
          Command command = Command.valueOfName(arg);
          if (command != null) {
            setCommand(command);
            break;
          }
        }
      }
    }

    /**
     * Iterates the list of arguments in search of the Server's GemFire member name. If the argument
     * does not start with '-' or is not the name of a Server launcher command, then the value is
     * presumed to be the member name for the Server in GemFire.
     *
     * @param args the array of arguments from which to search for the Server's member name in
     *        GemFire.
     * @see ServerLauncher.Command#isCommand(String)
     * @see #parseArguments(String...)
     */
    protected void parseMemberName(final String... args) {
      if (args != null) {
        for (String arg : args) {
          if (!(arg.startsWith(OPTION_PREFIX) || Command.isCommand(arg))) {
            setMemberName(arg);
            break;
          }
        }
      }
    }

    /**
     * Gets the CacheConfig object used to configure PDX on the GemFire Cache by the Builder.
     *
     * @return the CacheConfig object used to configure PDX on the GemFire Cache by the Builder.
     */
    CacheConfig getCacheConfig() {
      return cacheConfig;
    }

    /**
     * Gets the Server launcher command used during the invocation of the ServerLauncher.
     *
     * @return the Server launcher command used to invoke (run) the ServerLauncher class.
     * @see #setCommand(ServerLauncher.Command)
     * @see ServerLauncher.Command
     */
    public Command getCommand() {
      return command != null ? command : DEFAULT_COMMAND;
    }

    /**
     * Sets the Sever launcher command used during the invocation of the ServerLauncher
     *
     * @param command the targeted Server launcher command used during the invocation (run) of
     *        ServerLauncher.
     * @return this Builder instance.
     * @see #getCommand()
     * @see ServerLauncher.Command
     */
    public Builder setCommand(final Command command) {
      this.command = command;
      return this;
    }

    /**
     * Determines whether buckets should be assigned to partitioned regions in the cache upon Server
     * start.
     *
     * @return a boolean indicating if buckets should be assigned upon Server start.
     * @see #setAssignBuckets(Boolean)
     */
    public Boolean getAssignBuckets() {
      return assignBuckets;
    }

    /**
     * Sets whether buckets should be assigned to partitioned regions in the cache upon Server
     * start.
     *
     * @param assignBuckets a boolean indicating if buckets should be assigned upon Server start.
     * @return this Builder instance.
     * @see #getAssignBuckets()
     */
    public Builder setAssignBuckets(final Boolean assignBuckets) {
      this.assignBuckets = assignBuckets;
      return this;
    }

    @VisibleForTesting
    Cache getCache() {
      return cache;
    }

    @VisibleForTesting
    Builder setCache(final Cache cache) {
      this.cache = cache;
      return this;
    }

    /**
     * Determines whether the new instance of the ServerLauncher will be set to debug mode.
     *
     * @return a boolean value indicating whether debug mode is enabled or disabled.
     * @see #setDebug(Boolean)
     */
    public Boolean getDebug() {
      return debug;
    }

    /**
     * Sets whether the new instance of the ServerLauncher will be set to debug mode.
     *
     * @param debug a boolean value indicating whether debug mode is to be enabled or disabled.
     * @return this Builder instance.
     * @see #getDebug()
     */
    public Builder setDebug(final Boolean debug) {
      this.debug = debug;
      return this;
    }

    /**
     * Determines whether the Geode Server should delete the pid file when its service stops or when
     * the JVM exits.
     *
     * @return a boolean value indicating if the pid file should be deleted when this service stops
     *         or when the JVM exits.
     * @see #setDeletePidFileOnStop(Boolean)
     */
    public Boolean getDeletePidFileOnStop() {
      return deletePidFileOnStop;
    }

    /**
     * Sets whether the Geode Server should delete the pid file when its service stops or when the
     * JVM exits.
     *
     * @param deletePidFileOnStop a boolean value indicating if the pid file should be deleted when
     *        this service stops or when the JVM exits.
     * @return this Builder instance.
     * @see #getDeletePidFileOnStop()
     */
    public Builder setDeletePidFileOnStop(final Boolean deletePidFileOnStop) {
      this.deletePidFileOnStop = deletePidFileOnStop;
      return this;
    }

    /**
     * Determines whether a default cache server will be added when the Geode Server comes online.
     *
     * @return a boolean value indicating whether to add a default cache server.
     * @see #setDisableDefaultServer(Boolean)
     */
    public Boolean getDisableDefaultServer() {
      return disableDefaultServer;
    }

    /**
     * Sets a boolean value indicating whether to add a default cache when the GemFire Server comes
     * online.
     *
     * @param disableDefaultServer a boolean value indicating whether to add a default cache server.
     * @return this Builder instance.
     * @see #getDisableDefaultServer()
     */
    public Builder setDisableDefaultServer(final Boolean disableDefaultServer) {
      this.disableDefaultServer = disableDefaultServer;
      return this;
    }

    /**
     * Gets the GemFire Distributed System (cluster) Properties configuration.
     *
     * @return a Properties object containing configuration settings for the GemFire Distributed
     *         System (cluster).
     * @see Properties
     */
    public Properties getDistributedSystemProperties() {
      return distributedSystemProperties;
    }

    /**
     * Gets the boolean value used by the Server to determine if it should overwrite the PID file if
     * it already exists.
     *
     * @return the boolean value specifying whether or not to overwrite the PID file if it already
     *         exists.
     * @see #setForce(Boolean)
     */
    public Boolean getForce() {
      return force != null ? force : DEFAULT_FORCE;
    }

    /**
     * Sets the boolean value used by the Server to determine if it should overwrite the PID file if
     * it already exists.
     *
     * @param force a boolean value indicating whether to overwrite the PID file when it already
     *        exists.
     * @return this Builder instance.
     * @see #getForce()
     */
    public Builder setForce(final Boolean force) {
      this.force = force;
      return this;
    }

    /**
     * Determines whether the new instance of the ServerLauncher will be used to output help
     * information for either a specific command, or for using ServerLauncher in general.
     *
     * @return a boolean value indicating whether help will be output during the invocation of the
     *         ServerLauncher.
     * @see #setHelp(Boolean)
     */
    public Boolean getHelp() {
      return help;
    }

    /**
     * Determines whether help has been enabled.
     *
     * @return a boolean indicating if help was enabled.
     */
    protected boolean isHelping() {
      return Boolean.TRUE.equals(getHelp());
    }

    /**
     * Sets whether the new instance of ServerLauncher will be used to output help information for
     * either a specific command, or for using ServerLauncher in general.
     *
     * @param help a boolean indicating whether help information is to be displayed during
     *        invocation of ServerLauncher.
     * @return this Builder instance.
     * @see #getHelp()
     */
    public Builder setHelp(final Boolean help) {
      this.help = help;
      return this;
    }

    /**
     * Determines whether a rebalance operation on the cache will occur upon starting the GemFire
     * server.
     *
     * @return a boolean indicating if the cache will be rebalance when the GemFire server starts.
     * @see #setRebalance(Boolean)
     */
    public Boolean getRebalance() {
      return rebalance;
    }

    /**
     * Set a boolean value indicating whether a rebalance operation on the cache should occur upon
     * starting the GemFire server.
     *
     * @param rebalance a boolean indicating if the cache will be rebalanced when the GemFire server
     *        starts.
     * @return this Builder instance.
     * @see #getRebalance()
     */
    public Builder setRebalance(final Boolean rebalance) {
      this.rebalance = rebalance;
      return this;
    }

    /**
     * Gets the member name of this Server in GemFire.
     *
     * @return a String indicating the member name of this Server in GemFire.
     * @see #setMemberName(String)
     */
    public String getMemberName() {
      return memberName;
    }

    /**
     * Sets the member name of the Server in GemFire.
     *
     * @param memberName a String indicating the member name of this Server in GemFire.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the member name is invalid.
     * @see #getMemberName()
     */
    public Builder setMemberName(final String memberName) {
      if (isBlank(memberName)) {
        throw new IllegalArgumentException("The Server member name must be specified.");
      }
      this.memberName = memberName;
      return this;
    }

    /**
     * Gets the process ID (PID) of the running Server indicated by the user as an argument to the
     * ServerLauncher. This PID is used by the Server launcher to determine the Server's status, or
     * invoke shutdown on the Server.
     *
     * @return a user specified Integer value indicating the process ID of the running Server.
     * @see #setPid(Integer)
     */
    public Integer getPid() {
      return pid;
    }

    /**
     * Sets the process ID (PID) of the running Server indicated by the user as an argument to the
     * ServerLauncher. This PID will be used by the Server launcher to determine the Server's
     * status, or invoke shutdown on the Server.
     *
     * @param pid a user specified Integer value indicating the process ID of the running Server.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the process ID (PID) is not valid (greater than zero if
     *         not null).
     * @see #getPid()
     */
    public Builder setPid(final Integer pid) {
      if (pid != null && pid < 0) {
        throw new IllegalArgumentException(
            "A process ID (PID) must be a non-negative integer value.");
      }
      this.pid = pid;
      return this;
    }

    /**
     * Determines whether the new instance of ServerLauncher will redirect output to system logs
     * when starting a Server.
     *
     * @return a boolean value indicating if output will be redirected to system logs when starting
     *         a Server
     * @see #setRedirectOutput(Boolean)
     */
    public Boolean getRedirectOutput() {
      return redirectOutput;
    }

    /**
     * Determines whether redirecting of output has been enabled.
     *
     * @return a boolean indicating if redirecting of output was enabled.
     */
    private boolean isRedirectingOutput() {
      return Boolean.TRUE.equals(getRedirectOutput());
    }

    /**
     * Sets whether the new instance of ServerLauncher will redirect output to system logs when
     * starting a Server.
     *
     * @param redirectOutput a boolean value indicating if output will be redirected to system logs
     *        when starting a Server.
     * @return this Builder instance.
     * @see #getRedirectOutput()
     */
    public Builder setRedirectOutput(final Boolean redirectOutput) {
      this.redirectOutput = redirectOutput;
      return this;
    }

    /**
     * Gets the IP address to which the Server will be bound listening for and accepting cache
     * client connections in a client/server topology.
     *
     * @return an InetAddress indicating the IP address that the Server is bound to listening for
     *         and accepting cache client connections in a client/server topology.
     * @see #setServerBindAddress(String)
     */
    public InetAddress getServerBindAddress() {
      return serverBindAddress;
    }

    boolean isServerBindAddressSetByUser() {
      return serverBindAddressSetByUser;
    }

    /**
     * Sets the IP address to which the Server will be bound listening for and accepting cache
     * client connections in a client/server topology.
     *
     * @param serverBindAddress a String specifying the IP address or hostname that the Server will
     *        be bound to listen for and accept cache client connections in a client/server
     *        topology.
     * @return this Builder instance.
     * @throws IllegalArgumentException wrapping the UnknownHostException if the IP address or
     *         hostname for the server bind address is unknown.
     * @see #getServerBindAddress()
     */
    public Builder setServerBindAddress(final String serverBindAddress) {
      if (isBlank(serverBindAddress)) {
        this.serverBindAddress = null;
        return this;
      }

      // NOTE only set the 'bind address' if the user specified a value
      try {
        InetAddress bindAddress = InetAddress.getByName(serverBindAddress);
        if (LocalHostUtil.isLocalHost(bindAddress)) {
          this.serverBindAddress = bindAddress;
          serverBindAddressSetByUser = true;
          return this;
        }

        throw new IllegalArgumentException(
            serverBindAddress + " is not an address for this machine.");
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException(
            String.format("The hostname/IP address to which the %s will be bound is unknown.",
                "Server"),
            e);
      }
    }

    /**
     * Gets the port on which the Server will listen for and accept cache client connections in a
     * client/server topology.
     *
     * @return an Integer value specifying the port the Server will listen on and accept cache
     *         client connections in a client/server topology.
     * @see #setServerPort(Integer)
     */
    public Integer getServerPort() {
      return serverPort != null ? serverPort : getDefaultServerPort();
    }

    boolean isServerPortSetByUser() {
      return serverPortSetByUser;
    }

    /**
     * Sets the port on which the Server will listen for and accept cache client connections in a
     * client/server topology.
     *
     * @param serverPort an Integer value specifying the port the Server will listen on and accept
     *        cache client connections in a client/server topology.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the port number is not valid.
     * @see #getServerPort()
     */
    public Builder setServerPort(final Integer serverPort) {
      if (serverPort == null) {
        this.serverPort = null;
        serverPortSetByUser = false;
        return this;
      }

      if (serverPort < 0 || serverPort > 65535) {
        throw new IllegalArgumentException(
            "The port on which the Server will listen must be between 1 and 65535 inclusive.");
      }

      if (serverPort == 0) {
        this.serverPort = 0;
        serverPortSetByUser = false;
      } else {
        this.serverPort = serverPort;
        serverPortSetByUser = true;
      }

      return this;
    }

    /**
     * Gets the location of the Spring XML configuration meta-data file used to bootstrap, configure
     * and initialize the GemFire Server on start.
     * <p>
     *
     * @return a String indicating the location of the Spring XML configuration file.
     * @see #setSpringXmlLocation(String)
     */
    public String getSpringXmlLocation() {
      return springXmlLocation;
    }

    /**
     * Sets the location of the Spring XML configuration meta-data file used to bootstrap, configure
     * and initialize the GemFire Server on start.
     * <p>
     *
     * @param springXmlLocation a String indicating the location of the Spring XML configuration
     *        file.
     * @return this Builder instance.
     * @see #getSpringXmlLocation()
     */
    public Builder setSpringXmlLocation(final String springXmlLocation) {
      this.springXmlLocation = springXmlLocation;
      return this;
    }

    /**
     * Gets the working directory pathname in which the Server will be ran. If the directory is
     * unspecified, then working directory defaults to the current directory.
     *
     * @return a String indicating the working directory pathname.
     * @see #setWorkingDirectory(String)
     */
    public String getWorkingDirectory() {
      return tryGetCanonicalPathElseGetAbsolutePath(
          new File(defaultIfBlank(workingDirectory, DEFAULT_WORKING_DIRECTORY)));
    }

    /**
     * Sets the working directory in which the Server will be ran. This also the directory in which
     * all Server files (such as log and license files) will be written. If the directory is
     * unspecified, then the working directory defaults to the current directory.
     *
     * @param workingDirectory a String indicating the pathname of the directory in which the Server
     *        will be ran.
     * @return this Builder instance.
     * @throws IllegalArgumentException wrapping a FileNotFoundException if the working directory
     *         pathname cannot be found.
     * @see #getWorkingDirectory()
     * @see FileNotFoundException
     */
    public Builder setWorkingDirectory(final String workingDirectory) {
      if (!new File(defaultIfBlank(workingDirectory, DEFAULT_WORKING_DIRECTORY)).isDirectory()) {
        throw new IllegalArgumentException(
            String.format(AbstractLauncher.WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE,
                "Server"),
            new FileNotFoundException(workingDirectory));
      }
      this.workingDirectory = workingDirectory;
      return this;
    }

    public Float getCriticalHeapPercentage() {
      return criticalHeapPercentage;
    }

    public Builder setCriticalHeapPercentage(final Float criticalHeapPercentage) {
      if (criticalHeapPercentage != null) {
        if (criticalHeapPercentage < 0 || criticalHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(
              String.format("Critical heap percentage (%1$s) must be between 0 and 100!",
                  criticalHeapPercentage));
        }
      }
      this.criticalHeapPercentage = criticalHeapPercentage;
      return this;
    }

    public Float getCriticalOffHeapPercentage() {
      return criticalOffHeapPercentage;
    }

    public Builder setCriticalOffHeapPercentage(final Float criticalOffHeapPercentage) {
      if (criticalOffHeapPercentage != null) {
        if (criticalOffHeapPercentage < 0 || criticalOffHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(
              String.format("Critical off-heap percentage (%1$s) must be between 0 and 100!",
                  criticalOffHeapPercentage));
        }
      }
      this.criticalOffHeapPercentage = criticalOffHeapPercentage;
      return this;
    }

    public Float getEvictionHeapPercentage() {
      return evictionHeapPercentage;
    }

    public Builder setEvictionHeapPercentage(final Float evictionHeapPercentage) {
      if (evictionHeapPercentage != null) {
        if (evictionHeapPercentage < 0 || evictionHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(
              String.format("Eviction heap percentage (%1$s) must be between 0 and 100!",
                  evictionHeapPercentage));
        }
      }
      this.evictionHeapPercentage = evictionHeapPercentage;
      return this;
    }

    public Float getEvictionOffHeapPercentage() {
      return evictionOffHeapPercentage;
    }

    public Builder setEvictionOffHeapPercentage(final Float evictionOffHeapPercentage) {
      if (evictionOffHeapPercentage != null) {
        if (evictionOffHeapPercentage < 0 || evictionOffHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(
              String.format("Eviction off-heap percentage (%1$s) must be between 0 and 100",
                  evictionOffHeapPercentage));
        }
      }
      this.evictionOffHeapPercentage = evictionOffHeapPercentage;
      return this;
    }

    public String getHostNameForClients() {
      return hostNameForClients;
    }

    public Builder setHostNameForClients(String hostNameForClients) {
      if (isBlank(hostNameForClients)) {
        throw new IllegalArgumentException(
            "The hostname used by clients to connect to the Server must have an argument if the "
                + "--hostname-for-clients command-line option is specified!");
      }
      this.hostNameForClients = hostNameForClients;
      return this;
    }

    public Integer getMaxConnections() {
      return maxConnections;
    }

    public Builder setMaxConnections(Integer maxConnections) {
      if (maxConnections != null && maxConnections < 1) {
        throw new IllegalArgumentException(
            String.format("Max Connections (%1$s) must be greater than 0!", maxConnections));
      }
      this.maxConnections = maxConnections;
      return this;
    }

    public Integer getMaxMessageCount() {
      return maxMessageCount;
    }

    public Builder setMaxMessageCount(Integer maxMessageCount) {
      if (maxMessageCount != null && maxMessageCount < 1) {
        throw new IllegalArgumentException(
            String.format("Max Message Count (%1$s) must be greater than 0!", maxMessageCount));
      }
      this.maxMessageCount = maxMessageCount;
      return this;
    }

    public Integer getMaxThreads() {
      return maxThreads;
    }

    public Builder setMaxThreads(Integer maxThreads) {
      if (maxThreads != null && maxThreads < 1) {
        throw new IllegalArgumentException(
            String.format("Max Threads (%1$s) must be greater than 0!", maxThreads));
      }
      this.maxThreads = maxThreads;
      return this;
    }

    public Integer getMessageTimeToLive() {
      return messageTimeToLive;
    }

    public Builder setMessageTimeToLive(Integer messageTimeToLive) {
      if (messageTimeToLive != null && messageTimeToLive < 1) {
        throw new IllegalArgumentException(String
            .format("Message Time To Live (%1$s) must be greater than 0!", messageTimeToLive));
      }
      this.messageTimeToLive = messageTimeToLive;
      return this;
    }

    public Integer getSocketBufferSize() {
      return socketBufferSize;
    }

    public Builder setSocketBufferSize(Integer socketBufferSize) {
      if (socketBufferSize != null && socketBufferSize < 1) {
        throw new IllegalArgumentException(String.format(
            "The Server's Socket Buffer Size (%1$s) must be greater than 0!", socketBufferSize));
      }
      this.socketBufferSize = socketBufferSize;
      return this;
    }


    /**
     * Sets a GemFire Distributed System Property.
     *
     * @param propertyName a String indicating the name of the GemFire Distributed System property
     *        as described in {@link ConfigurationProperties}
     * @param propertyValue a String value for the GemFire Distributed System property.
     * @return this Builder instance.
     */
    public Builder set(final String propertyName, final String propertyValue) {
      distributedSystemProperties.setProperty(propertyName, propertyValue);
      return this;
    }

    /**
     * add the properties in the Gemfire Distributed System Property
     *
     * @param properties a property object that holds one or more Gemfire Distributed System
     *        properties as described in {@link ConfigurationProperties}
     * @return this Builder instance
     * @since Geode 1.12
     */
    public Builder set(final Properties properties) {
      distributedSystemProperties.putAll(properties);
      return this;
    }

    /**
     * Sets whether the PDX type meta-data should be persisted to disk.
     *
     * @param persistent a boolean indicating whether PDX type meta-data should be persisted to
     *        disk.
     * @return this Builder instance.
     */
    public Builder setPdxPersistent(final boolean persistent) {
      cacheConfig.setPdxPersistent(persistent);
      return this;
    }

    /**
     * Sets the GemFire Disk Store to be used to persist PDX type meta-data.
     *
     * @param pdxDiskStore a String indicating the name of the GemFire Disk Store to use to store
     *        PDX type meta-data
     * @return this Builder instance.
     */
    public Builder setPdxDiskStore(final String pdxDiskStore) {
      cacheConfig.setPdxDiskStore(pdxDiskStore);
      return this;
    }

    /**
     * Sets whether fields in the PDX instance should be ignored when unread.
     *
     * @param ignore a boolean indicating whether unread fields in the PDX instance should be
     *        ignored.
     * @return this Builder instance.
     */
    public Builder setPdxIgnoreUnreadFields(final boolean ignore) {
      cacheConfig.setPdxIgnoreUnreadFields(ignore);
      return this;
    }

    /**
     * Sets whether PDX instances should be returned as is when Region.get(key:String):Object is
     * called.
     *
     * @param readSerialized a boolean indicating whether the PDX instance should be returned from a
     *        call to Region.get(key:String):Object
     * @return this Builder instance.
     */
    public Builder setPdxReadSerialized(final boolean readSerialized) {
      cacheConfig.setPdxReadSerialized(readSerialized);
      return this;
    }

    /**
     * Set the PdxSerializer to use to serialize POJOs to the GemFire Cache Region or when sent
     * between peers, client/server, or during persistence to disk.
     *
     * @param pdxSerializer the PdxSerializer that is used to serialize application domain objects
     *        into PDX.
     * @return this Builder instance.
     */
    public Builder setPdxSerializer(final PdxSerializer pdxSerializer) {
      cacheConfig.setPdxSerializer(pdxSerializer);
      return this;
    }

    /**
     * Validates the configuration settings and properties of this Builder, ensuring that all
     * invariants have been met. Currently, the only invariant constraining the Builder is that the
     * user must specify the member name for the Server in the GemFire distributed system as a
     * command-line argument, or by setting the memberName property programmatically using the
     * corresponding setter method.
     *
     * @throws IllegalStateException if the Builder is not properly configured.
     */
    protected void validate() {
      if (!isHelping()) {
        validateOnStart();
        validateOnStatus();
        validateOnStop();
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'start' command has been issued.
     *
     * @see ServerLauncher.Command#START
     */
    void validateOnStart() {
      if (Command.START == getCommand()) {
        if (isBlank(getMemberName())
            && !isSet(System.getProperties(), GeodeGlossary.GEMFIRE_PREFIX + NAME)
            && !isSet(getDistributedSystemProperties(), NAME)
            && !isSet(loadGemFireProperties(DistributedSystem.getPropertyFileURL()), NAME)) {
          throw new IllegalStateException(
              String.format(
                  MEMBER_NAME_ERROR_MESSAGE,
                  "Server", "Server"));
        }

        if (!CURRENT_DIRECTORY.equalsIgnoreCase(getWorkingDirectory())) {
          throw new IllegalStateException(
              String.format(
                  AbstractLauncher.WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE,
                  "Server", "Server"));
        }
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'status' command has been issued.
     *
     * @see ServerLauncher.Command#STATUS
     */
    void validateOnStatus() {
      if (Command.STATUS == getCommand()) {
        // do nothing
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'stop' command has been issued.
     *
     * @see ServerLauncher.Command#STOP
     */
    void validateOnStop() {
      if (Command.STOP == getCommand()) {
        // do nothing
      }
    }

    /**
     * Validates the Builder configuration settings and then constructs an instance of the
     * ServerLauncher class to invoke operations on a GemFire Server.
     *
     * @return a newly constructed instance of the ServerLauncher configured with this Builder.
     * @see #validate()
     * @see ServerLauncher
     */
    public ServerLauncher build() {
      validate();
      return new ServerLauncher(this);
    }

    /**
     * Sets the action to run when the server is online.
     *
     * @param startupCompletionAction the action to run
     * @return this builder
     */
    Builder setStartupCompletionAction(Runnable startupCompletionAction) {
      this.startupCompletionAction = startupCompletionAction;
      return this;
    }

    /**
     * Gets the action to run when the server is online.
     *
     * @return the action to run
     */
    Runnable getStartupCompletionAction() {
      return startupCompletionAction;
    }

    /**
     * Sets the action to run when server startup completes with errors.
     *
     * @param startupExceptionAction the action to run
     * @return this builder
     */
    Builder setStartupExceptionAction(Consumer<Throwable> startupExceptionAction) {
      this.startupExceptionAction = startupExceptionAction;
      return this;
    }

    /**
     * Gets the action to run when server startup completed with errors.
     *
     * @return the action to run
     */
    Consumer<Throwable> getStartupExceptionAction() {
      return startupExceptionAction;
    }

    /**
     * Sets the ServerLauncherCacheProvider to use when creating the cache.
     *
     * @param serverLauncherCacheProvider the cache provider to use
     * @return this builder
     */
    Builder setServerLauncherCacheProvider(
        ServerLauncherCacheProvider serverLauncherCacheProvider) {
      this.serverLauncherCacheProvider = serverLauncherCacheProvider;
      return this;
    }

    /**
     * Gets the ServerLauncherCacheProvider to use when creating the cache.
     *
     * @return the cache provider
     */
    ServerLauncherCacheProvider getServerLauncherCacheProvider() {
      return serverLauncherCacheProvider;
    }

    /**
     * Sets the factory to use to get a {@code ControllableProcess} when starting the server.
     *
     * @param controllableProcessFactory the controllable process factory to use
     * @return this builder
     */
    Builder setControllableProcessFactory(
        Supplier<ControllableProcess> controllableProcessFactory) {
      this.controllableProcessFactory = controllableProcessFactory;
      return this;
    }

    /**
     * Gets the factory used to get a {@code ControllableProcess} when starting the server.
     *
     * @return the controllable process factory
     */
    Supplier<ControllableProcess> getControllableProcessFactory() {
      return controllableProcessFactory;
    }
  }

  /**
   * An enumerated type representing valid commands to the Server launcher.
   */
  public enum Command {
    START("start", "assign-buckets", "disable-default-server", "rebalance", SERVER_BIND_ADDRESS,
        "server-port", "force", "debug", "help"),
    STATUS("status", "member", "pid", "dir", "debug", "help"),
    STOP("stop", "member", "pid", "dir", "debug", "help"),
    UNSPECIFIED("unspecified"),
    VERSION("version");

    private final List<String> options;

    private final String name;

    Command(final String name, final String... options) {
      assert isNotBlank(name) : "The name of the command must be specified!";
      this.name = name;
      this.options = options != null ? Collections.unmodifiableList(Arrays.asList(options))
          : Collections.emptyList();
    }

    /**
     * Determines whether the specified name refers to a valid Server launcher command, as defined
     * by this enumerated type.
     *
     * @param name a String value indicating the potential name of a Server launcher command.
     * @return a boolean indicating whether the specified name for a Server launcher command is
     *         valid.
     */
    public static boolean isCommand(final String name) {
      return valueOfName(name) != null;
    }

    /**
     * Determines whether the given Server launcher command has been properly specified. The command
     * is deemed unspecified if the reference is null or the Command is UNSPECIFIED.
     *
     * @param command the Server launcher command.
     * @return a boolean value indicating whether the Server launcher command is unspecified.
     * @see Command#UNSPECIFIED
     */
    public static boolean isUnspecified(final Command command) {
      return command == null || command.isUnspecified();
    }

    /**
     * Looks up a Server launcher command by name. The equality comparison on name is
     * case-insensitive.
     *
     * @param name a String value indicating the name of the Server launcher command.
     * @return an enumerated type representing the command name or null if the no such command with
     *         the specified name exists.
     */
    public static Command valueOfName(final String name) {
      for (final Command command : values()) {
        if (command.getName().equalsIgnoreCase(name)) {
          return command;
        }
      }

      return null;
    }

    /**
     * Gets the name of the Server launcher command.
     *
     * @return a String value indicating the name of the Server launcher command.
     */
    public String getName() {
      return name;
    }

    /**
     * Gets a set of valid options that can be used with the Server launcher command when used from
     * the command-line.
     *
     * @return a Set of Strings indicating the names of the options available to the Server launcher
     *         command.
     */
    public List<String> getOptions() {
      return options;
    }

    /**
     * Determines whether this Server launcher command has the specified command-line option.
     *
     * @param option a String indicating the name of the command-line option to this command.
     * @return a boolean value indicating whether this command has the specified named command-line
     *         option.
     */
    public boolean hasOption(final String option) {
      return getOptions().contains(lowerCase(option));
    }

    /**
     * Convenience method for determining whether this is the UNSPECIFIED Server launcher command.
     *
     * @return a boolean indicating if this command is UNSPECIFIED.
     * @see #UNSPECIFIED
     */
    public boolean isUnspecified() {
      return this == UNSPECIFIED;
    }

    /**
     * Gets the String representation of this Server launcher command.
     *
     * @return a String value representing this Server launcher command.
     */
    @Override
    public String toString() {
      return getName();
    }
  }

  /**
   * The ServerState is an immutable type representing the state of the specified Server at any
   * given moment in time. The state of the Server is assessed at the exact moment an instance of
   * this class is constructed.
   *
   * @see AbstractLauncher.ServiceState
   */
  public static class ServerState extends ServiceState<String> {

    /**
     * Unmarshals a ServerState instance from the JSON String.
     *
     * @return a ServerState value unmarshalled from the JSON String.
     */
    public static ServerState fromJson(final String json) {
      try {
        final JsonNode jsonObject = new ObjectMapper().readTree(json);

        final Status status = Status.valueOfDescription(jsonObject.get(JSON_STATUS).asText());
        final List<String> jvmArguments = JsonUtil.toStringList(jsonObject.get(JSON_JVMARGUMENTS));

        return new ServerState(status, jsonObject.get(JSON_STATUSMESSAGE).asText(),
            jsonObject.get(JSON_TIMESTAMP).asLong(), jsonObject.get(JSON_LOCATION).asText(),
            jsonObject.get(JSON_PID).asInt(), jsonObject.get(JSON_UPTIME).asLong(),
            jsonObject.get(JSON_WORKINGDIRECTORY).asText(), jvmArguments,
            jsonObject.get(JSON_CLASSPATH).asText(), jsonObject.get(JSON_GEMFIREVERSION).asText(),
            jsonObject.get(JSON_JAVAVERSION).asText(), jsonObject.get(JSON_LOGFILE).asText(),
            jsonObject.get(JSON_HOST).asText(), jsonObject.get(JSON_PORT).asText(),
            jsonObject.get(JSON_MEMBERNAME).asText());
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to create ServerStatus from JSON: " + json, e);
      }
    }

    public static ServerState fromDirectory(final String workingDirectory,
        final String memberName) {
      ServerState serverState = new ServerLauncher.Builder().setWorkingDirectory(workingDirectory)
          .setDisableDefaultServer(true).build().status();

      if (ObjectUtils.equals(serverState.getMemberName(), memberName)) {
        return serverState;
      }

      return new ServerState(new ServerLauncher.Builder().build(), Status.NOT_RESPONDING);
    }

    public ServerState(final ServerLauncher launcher, final Status status) {
      this(status,
          launcher.statusMessage,
          System.currentTimeMillis(),
          launcher.getId(),
          identifyPid(),
          ManagementFactory.getRuntimeMXBean().getUptime(),
          launcher.getWorkingDirectory(),
          ManagementFactory.getRuntimeMXBean().getInputArguments(),
          System.getProperty("java.class.path"),
          GemFireVersion.getGemFireVersion(),
          System.getProperty("java.version"),
          getServerLogFileCanonicalPath(launcher),
          getServerBindAddressAsString(launcher),
          getServerPortAsString(launcher),
          launcher.getMemberName());
    }

    public ServerState(final ServerLauncher launcher, final Status status,
        final String errorMessage) {
      this(status,
          errorMessage,
          System.currentTimeMillis(),
          getServerLocation(launcher),
          null,
          0L,
          launcher.getWorkingDirectory(),
          ManagementFactory.getRuntimeMXBean().getInputArguments(),
          null,
          GemFireVersion.getGemFireVersion(),
          System.getProperty("java.version"),
          null,
          getServerBindAddressAsString(launcher),
          launcher.getServerPortAsString(),
          null);
    }

    /*
     * Guards against throwing NPEs due to incorrect or missing host information while constructing
     * error states
     */
    private static String getServerLocation(ServerLauncher launcher) {
      if (launcher.getServerPort() == null) {
        return launcher.getId();
      }
      if (launcher.getServerBindAddress() == null) {
        return HostUtils.getLocatorId(HostUtils.getLocalHost(), launcher.getServerPort());
      }
      return HostUtils.getServerId(launcher.getServerBindAddress().getCanonicalHostName(),
          launcher.getServerPort());
    }

    protected ServerState(final Status status, final String statusMessage, final long timestamp,
        final String serverLocation, final Integer pid, final Long uptime,
        final String workingDirectory, final List<String> jvmArguments, final String classpath,
        final String gemfireVersion, final String javaVersion, final String logFile,
        final String host, final String port, final String memberName) {
      super(status, statusMessage, timestamp, serverLocation, pid, uptime, workingDirectory,
          jvmArguments, classpath, gemfireVersion, javaVersion, logFile, host, port, memberName);
    }

    private static String getServerLogFileCanonicalPath(final ServerLauncher launcher) {
      final InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();

      if (system != null) {
        final File logFile = system.getConfig().getLogFile();
        if (logFile != null && logFile.isFile()) {
          final String logFileCanonicalPath = tryGetCanonicalPathElseGetAbsolutePath(logFile);
          if (isNotBlank(logFileCanonicalPath)) {
            return logFileCanonicalPath;
          }
        }
      }
      return launcher.getLogFileCanonicalPath();
    }

    private static String getServerBindAddressAsString(final ServerLauncher launcher) {
      final InternalCache internalCache = GemFireCacheImpl.getInstance();

      if (internalCache != null) {
        final List<CacheServer> csList = internalCache.getCacheServers();
        if (csList != null && !csList.isEmpty()) {
          final CacheServer cs = csList.get(0);
          final String serverBindAddressAsString = cs.getBindAddress();
          if (isNotBlank(serverBindAddressAsString)) {
            return serverBindAddressAsString;
          }
        }
      }
      return launcher.getServerBindAddressAsString();
    }

    private static String getServerPortAsString(final ServerLauncher launcher) {
      final InternalCache internalCache = GemFireCacheImpl.getInstance();

      if (internalCache != null) {
        final List<CacheServer> csList = internalCache.getCacheServers();
        if (csList != null && !csList.isEmpty()) {
          final CacheServer cs = csList.get(0);
          final String portAsString = String.valueOf(cs.getPort());
          if (isNotBlank(portAsString)) {
            return portAsString;
          }
        }
      }
      return launcher.isDisableDefaultServer() ? EMPTY : launcher.getServerPortAsString();
    }

    @Override
    protected String getServiceName() {
      return SERVER_SERVICE_NAME;
    }
  }
}
