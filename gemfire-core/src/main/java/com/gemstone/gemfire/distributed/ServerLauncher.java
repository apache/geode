/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.distributed;

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
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.AbstractCacheServer;
import com.gemstone.gemfire.internal.cache.CacheConfig;
import com.gemstone.gemfire.internal.cache.CacheServerLauncher;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.internal.process.ClusterConfigurationNotAvailableException;
import com.gemstone.gemfire.internal.process.ConnectionFailedException;
import com.gemstone.gemfire.internal.process.ControlNotificationHandler;
import com.gemstone.gemfire.internal.process.ControllableProcess;
import com.gemstone.gemfire.internal.process.FileAlreadyExistsException;
import com.gemstone.gemfire.internal.process.MBeanInvocationFailedException;
import com.gemstone.gemfire.internal.process.PidUnavailableException;
import com.gemstone.gemfire.internal.process.ProcessController;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessControllerParameters;
import com.gemstone.gemfire.internal.process.ProcessLauncherContext;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.StartupStatusListener;
import com.gemstone.gemfire.internal.process.UnableToControlProcessException;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.lang.AttachAPINotFoundException;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonArray;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonObject;
import com.gemstone.gemfire.pdx.PdxSerializer;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.springframework.data.gemfire.support.SpringContextBootstrappingInitializer;

/**
 * The ServerLauncher class is a launcher class with main method to start a GemFire Server (implying a GemFire Cache
 * Server process).
 * 
 * @author John Blum
 * @author Kirk Lund
 * @see com.gemstone.gemfire.distributed.AbstractLauncher
 * @see com.gemstone.gemfire.distributed.LocatorLauncher
 * @since 7.0
 */
@SuppressWarnings({ "unused" })
public final class ServerLauncher extends AbstractLauncher<String> {

  /**
   * @deprecated This is specific to the internal implementation and may go away in a future release.
   */
  protected static final Integer DEFAULT_SERVER_PORT = getDefaultServerPort();

  private static final Map<String, String> helpMap = new HashMap<>();

  static {
    helpMap.put("launcher", LocalizedStrings.ServerLauncher_SERVER_LAUNCHER_HELP.toLocalizedString());
    helpMap.put(Command.START.getName(), LocalizedStrings.ServerLauncher_START_SERVER_HELP.toLocalizedString(String.valueOf(getDefaultServerPort())));
    helpMap.put(Command.STATUS.getName(), LocalizedStrings.ServerLauncher_STATUS_SERVER_HELP.toLocalizedString());
    helpMap.put(Command.STOP.getName(), LocalizedStrings.ServerLauncher_STOP_SERVER_HELP.toLocalizedString());
    helpMap.put(Command.VERSION.getName(), LocalizedStrings.ServerLauncher_VERSION_SERVER_HELP.toLocalizedString());
    helpMap.put("assign-buckets", LocalizedStrings.ServerLauncher_SERVER_ASSIGN_BUCKETS_HELP.toLocalizedString());
    helpMap.put("debug", LocalizedStrings.ServerLauncher_SERVER_DEBUG_HELP.toLocalizedString());
    helpMap.put("dir", LocalizedStrings.ServerLauncher_SERVER_DIR_HELP.toLocalizedString());
    helpMap.put("disable-default-server", LocalizedStrings.ServerLauncher_SERVER_DISABLE_DEFAULT_SERVER_HELP.toLocalizedString());
    helpMap.put("force", LocalizedStrings.ServerLauncher_SERVER_FORCE_HELP.toLocalizedString());
    helpMap.put("help", LocalizedStrings.SystemAdmin_CAUSES_GEMFIRE_TO_PRINT_OUT_INFORMATION_INSTEAD_OF_PERFORMING_THE_COMMAND_THIS_OPTION_IS_SUPPORTED_BY_ALL_COMMANDS.toLocalizedString());
    helpMap.put("member", LocalizedStrings.ServerLauncher_SERVER_MEMBER_HELP.toLocalizedString());
    helpMap.put("pid", LocalizedStrings.ServerLauncher_SERVER_PID_HELP.toLocalizedString());
    helpMap.put("rebalance", LocalizedStrings.ServerLauncher_SERVER_REBALANCE_HELP.toLocalizedString());
    helpMap.put("redirect-output", LocalizedStrings.ServerLauncher_SERVER_REDIRECT_OUTPUT_HELP.toLocalizedString());
    helpMap.put("server-bind-address", LocalizedStrings.ServerLauncher_SERVER_BIND_ADDRESS_HELP.toLocalizedString());
    helpMap.put("hostname-for-clients", LocalizedStrings.ServerLauncher_SERVER_HOSTNAME_FOR_CLIENT_HELP.toLocalizedString());
    helpMap.put("server-port", LocalizedStrings.ServerLauncher_SERVER_PORT_HELP.toLocalizedString(String.valueOf(getDefaultServerPort())));
  }

  private static final Map<Command, String> usageMap = new TreeMap<>();

  static {
    usageMap.put(Command.START, "start <member-name> [--assign-buckets] [--disable-default-server] [--rebalance] [--server-bind-address=<IP-address>] [--server-port=<port>] [--force] [--debug] [--help]");
    usageMap.put(Command.STATUS, "status [--member=<member-ID/Name>] [--pid=<process-ID>] [--dir=<Server-working-directory>] [--debug] [--help]");
    usageMap.put(Command.STOP, "stop [--member=<member-ID/Name>] [--pid=<process-ID>] [--dir=<Server-working-directory>] [--debug] [--help]");
    usageMap.put(Command.VERSION, "version");
  }

  /**
   * @deprecated This is specific to the internal implementation and may go away in a future release.
   */
  public static final String DEFAULT_SERVER_PID_FILE = "vf.gf.server.pid";

  private static final String DEFAULT_SERVER_LOG_EXT = ".log";
  private static final String DEFAULT_SERVER_LOG_NAME = "gemfire";
  private static final String SERVER_SERVICE_NAME = "Server";

  private static final AtomicReference<ServerLauncher> INSTANCE = new AtomicReference<>();

  private volatile transient boolean debug;

  private final transient ControlNotificationHandler controlHandler;
  
  private final AtomicBoolean starting = new AtomicBoolean(false);

  private final boolean assignBuckets;
  private final boolean disableDefaultServer;
  private final boolean force;
  private final boolean help;
  private final boolean rebalance;
  private final boolean redirectOutput;

  private volatile transient Cache cache;

  private final transient CacheConfig cacheConfig;

  private final Command command;

  private final InetAddress serverBindAddress;

  private final Integer pid;
  private final Integer serverPort;

  private final Properties distributedSystemProperties;

  private final String memberName;
  private final String springXmlLocation;
  private final String workingDirectory;

  // NOTE in addition to debug, the other shared, mutable state
  private volatile transient String statusMessage;
  
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

  private volatile transient ControllableProcess process;

  private final transient ServerControllerParameters controllerParameters;
  
  /**
   * Launches a GemFire Server from the command-line configured with the given arguments.
   * 
   * @param args the command-line arguments used to configure the GemFire Server at runtime.
   */
  public static void main(final String... args) {
    try {
      new Builder(args).build().run();
    }
    catch (AttachAPINotFoundException e) {
      System.err.println(e.getMessage());
    }
  }

  private static Integer getDefaultServerPort() {
    return Integer.getInteger(AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, CacheServer.DEFAULT_PORT);
  }

  /**
   * Gets the instance of the ServerLauncher used to launch the GemFire Cache Server, or null if this VM does not
   * have an instance of ServerLauncher indicating no GemFire Cache Server is running.
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
    return (getInstance() != null ? getInstance().status() : null);
  }

  /**
   * Private constructor used to properly construct an immutable instance of the ServerLauncher using a Builder.
   * The Builder is used to configure a ServerLauncher instance.  The Builder can process user input from the
   * command-line or be used programmatically to properly construct an instance of the ServerLauncher using the API.
   * 
   * @param builder an instance of ServerLauncher.Builder for configuring and constructing an instance of the
   * ServerLauncher.
   * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder
   */
  private ServerLauncher(final Builder builder) {
    this.cache = builder.getCache(); // testing
    this.cacheConfig = builder.getCacheConfig();
    this.command = builder.getCommand();
    this.assignBuckets = Boolean.TRUE.equals(builder.getAssignBuckets());
    setDebug(Boolean.TRUE.equals(builder.getDebug()));
    this.disableDefaultServer = Boolean.TRUE.equals(builder.getDisableDefaultServer());
    CacheServerLauncher.disableDefaultServer.set(this.disableDefaultServer);
    this.distributedSystemProperties = builder.getDistributedSystemProperties();
    this.force = Boolean.TRUE.equals(builder.getForce());
    this.help = Boolean.TRUE.equals(builder.getHelp());
    this.hostNameForClients = builder.getHostNameForClients();
    this.memberName = builder.getMemberName();
    // TODO:KIRK: set ThreadLocal for LogService with getLogFile or getLogFileName
    this.pid = builder.getPid();
    this.rebalance = Boolean.TRUE.equals(builder.getRebalance());
    this.redirectOutput = Boolean.TRUE.equals(builder.getRedirectOutput());
    this.serverBindAddress = builder.getServerBindAddress();
    if (builder.isServerBindAddressSetByUser() && this.serverBindAddress != null) {
      CacheServerLauncher.serverBindAddress.set(this.serverBindAddress.getHostAddress());
    }
    this.serverPort = builder.getServerPort();
    if (builder.isServerPortSetByUser() && this.serverPort != null) {
      CacheServerLauncher.serverPort.set(this.serverPort);
    }
    this.springXmlLocation = builder.getSpringXmlLocation();
    this.workingDirectory = builder.getWorkingDirectory();
    this.criticalHeapPercentage = builder.getCriticalHeapPercentage();
    this.evictionHeapPercentage = builder.getEvictionHeapPercentage();
    this.criticalOffHeapPercentage = builder.getCriticalOffHeapPercentage();
    this.evictionOffHeapPercentage = builder.getEvictionOffHeapPercentage();
    this.maxConnections = builder.getMaxConnections();
    this.maxMessageCount = builder.getMaxMessageCount();
    this.maxThreads = builder.getMaxThreads();
    this.messageTimeToLive = builder.getMessageTimeToLive();
    this.socketBufferSize = builder.getSocketBufferSize();
    this.controllerParameters = new ServerControllerParameters();
    this.controlHandler = new ControlNotificationHandler() {
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
  }

  /**
   * Gets a reference to the Cache that was created when the GemFire Server was started.
   * 
   * @return a reference to the Cache created by the GemFire Server start operation.
   * @see com.gemstone.gemfire.cache.Cache
   */
  final Cache getCache() {
    return this.cache;
  }

  /**
   * Gets the CacheConfig object used to configure additional GemFire Cache components and features (e.g. PDX).
   *
   * @return a CacheConfig object with additional GemFire Cache configuration meta-data used on startup to configure
   * the Cache.
   */
  final CacheConfig getCacheConfig() {
    final CacheConfig copy = new CacheConfig();
    copy.setDeclarativeConfig(this.cacheConfig);
    return copy;
  }

  /**
   * Gets an identifier that uniquely identifies and represents the Server associated with this launcher.
   * 
   * @return a String value identifier to uniquely identify the Server and it's launcher.
   * @see #getServerBindAddressAsString()
   * @see #getServerPortAsString()
   */
  public final String getId() {
    final StringBuilder buffer = new StringBuilder(ServerState.getServerBindAddressAsString(this));
    final String serverPort = ServerState.getServerPortAsString(this);

    if (!StringUtils.isBlank(serverPort)) {
      buffer.append("[").append(serverPort).append("]");
    }

    return buffer.toString();
  }

  /**
   * Get the Server launcher command used to invoke the Server.
   * 
   * @return the Server launcher command used to invoke the Server.
   * @see com.gemstone.gemfire.distributed.ServerLauncher.Command
   */
  public Command getCommand() {
    return this.command;
  }

  /**
   * Determines whether buckets should be assigned to partitioned regions in the cache upon Server start.
   * 
   * @return a boolean indicating if buckets should be assigned upon Server start.
   */
  public boolean isAssignBuckets() {
    return this.assignBuckets;
  }

  /**
   * Determines whether a default cache server will be added when the GemFire Server comes online.
   * 
   * @return a boolean value indicating whether to add a default cache server.
   */
  public boolean isDisableDefaultServer() {
    return this.disableDefaultServer;
  }

  /**
   * Determines whether the PID file is allowed to be overwritten when the Server is started and a PID file
   * already exists in the Server's specified working directory.
   * 
   * @return boolean indicating if force has been enabled.
   */
  public boolean isForcing() {
    return this.force;
  }

  /**
   * Determines whether this launcher will be used to display help information.  If so, then none of the standard
   * Server launcher commands will be used to affect the state of the Server.  A launcher is said to be 'helping'
   * if the user entered the "--help" option (switch) on the command-line.
   * 
   * @return a boolean value indicating if this launcher is used for displaying help information.
   * @see com.gemstone.gemfire.distributed.ServerLauncher.Command
   */
  public boolean isHelping() {
    return this.help;
  }

  /**
   * Determines whether a rebalance operation on the cache will occur upon starting the GemFire server using this
   * launcher.
   * 
   * @return a boolean indicating if the cache will be rebalance when the GemFire server starts.
   */
  public boolean isRebalancing() {
    return this.rebalance;
  }

  /**
   * Determines whether this launcher will redirect output to system logs when
   * starting a new Locator process.
   * 
   * @return a boolean value indicating if this launcher will redirect output 
   * to system logs when starting a new Locator process
   */
  public boolean isRedirectingOutput() {
    return this.redirectOutput;
  }

  /**
   * Gets the name of the log file used to log information about this Server.
   * 
   * @return a String value indicating the name of this Server's log file.
   */
  public String getLogFileName() {
    return StringUtils.defaultIfBlank(getMemberName(), DEFAULT_SERVER_LOG_NAME).concat(DEFAULT_SERVER_LOG_EXT);
  }

  /**
   * Gets the name of this member (this Server) in the GemFire distributed system as determined by the 'name' GemFire
   * property.
   * 
   * @return a String indicating the name of the member (this Server) in the GemFire distributed system.
   * @see AbstractLauncher#getMemberName()
   */
  public String getMemberName() {
    return StringUtils.defaultIfBlank(this.memberName, super.getMemberName());
  }

  /**
   * Gets the user-specified process ID (PID) of the running Server that ServerLauncher uses to issue status and
   * stop commands to the Server.
   * 
   * @return an Integer value indicating the process ID (PID) of the running Server.
   */
  @Override
  public Integer getPid() {
    return this.pid;
  }

  /**
   * Gets the GemFire Distributed System (cluster) Properties.
   *
   * @return a Properties object containing the configuration settings for the GemFire Distributed System (cluster).
   * @see java.util.Properties
   */
  public Properties getProperties() {
    return (Properties) this.distributedSystemProperties.clone();
  }

  /**
   * Gets the IP address to which the Server is bound listening for and accepting cache client connections.  This
   * property should not be confused with 'bindAddress' ServerLauncher property, which is the port for binding the
   * Server's ServerSocket used in distribution and messaging between the peers of the GemFire distributed system.
   * 
   * @return an InetAddress indicating the IP address that the Server is bound to listening for and accepting cache
   * client connections in a client/server topology.
   */
  public InetAddress getServerBindAddress() {
    return this.serverBindAddress;
  }

  /**
   * Gets the host, as either hostname or IP address, on which the Server was bound and running.  An attempt is made
   * to get the canonical hostname for IP address to which the Server was bound for accepting client requests.  If
   * the server bind address is null or localhost is unknown, then a default String value of "localhost/127.0.0.1"
   * is returned.
   * 
   * Note, this information is purely information and should not be used to re-construct state or for
   * other purposes.
   * 
   * @return the hostname or IP address of the host running the Server, based on the bind-address, or
   * 'localhost/127.0.0.1' if the bind address is null and localhost is unknown.
   * @see java.net.InetAddress
   * @see #getServerBindAddress()
   */
  public String getServerBindAddressAsString() {
    try {
      if (getServerBindAddress() != null) {
        return getServerBindAddress().getCanonicalHostName();
      }

      final InetAddress localhost = SocketCreator.getLocalHost();

      return localhost.getCanonicalHostName();
    }
    catch (UnknownHostException ignore) {
      // TODO determine a better value for the host on which the Server is running to return here...
      // NOTE returning localhost/127.0.0.1 implies the serverBindAddress was null and no IP address for localhost
      // could be found
      return "localhost/127.0.0.1";
    }
  }

  /**
   * Gets the port on which the Server is listening for cache client connections.  This property should not be confused
   * with the 'port' ServerLauncher property, which is used by the Server to set the 'tcp-port' distribution config
   * property and is used by the ServerSocket for peer distribution and messaging.
   * 
   * @return an Integer value indicating the port the Server is listening on for cache client connections in the
   * client/server topology.
   */
  public Integer getServerPort() {
    return this.serverPort;
  }

  /**
   * Gets the server port on which the Server is listening for client requests represented as a String value.
   * 
   * @return a String representing the server port on which the Server is listening for client requests.
   * @see #getServerPort()
   */
  public String getServerPortAsString() {
    return ObjectUtils.defaultIfNull(getServerPort(), getDefaultServerPort()).toString();
  }

  /**
   * Gets the name for a GemFire Server.
   * 
   * @return a String indicating the name for a GemFire Server.
   */
  public String getServiceName() {
    return SERVER_SERVICE_NAME;
  }

  /**
   * Gets the location of the Spring XML configuration meta-data file used to bootstrap, configure and initialize
   * the GemFire Server on start.
   * <p>
   * @return a String indicating the location of the Spring XML configuration file.
   * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder#getSpringXmlLocation()
   */
  public String getSpringXmlLocation() {
    return this.springXmlLocation;
  }

  /**
   * Determines whether this GemFire Server was configured and initialized with Spring configuration meta-data.
   * <p>
   * @return a boolean value indicating whether this GemFire Server was configured with Spring configuration meta-data.
   */
  public boolean isSpringXmlLocationSpecified() {
    return !StringUtils.isBlank(this.springXmlLocation);
  }

  /**
   * Gets the working directory pathname in which the Server will be run.
   * 
   * @return a String value indicating the pathname of the Server's working directory.
   */
  @Override
  public String getWorkingDirectory() {
    return this.workingDirectory;
  }
  
  public Float getCriticalHeapPercentage() {
    return this.criticalHeapPercentage;
  }
  
  public Float getEvictionHeapPercentage() {
    return this.evictionHeapPercentage;
  }
  
  public Float getCriticalOffHeapPercentage() {
    return this.criticalOffHeapPercentage;
  }
  
  public Float getEvictionOffHeapPercentage() {
    return this.evictionOffHeapPercentage;
  }
  
  public String getHostNameForClients() {
    return this.hostNameForClients;
  }

  public Integer getMaxConnections() {
    return this.maxConnections;
  }

  public Integer getMaxMessageCount() {
    return this.maxMessageCount;
  }

  public Integer getMessageTimeToLive() {
    return this.messageTimeToLive;
  }

  public Integer getMaxThreads() {
    return this.maxThreads;
  }

  public Integer getSocketBufferSize() {
    return this.socketBufferSize;
  }
  
  /**
   * Displays help for the specified Server launcher command to standard err.  If the Server launcher command
   * is unspecified, then usage information is displayed instead.
   * 
   * @param command the Server launcher command in which to display help information.
   * @see #usage()
   */
  public void help(final Command command) {
    if (Command.isUnspecified(command)) {
      usage();
    }
    else {
      info(StringUtils.wrap(helpMap.get(command.getName()), 80, ""));
      info("\n\nusage: \n\n");
      info(StringUtils.wrap("> java ... " + getClass().getName() + " " + usageMap.get(command), 80, "\t\t"));
      info("\n\noptions: \n\n");

      for (final String option : command.getOptions()) {
        info(StringUtils.wrap("--" + option + ": " + helpMap.get(option) + "\n", 80, "\t"));
      }

      info("\n\n");
    }
  }

  /**
   * Displays usage information on the proper invocation of the ServerLauncher from the command-line to standard err.
   * 
   * @see #help(com.gemstone.gemfire.distributed.ServerLauncher.Command)
   */
  public void usage() {
    info(StringUtils.wrap(helpMap.get("launcher"), 80, "\t"));
    info("\n\nSTART\n\n");
    help(Command.START);
    info("STATUS\n\n");
    help(Command.STATUS);
    info("STOP\n\n");
    help(Command.STOP);
  }

  /**
   * A Runnable method used to invoke the GemFire server (cache server) with the specified command.  From run, a user
   * can invoke 'start', 'status', 'stop' and 'version'.  Note, that 'version' is also a command-line option, but can
   * be treated as a "command" as well.
   * 
   * @see java.lang.Runnable
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
    }
    else {
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
   * @return a boolean indicating whether a GemFire Cache Server can be started with this instance of ServerLauncher,
   * which is true if the ServerLauncher has not already started a Server or a Server is not already running.
   * @see #start()
   */
  private boolean isStartable() {
    return (!isRunning() && this.starting.compareAndSet(false, true));
  }

  /**
   * Invokes the 'start' command and operation to startup a GemFire server (a cache server).  Note, this method will
   * cause the JVM to block upon server start, providing the calling Thread is a non-daemon Thread.
   *
   * @see #run()
   */
  public ServerState start() {
    if (isStartable()) {
      INSTANCE.compareAndSet(null, this);

      try {
        process = new ControllableProcess(this.controlHandler, new File(getWorkingDirectory()), ProcessType.SERVER, isForcing());

        if (!isDisableDefaultServer()) {
          assertPortAvailable(getServerBindAddress(), getServerPort());
        }

        SystemFailure.setExitOK(true);

        ProcessLauncherContext.set(isRedirectingOutput(), getOverriddenDefaults(), new StartupStatusListener() {
          @Override
          public void setStatus(final String statusMessage) {
            debug("Callback setStatus(String) called with message (%1$s)...", statusMessage);
            ServerLauncher.this.statusMessage = statusMessage;
          }
        });

        try {
          final Properties gemfireProperties = getDistributedSystemProperties(getProperties());
          this.cache = (isSpringXmlLocationSpecified() ? startWithSpring() : startWithGemFireApi(gemfireProperties));
          
          //Set the resource manager options
          if (this.criticalHeapPercentage != null) {
            this.cache.getResourceManager().setCriticalHeapPercentage(getCriticalHeapPercentage());
          } 
          if (this.evictionHeapPercentage != null) {
            this.cache.getResourceManager().setEvictionHeapPercentage(getEvictionHeapPercentage());
          }
          if (this.criticalOffHeapPercentage != null) {
            this.cache.getResourceManager().setCriticalOffHeapPercentage(getCriticalOffHeapPercentage());
          } 
          if (this.evictionOffHeapPercentage != null) {
            this.cache.getResourceManager().setEvictionOffHeapPercentage(getEvictionOffHeapPercentage());
          }
          
          this.cache.setIsServer(true);
          startCacheServer(this.cache);
          assignBuckets(this.cache);
          rebalance(this.cache);
        }
        finally {
          ProcessLauncherContext.remove();
        }
        
        debug("Running Server on (%1$s) in (%2$s) as (%2$s)...", getId(), getWorkingDirectory(), getMember());
        this.running.set(true);

        return new ServerState(this, Status.ONLINE);
      }
      catch (IOException e) {
        failOnStart(e);
        throw new RuntimeException(LocalizedStrings.Launcher_Command_START_IO_ERROR_MESSAGE.toLocalizedString(
          getServiceName(), getWorkingDirectory(), getId(), e.getMessage()), e);
      }
      catch (FileAlreadyExistsException e) {
        failOnStart(e);
        throw new RuntimeException(LocalizedStrings.Launcher_Command_START_PID_FILE_ALREADY_EXISTS_ERROR_MESSAGE.
           toLocalizedString(getServiceName(), getWorkingDirectory(), getId()), e);
      }
      catch (PidUnavailableException e) {
        failOnStart(e);
        throw new RuntimeException(LocalizedStrings.Launcher_Command_START_PID_UNAVAILABLE_ERROR_MESSAGE
          .toLocalizedString(getServiceName(), getId(), getWorkingDirectory(), e.getMessage()), e);
      }
      catch (ClusterConfigurationNotAvailableException e) {
        failOnStart(e);
        throw e;
      }
      catch (RuntimeException e) {
        failOnStart(e);
        throw e;
      }
      catch (Exception e) {
        failOnStart(e);
        throw new RuntimeException(e);
      }
      catch (Error e) {
        failOnStart(e);
        throw e;
      }
      finally {
        this.starting.set(false);
      }
    }
    else {
      throw new IllegalStateException(LocalizedStrings.Launcher_Command_START_SERVICE_ALREADY_RUNNING_ERROR_MESSAGE
        .toLocalizedString(getServiceName(), getWorkingDirectory(), getId()));
    }
  }

  private Cache startWithSpring() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME, getMemberName());

    new SpringContextBootstrappingInitializer().init(CollectionUtils.createProperties(Collections.singletonMap(
      SpringContextBootstrappingInitializer.CONTEXT_CONFIG_LOCATIONS_PARAMETER, getSpringXmlLocation())));

    return SpringContextBootstrappingInitializer.getApplicationContext().getBean(Cache.class);
  }

  private Cache startWithGemFireApi(final Properties gemfireProperties ) {
    final CacheConfig cacheConfig = getCacheConfig();
    final CacheFactory cacheFactory = new CacheFactory(gemfireProperties);

    if (cacheConfig.pdxPersistentUserSet) {
      cacheFactory.setPdxPersistent(cacheConfig.isPdxPersistent());
    }

    if (cacheConfig.pdxDiskStoreUserSet) {
      cacheFactory.setPdxDiskStore(cacheConfig.getPdxDiskStore());
    }

    if (cacheConfig.pdxIgnoreUnreadFieldsUserSet) {
      cacheFactory.setPdxIgnoreUnreadFields(cacheConfig.getPdxIgnoreUnreadFields());
    }

    if (cacheConfig.pdxReadSerializedUserSet) {
      cacheFactory.setPdxReadSerialized(cacheConfig.isPdxReadSerialized());
    }

    if (cacheConfig.pdxSerializerUserSet) {
      cacheFactory.setPdxSerializer(cacheConfig.getPdxSerializer());
    }

    return cacheFactory.create();
  }

  /**
   * A helper method to ensure the same sequence of actions are taken when the Server fails to start
   * caused by some exception.
   * 
   * @param cause the Throwable thrown during the startup operation on the Server.
   */
  private void failOnStart(final Throwable cause) {
    if (this.cache != null) {
      this.cache.close();
      this.cache = null;
    }
    if (this.process != null) {
      this.process.stop();
      this.process = null;
    }

    INSTANCE.compareAndSet(this, null);

    this.running.set(false);
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
   * @return a boolean value indicating whether the GemFire data member should continue running, as determined
   * by the running flag and a connection to the distributed system (GemFire cluster).
   */
  final boolean isWaiting(final Cache cache) {
    //return (isRunning() && !getCache().isClosed());
    return (isRunning() && cache.getDistributedSystem().isConnected());
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
              wait(500l);
            }
          }
          catch (InterruptedException ignore) {
          }
        }
      }
      catch (RuntimeException e) {
        cause = e;
        throw e;
      }
      finally {
        failOnStart(cause);
      }
    }
  }

  /**
   * Determines whether a default server (a cache server) should be created on startup as determined by the absence
   * of specifying the --disable-default-server command-line option (switch).  In addition, a default cache server
   * is started only if no cache servers have been added to the Cache by way of cache.xml.
   * 
   * @param cache the reference to the Cache to check for any existing cache servers.
   * @return a boolean indicating whether a default server should be added to the Cache.
   * @see #isDisableDefaultServer()
   */
  protected boolean isDefaultServerEnabled(final Cache cache) {
    return (cache.getCacheServers().isEmpty() && !isDisableDefaultServer());
  }

  /**
   * If the default server (cache server) has not been disabled and no prior cache servers were added to the cache,
   * then this method will add a cache server to the Cache and start the server Thread on the specified bind address
   * and port.
   * 
   * @param cache the Cache to which the server will be added.
   * @throws IOException if the Cache server fails to start due to IO error.
   */
  final void startCacheServer(final Cache cache) throws IOException {
    if (isDefaultServerEnabled(cache)) {
      final String serverBindAddress = (getServerBindAddress() == null ? null : getServerBindAddress().getHostAddress());
      final Integer serverPort = getServerPort();
      CacheServerLauncher.serverBindAddress.set(serverBindAddress);
      CacheServerLauncher.serverPort.set(serverPort);
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

      cacheServer.start();
    }
  }

  /**
   * Causes a rebalance operation to occur on the given Cache.
   * 
   * @param cache the reference to the Cache to rebalance.
   * @see com.gemstone.gemfire.cache.control.ResourceManager#createRebalanceFactory()
   */
  private void rebalance(final Cache cache) {
    if (isRebalancing()) {
      cache.getResourceManager().createRebalanceFactory().start();
    }
  }

  /**
   * Determines whether the user indicated that buckets should be assigned on cache server start using the
   * --assign-buckets command-line option (switch) at the command-line as well as whether the option is technically
   * allowed.  The option is only allowed if the instance of the Cache is the internal GemFireCacheImpl at present.
   * @param cache the Cache reference to check for instance type.
   * @return a boolean indicating if bucket assignment is both enabled and allowed.
   * @see #isAssignBuckets()
   */
  protected boolean isAssignBucketsAllowed(final Cache cache) {
    return (isAssignBuckets() && (cache instanceof GemFireCacheImpl));
  }

  /**
   * Assigns buckets to individual Partitioned Regions of the Cache.
   * 
   * @param cache the Cache who's Partitioned Regions are accessed to assign buckets to.
   * @see PartitionRegionHelper#assignBucketsToPartitions(com.gemstone.gemfire.cache.Region)
   */
  final void assignBuckets(final Cache cache) {
    if (isAssignBucketsAllowed(cache)) {
      for (PartitionedRegion region : ((GemFireCacheImpl) cache).getPartitionedRegions()) {
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
    return (this.starting.get() || isRunning());
  }

  /**
   * Invokes the 'status' command and operation to check the status of a GemFire server (a cache server).
   */
  public ServerState status() {
    final ServerLauncher launcher = getInstance();
    // if this instance is running then return local status
    if (isStartingOrRunning()) {
      debug("Getting status from the ServerLauncher instance that actually launched the GemFire Cache Server.%n");
      return new ServerState(this, (isRunning() ? Status.ONLINE : Status.STARTING));
    }
    else if (isPidInProcess() && launcher != null) {
      return launcher.statusInProcess();
    }
    else if (getPid() != null) {
      debug("Getting Server status using process ID (%1$s)%n", getPid());
      return statusWithPid();
    }
    // attempt to get status using workingDirectory
    else if (getWorkingDirectory() != null) {
      debug("Getting Server status using working directory (%1$s)%n", getWorkingDirectory());
      return statusWithWorkingDirectory();
    }

    debug("This ServerLauncher was not the instance used to launch the GemFire Cache Server, and neither PID "
      .concat("nor working directory were specified; the Server's state is unknown.%n"));

    return new ServerState(this, Status.NOT_RESPONDING);
  }
  
  private ServerState statusInProcess() {
    if (isStartingOrRunning()) {
      debug("Getting status from the ServerLauncher instance that actually launched the GemFire Cache Server.%n");
      return new ServerState(this, (isRunning() ? Status.ONLINE : Status.STARTING));
    } else {
      return new ServerState(this, Status.NOT_RESPONDING);
    }
  }
  
  private ServerState statusWithPid() {
    try {
      final ProcessController controller = new ProcessControllerFactory().createProcessController(this.controllerParameters, getPid());
      controller.checkPidSupport();
      final String statusJson = controller.status();
      return ServerState.fromJson(statusJson);
    }
//    catch (NoClassDefFoundError error) {
//      if (isAttachAPINotFound(error)) {
//        throw new AttachAPINotFoundException(LocalizedStrings.Launcher_ATTACH_API_NOT_FOUND_ERROR_MESSAGE
//          .toLocalizedString(), error);
//      }
//
//      throw error;
//    }
    catch (ConnectionFailedException e) {
      // failed to attach to server JVM
      return createNoResponseState(e, "Failed to connect to server with process id " + getPid());
    } 
    catch (IOException e) {
      // failed to open or read file or dir
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    } 
//    catch (MalformedObjectNameException e) { // impossible
//      // JMX object name is bad
//      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
//    } 
    catch (MBeanInvocationFailedException e) {
      // MBean either doesn't exist or method or attribute don't exist
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    } 
//    catch (PidUnavailableException e) {
//      // couldn't determine pid from within server JVM
//      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
//    } 
    catch (UnableToControlProcessException e) {
      // TODO comment me
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    } 
    catch (InterruptedException e) {
      // TODO comment me
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    } 
    catch (TimeoutException e) {
      // TODO comment me
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    }
  }

  private ServerState statusWithWorkingDirectory() {
    int parsedPid = 0;
    try {
      final ProcessController controller = new ProcessControllerFactory().createProcessController(this.controllerParameters, new File(getWorkingDirectory()), ProcessType.SERVER.getPidFileName(), READ_PID_FILE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
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
    }
    catch (ConnectionFailedException e) {
      // failed to attach to server JVM
      return createNoResponseState(e, "Failed to connect to server with process id " + parsedPid);
    } 
    catch (FileNotFoundException e) {
      // could not find pid file
      return createNoResponseState(e, "Failed to find process file " + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } 
    catch (IOException e) {
      // failed to open or read file or dir
      return createNoResponseState(e, "Failed to communicate with server with process id " + parsedPid);
    } 
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return createNoResponseState(e, "Interrupted while trying to communicate with server with process id " + parsedPid);
    } 
    catch (MBeanInvocationFailedException e) {
      // MBean either doesn't exist or method or attribute don't exist
      return createNoResponseState(e, "Failed to communicate with server with process id " + parsedPid);
    } 
    catch (PidUnavailableException e) {
      // couldn't determine pid from within server JVM
      return createNoResponseState(e, "Failed to find usable process id within file " + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } 
    catch (UnableToControlProcessException e) {
      return createNoResponseState(e, "Failed to communicate with server with process id " + parsedPid);
    } 
    catch (TimeoutException e) {
      return createNoResponseState(e, "Failed to communicate with server with process id " + parsedPid);
    }
  }

  /**
   * Determines whether the Server can be stopped in-process, such as when a Server is embedded in an application
   * and the ServerLauncher API is being used.
   * 
   * @return a boolean indicating whether the Server can be stopped in-process (the application's process with
   * an embedded Server).
   */
  private boolean isStoppable() {
    return (isRunning() && getCache() != null);
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
    else if (isPidInProcess() && launcher != null) {
      return launcher.stopInProcess();
    }
    // attempt to stop using pid if provided
    else if (getPid() != null) {
      return stopWithPid();
    }
    // attempt to stop using workingDirectory
    else if (getWorkingDirectory() != null) {
      return stopWithWorkingDirectory();
    }

    // TODO give user detailed error message?
    return new ServerState(this, Status.NOT_RESPONDING);
  }
  
  private ServerState stopInProcess() {
    if (isStoppable()) {
      this.cache.close();
      this.cache = null;
      this.process.stop();
      this.process = null;
      INSTANCE.compareAndSet(this, null); // note: other thread may return Status.NOT_RESPONDING now
      this.running.set(false);
      return new ServerState(this, Status.STOPPED);
    } else {
      return new ServerState(this, Status.NOT_RESPONDING);
    }
  }

  private ServerState stopWithPid() {
    try {
      final ProcessController controller = new ProcessControllerFactory().createProcessController(this.controllerParameters, getPid());
      controller.checkPidSupport();
      controller.stop();
      return new ServerState(this, Status.STOPPED);
    }
//    catch (NoClassDefFoundError error) {
//      if (isAttachAPINotFound(error)) {
//        throw new AttachAPINotFoundException(LocalizedStrings.Launcher_ATTACH_API_NOT_FOUND_ERROR_MESSAGE
//          .toLocalizedString(), error);
//      }
//
//      throw error;
//    }
    catch (ConnectionFailedException e) {
      // failed to attach to server JVM
      return createNoResponseState(e, "Failed to connect to server with process id " + getPid());
    } 
    catch (IOException e) {
      // failed to open or read file or dir
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    } 
//    catch (MalformedObjectNameException e) { // impossible
//      // JMX object name is bad
//      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
//    } 
    catch (MBeanInvocationFailedException e) {
      // MBean either doesn't exist or method or attribute don't exist
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    } 
//    catch (PidUnavailableException e) {
//      // couldn't determine pid from within server JVM
//      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
//    } 
    catch (UnableToControlProcessException e) {
      // TODO comment me
      return createNoResponseState(e, "Failed to communicate with server with process id " + getPid());
    }
  }

  private ServerState stopWithWorkingDirectory() {
    int parsedPid = 0;
    try {
      final ProcessController controller = new ProcessControllerFactory().createProcessController(this.controllerParameters, new File(getWorkingDirectory()), ProcessType.SERVER.getPidFileName(), READ_PID_FILE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
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
    }
    catch (ConnectionFailedException e) {
      // failed to attach to server JVM
      return createNoResponseState(e, "Failed to connect to server with process id " + parsedPid);
    } 
    catch (FileNotFoundException e) {
      // could not find pid file
      return createNoResponseState(e, "Failed to find process file " + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } 
    catch (IOException e) {
      // failed to open or read file or dir
      return createNoResponseState(e, "Failed to communicate with server with process id " + parsedPid);
    } 
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return createNoResponseState(e, "Interrupted while trying to communicate with server with process id " + parsedPid);
    } 
    catch (MBeanInvocationFailedException e) {
      // MBean either doesn't exist or method or attribute don't exist
      return createNoResponseState(e, "Failed to communicate with server with process id " + parsedPid);
    } 
    catch (PidUnavailableException e) {
      // couldn't determine pid from within server JVM
      return createNoResponseState(e, "Failed to find usable process id within file " + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } 
    catch (TimeoutException e) {
      return createNoResponseState(e, "Timed out trying to find usable process id within file " + ProcessType.SERVER.getPidFileName() + " in " + getWorkingDirectory());
    } 
    catch (UnableToControlProcessException e) {
      return createNoResponseState(e, "Failed to communicate with server with process id " + parsedPid);
    }
  }

  private ServerState createNoResponseState(final Exception cause, final String errorMessage) {
    debug(cause);
    return new ServerState(this, Status.NOT_RESPONDING, errorMessage);
  }

  private Properties getOverriddenDefaults() {
    final Properties overriddenDefaults = new Properties();
    
    overriddenDefaults.put(
      ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX.concat(DistributionConfig.LOG_FILE_NAME), 
      getLogFileName());

    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX)) {
        overriddenDefaults.put(key, System.getProperty(key));
      }
    }

    return overriddenDefaults;
  }

  private class ServerControllerParameters implements ProcessControllerParameters {
    @Override
    public File getPidFile() {
      return getServerPidFile();
    }
  
    @Override
    public File getWorkingDirectory() {
      return new File(ServerLauncher.this.getWorkingDirectory());
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
      } catch (MalformedObjectNameException e) {
        return null;
      } catch (NullPointerException e) {
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
   * The Builder class, modeled after the Builder creational design pattern, is used to construct a properly configured
   * and initialized instance of the ServerLauncher to control and run GemFire servers (in particular, cache servers).
   */
  public static class Builder {

    protected static final Command DEFAULT_COMMAND = Command.UNSPECIFIED;

    private boolean serverBindAddressSetByUser;
    private boolean serverPortSetByUser;

    private Boolean assignBuckets;
    private Boolean debug;
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

    /**
     * Default constructor used to create an instance of the Builder class for programmatical access.
     */
    public Builder() {
    }

    /**
     * Constructor used to create and configure an instance of the Builder class with the specified arguments, passed in
     * from the command-line when launching an instance of this class from the command-line using the Java launcher.
     * 
     * @param args the array of arguments used to configure the Builder.
     * @see #parseArguments(String...)
     */
    public Builder(final String... args) {
      parseArguments(args != null ? args : new String[0]);
    }

    /**
     * Gets an instance of the JOptSimple OptionParser to parse the command-line arguments for Server.
     * 
     * @return an instance of the JOptSimple OptionParser configured with the command-line options used by the Server.
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
      parser.accepts("server-bind-address").withRequiredArg().ofType(String.class);
      parser.accepts("server-port").withRequiredArg().ofType(Integer.class);
      parser.accepts("spring-xml-location").withRequiredArg().ofType(String.class);
      parser.accepts("version");
      parser.accepts(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE).withRequiredArg().ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE).withRequiredArg().ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE).withRequiredArg().ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE).withRequiredArg().ofType(Float.class);
      parser.accepts(CliStrings.START_SERVER__MAX__CONNECTIONS).withRequiredArg().ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__MAX__MESSAGE__COUNT).withRequiredArg().ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__MAX__THREADS).withRequiredArg().ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE).withRequiredArg().ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE).withRequiredArg().ofType(Integer.class);
      parser.accepts(CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS).withRequiredArg().ofType(String.class);

      return parser;
    }

    /**
     * Parses the list of arguments to configure this Builder with the intent of constructing a Server launcher to
     * invoke a Cache Server.  This method is called to parse the arguments specified by the user on the command-line.
     * 
     * @param args the array of arguments used to configure this Builder and create an instance of ServerLauncher.
     */
    protected void parseArguments(final String... args) {
      try {
        OptionSet options = getParser().parse(args);

        parseCommand(args);
        parseMemberName(args); // TODO:KIRK: need to get the name to LogService for log file name

        setAssignBuckets(options.has("assign-buckets"));
        setDebug(options.has("debug"));
        setDisableDefaultServer(options.has("disable-default-server"));
        setForce(options.has("force"));
        setHelp(options.has("help"));
        setRebalance(options.has("rebalance"));
        setRedirectOutput(options.has("redirect-output"));
        
        if (options.hasArgument(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE)) {
          setCriticalHeapPercentage(Float.parseFloat(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE)) {
          setEvictionHeapPercentage(Float.parseFloat(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE)) {
          setCriticalOffHeapPercentage(Float.parseFloat(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__CRITICAL_OFF_HEAP_PERCENTAGE))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE)) {
          setEvictionOffHeapPercentage(Float.parseFloat(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__EVICTION_OFF_HEAP_PERCENTAGE))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__MAX__CONNECTIONS)) {
          setMaxConnections(Integer.parseInt(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__CONNECTIONS))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__MAX__MESSAGE__COUNT)) {
          setMaxConnections(Integer.parseInt(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__MESSAGE__COUNT))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE)) {
          setMaxConnections(Integer.parseInt(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE)) {
          setMaxConnections(Integer.parseInt(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE))));
        } 
        
        if (options.hasArgument(CliStrings.START_SERVER__MAX__THREADS)) {
          setMaxThreads(Integer.parseInt(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__THREADS))));
        }
        
        if (!isHelping()) {
          if (options.has("dir")) {
            setWorkingDirectory(ObjectUtils.toString(options.valueOf("dir")));
          }

          if (options.has("pid")) {
            setPid((Integer) options.valueOf("pid"));
          }

          if (options.has("server-bind-address")) {
            setServerBindAddress(ObjectUtils.toString(options.valueOf("server-bind-address")));
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

        // TODO why are these option not inside the 'if (!isHelping())' conditional block!?

        if (options.hasArgument(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE)) {
          setCriticalHeapPercentage(Float.parseFloat(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__CRITICAL__HEAP__PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE)) {
          setEvictionHeapPercentage(Float.parseFloat(ObjectUtils.toString(options.valueOf(
            CliStrings.START_SERVER__EVICTION__HEAP__PERCENTAGE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__CONNECTIONS)) {
          setMaxConnections(Integer.parseInt(ObjectUtils.toString(options.valueOf(
            CliStrings.START_SERVER__MAX__CONNECTIONS))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__MESSAGE__COUNT)) {
          setMaxMessageCount(Integer.parseInt(ObjectUtils.toString(options.valueOf(
            CliStrings.START_SERVER__MAX__MESSAGE__COUNT))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MAX__THREADS)) {
          setMaxThreads(Integer.parseInt(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__MAX__THREADS))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE)) {
          setMessageTimeToLive(Integer.parseInt(ObjectUtils.toString(options.valueOf(
            CliStrings.START_SERVER__MESSAGE__TIME__TO__LIVE))));
        }
        
        if (options.hasArgument(CliStrings.START_SERVER__SOCKET__BUFFER__SIZE)) {
          setSocketBufferSize(Integer.parseInt(ObjectUtils.toString(options.valueOf(
            CliStrings.START_SERVER__SOCKET__BUFFER__SIZE))));
        }

        if (options.hasArgument(CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS)) {
          setHostNameForClients(ObjectUtils.toString(options.valueOf(CliStrings.START_SERVER__HOSTNAME__FOR__CLIENTS)));
        }

      }
      catch (OptionException e) {
        throw new IllegalArgumentException(LocalizedStrings.Launcher_Builder_PARSE_COMMAND_LINE_ARGUMENT_ERROR_MESSAGE
          .toLocalizedString("Server", e.getMessage()), e);
      }
      catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    /**
     * Iterates the list of arguments in search of the target Server launcher command.
     * 
     * @param args an array of arguments from which to search for the Server launcher command.
     * @see com.gemstone.gemfire.distributed.ServerLauncher.Command#valueOfName(String)
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
     * Iterates the list of arguments in search of the Server's GemFire member name.  If the argument does not
     * start with '-' or is not the name of a Server launcher command, then the value is presumed to be the member name
     * for the Server in GemFire.
     * 
     * @param args the array of arguments from which to search for the Server's member name in GemFire.
     * @see com.gemstone.gemfire.distributed.ServerLauncher.Command#isCommand(String)
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
      return this.cacheConfig;
    }

    /**
     * Gets the Server launcher command used during the invocation of the ServerLauncher.
     * 
     * @return the Server launcher command used to invoke (run) the ServerLauncher class.
     * @see #setCommand(com.gemstone.gemfire.distributed.ServerLauncher.Command)
     * @see com.gemstone.gemfire.distributed.ServerLauncher.Command
     */
    public Command getCommand() {
      return ObjectUtils.defaultIfNull(this.command, DEFAULT_COMMAND);
    }

    /**
     * Sets the Sever launcher command used during the invocation of the ServerLauncher
     * 
     * @param command the targeted Server launcher command used during the invocation (run) of ServerLauncher.
     * @return this Builder instance.
     * @see #getCommand()
     * @see com.gemstone.gemfire.distributed.ServerLauncher.Command
     */
    public Builder setCommand(final Command command) {
      this.command = command;
      return this;
    }

    /**
     * Determines whether buckets should be assigned to partitioned regions in the cache upon Server start.
     * 
     * @return a boolean indicating if buckets should be assigned upon Server start.
     * @see #setAssignBuckets(Boolean)
     */
    public Boolean getAssignBuckets() {
      return this.assignBuckets;
    }

    /**
     * Sets whether buckets should be assigned to partitioned regions in the cache upon Server start.
     * 
     * @param assignBuckets a boolean indicating if buckets should be assigned upon Server start.
     * @return this Builder instance.
     * @see #getAssignBuckets()
     */
    public Builder setAssignBuckets(final Boolean assignBuckets) {
      this.assignBuckets = assignBuckets;
      return this;
    }

    // For testing purposes only!
    Cache getCache() {
      return this.cache;
    }

    // For testing purposes only!
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
      return this.debug;
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
     * Determines whether a default cache server will be added when the GemFire Server comes online.
     * 
     * @return a boolean value indicating whether to add a default cache server.
     * @see #setDisableDefaultServer(Boolean)
     */
    public Boolean getDisableDefaultServer() {
      return this.disableDefaultServer;
    }

    /**
     * Sets a boolean value indicating whether to add a default cache when the GemFire Server comes online.
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
     * @return a Properties object containing configuration settings for the GemFire Distributed System (cluster).
     * @see java.util.Properties
     */
    public Properties getDistributedSystemProperties() {
      return this.distributedSystemProperties;
    }

    /**
     * Gets the boolean value used by the Server to determine if it should overwrite the PID file if it already exists.
     * 
     * @return the boolean value specifying whether or not to overwrite the PID file if it already exists.
     * @see com.gemstone.gemfire.internal.process.LocalProcessLauncher
     * @see #setForce(Boolean)
     */
    public Boolean getForce() {
      return ObjectUtils.defaultIfNull(this.force, DEFAULT_FORCE);
    }

    /**
     * Sets the boolean value used by the Server to determine if it should overwrite the PID file if it already exists.
     * 
     * @param force a boolean value indicating whether to overwrite the PID file when it already exists.
     * @return this Builder instance.
     * @see com.gemstone.gemfire.internal.process.LocalProcessLauncher
     * @see #getForce()
     */
    public Builder setForce(final Boolean force) {
      this.force = force;
      return this;
    }

    /**
     * Determines whether the new instance of the ServerLauncher will be used to output help information for either
     * a specific command, or for using ServerLauncher in general.
     * 
     * @return a boolean value indicating whether help will be output during the invocation of the ServerLauncher.
     * @see #setHelp(Boolean)
     */
    public Boolean getHelp() {
      return this.help;
    }

    /**
     * Determines whether help has been enabled.
     * 
     * @return a boolean indicating if help was enabled.
     */
    protected final boolean isHelping() {
      return Boolean.TRUE.equals(getHelp());
    }

    /**
     * Sets whether the new instance of ServerLauncher will be used to output help information for either a specific
     * command, or for using ServerLauncher in general.
     * 
     * @param help a boolean indicating whether help information is to be displayed during invocation of ServerLauncher.
     * @return this Builder instance.
     * @see #getHelp()
     */
    public Builder setHelp(final Boolean help) {
      this.help = help;
      return this;
    }

    /**
     * Determines whether a rebalance operation on the cache will occur upon starting the GemFire server.
     * 
     * @return a boolean indicating if the cache will be rebalance when the GemFire server starts.
     * @see #setRebalance(Boolean)
     */
    public Boolean getRebalance() {
      return this.rebalance;
    }

    /**
     * Set a boolean value indicating whether a rebalance operation on the cache should occur upon starting
     * the GemFire server.
     * 
     * @param rebalance a boolean indicating if the cache will be rebalanced when the GemFire server starts.
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
      return this.memberName;
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
      if (StringUtils.isEmpty(StringUtils.trim(memberName))) {
        throw new IllegalArgumentException(LocalizedStrings.Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE
          .toLocalizedString("Server"));
      }
      this.memberName = memberName;
      return this;
    }

    /**
     * Gets the process ID (PID) of the running Server indicated by the user as an argument to the ServerLauncher.
     * This PID is used by the Server launcher to determine the Server's status, or invoke shutdown on the Server.
     * 
     * @return a user specified Integer value indicating the process ID of the running Server.
     * @see #setPid(Integer)
     */
    public Integer getPid() {
      return this.pid;
    }

    /**
     * Sets the process ID (PID) of the running Server indicated by the user as an argument to the ServerLauncher.
     * This PID will be used by the Server launcher to determine the Server's status, or invoke shutdown on the Server.
     * 
     * @param pid a user specified Integer value indicating the process ID of the running Server.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the process ID (PID) is not valid (greater than zero if not null).
     * @see #getPid()
     */
    public Builder setPid(final Integer pid) {
      if (pid != null && pid < 0) {
        throw new IllegalArgumentException(LocalizedStrings.Launcher_Builder_PID_ERROR_MESSAGE.toLocalizedString());
      }
      this.pid = pid;
      return this;
    }

    /**
     * Determines whether the new instance of LocatorLauncher will redirect
     * output to system logs when starting a Locator.
     * 
     * @return a boolean value indicating if output will be redirected to system 
     * logs when starting a Locator
     * 
     * @see #setRedirectOutput(Boolean)
     */
    public Boolean getRedirectOutput() {
      return this.redirectOutput;
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
     * Sets whether the new instance of LocatorLauncher will redirect output to system logs when starting a Locator.
     * 
     * @param redirectOutput a boolean value indicating if output will be redirected to system logs when starting
     * a Locator.
     * @return this Builder instance.
     * @see #getRedirectOutput()
     */
    public Builder setRedirectOutput(final Boolean redirectOutput) {
      this.redirectOutput = redirectOutput;
      return this;
    }

    /**
     * Gets the IP address to which the Server will be bound listening for and accepting cache client connections in
     * a client/server topology.
     * 
     * @return an InetAddress indicating the IP address that the Server is bound to listening for and accepting cache
     * client connections in a client/server topology.
     * @see #setServerBindAddress(String)
     */
    public InetAddress getServerBindAddress() {
      return this.serverBindAddress;
    }
    
    boolean isServerBindAddressSetByUser() {
      return this.serverBindAddressSetByUser;
    }

    /**
     * Sets the IP address to which the Server will be bound listening for and accepting cache client connections in
     * a client/server topology.
     * 
     * @param serverBindAddress a String specifying the IP address or hostname that the Server will be bound to listen
     * for and accept cache client connections in a client/server topology.
     * @return this Builder instance.
     * @throws IllegalArgumentException wrapping the UnknownHostException if the IP address or hostname for the
     * server bind address is unknown.
     * @see #getServerBindAddress()
     */
    public Builder setServerBindAddress(final String serverBindAddress) {
      if (StringUtils.isBlank(serverBindAddress)) {
        this.serverBindAddress = null;
        return this;
      }
      // NOTE only set the 'bind address' if the user specified a value
      else {
        try {
          this.serverBindAddress = InetAddress.getByName(serverBindAddress);
          this.serverBindAddressSetByUser = true;
          return this;
        }
        catch (UnknownHostException e) {
          throw new IllegalArgumentException(LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE
            .toLocalizedString("Server"), e);
        }
      }
    }

    /**
     * Gets the port on which the Server will listen for and accept cache client connections in a client/server topology.
     * 
     * @return an Integer value specifying the port the Server will listen on and accept cache client connections in
     * a client/server topology.
     * @see #setServerPort(Integer)
     */
    public Integer getServerPort() {
      return ObjectUtils.defaultIfNull(this.serverPort, getDefaultServerPort());
    }
    
    boolean isServerPortSetByUser() {
      return this.serverPortSetByUser;
    }

    /**
     * Sets the port on which the Server will listen for and accept cache client connections in a client/server topology.
     * 
     * @param serverPort an Integer value specifying the port the Server will listen on and accept cache client
     * connections in a client/server topology.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the port number is not valid.
     * @see #getServerPort()
     */
    public Builder setServerPort(final Integer serverPort) {
      if (serverPort != null && (serverPort < 0 || serverPort > 65535)) {
        throw new IllegalArgumentException(LocalizedStrings.Launcher_Builder_INVALID_PORT_ERROR_MESSAGE
          .toLocalizedString("Server"));
      }
      this.serverPort = serverPort;
      this.serverPortSetByUser = true;
      return this;
    }

    /**
     * Gets the location of the Spring XML configuration meta-data file used to bootstrap, configure and initialize
     * the GemFire Server on start.
     * <p>
     * @return a String indicating the location of the Spring XML configuration file.
     * @see #setSpringXmlLocation(String)
     */
    public String getSpringXmlLocation() {
      return this.springXmlLocation;
    }

    /**
     * Sets the location of the Spring XML configuration meta-data file used to bootstrap, configure and initialize
     * the GemFire Server on start.
     * <p>
     * @param springXmlLocation a String indicating the location of the Spring XML configuration file.
     * @return this Builder instance.
     * @see #getSpringXmlLocation()
     */
    public Builder setSpringXmlLocation(final String springXmlLocation) {
      this.springXmlLocation = springXmlLocation;
      return this;
    }

    /**
     * Gets the working directory pathname in which the Server will be ran.  If the directory is unspecified,
     * then working directory defaults to the current directory.
     * 
     * @return a String indicating the working directory pathname.
     * @see #setWorkingDirectory(String)
     */
    public String getWorkingDirectory() {
      return IOUtils.tryGetCanonicalPathElseGetAbsolutePath(
        new File(StringUtils.defaultIfBlank(this.workingDirectory, DEFAULT_WORKING_DIRECTORY)));
    }

    /**
     * Sets the working directory in which the Server will be ran.  This also the directory in which all Server files
     * (such as log and license files) will be written.  If the directory is unspecified, then the working directory
     * defaults to the current directory.
     * 
     * @param workingDirectory a String indicating the pathname of the directory in which the Server will be ran.
     * @return this Builder instance.
     * @throws IllegalArgumentException wrapping a FileNotFoundException if the working directory pathname cannot be
     * found.
     * @see #getWorkingDirectory()
     * @see java.io.FileNotFoundException
     */
    public Builder setWorkingDirectory(final String workingDirectory) {
      if (!(new File(StringUtils.defaultIfBlank(workingDirectory, DEFAULT_WORKING_DIRECTORY)).isDirectory())) {
        throw new IllegalArgumentException(
          LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE.toLocalizedString("Server"),
            new FileNotFoundException(workingDirectory));
      }
      this.workingDirectory = workingDirectory;
      return this;
    }
    
    public Float getCriticalHeapPercentage() {
      return this.criticalHeapPercentage;
    }

    public Builder setCriticalHeapPercentage(final Float criticalHeapPercentage) {
      if (criticalHeapPercentage != null) {
        if (criticalHeapPercentage < 0 || criticalHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(String.format("Critical heap percentage (%1$s) must be between 0 and 100!",
            criticalHeapPercentage));
        }
      }
      this.criticalHeapPercentage = criticalHeapPercentage;
      return this;
    }

    public Float getCriticalOffHeapPercentage() {
      return this.criticalOffHeapPercentage;
    }
    
    public Builder setCriticalOffHeapPercentage(final Float criticalOffHeapPercentage) {
      if (criticalOffHeapPercentage != null) {
        if (criticalOffHeapPercentage < 0 || criticalOffHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(String.format("Critical off-heap percentage (%1$s) must be between 0 and 100!", criticalOffHeapPercentage));
        }
      }
     this.criticalOffHeapPercentage = criticalOffHeapPercentage;
     return this;
    }
    
    public Float getEvictionHeapPercentage() {
      return this.evictionHeapPercentage;
    }

    public Builder setEvictionHeapPercentage(final Float evictionHeapPercentage) {
      if (evictionHeapPercentage != null) {
        if (evictionHeapPercentage < 0 || evictionHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(String.format("Eviction heap percentage (%1$s) must be between 0 and 100!",
            evictionHeapPercentage));
        }
      }
      this.evictionHeapPercentage = evictionHeapPercentage;
      return this;
    }
    
    public Float getEvictionOffHeapPercentage() {
      return this.evictionOffHeapPercentage;
    }
    
    public Builder setEvictionOffHeapPercentage(final Float evictionOffHeapPercentage) {
      if (evictionOffHeapPercentage != null) {
        if (evictionOffHeapPercentage < 0 || evictionOffHeapPercentage > 100.0f) {
          throw new IllegalArgumentException(String.format("Eviction off-heap percentage (%1$s) must be between 0 and 100", evictionOffHeapPercentage));
        }
      }
      this.evictionOffHeapPercentage = evictionOffHeapPercentage;
      return this;
    }
    
    public String getHostNameForClients() {
      return this.hostNameForClients;
    }

    public Builder setHostNameForClients(String hostNameForClients) {
      this.hostNameForClients = hostNameForClients;
      return this;
    }

    public Integer getMaxConnections() {
      return this.maxConnections;
    }

    public Builder setMaxConnections(Integer maxConnections) {
      if (maxConnections != null && maxConnections < 1) {
        throw new IllegalArgumentException(String.format("Max Connections (%1$s) must be greater than 0!",
          maxConnections));
      }
      this.maxConnections = maxConnections;
      return this;
    }

    public Integer getMaxMessageCount() {
      return this.maxMessageCount;
    }

    public Builder setMaxMessageCount(Integer maxMessageCount) {
      if (maxMessageCount != null && maxMessageCount < 1) {
        throw new IllegalArgumentException(String.format("Max Message Count (%1$s) must be greater than 0!",
          maxMessageCount));
      }
      this.maxMessageCount = maxMessageCount;
      return this;
    }

    public Integer getMaxThreads() {
      return this.maxThreads;
    }

    public Builder setMaxThreads(Integer maxThreads) {
      if (maxThreads != null && maxThreads < 1) {
        throw new IllegalArgumentException(String.format("Max Threads (%1$s) must be greater than 0!", maxThreads));
      }
      this.maxThreads = maxThreads;
      return this;
    }

    public Integer getMessageTimeToLive() {
      return this.messageTimeToLive;
    }

    public Builder setMessageTimeToLive(Integer messageTimeToLive) {
      if (messageTimeToLive != null && messageTimeToLive < 1) {
        throw new IllegalArgumentException(String.format("Message Time To Live (%1$s) must be greater than 0!",
          messageTimeToLive));
      }
      this.messageTimeToLive = messageTimeToLive;
      return this;
    }

    public Integer getSocketBufferSize() {
      return this.socketBufferSize;
    }

    public Builder setSocketBufferSize(Integer socketBufferSize) {
      if (socketBufferSize != null && socketBufferSize < 1) {
        throw new IllegalArgumentException(String.format("The Server's Socket Buffer Size (%1$s) must be greater than 0!",
          socketBufferSize));
      }
      this.socketBufferSize = socketBufferSize;
      return this;
    }
    
    

    /**
     * Sets a GemFire Distributed System Property.
     *
     * @param propertyName a String indicating the name of the GemFire Distributed System property.
     * @param propertyValue a String value for the GemFire Distributed System property.
     * @return this Builder instance.
     */
    public Builder set(final String propertyName, final String propertyValue) {
      this.distributedSystemProperties.setProperty(propertyName, propertyValue);
      return this;
    }

    /**
     * Sets whether the PDX type meta-data should be persisted to disk.
     *
     * @param persistent a boolean indicating whether PDX type meta-data should be persisted to disk.
     * @return this Builder instance.
     */
    public Builder setPdxPersistent(final boolean persistent) {
      this.cacheConfig.setPdxPersistent(persistent);
      return this;
    }

    /**
     * Sets the GemFire Disk Store to be used to persist PDX type meta-data.
     *
     * @param pdxDiskStore a String indicating the name of the GemFire Disk Store to use to store PDX type meta-data
     * @return this Builder instance.
     */
    public Builder setPdxDiskStore(final String pdxDiskStore) {
      this.cacheConfig.setPdxDiskStore(pdxDiskStore);
      return this;
    }

    /**
     * Sets whether fields in the PDX instance should be ignored when unread.
     *
     * @param ignore a boolean indicating whether unread fields in the PDX instance should be ignored.
     * @return this Builder instance.
     */
    public Builder setPdxIgnoreUnreadFields(final boolean ignore) {
      this.cacheConfig.setPdxIgnoreUnreadFields(ignore);
      return this;
    }

    /**
     * Sets whether PDX instances should be returned as is when Region.get(key:String):Object is called.
     *
     * @param readSerialized a boolean indicating whether the PDX instance should be returned from a call to
     * Region.get(key:String):Object
     * @return this Builder instance.
     */
    public Builder setPdxReadSerialized(final boolean readSerialized) {
      this.cacheConfig.setPdxReadSerialized(readSerialized);
      return this;
    }

    /**
     * Set the PdxSerializer to use to serialize POJOs to the GemFire Cache Region or when sent between peers,
     * client/server, or during persistence to disk.
     *
     * @param pdxSerializer the PdxSerializer that is used to serialize application domain objects into PDX.
     * @return this Builder instance.
     */
    public Builder setPdxSerializer(final PdxSerializer pdxSerializer) {
      this.cacheConfig.setPdxSerializer(pdxSerializer);
      return this;
    }

    /**
     * Validates the configuration settings and properties of this Builder, ensuring that all invariants have been met.
     * Currently, the only invariant constraining the Builder is that the user must specify the member name for the
     * Server in the GemFire distributed system as a command-line argument, or by setting the memberName property
     * programmatically using the corresponding setter method.
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
     * @see com.gemstone.gemfire.distributed.ServerLauncher.Command#START
     */
    protected void validateOnStart() {
      if (Command.START.equals(getCommand())) {
        if (StringUtils.isBlank(getMemberName())
          && !isSet(System.getProperties(), DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME)
          && !isSet(getDistributedSystemProperties(), DistributionConfig.NAME_NAME)
          && !isSet(loadGemFireProperties(DistributedSystem.getPropertyFileURL()), DistributionConfig.NAME_NAME))
        {
          throw new IllegalStateException(LocalizedStrings.Launcher_Builder_MEMBER_NAME_VALIDATION_ERROR_MESSAGE
            .toLocalizedString("Server"));
        }

        if (!SystemUtils.CURRENT_DIRECTORY.equals(getWorkingDirectory())) {
          throw new IllegalStateException(LocalizedStrings.Launcher_Builder_WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE
            .toLocalizedString("Server"));
        }
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'status' command has been issued.
     * 
     * @see com.gemstone.gemfire.distributed.ServerLauncher.Command#STATUS
     */
    protected void validateOnStatus() {
      if (Command.STATUS.equals(getCommand())) {
        // do nothing
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'stop' command has been issued.
     * 
     * @see com.gemstone.gemfire.distributed.ServerLauncher.Command#STOP
     */
    protected void validateOnStop() {
      if (Command.STOP.equals(getCommand())) {
        // do nothing
      }
    }

    /**
     * Validates the Builder configuration settings and then constructs an instance of the ServerLauncher class
     * to invoke operations on a GemFire Server.
     * 
     * @return a newly constructed instance of the ServerLauncher configured with this Builder.
     * @see #validate()
     * @see com.gemstone.gemfire.distributed.ServerLauncher
     */
    public ServerLauncher build() {
      validate();
      return new ServerLauncher(this);
    }
  }

  /**
   * An enumerated type representing valid commands to the Server launcher.
   */
  public static enum Command {
    START("start", "assign-buckets", "disable-default-server", "rebalance", "server-bind-address", "server-port", "force", "debug", "help"),
    STATUS("status", "member", "pid", "dir", "debug", "help"),
    STOP("stop", "member", "pid", "dir", "debug", "help"),
    UNSPECIFIED("unspecified"),
    VERSION("version");

    private final List<String> options;

    private final String name;

    Command(final String name, final String... options) {
      assert !StringUtils.isBlank(name) : "The name of the command must be specified!";
      this.name = name;
      this.options = (options != null ? Collections.unmodifiableList(Arrays.asList(options))
        : Collections.<String>emptyList());
    }

    /**
     * Determines whether the specified name refers to a valid Server launcher command, as defined by this
     * enumerated type.
     * 
     * @param name a String value indicating the potential name of a Server launcher command.
     * @return a boolean indicating whether the specified name for a Server launcher command is valid.
     */
    public static boolean isCommand(final String name) {
      return (valueOfName(name) != null);
    }

    /**
     * Determines whether the given Server launcher command has been properly specified.  The command is deemed
     * unspecified if the reference is null or the Command is UNSPECIFIED.
     * 
     * @param command the Server launcher command.
     * @return a boolean value indicating whether the Server launcher command is unspecified.
     * @see Command#UNSPECIFIED
     */
    public static boolean isUnspecified(final Command command) {
      return (command == null || command.isUnspecified());
    }

    /**
     * Looks up a Server launcher command by name.  The equality comparison on name is case-insensitive.
     * 
     * @param name a String value indicating the name of the Server launcher command.
     * @return an enumerated type representing the command name or null if the no such command with the specified name
     * exists.
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
      return this.name;
    }

    /**
     * Gets a set of valid options that can be used with the Locator launcher command when used from the command-line.
     * 
     * @return a Set of Strings indicating the names of the options available to the Server launcher command.
     */
    public List<String> getOptions() {
      return this.options;
    }

    /**
     * Determines whether this Locator launcher command has the specified command-line option.
     * 
     * @param option a String indicating the name of the command-line option to this command.
     * @return a boolean value indicating whether this command has the specified named command-line option.
     */
    public boolean hasOption(final String option) {
      return getOptions().contains(StringUtils.toLowerCase(option));
    }

    /**
     * Convenience method for determining whether this is the UNSPECIFIED Server launcher command.
     * 
     * @return a boolean indicating if this command is UNSPECIFIED.
     * @see #UNSPECIFIED
     */
    public boolean isUnspecified() {
      return (this == UNSPECIFIED);
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
   * The ServerState is an immutable type representing the state of the specified Locator at any given moment in time.
   * The state of the Locator is assessed at the exact moment an instance of this class is constructed.
   * 
   * @see com.gemstone.gemfire.distributed.AbstractLauncher.ServiceState
   */
  public static final class ServerState extends ServiceState<String> {

    /**
     * Unmarshals a ServerState instance from the JSON String.
     * 
     * @return a ServerState value unmarshalled from the JSON String.
     */
    public static ServerState fromJson(final String json) {
      try {
        final GfJsonObject gfJsonObject = new GfJsonObject(json);

        final Status status = Status.valueOfDescription(gfJsonObject.getString(JSON_STATUS));
        final List<String> jvmArguments = Arrays.asList(GfJsonArray.toStringArray(gfJsonObject.getJSONArray(
          JSON_JVMARGUMENTS)));

        return new ServerState(status,
          gfJsonObject.getString(JSON_STATUSMESSAGE),
          gfJsonObject.getLong(JSON_TIMESTAMP),
          gfJsonObject.getString(JSON_LOCATION),
          gfJsonObject.getInt(JSON_PID),
          gfJsonObject.getLong(JSON_UPTIME),
          gfJsonObject.getString(JSON_WORKINGDIRECTORY),
          jvmArguments,
          gfJsonObject.getString(JSON_CLASSPATH),
          gfJsonObject.getString(JSON_GEMFIREVERSION),
          gfJsonObject.getString(JSON_JAVAVERSION),
          gfJsonObject.getString(JSON_LOGFILE),
          gfJsonObject.getString(JSON_HOST),
          gfJsonObject.getString(JSON_PORT),
          gfJsonObject.getString(JSON_MEMBERNAME));
      }
      catch (GfJsonException e) {
        // TODO: or should we return OFFLINE?
        throw new IllegalArgumentException("Unable to create ServerStatus from JSON: " + json);
      }
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

    public ServerState(final ServerLauncher launcher, final Status status, final String errorMessage) {
      this(status, // status
          errorMessage, // statusMessage
          System.currentTimeMillis(), // timestamp
          null, // serverLocation
          null, // pid
          0L, // uptime
          launcher.getWorkingDirectory(), // workingDirectory
          Collections.<String>emptyList(), // jvmArguments
          null, // classpath
          GemFireVersion.getGemFireVersion(), // gemfireVersion
          null, // javaVersion
          null, // logFile
          null, // host
          null, // port
          null);// memberName
    }
    
    protected ServerState(final Status status,
                          final String statusMessage,
                          final long timestamp,
                          final String serverLocation,
                          final Integer pid,
                          final Long uptime,
                          final String workingDirectory,
                          final List<String> jvmArguments,
                          final String classpath,
                          final String gemfireVersion,
                          final String javaVersion,
                          final String logFile,
                          final String host,
                          final String port,
                          final String memberName)
    {
      super(status, statusMessage, timestamp, serverLocation, pid, uptime, workingDirectory, jvmArguments, classpath,
        gemfireVersion, javaVersion, logFile, host, port, memberName);
    }

    private static String getServerLogFileCanonicalPath(final ServerLauncher launcher) {
      final InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();

      if (system != null) {
        final File logFile = system.getConfig().getLogFile();
        if (logFile != null && logFile.isFile()) {
          final String logFileCanonicalPath = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(logFile);
          if (!StringUtils.isBlank(logFileCanonicalPath)) { 
            return logFileCanonicalPath;
          }
        }
      }

      return launcher.getLogFileCanonicalPath();
    }

    @SuppressWarnings("unchecked")
    private static String getServerBindAddressAsString(final ServerLauncher launcher) {
      final GemFireCacheImpl gemfireCache = GemFireCacheImpl.getInstance();
      
      if (gemfireCache != null) {
        final List<CacheServer> csList = gemfireCache.getCacheServers();
        if (csList != null && !csList.isEmpty()) {
          final CacheServer cs = csList.get(0);
          final String serverBindAddressAsString = cs.getBindAddress();
          if (!StringUtils.isBlank(serverBindAddressAsString)) {
            return serverBindAddressAsString;
          }
        }
      }

      return launcher.getServerBindAddressAsString();
    }

    @SuppressWarnings("unchecked")
    private static String getServerPortAsString(final ServerLauncher launcher) {
      final GemFireCacheImpl gemfireCache = GemFireCacheImpl.getInstance();

      if (gemfireCache != null) {
        final List<CacheServer> csList = gemfireCache.getCacheServers();
        if (csList != null && !csList.isEmpty()) {
          final CacheServer cs = csList.get(0);
          final String portAsString = String.valueOf(cs.getPort());
          if (!StringUtils.isBlank(portAsString)) {
            return portAsString;
          }
        }
      }

      return (launcher.isDisableDefaultServer() ? StringUtils.EMPTY_STRING : launcher.getServerPortAsString());
    }

    @Override
    protected String getServiceName() {
      return SERVER_SERVICE_NAME;
    }
  }

}
