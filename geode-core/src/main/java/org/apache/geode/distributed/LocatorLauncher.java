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

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.internal.lang.StringUtils.wrap;
import static org.apache.geode.internal.lang.SystemUtils.CURRENT_DIRECTORY;
import static org.apache.geode.internal.util.IOUtils.tryGetCanonicalPathElseGetAbsolutePath;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.internal.locator.LocatorStatusRequest;
import org.apache.geode.cache.client.internal.locator.LocatorStatusResponse;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketCreator;
import org.apache.geode.internal.DistributionLocator;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
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
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.process.UnableToControlProcessException;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.lang.AttachAPINotFoundException;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.util.HostUtils;
import org.apache.geode.management.internal.util.JsonUtil;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * The LocatorLauncher class is a launcher for a GemFire Locator.
 *
 * @see org.apache.geode.distributed.AbstractLauncher
 * @see org.apache.geode.distributed.ServerLauncher
 * @since GemFire 7.0
 */
@SuppressWarnings({"unused"})
public class LocatorLauncher extends AbstractLauncher<String> {

  private static final Logger log = LogService.getLogger();

  @Immutable
  private static final Boolean DEFAULT_LOAD_SHARED_CONFIG_FROM_DIR = Boolean.FALSE;

  @Immutable
  private static final Map<String, String> helpMap;

  static {
    final Map<String, String> help = new HashMap<>();
    help.put("launcher",
        "A GemFire launcher used to start, stop and determine a Locator's status.");
    help.put(Command.START.getName(), String.format(
        "Starts a Locator running in the current working directory listening on the default port (%s) bound to all IP addresses available to the localhost.  The Locator must be given a member name in the GemFire cluster.  The default bind-address and port may be overridden using the corresponding command-line options.",
        String.valueOf(getDefaultLocatorPort())));
    help.put(Command.STATUS.getName(),
        "Displays the status of a Locator given any combination of the bind-address[port], member name/ID, PID, or the directory in which the Locator is running.");
    help.put(Command.STOP.getName(),
        "Stops a running Locator given a member name/ID, PID, or the directory in which the Locator is running.");
    help.put(Command.VERSION.getName(),
        "Displays GemFire product version information.");
    help.put("bind-address",
        "Specifies the IP address on which to bind, or on which the Locator is bound, listening for client requests.  Defaults to all IP addresses available to the localhost.");
    help.put("debug", "Displays verbose information during the invocation of the launcher.");
    help.put("delete-pid-file-on-stop",
        "Specifies that this Locator's PID file should be deleted on stop.  The default is to not delete this Locator's PID file until JVM exit if --delete-pid-file-on-stop is not specified.");
    help.put("dir",
        "Specifies the working directory where the Locator is running.  Defaults to the current working directory.");
    help.put("force",
        "Enables any existing Locator PID file to be overwritten on start.  The default is to throw an error if a PID file already exists and --force is not specified.");
    help.put("help",
        "Causes GemFire to print out information instead of performing the command. This option is supported by all commands.");
    help.put("hostname-for-clients",
        "An option to specify the hostname or IP address to send to clients so they can connect to this Locator. The default is to use the IP address to which the Locator is bound.");
    help.put("member", "Identifies the Locator by member name or ID in the GemFire cluster.");
    help.put("pid", "Indicates the OS process ID of the running Locator.");
    help.put("port", String.format(
        "Specifies the port on which the Locator is listening for client requests. Defaults to %s.",
        String.valueOf(getDefaultLocatorPort())));
    help.put("redirect-output",
        "An option to cause the Locator to redirect standard out and standard error to the GemFire log file.");

    helpMap = Collections.unmodifiableMap(help);
  }

  @Immutable
  private static final Map<Command, String> usageMap;

  static {
    Map<Command, String> usage = new TreeMap<>();
    usage.put(Command.START,
        "start <member-name> [--bind-address=<IP-address>] [--hostname-for-clients=<IP-address>] [--port=<port>] [--dir=<Locator-working-directory>] [--force] [--debug] [--help]");
    usage.put(Command.STATUS,
        "status [--bind-address=<IP-address>] [--port=<port>] [--member=<member-ID/Name>] [--pid=<process-ID>] [--dir=<Locator-working-directory>] [--debug] [--help]");
    usage.put(Command.STOP,
        "stop [--member=<member-ID/Name>] [--pid=<process-ID>] [--dir=<Locator-working-directory>] [--debug] [--help]");
    usage.put(Command.VERSION, "version");
    usageMap = usage;
  }

  private static final String DEFAULT_LOCATOR_LOG_EXT = ".log";
  private static final String DEFAULT_LOCATOR_LOG_NAME = "locator";
  private static final String LOCATOR_SERVICE_NAME = "Locator";

  @MakeNotStatic
  private static final AtomicReference<LocatorLauncher> INSTANCE = new AtomicReference<>();

  // private transient volatile boolean debug;

  private final transient ControlNotificationHandler controlHandler;

  private final AtomicBoolean starting = new AtomicBoolean(false);

  private final boolean deletePidFileOnStop;
  private final boolean force;
  private final boolean help;
  private final boolean redirectOutput;

  private final Command command;

  private final boolean bindAddressSpecified;
  private final boolean portSpecified;
  private final boolean workingDirectorySpecified;

  private final InetAddress bindAddress;

  private final Integer pid;
  private final Integer port;

  private transient volatile InternalLocator locator;

  private final Properties distributedSystemProperties;

  private final String hostnameForClients;
  private final String memberName;
  private final String workingDirectory;

  // NOTE in addition to debug and locator, the other shared, mutable state
  private transient volatile String statusMessage;

  private transient volatile ControllableProcess process;

  private final transient LocatorControllerParameters controllerParameters;

  /**
   * Launches a GemFire Locator from the command-line configured with the given arguments.
   *
   * @param args the command-line arguments used to configure the GemFire Locator at runtime.
   */
  public static void main(final String... args) {
    try {
      new Builder(args).build().run();
    } catch (AttachAPINotFoundException handled) {
      System.err.println(handled.getMessage());
    }
  }

  private static Integer getDefaultLocatorPort() {
    return Integer.getInteger(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY,
        DistributionLocator.DEFAULT_LOCATOR_PORT);
  }

  /**
   * Gets the instance of the LocatorLauncher used to launch the GemFire Locator, or null if this VM
   * does not have an instance of LocatorLauncher indicating no GemFire Locator is running.
   *
   * @return the instance of LocatorLauncher used to launcher a GemFire Locator in this VM.
   */
  public static LocatorLauncher getInstance() {
    return INSTANCE.get();
  }

  /**
   * Gets the LocatorState for this process or null if this process was not launched using this VM's
   * LocatorLauncher reference.
   *
   * @return the LocatorState for this process or null.
   */
  public static LocatorState getLocatorState() {
    return (getInstance() != null ? getInstance().status() : null);
  }

  /**
   * Private constructor used to properly construct an immutable instance of the LocatorLauncher
   * using a Builder. The Builder is used to configure a LocatorLauncher instance. The Builder can
   * process user input from the command-line or be used to properly construct an instance of the
   * LocatorLauncher programmatically using the API.
   *
   * @param builder an instance of LocatorLauncher.Builder for configuring and constructing an
   *        instance of the LocatorLauncher.
   * @see org.apache.geode.distributed.LocatorLauncher.Builder
   */
  private LocatorLauncher(final Builder builder) {
    this.command = builder.getCommand();
    this.help = Boolean.TRUE.equals(builder.getHelp());
    this.bindAddressSpecified = builder.isBindAddressSpecified();
    this.bindAddress = builder.getBindAddress();
    setDebug(Boolean.TRUE.equals(builder.getDebug()));
    this.deletePidFileOnStop = Boolean.TRUE.equals(builder.getDeletePidFileOnStop());
    this.distributedSystemProperties = builder.getDistributedSystemProperties();
    this.force = Boolean.TRUE.equals(builder.getForce());
    this.hostnameForClients = builder.getHostnameForClients();
    this.memberName = builder.getMemberName();
    this.pid = builder.getPid();
    this.portSpecified = builder.isPortSpecified();
    this.port = builder.getPort();
    this.redirectOutput = Boolean.TRUE.equals(builder.getRedirectOutput());
    this.workingDirectorySpecified = builder.isWorkingDirectorySpecified();
    this.workingDirectory = builder.getWorkingDirectory();
    this.controllerParameters = new LocatorControllerParameters();
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
   * Returns the status of the locator on the given host & port
   *
   * @deprecated in Geode 1.12. Use statusForLocator() instance method instead.
   *             This static method does not use the properties set via the Builder.
   */
  @Deprecated
  public static LocatorStatusResponse statusLocator(int port, InetAddress bindAddress)
      throws IOException {
    return statusLocator(port, bindAddress, new Properties());
  }

  /**
   * Returns the status of the locator on the given host & port
   */
  public LocatorStatusResponse statusForLocator(int port, InetAddress bindAddress)
      throws IOException {
    return statusLocator(port, bindAddress, getProperties());
  }

  private static LocatorStatusResponse statusLocator(
      final int port, InetAddress bindAddress,
      final Properties properties)
      throws IOException {

    // final int timeout = (60 * 2 * 1000); // 2 minutes
    final int timeout = Integer.MAX_VALUE; // 2 minutes

    try {

      final SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(
          properties,
          SecurableCommunicationChannel.LOCATOR);
      final TcpSocketCreator socketCreator = new SocketCreator(sslConfig);
      final TcpClient client = new TcpClient(socketCreator,
          InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
          InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer());

      return (LocatorStatusResponse) client.requestToServer(
          new HostAndPort(bindAddress == null ? null : bindAddress.getCanonicalHostName(), port),
          new LocatorStatusRequest(), timeout, true);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets a reference to the {@code Cache} that was created by this {@code LocatorLauncher}.
   *
   * @return a reference to the Cache
   * @see Cache
   */
  public Cache getCache() {
    return getInternalLocator().getCache();
  }

  /**
   * Gets a reference to the {@code Locator} that was created by this {@code LocatorLauncher}.
   *
   * @return a reference to the Locator.
   * @see Locator
   */
  public Locator getLocator() {
    return locator;
  }

  /**
   * Gets a reference to the {@code Locator} as an {@code InternalLocator}. For internal use only.
   *
   * @return a reference to the Locator as an InternalLocator.
   */
  InternalLocator getInternalLocator() {
    return locator;
  }

  /**
   * Gets an identifier that uniquely identifies and represents the Locator associated with this
   * launcher.
   *
   * @return a String value identifier to uniquely identify the Locator and it's launcher.
   * @see #getBindAddressAsString()
   * @see #getPortAsString()
   */
  public String getId() {
    return LocatorState.getBindAddressAsString(this).concat("[")
        .concat(LocatorState.getPortAsString(this)).concat("]");
  }

  /**
   * Get the Locator launcher command used to invoke the Locator.
   *
   * @return the Locator launcher command used to invoke the Locator.
   * @see org.apache.geode.distributed.LocatorLauncher.Command
   */
  public Command getCommand() {
    return this.command;
  }

  /**
   * Determines whether the PID file is allowed to be overwritten when the Locator is started and a
   * PID file already exists in the Locator's specified working directory.
   *
   * @return boolean indicating if force has been enabled.
   */
  public boolean isForcing() {
    return this.force;
  }

  /**
   * Determines whether this launcher will be used to display help information. If so, then none of
   * the standard Locator launcher commands will be used to affect the state of the Locator. A
   * launcher is said to be 'helping' if the user entered the "--help" option (switch) on the
   * command-line.
   *
   * @return a boolean value indicating if this launcher is used for displaying help information.
   * @see org.apache.geode.distributed.LocatorLauncher.Command
   */
  public boolean isHelping() {
    return this.help;
  }

  /**
   * Determines whether this launcher will redirect output to system logs when starting a new
   * Locator process.
   *
   * @return a boolean value indicating if this launcher will redirect output to system logs when
   *         starting a new Locator process
   */
  public boolean isRedirectingOutput() {
    return this.redirectOutput;
  }

  /**
   * Gets the IP address of the NIC to which the Locator has bound itself listening for client
   * requests.
   *
   * @return an InetAddress object representing the configured bind address for the Locator.
   * @see java.net.InetAddress
   */
  public InetAddress getBindAddress() {
    return this.bindAddress;
  }

  /**
   * Gets the host, as either hostname or IP address, on which the Locator was bound and running. An
   * attempt is made to get the canonical hostname for IP address to which the Locator was bound for
   * accepting client requests. If the bind address is null or localhost is unknown, then a default
   * String value of "localhost/127.0.0.1" is returned.
   *
   * Note, this information is purely information and should not be used to re-construct state or
   * for other purposes.
   *
   * @return the hostname or IP address of the host running the Locator, based on the bind-address,
   *         or 'localhost/127.0.0.1' if the bind address is null and localhost is unknown.
   * @see java.net.InetAddress
   * @see #getBindAddress()
   */
  protected String getBindAddressAsString() {
    try {
      if (getBindAddress() != null) {
        return getBindAddress().getCanonicalHostName();
      }

      return LocalHostUtil.getCanonicalLocalHostName();
    } catch (UnknownHostException handled) {
      // Returning localhost/127.0.0.1 implies the bindAddress was null and no IP address for
      // localhost could be found
      return "localhost/127.0.0.1";
    }
  }

  /**
   * Gets the hostname that clients will use to lookup the running Locator.
   *
   * @return a String indicating the hostname used by clients to lookup the Locator.
   */
  public String getHostnameForClients() {
    return this.hostnameForClients;
  }

  /**
   * Gets the name of the log file used to log information about this Locator.
   *
   * @return a String value indicating the name of this Locator's log file.
   */
  @Override
  public String getLogFileName() {
    return defaultIfBlank(getMemberName(), DEFAULT_LOCATOR_LOG_NAME)
        .concat(DEFAULT_LOCATOR_LOG_EXT);
  }

  /**
   * Gets the name of this member (this Locator) in the GemFire distributed system and determined by
   * the 'name' GemFire property.
   *
   * @return a String indicating the name of the member (this Locator) in the GemFire distributed
   *         system.
   */
  @Override
  public String getMemberName() {
    return defaultIfBlank(this.memberName, super.getMemberName());
  }

  /**
   * Gets the user-specified process ID (PID) of the running Locator that LocatorLauncher uses to
   * issue status and stop commands to the Locator.
   *
   * @return an Integer value indicating the process ID (PID) of the running Locator.
   */
  @Override
  public Integer getPid() {
    return this.pid;
  }

  /**
   * Gets the port number on which the Locator listens for client requests.
   *
   * @return an Integer value indicating the port number on which the Locator is listening for
   *         client requests.
   */
  public Integer getPort() {
    if (locator != null) {
      return locator.getPort();
    }

    return this.port;
  }

  /**
   * Gets the port number represented as a String value. If the port number is null, the the default
   * Locator port (10334) is returned;
   *
   * @return the port number as a String value.
   * @see #getPort()
   */
  public String getPortAsString() {
    Integer port = getPort();
    return (port != null ? port : getDefaultLocatorPort()).toString();
  }

  /**
   * Gets the GemFire Distributed System (cluster) Properties.
   *
   * @return a Properties object containing the configuration settings for the GemFire Distributed
   *         System (cluster).
   * @see java.util.Properties
   */
  public Properties getProperties() {
    return (Properties) this.distributedSystemProperties.clone();
  }

  /**
   * Gets the name for a GemFire Locator.
   *
   * @return a String indicating the name for a GemFire Locator.
   */
  @Override
  public String getServiceName() {
    return LOCATOR_SERVICE_NAME;
  }

  /**
   * Gets the working directory pathname in which the Locator will be run.
   *
   * @return a String value indicating the pathname of the Locator's working directory.
   */
  @Override
  public String getWorkingDirectory() {
    return this.workingDirectory;
  }

  /**
   * Displays help for the specified Locator launcher command to standard err. If the Locator
   * launcher command is unspecified, then usage information is displayed instead.
   *
   * @param command the Locator launcher command in which to display help information.
   * @see #usage()
   */
  public void help(final Command command) {
    if (Command.isUnspecified(command)) {
      usage();
    } else {
      info(wrap(helpMap.get(command.getName()), 80, ""));
      info("\n\nusage: \n\n");
      info(wrap("> java ... " + getClass().getName() + " " + usageMap.get(command), 80, "\t\t"));
      info("\n\noptions: \n\n");

      for (String option : command.getOptions()) {
        info(wrap("--" + option + ": " + helpMap.get(option) + "\n", 80, "\t"));
      }

      info("\n\n");
    }
  }

  /**
   * Displays usage information on the proper invocation of the LocatorLauncher from the
   * command-line to standard err.
   *
   * @see #help(org.apache.geode.distributed.LocatorLauncher.Command)
   */
  public void usage() {
    info(wrap(helpMap.get("launcher"), 80, "\t"));
    info("\n\nSTART\n\n");
    help(Command.START);
    info("STATUS\n\n");
    help(Command.STATUS);
    info("STOP\n\n");
    help(Command.STOP);
  }

  /**
   * The Runnable method used to launch the Locator with the specified command. If 'start' has been
   * issued, then run will block as expected for the Locator to stop. The 'start' command is
   * implemented with a call to start() followed by a call to waitOnLocator().
   *
   * @see java.lang.Runnable
   * @see LocatorLauncher.Command
   * @see LocatorLauncher#start()
   * @see LocatorLauncher#waitOnLocator()
   * @see LocatorLauncher#status()
   * @see LocatorLauncher#stop()
   * @see LocatorLauncher#version()
   * @see LocatorLauncher#help(org.apache.geode.distributed.LocatorLauncher.Command)
   * @see LocatorLauncher#usage()
   */
  @Override
  public void run() {
    if (!isHelping()) {
      switch (getCommand()) {
        case START:
          info(start());
          waitOnLocator();
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
   * Gets a File reference with the path to the PID file for the Locator.
   *
   * @return a File reference to the path of the Locator's PID file.
   */
  protected File getLocatorPidFile() {
    return new File(getWorkingDirectory(), ProcessType.LOCATOR.getPidFileName());
  }

  /**
   * Determines whether a GemFire Locator can be started with this instance of LocatorLauncher.
   *
   * @return a boolean indicating whether a GemFire Locator can be started with this instance of
   *         LocatorLauncher, which is true if the LocatorLauncher has not already started a Locator
   *         or a Locator is not already running.
   * @see #start()
   */
  private boolean isStartable() {
    return (!isRunning() && this.starting.compareAndSet(false, true));
  }

  /**
   * Starts a Locator running on the specified port and bind address, as determined by getPort and
   * getSocketAddress respectively, defaulting to 10334 and 'localhost' if not specified, with both
   * peer and server location enabled.
   *
   * 'start' is an asynchronous invocation of the Locator. As such, this method makes no guarantees
   * whether the Locator's location services (peer and server) are actually running before it
   * returns. The Locator's location-based services are initiated in separate, daemon Threads and
   * depends on the relative timing and scheduling of those Threads by the JVM. If the application
   * using this API wishes for the Locator to continue running after normal application processing
   * completes, then one must call <code>waitOnLocator</code>.
   *
   * Given the nature of start, the Locator's status will be in either 1 of 2 possible states. If
   * the 'request' to start the Locator proceeds without exception, the status will be 'STARTED'.
   * However, if any exception is encountered during the normal startup sequence, then a
   * RuntimeException is thrown and the status is set to 'STOPPED'.
   *
   * @return a LocatorState to reflect the state of the Locator after start.
   * @throws RuntimeException if the Locator failed to start for any reason.
   * @throws IllegalStateException if the Locator is already running.
   * @see #failOnStart(Throwable)
   * @see #getBindAddress()
   * @see #getDistributedSystemProperties()
   * @see #isForcing()
   * @see #getLogFile()
   * @see #getLocatorPidFile
   * @see #getPort()
   * @see #status()
   * @see #stop()
   * @see #waitOnLocator()
   * @see #waitOnStatusResponse(long, long, java.util.concurrent.TimeUnit)
   * @see org.apache.geode.distributed.LocatorLauncher.LocatorState
   * @see org.apache.geode.distributed.AbstractLauncher.Status#NOT_RESPONDING
   * @see org.apache.geode.distributed.AbstractLauncher.Status#ONLINE
   * @see org.apache.geode.distributed.AbstractLauncher.Status#STARTING
   */
  @SuppressWarnings("deprecation")
  public LocatorState start() {
    if (isStartable()) {
      INSTANCE.compareAndSet(null, this);

      try {
        this.process =
            new FileControllableProcess(this.controlHandler, new File(getWorkingDirectory()),
                ProcessType.LOCATOR, isForcing());

        assertPortAvailable(getBindAddress(), getPort());

        ProcessLauncherContext.set(isRedirectingOutput(), getOverriddenDefaults(),
            statusMessage -> LocatorLauncher.this.statusMessage = statusMessage);

        try {
          this.locator = InternalLocator.startLocator(getPort(), getLogFile(), null, null,
              getBindAddress(), true, getDistributedSystemProperties(), getHostnameForClients(),
              Paths.get(workingDirectory));
        } finally {
          ProcessLauncherContext.remove();
        }

        debug("Running Locator on (%1$s) in (%2$s) as (%3$s)...", getId(), getWorkingDirectory(),
            getMember());
        log.debug("Locator is online");
        running.set(true);

        return new LocatorState(this, Status.ONLINE);
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
      } catch (Error | RuntimeException e) {
        failOnStart(e);
        throw e;
      } catch (Exception e) {
        failOnStart(e);
        throw new RuntimeException(e);
      } finally {
        this.starting.set(false);
      }
    } else {
      throw new IllegalStateException(
          String.format("A %s is already running in %s on %s.",
              getServiceName(), getWorkingDirectory(), getId()));
    }
  }

  @Override
  protected Properties getDistributedSystemProperties() {
    return super.getDistributedSystemProperties(getProperties());
  }

  /**
   * A helper method to ensure the same sequence of actions are taken when the Locator fails to
   * start caused by some exception.
   *
   * @param cause the Throwable thrown during the startup or wait operation on the Locator.
   */
  private void failOnStart(final Throwable cause) {
    if (cause != null) {
      log.info("locator is exiting due to an exception", cause);
    } else {
      log.info("locator is exiting normally");
    }

    if (this.locator != null) {
      this.locator.stop();
      this.locator = null;
    }
    if (this.process != null) {
      this.process.stop(this.deletePidFileOnStop);
      this.process = null;
    }

    INSTANCE.compareAndSet(this, null);

    this.running.set(false);
  }

  /**
   * Waits on the Locator to stop causing the calling Thread to join with the Locator's
   * location-based services Thread.
   *
   * @return the Locator's status once it stops.
   * @throws AssertionError if the Locator has not been started and the reference is null
   *         (assertions must be enabled for the error to be thrown).
   * @see #failOnStart(Throwable)
   * @see org.apache.geode.distributed.AbstractLauncher.Status
   * @see org.apache.geode.distributed.LocatorLauncher.LocatorState
   */
  public LocatorState waitOnLocator() {
    Throwable t = null;

    try {
      // make sure the Locator was started and the reference was set
      assert getInternalLocator() != null : "The Locator must first be started with a call to start!";

      debug("Waiting on Locator (%1$s) to stop...", getId());

      // prevent the JVM from exiting by joining the Locator Thread
      getInternalLocator().waitToStop();
    } catch (InterruptedException handled) {
      Thread.currentThread().interrupt();
      t = handled;
      debug(handled);
    } catch (Throwable e) {
      t = e;
      throw e;
    } finally {
      failOnStart(t);
    }

    return new LocatorState(this, Status.STOPPED);
  }

  /**
   * Waits for a Locator status request response to be returned up to the specified timeout in the
   * given unit of time. This call will send status requests at fixed intervals in the given unit of
   * time until the timeout expires. If the request to determine the Locator's status is successful,
   * then the Locator is considered to be 'ONLINE'. Otherwise, the Locator is considered to be
   * unresponsive to the status request.
   *
   * However, this does not necessarily imply the Locator start was unsuccessful, only that a
   * response was not received in the given time period.
   *
   * Note, this method does not block or cause the Locator's location-based services (daemon
   * Threads) to continue running in anyway if the main application Thread terminates when running
   * the Locator in-process. If the caller wishes to start a Locator in an asynchronous manner
   * within the application process, then a call should be made to <code>waitOnLocator</code>.
   *
   * @param timeout a long value in time unit indicating when the period of time should expire in
   *        attempting to determine the Locator's status.
   * @param interval a long value in time unit for how frequent the requests should be sent to the
   *        Locator.
   * @param timeUnit the unit of time in which the timeout and interval are measured.
   * @return the state of the Locator, which will either be 'ONLINE' or "NOT RESPONDING'. If the
   *         status returned is 'NOT RESPONDING', it just means the Locator did not respond to the
   *         status request within the given time period. It should not be taken as the Locator
   *         failed to start.
   * @see #waitOnLocator()
   */
  public LocatorState waitOnStatusResponse(final long timeout, final long interval,
      final TimeUnit timeUnit) {
    final long endTimeInMilliseconds = (System.currentTimeMillis() + timeUnit.toMillis(timeout));

    while (System.currentTimeMillis() < endTimeInMilliseconds) {
      try {
        LocatorStatusResponse response = statusForLocator(getPort(), getBindAddress());
        return new LocatorState(this, Status.ONLINE, response);
      } catch (Exception handled) {
        timedWait(interval, timeUnit);
      }
    }

    // NOTE just because we were not able to communicate with the Locator in the given amount of
    // time does not mean
    // the Locator is having problems. The Locator could be slow in starting up and the timeout may
    // not be
    // long enough.
    return new LocatorState(this, Status.NOT_RESPONDING);
  }

  private void timedWait(final long interval, final TimeUnit timeUnit) {
    try {
      synchronized (this) {
        timeUnit.timedWait(this, interval);
      }
    } catch (InterruptedException handled) {
      // NOTE just go and send another status request to the Locator...
    }
  }

  /**
   * Attempts to determine the state of the Locator. The Locator's status will be in only 1 of 2
   * possible states, either ONLINE or OFFLINE. This method behaves differently depending on which
   * parameters were specified when the LocatorLauncher was constructed with an instance of Builder.
   * If either the 'dir' or the 'pid' command-line option were specified, then an attempt is made to
   * determine the Locator's status by using the dir or pid to correctly identify the Locator's
   * MemberMXBean registered in the MBeanServer of the Locator's JVM, and invoking the 'status'
   * operation. The same behavior occurs if the caller specified the Locator's GemFire member name
   * or ID.
   *
   * However, if 'dir' or 'pid' were not specified, then determining the Locator's status defaults
   * to using the configured bind address and port. If the bind address or port was not specified
   * when using the Builder to construct a LocatorLauncher instance, then the defaults for both bind
   * address and port are used. In either case, an actual TCP/IP request is made to the Locator's
   * ServerSocket to ensure it is listening for client requests. This is true even when the
   * LocatorLauncher is used in-process by calling the API.
   *
   * If the conditions above hold, then the Locator is deemed to be 'ONLINE', otherwise, the Locator
   * is considered 'OFFLINE'.
   *
   * @return the Locator's state.
   * @see #start()
   * @see #stop()
   * @see org.apache.geode.distributed.AbstractLauncher.Status
   * @see org.apache.geode.distributed.LocatorLauncher.LocatorState
   */
  public LocatorState status() {
    final LocatorLauncher launcher = getInstance();
    // if this instance is starting then return local status
    if (this.starting.get()) {
      debug(
          "Getting status from the LocatorLauncher instance that actually launched the GemFire Locator.%n");
      return new LocatorState(this, Status.STARTING);
    }
    // if this instance is running then return local status
    else if (isRunning()) {
      debug("Getting Locator status using host (%1$s) and port (%2$s)%n", getBindAddressAsString(),
          getPortAsString());
      return statusWithPort();
    }
    // if in-process do not use ProcessController
    else if (isPidInProcess() && launcher != null) {
      return launcher.statusInProcess();
    }
    // attempt to get status using pid if provided
    else if (getPid() != null) {
      debug("Getting Locator status using process ID (%1$s)%n", getPid());
      return statusWithPid();
    }
    // attempt to get status using workingDirectory unless port was specified
    else if (!(this.bindAddressSpecified || this.portSpecified)) {
      debug("Getting Locator status using working directory (%1$s)%n", getWorkingDirectory());
      return statusWithWorkingDirectory();
    }
    // attempt to get status using host and port (Note, bind address doubles as host when the
    // launcher is used to get the Locator's status).
    else {
      debug("Getting Locator status using host (%1$s) and port (%2$s)%n", getBindAddressAsString(),
          getPortAsString());
      return statusWithPort();
    }
  }

  private LocatorState statusInProcess() {
    if (this.starting.get()) {
      debug(
          "Getting status from the LocatorLauncher instance that actually launched the GemFire Locator.%n");
      return new LocatorState(this, Status.STARTING);
    } else {
      debug("Getting Locator status using host (%1$s) and port (%2$s)%n", getBindAddressAsString(),
          getPortAsString());
      return statusWithPort();
    }

  }

  private LocatorState statusWithPid() {
    try {
      final ProcessController controller = new ProcessControllerFactory()
          .createProcessController(this.controllerParameters, getPid());
      controller.checkPidSupport();
      final String statusJson = controller.status();
      return LocatorState.fromJson(statusJson);
    } catch (ConnectionFailedException handled) {
      // failed to attach to locator JVM
      return createNoResponseState(handled,
          "Failed to connect to locator with process id " + getPid());
    } catch (IOException | MBeanInvocationFailedException | UnableToControlProcessException
        | TimeoutException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with locator with process id " + getPid());
    } catch (InterruptedException handled) {
      Thread.currentThread().interrupt();
      return createNoResponseState(handled,
          "Interrupted while trying to communicate with locator with process id " + getPid());
    }
  }

  private LocatorState statusWithPort() {
    try {
      LocatorStatusResponse response = statusForLocator(getPort(), getBindAddress());
      return new LocatorState(this, Status.ONLINE, response);
    } catch (Exception handled) {
      return createNoResponseState(handled,
          "Failed to connect to locator " + getBindAddressAsString() + "[" + getPort() + "]");
    }
  }

  private LocatorState statusWithWorkingDirectory() {
    int parsedPid = 0;
    try {
      final ProcessController controller =
          new ProcessControllerFactory().createProcessController(this.controllerParameters,
              new File(getWorkingDirectory()), ProcessType.LOCATOR.getPidFileName());
      parsedPid = controller.getProcessId();

      // note: in-process request will go infinite loop unless we do the following
      if (parsedPid == ProcessUtils.identifyPid()) {
        LocatorLauncher runningLauncher = getInstance();
        if (runningLauncher != null) {
          return runningLauncher.status();
        }
      }

      final String statusJson = controller.status();
      return LocatorState.fromJson(statusJson);
    } catch (ConnectionFailedException handled) {
      // failed to attach to locator JVM
      return createNoResponseState(handled,
          "Failed to connect to locator with process id " + parsedPid);
    } catch (FileNotFoundException handled) {
      // could not find pid file
      return createNoResponseState(handled, "Failed to find process file "
          + ProcessType.LOCATOR.getPidFileName() + " in " + getWorkingDirectory());
    } catch (IOException | MBeanInvocationFailedException | UnableToControlProcessException
        | TimeoutException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with locator with process id " + parsedPid);
    } catch (PidUnavailableException e) {
      // couldn't determine pid from within locator JVM
      return createNoResponseState(e, "Failed to find usable process id within file "
          + ProcessType.LOCATOR.getPidFileName() + " in " + getWorkingDirectory());
    } catch (InterruptedException handled) {
      Thread.currentThread().interrupt();
      return createNoResponseState(handled,
          "Interrupted while trying to communicate with locator with process id " + parsedPid);
    }
  }

  /**
   * Determines whether the Locator can be stopped in-process, such as when a Locator is embedded in
   * an application and the LocatorLauncher API is being used.
   *
   * @return a boolean indicating whether the Locator can be stopped in-process (the application's
   *         process with an embedded Locator).
   */
  protected boolean isStoppable() {
    return (isRunning() && getInternalLocator() != null);
  }

  /**
   * Stop shuts the running Locator down. Using the API, the Locator is requested to stop by calling
   * the Locator object's 'stop' method. Internally, this method is no different than using the
   * LocatorLauncher class from the command-line or from within GemFire shell (Gfsh). In every
   * single case, stop sends a TCP/IP 'shutdown' request on the configured address/port to which the
   * Locator is bound and listening.
   *
   * If the "shutdown" request is successful, then the Locator will be 'STOPPED'. Otherwise, the
   * Locator is considered 'OFFLINE' since the actual state cannot be fully assessed (as in the
   * application process in which the Locator was hosted may still be running and the Locator object
   * may still exist even though it is no longer responding to location-based requests). The later
   * is particularly important in cases where the system resources (such as Sockets) may not have
   * been cleaned up yet. Therefore, by returning a status of 'OFFLINE', the value is meant to
   * reflect this in-deterministic state.
   *
   * @return a LocatorState indicating the state of the Locator after stop has been requested.
   * @see #start()
   * @see #status()
   * @see org.apache.geode.distributed.LocatorLauncher.LocatorState
   * @see org.apache.geode.distributed.AbstractLauncher.Status#NOT_RESPONDING
   * @see org.apache.geode.distributed.AbstractLauncher.Status#STOPPED
   */
  public LocatorState stop() {
    final LocatorLauncher launcher = getInstance();
    // if this instance is running then stop it
    if (isStoppable()) {
      return stopInProcess();
    }
    // if in-process but difference instance of LocatorLauncher
    else if (isPidInProcess() && launcher != null) {
      return launcher.stopInProcess();
    }
    // attempt to stop Locator using pid...
    else if (getPid() != null) {
      return stopWithPid();
    }
    // attempt to stop Locator using the working directory...
    else if (getWorkingDirectory() != null) {
      return stopWithWorkingDirectory();
    } else {
      return new LocatorState(this, Status.NOT_RESPONDING);
    }
  }

  private LocatorState stopInProcess() {
    if (isStoppable()) {
      this.locator.stop();
      this.locator = null;
      this.process.stop(this.deletePidFileOnStop);
      this.process = null;
      INSTANCE.compareAndSet(this, null); // note: other thread may return Status.NOT_RESPONDING now
      this.running.set(false);
      return new LocatorState(this, Status.STOPPED);
    } else {
      return new LocatorState(this, Status.NOT_RESPONDING);
    }
  }

  private LocatorState stopWithPid() {
    try {
      final ProcessController controller = new ProcessControllerFactory()
          .createProcessController(new LocatorControllerParameters(), getPid());
      controller.checkPidSupport();
      controller.stop();
      return new LocatorState(this, Status.STOPPED);
    } catch (ConnectionFailedException handled) {
      // failed to attach to locator JVM
      return createNoResponseState(handled,
          "Failed to connect to locator with process id " + getPid());
    } catch (IOException | MBeanInvocationFailedException
        | UnableToControlProcessException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with locator with process id " + getPid());
    }
  }

  private LocatorState stopWithWorkingDirectory() {
    int parsedPid = 0;
    try {
      final ProcessController controller =
          new ProcessControllerFactory().createProcessController(this.controllerParameters,
              new File(getWorkingDirectory()), ProcessType.LOCATOR.getPidFileName());
      parsedPid = controller.getProcessId();

      // NOTE in-process request will go infinite loop unless we do the following
      if (parsedPid == ProcessUtils.identifyPid()) {
        final LocatorLauncher runningLauncher = getInstance();
        if (runningLauncher != null) {
          return runningLauncher.stopInProcess();
        }
      }

      controller.stop();
      return new LocatorState(this, Status.STOPPED);
    } catch (ConnectionFailedException handled) {
      // failed to attach to locator JVM
      return createNoResponseState(handled,
          "Failed to connect to locator with process id " + parsedPid);
    } catch (FileNotFoundException handled) {
      // could not find pid file
      return createNoResponseState(handled, "Failed to find process file "
          + ProcessType.LOCATOR.getPidFileName() + " in " + getWorkingDirectory());
    } catch (IOException | MBeanInvocationFailedException
        | UnableToControlProcessException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with locator with process id " + parsedPid);
    } catch (InterruptedException handled) {
      Thread.currentThread().interrupt();
      return createNoResponseState(handled,
          "Interrupted while trying to communicate with locator with process id " + parsedPid);
    } catch (PidUnavailableException handled) {
      // couldn't determine pid from within locator JVM
      return createNoResponseState(handled, "Failed to find usable process id within file "
          + ProcessType.LOCATOR.getPidFileName() + " in " + getWorkingDirectory());
    } catch (TimeoutException handled) {
      return createNoResponseState(handled,
          "Timed out trying to find usable process id within file "
              + ProcessType.LOCATOR.getPidFileName() + " in " + getWorkingDirectory());
    }
  }

  private LocatorState createNoResponseState(final Exception cause, final String errorMessage) {
    debug(ExceptionUtils.getStackTrace(cause) + errorMessage);
    return new LocatorState(this, Status.NOT_RESPONDING, errorMessage);
  }

  private Properties getOverriddenDefaults() throws IOException {
    Properties overriddenDefaults = new Properties();

    overriddenDefaults.put(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX.concat(LOG_FILE),
        getLogFile().getCanonicalPath());

    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX)) {
        overriddenDefaults.put(key, System.getProperty(key));
      }
    }

    return overriddenDefaults;
  }

  private class LocatorControllerParameters implements ProcessControllerParameters {
    @Override
    public File getPidFile() {
      return getLocatorPidFile();
    }

    @Override
    public File getDirectory() {
      return new File(LocatorLauncher.this.getWorkingDirectory());
    }

    @Override
    public int getProcessId() {
      return getPid();
    }

    @Override
    public ProcessType getProcessType() {
      return ProcessType.LOCATOR;
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
      return new String[] {"Locator", "Server"};
    }

    @Override
    public Object[] getValues() {
      return new Object[] {Boolean.TRUE, Boolean.FALSE};
    }
  }

  /**
   * Following the Builder design pattern, the LocatorLauncher Builder is used to configure and
   * create a properly initialized instance of the LocatorLauncher class for running the Locator and
   * performing other Locator operations.
   */
  public static class Builder {

    @Immutable
    protected static final Command DEFAULT_COMMAND = Command.UNSPECIFIED;

    private Boolean debug;
    private Boolean deletePidFileOnStop;
    private Boolean force;
    private Boolean help;
    private Boolean redirectOutput;
    private Boolean loadSharedConfigFromDir;
    private Command command;

    private InetAddress bindAddress;

    private Integer pid;
    private Integer port;

    private final Properties distributedSystemProperties = new Properties();

    private String hostnameForClients;
    private String memberName;
    private String workingDirectory;

    /**
     * Default constructor used to create an instance of the Builder class for programmatical
     * access.
     */
    public Builder() {}

    /**
     * Constructor used to create and configure an instance of the Builder class with the specified
     * arguments, often passed from the command-line when launching an instance of this class from
     * the command-line using the Java launcher.
     *
     * @param args the array of arguments used to configure the Builder.
     */
    public Builder(final String... args) {
      parseArguments(args != null ? args : new String[0]);
    }

    /**
     * Gets an instance of the JOpt Simple OptionParser to parse the command-line arguments.
     *
     * @return an instance of the JOpt Simple OptionParser configured with the command-line options
     *         used by the Locator.
     */
    private OptionParser getParser() {
      final OptionParser parser = new OptionParser(true);

      parser.accepts("bind-address").withRequiredArg().ofType(String.class);
      parser.accepts("debug");
      parser.accepts("delete-pid-file-on-stop");
      parser.accepts("dir").withRequiredArg().ofType(String.class);
      parser.accepts("force");
      parser.accepts("help");
      parser.accepts("hostname-for-clients").withRequiredArg().ofType(String.class);
      parser.accepts("pid").withRequiredArg().ofType(Integer.class);
      parser.accepts("port").withRequiredArg().ofType(Integer.class);
      parser.accepts("redirect-output");
      parser.accepts("version");

      return parser;
    }

    /**
     * Parses an array of arguments to configure this Builder with the intent of constructing a
     * Locator launcher to invoke a Locator. This method is called to parse the arguments specified
     * by the user on the command-line.
     *
     * @param args the array of arguments used to configure this Builder and create an instance of
     *        LocatorLauncher.
     */
    protected void parseArguments(final String... args) {
      try {
        parseCommand(args);
        parseMemberName(args);

        final OptionSet options = getParser().parse(args);

        setDebug(options.has("debug"));
        setDeletePidFileOnStop(options.has("delete-pid-file-on-stop"));
        setForce(options.has("force"));
        setHelp(options.has("help"));
        setRedirectOutput(options.has("redirect-output"));

        if (!isHelping()) {
          if (options.has("bind-address")) {
            setBindAddress(ObjectUtils.toString(options.valueOf("bind-address")));
          }

          if (options.has("dir")) {
            setWorkingDirectory(ObjectUtils.toString(options.valueOf("dir")));
          }

          if (options.has("hostname-for-clients")) {
            setHostnameForClients(ObjectUtils.toString(options.valueOf("hostname-for-clients")));
          }

          if (options.has("pid")) {
            setPid((Integer) options.valueOf("pid"));
          }

          if (options.has("port")) {
            setPort((Integer) options.valueOf("port"));
          }

          if (options.has("version")) {
            setCommand(Command.VERSION);
          }
        }
      } catch (OptionException e) {
        throw new IllegalArgumentException(
            String.format("An error occurred while parsing command-line arguments for the %s: %s",
                "Locator", e.getMessage()),
            e);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    /**
     * Iterates the list of arguments in search of the target Locator launcher command.
     *
     * @param args an array of arguments from which to search for the Locator launcher command.
     * @see org.apache.geode.distributed.LocatorLauncher.Command#valueOfName(String)
     * @see #parseArguments(String...)
     */
    protected void parseCommand(final String... args) {
      // search the list of arguments for the command; technically, the command should be the first
      // argument in the
      // list, but does it really matter? stop after we find one valid command.
      if (args != null) {
        for (String arg : args) {
          final Command command = Command.valueOfName(arg);
          if (command != null) {
            setCommand(command);
            break;
          }
        }
      }
    }

    /**
     * Iterates the list of arguments in search of the Locator's GemFire member name. If the
     * argument does not start with '-' or is not the name of a Locator launcher command, then the
     * value is presumed to be the member name for the Locator in GemFire.
     *
     * @param args the array of arguments from which to search for the Locator's member name in
     *        GemFire.
     * @see org.apache.geode.distributed.LocatorLauncher.Command#isCommand(String)
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
     * Gets the Locator launcher command used during the invocation of the LocatorLauncher.
     *
     * @return the Locator launcher command used to invoke (run) the LocatorLauncher class.
     * @see #setCommand(org.apache.geode.distributed.LocatorLauncher.Command)
     * @see LocatorLauncher.Command
     */
    public Command getCommand() {
      return this.command != null ? this.command : DEFAULT_COMMAND;
    }

    /**
     * Sets the Locator launcher command used during the invocation of the LocatorLauncher
     *
     * @param command the targeted Locator launcher command used during the invocation (run) of
     *        LocatorLauncher.
     * @return this Builder instance.
     * @see #getCommand()
     * @see LocatorLauncher.Command
     */
    public Builder setCommand(final Command command) {
      this.command = command;
      return this;
    }

    /**
     * Determines whether the new instance of the LocatorLauncher will be set to debug mode.
     *
     * @return a boolean value indicating whether debug mode is enabled or disabled.
     * @see #setDebug(Boolean)
     */
    public Boolean getDebug() {
      return this.debug;
    }

    /**
     * Sets whether the new instance of the LocatorLauncher will be set to debug mode.
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
     * Determines whether the Geode Locator should delete the pid file when its service stops or
     * when the JVM exits.
     *
     * @return a boolean value indicating if the pid file should be deleted when this service stops
     *         or when the JVM exits.
     * @see #setDeletePidFileOnStop(Boolean)
     */
    public Boolean getDeletePidFileOnStop() {
      return this.deletePidFileOnStop;
    }

    /**
     * Sets whether the Geode Locator should delete the pid file when its service stops or when the
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
     * Gets the GemFire Distributed System (cluster) Properties configuration.
     *
     * @return a Properties object containing configuration settings for the GemFire Distributed
     *         System (cluster).
     * @see java.util.Properties
     */
    public Properties getDistributedSystemProperties() {
      return this.distributedSystemProperties;
    }

    /**
     * Gets the boolean value used by the Locator to determine if it should overwrite the PID file
     * if it already exists.
     *
     * @return the boolean value specifying whether or not to overwrite the PID file if it already
     *         exists.
     * @see #setForce(Boolean)
     */
    public Boolean getForce() {
      return this.force != null ? this.force : DEFAULT_FORCE;
    }

    /**
     * Sets the boolean value used by the Locator to determine if it should overwrite the PID file
     * if it already exists.
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
     * Determines whether the new instance of LocatorLauncher will be used to output help
     * information for either a specific command, or for using LocatorLauncher in general.
     *
     * @return a boolean value indicating whether help will be output during the invocation of
     *         LocatorLauncher.
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
    private boolean isHelping() {
      return Boolean.TRUE.equals(getHelp());
    }

    /**
     * Sets whether the new instance of LocatorLauncher will be used to output help information for
     * either a specific command, or for using LocatorLauncher in general.
     *
     * @param help a boolean indicating whether help information is to be displayed during
     *        invocation of LocatorLauncher.
     * @return this Builder instance.
     * @see #getHelp()
     */
    public Builder setHelp(final Boolean help) {
      this.help = help;
      return this;
    }

    boolean isBindAddressSpecified() {
      return (getBindAddress() != null);

    }

    /**
     * Gets the IP address to which the Locator has bound itself listening for client requests.
     *
     * @return an InetAddress with the IP address or hostname on which the Locator is bound and
     *         listening.
     * @see #setBindAddress(String)
     * @see java.net.InetAddress
     */
    public InetAddress getBindAddress() {
      return this.bindAddress;
    }

    /**
     * Sets the IP address as an java.net.InetAddress to which the Locator has bound itself
     * listening for client requests.
     *
     * @param bindAddress the InetAddress with the IP address or hostname on which the Locator is
     *        bound and listening.
     * @return this Builder instance.
     * @throws IllegalArgumentException wrapping the UnknownHostException if the IP address or
     *         hostname for the bind address is unknown.
     * @see #getBindAddress()
     * @see java.net.InetAddress
     */
    public Builder setBindAddress(final String bindAddress) {
      if (isBlank(bindAddress)) {
        this.bindAddress = null;
        return this;
      } else {
        try {
          InetAddress address = InetAddress.getByName(bindAddress);
          if (LocalHostUtil.isLocalHost(address)) {
            this.bindAddress = address;
            return this;
          } else {
            throw new IllegalArgumentException(
                bindAddress + " is not an address for this machine.");
          }
        } catch (UnknownHostException e) {
          throw new IllegalArgumentException(
              String.format("The hostname/IP address to which the %s will be bound is unknown.",
                  "Locator"),
              e);
        }
      }
    }

    /**
     * Gets the hostname used by clients to lookup the Locator.
     *
     * @return a String indicating the hostname Locator binding used in client lookups.
     * @see #setHostnameForClients(String)
     */
    public String getHostnameForClients() {
      return this.hostnameForClients;
    }

    /**
     * Sets the hostname used by clients to lookup the Locator.
     *
     * @param hostnameForClients a String indicating the hostname Locator binding used in client
     *        lookups.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the hostname was not specified (is blank or empty), such
     *         as when the --hostname-for-clients command-line option may have been specified
     *         without any argument.
     * @see #getHostnameForClients()
     */
    public Builder setHostnameForClients(final String hostnameForClients) {
      if (isBlank(hostnameForClients)) {
        throw new IllegalArgumentException(
            "The hostname used by clients to connect to the Locator must have an argument if the --hostname-for-clients command-line option is specified!");
      }
      this.hostnameForClients = hostnameForClients;
      return this;
    }

    /**
     * Gets the member name of this Locator in GemFire.
     *
     * @return a String indicating the member name of this Locator in GemFire.
     * @see #setMemberName(String)
     */
    public String getMemberName() {
      return this.memberName;
    }

    /**
     * Sets the member name of the Locator in GemFire.
     *
     * @param memberName a String indicating the member name of this Locator in GemFire.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the member name is invalid.
     * @see #getMemberName()
     */
    public Builder setMemberName(final String memberName) {
      if (isBlank(memberName)) {
        throw new IllegalArgumentException(
            String.format("The %s member name must be specified.",
                "Locator"));
      }
      this.memberName = memberName;
      return this;
    }

    /**
     * Gets the process ID (PID) of the running Locator indicated by the user as an argument to the
     * LocatorLauncher. This PID is used by the Locator launcher to determine the Locator's status,
     * or invoke shutdown on the Locator.
     *
     * @return a user specified Integer value indicating the process ID of the running Locator.
     * @see #setPid(Integer)
     */
    public Integer getPid() {
      return this.pid;
    }

    /**
     * Sets the process ID (PID) of the running Locator indicated by the user as an argument to the
     * LocatorLauncher. This PID will be used by the Locator launcher to determine the Locator's
     * status, or invoke shutdown on the Locator.
     *
     * @param pid a user specified Integer value indicating the process ID of the running Locator.
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

    boolean isPortSpecified() {
      return (this.port != null);
    }

    /**
     * Gets the port number used by the Locator to listen for client requests. If the port was not
     * specified, then the default Locator port (10334) is returned.
     *
     * @return the specified Locator port or the default port if unspecified.
     * @see #setPort(Integer)
     */
    public Integer getPort() {
      return port != null ? port : getDefaultLocatorPort();
    }

    /**
     * Sets the port number used by the Locator to listen for client requests. The port number must
     * be between 1 and 65535 inclusive.
     *
     * @param port an Integer value indicating the port used by the Locator to listen for client
     *        requests.
     * @return this Builder instance.
     * @throws IllegalArgumentException if the port number is not valid.
     * @see #getPort()
     */
    public Builder setPort(final Integer port) {
      // NOTE if the user were to specify a port number of 0, then java.net.ServerSocket will pick
      // an ephemeral port
      // to bind the socket, which we do not want.
      if (port != null && (port < 0 || port > 65535)) {
        throw new IllegalArgumentException(
            String.format(
                "The port on which the %s will listen must be between 1 and 65535 inclusive.",
                "Locator"));
      }
      this.port = port;
      return this;
    }

    /**
     * Determines whether the new instance of LocatorLauncher will redirect output to system logs
     * when starting a Locator.
     *
     * @return a boolean value indicating if output will be redirected to system logs when starting
     *         a Locator
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
     * Sets whether the new instance of LocatorLauncher will redirect output to system logs when
     * starting a Locator.
     *
     * @param redirectOutput a boolean value indicating if output will be redirected to system logs
     *        when starting a Locator.
     * @return this Builder instance.
     * @see #getRedirectOutput()
     */
    public Builder setRedirectOutput(final Boolean redirectOutput) {
      this.redirectOutput = redirectOutput;
      return this;
    }

    boolean isWorkingDirectorySpecified() {
      return isNotBlank(this.workingDirectory);
    }

    /**
     * Gets the working directory pathname in which the Locator will be ran. If the directory is
     * unspecified, then working directory defaults to the current directory.
     *
     * @return a String indicating the working directory pathname.
     * @see #setWorkingDirectory(String)
     */
    public String getWorkingDirectory() {
      return tryGetCanonicalPathElseGetAbsolutePath(
          new File(defaultIfBlank(this.workingDirectory, DEFAULT_WORKING_DIRECTORY)));
    }

    /**
     * Sets the working directory in which the Locator will be ran. This also the directory in which
     * all Locator files (such as log and license files) will be written. If the directory is
     * unspecified, then the working directory defaults to the current directory.
     *
     * @param workingDirectory a String indicating the pathname of the directory in which the
     *        Locator will be ran.
     * @return this Builder instance.
     * @throws IllegalArgumentException wrapping a FileNotFoundException if the working directory
     *         pathname cannot be found.
     * @see #getWorkingDirectory()
     * @see java.io.FileNotFoundException
     */
    public Builder setWorkingDirectory(final String workingDirectory) {
      if (!new File(defaultIfBlank(workingDirectory, DEFAULT_WORKING_DIRECTORY)).isDirectory()) {
        throw new IllegalArgumentException(
            String.format(AbstractLauncher.WORKING_DIRECTORY_NOT_FOUND_ERROR_MESSAGE,
                "Locator"),
            new FileNotFoundException(workingDirectory));
      }
      this.workingDirectory = workingDirectory;
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
      this.distributedSystemProperties.setProperty(propertyName, propertyValue);
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
    public Builder set(Properties properties) {
      this.distributedSystemProperties.putAll(properties);
      return this;
    }

    /**
     * Validates the configuration settings and properties of this Builder, ensuring that all
     * invariants have been met. Currently, the only invariant constraining the Builder is that the
     * user must specify the member name for the Locator in the GemFire distributed system as a
     * command-line argument, or by setting the memberName property programmatically using the
     * corresponding setter method. If the member name is not given, then the user must have
     * specified the pathname to the gemfire.properties file before validate is called. It is then
     * assumed, but not further validated, that the user has specified the Locator's member name in
     * the properties file.
     *
     * @throws IllegalStateException if the Builder is not properly configured.
     */
    protected void validate() {
      if (!isHelping()) {
        validateOnStart();
        validateOnStatus();
        validateOnStop();
        // no validation for 'version' required
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'start' command has been issued.
     *
     * @see org.apache.geode.distributed.LocatorLauncher.Command#START
     */
    protected void validateOnStart() {
      if (Command.START == getCommand()) {
        if (isBlank(getMemberName())
            && !isSet(System.getProperties(), GeodeGlossary.GEMFIRE_PREFIX + NAME)
            && !isSet(getDistributedSystemProperties(), NAME)
            && !isSet(loadGemFireProperties(DistributedSystem.getPropertyFileURL()), NAME)) {
          throw new IllegalStateException(
              String.format(
                  AbstractLauncher.MEMBER_NAME_ERROR_MESSAGE,
                  "Locator", "Locator"));
        }

        if (!CURRENT_DIRECTORY.equalsIgnoreCase(getWorkingDirectory())) {
          throw new IllegalStateException(
              String.format(
                  AbstractLauncher.WORKING_DIRECTORY_OPTION_NOT_VALID_ERROR_MESSAGE,
                  "Locator", "Locator"));
        }
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'status' command has been issued.
     *
     * @see org.apache.geode.distributed.LocatorLauncher.Command#STATUS
     */
    protected void validateOnStatus() {
      if (Command.STATUS == getCommand()) {
      }
    }

    /**
     * Validates the arguments passed to the Builder when the 'stop' command has been issued.
     *
     * @see org.apache.geode.distributed.LocatorLauncher.Command#STOP
     */
    protected void validateOnStop() {
      if (Command.STOP == getCommand()) {
      }
    }

    /**
     * Validates the Builder configuration settings and then constructs an instance of the
     * LocatorLauncher class to invoke operations on a GemFire Locator.
     *
     * @return a newly constructed instance of LocatorLauncher configured with this Builder.
     * @see #validate()
     * @see org.apache.geode.distributed.LocatorLauncher
     */
    public LocatorLauncher build() {
      validate();
      return new LocatorLauncher(this);
    }
  }

  /**
   * An enumerated type representing valid commands to the Locator launcher.
   */
  public enum Command {
    START("start", "bind-address", "hostname-for-clients", "port", "force", "debug", "help"),
    STATUS("status", "bind-address", "port", "member", "pid", "dir", "debug", "help"),
    STOP("stop", "member", "pid", "dir", "debug", "help"),
    VERSION("version"),
    UNSPECIFIED("unspecified");

    private final List<String> options;

    private final String name;

    Command(final String name, final String... options) {
      assert isNotBlank(name) : "The name of the locator launcher command must be specified!";
      this.name = name;
      this.options = (options != null ? Collections.unmodifiableList(Arrays.asList(options))
          : Collections.emptyList());
    }

    /**
     * Determines whether the specified name refers to a valid Locator launcher command, as defined
     * by this enumerated type.
     *
     * @param name a String value indicating the potential name of a Locator launcher command.
     * @return a boolean indicating whether the specified name for a Locator launcher command is
     *         valid.
     */
    public static boolean isCommand(final String name) {
      return (valueOfName(name) != null);
    }

    /**
     * Determines whether the given Locator launcher command has been properly specified. The
     * command is deemed unspecified if the reference is null or the Command is UNSPECIFIED.
     *
     * @param command the Locator launcher command.
     * @return a boolean value indicating whether the Locator launcher command is unspecified.
     * @see Command#UNSPECIFIED
     */
    public static boolean isUnspecified(final Command command) {
      return (command == null || command.isUnspecified());
    }

    /**
     * Looks up a Locator launcher command by name. The equality comparison on name is
     * case-insensitive.
     *
     * @param name a String value indicating the name of the Locator launcher command.
     * @return an enumerated type representing the command name or null if the no such command with
     *         the specified name exists.
     */
    public static Command valueOfName(final String name) {
      for (Command command : values()) {
        if (command.getName().equalsIgnoreCase(name)) {
          return command;
        }
      }

      return null;
    }

    /**
     * Gets the name of the Locator launcher command.
     *
     * @return a String value indicating the name of the Locator launcher command.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Gets a set of valid options that can be used with the Locator launcher command when used from
     * the command-line.
     *
     * @return a Set of Strings indicating the names of the options available to the Locator
     *         launcher command.
     */
    public List<String> getOptions() {
      return this.options;
    }

    /**
     * Determines whether this Locator launcher command has the specified command-line option.
     *
     * @param option a String indicating the name of the command-line option to this command.
     * @return a boolean value indicating whether this command has the specified named command-line
     *         option.
     */
    public boolean hasOption(final String option) {
      return getOptions().contains(lowerCase(option));
    }

    /**
     * Convenience method for determining whether this is the UNSPECIFIED Locator launcher command.
     *
     * @return a boolean indicating if this command is UNSPECIFIED.
     * @see #UNSPECIFIED
     */
    public boolean isUnspecified() {
      return UNSPECIFIED == this;
    }

    /**
     * Gets the String representation of this Locator launcher command.
     *
     * @return a String value representing this Locator launcher command.
     */
    @Override
    public String toString() {
      return getName();
    }
  }

  /**
   * The LocatorState is an immutable type representing the state of the specified Locator at any
   * given moment in time. The state of the Locator is assessed at the exact moment an instance of
   * this class is constructed.
   *
   * @see org.apache.geode.distributed.AbstractLauncher.ServiceState
   */
  public static class LocatorState extends ServiceState<String> {

    /**
     * Unmarshals a LocatorState instance from the JSON String.
     *
     * @return a LocatorState value unmarshalled from the JSON String.
     */
    public static LocatorState fromJson(final String json) {
      try {
        final JsonNode jsonObject = new ObjectMapper().readTree(json);

        final Status status = Status.valueOfDescription(jsonObject.get(JSON_STATUS).asText());

        final List<String> jvmArguments = JsonUtil.toStringList(jsonObject.get(JSON_JVMARGUMENTS));

        return new LocatorState(status, jsonObject.get(JSON_STATUSMESSAGE).asText(),
            jsonObject.get(JSON_TIMESTAMP).asLong(), jsonObject.get(JSON_LOCATION).asText(),
            jsonObject.get(JSON_PID).asInt(), jsonObject.get(JSON_UPTIME).asLong(),
            jsonObject.get(JSON_WORKINGDIRECTORY).asText(), jvmArguments,
            jsonObject.get(JSON_CLASSPATH).asText(), jsonObject.get(JSON_GEMFIREVERSION).asText(),
            jsonObject.get(JSON_JAVAVERSION).asText(), jsonObject.get(JSON_LOGFILE).asText(),
            jsonObject.get(JSON_HOST).asText(), jsonObject.get(JSON_PORT).asText(),
            jsonObject.get(JSON_MEMBERNAME).asText());
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Unable to create LocatorStatus from JSON: ".concat(json), e);
      }
    }

    public static LocatorState fromDirectory(final String workingDirectory,
        final String memberName) {
      LocatorState locatorState =
          new LocatorLauncher.Builder().setWorkingDirectory(workingDirectory).build().status();

      if (ObjectUtils.equals(locatorState.getMemberName(), memberName)) {
        return locatorState;
      }

      return new LocatorState(new LocatorLauncher.Builder().build(), Status.NOT_RESPONDING);
    }

    public LocatorState(final LocatorLauncher launcher, final Status status) {
      // if status is NOT_RESPONDING then this is executing inside the JVM asking for the status;
      // pid etc will be set according to the caller's JVM instead
      this(status, launcher.statusMessage, System.currentTimeMillis(), launcher.getId(),
          identifyPid(), ManagementFactory.getRuntimeMXBean().getUptime(),
          launcher.getWorkingDirectory(), ManagementFactory.getRuntimeMXBean().getInputArguments(),
          System.getProperty("java.class.path"), GemFireVersion.getGemFireVersion(),
          System.getProperty("java.version"), getLogFileCanonicalPath(launcher),
          launcher.getBindAddressAsString(), launcher.getPortAsString(), launcher.getMemberName());
    }

    public LocatorState(final LocatorLauncher launcher, final Status status,
        final String errorMessage) {
      this(status, // status
          errorMessage, // statusMessage
          System.currentTimeMillis(), // timestamp
          getLocatorLocation(launcher), // locatorLocation
          null, // pid
          0L, // uptime
          launcher.getWorkingDirectory(), // workingDirectory
          ManagementFactory.getRuntimeMXBean().getInputArguments(), // jvmArguments
          null, // classpath
          GemFireVersion.getGemFireVersion(), // gemfireVersion
          System.getProperty("java.version"), // javaVersion
          null, // logFile
          launcher.getBindAddressAsString(), // host
          launcher.getPortAsString(), // port
          null);// memberName
    }

    /*
     * Guards against throwing NPEs due to incorrect or missing host information while constructing
     * error states
     */
    private static String getLocatorLocation(LocatorLauncher launcher) {
      if (launcher.getPort() == null) {
        return launcher.getId();
      }
      if (launcher.getBindAddress() == null) {
        return HostUtils.getLocatorId(HostUtils.getLocalHost(), launcher.getPort());
      }
      return HostUtils.getLocatorId(launcher.getBindAddressAsString(), launcher.getPort());
    }

    private static String getBindAddressAsString(LocatorLauncher launcher) {
      if (InternalLocator.hasLocator()) {
        final InternalLocator locator = InternalLocator.getLocator();
        final InetAddress bindAddress = locator.getBindAddress();
        if (bindAddress != null) {
          if (isNotBlank(bindAddress.getHostAddress())) {
            return bindAddress.getHostAddress();
          }
        }
      }
      return launcher.getBindAddressAsString();
    }

    private static String getLogFileCanonicalPath(LocatorLauncher launcher) {
      if (InternalLocator.hasLocator()) {
        final InternalLocator locator = InternalLocator.getLocator();
        final File logFile = locator.getLogFile();

        if (logFile != null && logFile.isFile()) {
          final String logFileCanonicalPath = tryGetCanonicalPathElseGetAbsolutePath(logFile);
          if (isNotBlank(logFileCanonicalPath)) { // this is probably not need but a
            // safe check none-the-less.
            return logFileCanonicalPath;
          }
        }
      }
      return launcher.getLogFileCanonicalPath();
    }

    private static String getPortAsString(LocatorLauncher launcher) {
      if (InternalLocator.hasLocator()) {
        final InternalLocator locator = InternalLocator.getLocator();
        final String portAsString = String.valueOf(locator.getPort());
        if (isNotBlank(portAsString)) {
          return portAsString;
        }
      }
      return launcher.getPortAsString();
    }

    protected LocatorState(final Status status, final String statusMessage, final long timestamp,
        final String locatorLocation, final Integer pid, final Long uptime,
        final String workingDirectory, final List<String> jvmArguments, final String classpath,
        final String gemfireVersion, final String javaVersion, final String logFile,
        final String host, final String port, final String memberName) {
      super(status, statusMessage, timestamp, locatorLocation, pid, uptime, workingDirectory,
          jvmArguments, classpath, gemfireVersion, javaVersion, logFile, host, port, memberName);
    }

    private LocatorState(final LocatorLauncher launcher, final Status status,
        final LocatorStatusResponse response) {
      this(status, launcher.statusMessage, System.currentTimeMillis(), launcher.getId(),
          response.getPid(), response.getUptime(), response.getWorkingDirectory(),
          response.getJvmArgs(), response.getClasspath(), response.getGemFireVersion(),
          response.getJavaVersion(), response.getLogFile(), response.getHost(),
          String.valueOf(response.getPort()), response.getName());
    }

    @Override
    protected String getServiceName() {
      return LOCATOR_SERVICE_NAME;
    }
  }

}
