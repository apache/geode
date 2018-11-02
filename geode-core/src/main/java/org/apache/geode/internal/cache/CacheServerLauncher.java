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

package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_BIND_ADDRESS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.URL;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.geode.LogWriter;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.ServerLauncherParameters;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.process.StartupStatus;
import org.apache.geode.internal.process.StartupStatusListener;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.internal.util.JavaCommandBuilder;

/**
 * Launcher program to start a cache server.
 *
 * @since GemFire 2.0.2
 * @deprecated Since Geode 1.7.0. Use {@link ServerLauncher} instead.
 */
@Deprecated
public class CacheServerLauncher {

  /** Is this VM a dedicated Cache Server? This value is used mainly by the admin API. */
  public static boolean isDedicatedCacheServer =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "isDedicatedServer");

  public static boolean ASSIGN_BUCKETS = Boolean.getBoolean(
      DistributionConfig.GEMFIRE_PREFIX + "CacheServerLauncher.assignBucketsToPartitions");

  // default is to exit if property not defined
  public static boolean DONT_EXIT_AFTER_LAUNCH = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "CacheServerLauncher.dontExitAfterLaunch");

  /** Should the launch command be printed? */
  public static final boolean PRINT_LAUNCH_COMMAND =
      Boolean.getBoolean(CacheServerLauncher.class.getSimpleName() + ".PRINT_LAUNCH_COMMAND");

  private static final long STATUS_WAIT_TIME = Long.getLong(
      DistributionConfig.GEMFIRE_PREFIX + "CacheServerLauncher.STATUS_WAIT_TIME_MS", 15000);

  /** How long to wait for a cache server to stop */
  private static final long SHUTDOWN_WAIT_TIME = Long.getLong(
      DistributionConfig.GEMFIRE_PREFIX + "CacheServerLauncher.SHUTDOWN_WAIT_TIME_MS", 20000);

  protected final String baseName;
  protected final String defaultLogFileName;
  protected final String startLogFileName;
  protected final String statusName;
  protected Status status = null;
  protected File workingDir = null;
  protected PrintStream oldOut = System.out;
  protected PrintStream oldErr = System.err;
  protected LogWriter logger = null;
  protected String maxHeapSize;
  protected String initialHeapSize;
  protected String offHeapSize;


  public static final int SHUTDOWN = 0;
  public static final int STARTING = 1;
  public static final int RUNNING = 2;
  public static final int SHUTDOWN_PENDING = 3;

  private static final int FORCE_STATUS_FILE_READ_ITERATION_COUNT = 10;

  public CacheServerLauncher(final String baseName) {
    assert baseName != null : "The base name used for the cache server launcher files cannot be null!";
    this.baseName = baseName;
    final String baseNameLowerCase = baseName.toLowerCase().replace(" ", "");
    this.startLogFileName = "start_" + baseNameLowerCase + ".log";
    this.defaultLogFileName = baseNameLowerCase + ".log";
    this.statusName = "." + baseNameLowerCase + ".ser";
  }

  protected static Status createStatus(final String baseName, final int state, final int pid) {
    return createStatus(baseName, state, pid, null, null);
  }

  protected static Status createStatus(final String baseName, final int state, final int pid,
      final String msg, final Throwable t) {
    final Status status = new Status(baseName);
    status.state = state;
    status.pid = pid;
    status.msg = msg;
    status.exception = t;
    return status;
  }

  /**
   * Prints usage information about this program.
   */
  protected void usage() {
    PrintStream out = System.out;
    out.println(
        "cacheserver start [-J<vmarg>]* [<attName>=<attValue>]* [-dir=<workingdir>] [-classpath=<classpath>] [-disable-default-server] [-rebalance] [-lock-memory] [-server-port=<server-port>] [-server-bind-address=<server-bind-address>] [-critical-heap-percentage=<critical-heap-percentage>] [-eviction-heap-percentage=<eviction-heap-percentage>] [-critical-off-heap-percentage=<critical-off-heap-percentage>] [-eviction-off-heap-percentage=<eviction-off-heap-percentage>]\n");
    out.println("\t"
        + "Starts a GemFire CacheServer VM");
    out.println("\t"
        + "<vmarg> a VM-option passed to the spawned CacheServer VM, example -J-Xmx1024M for a 1 Gb heap");
    out.println("\t"
        + "<workingdir> Directory in which cacheserver runs, default is the current directory");
    out.println("\t"
        + "<classpath> Location of user classes required by the cache server.  This path is appended to the current classpath.");
    out.println(
        "\t<attName> Distributed system attribute such as mcast-port or cache-xml-file.");
    out.println("\t-rebalance  Indicates that the Cache should immediately be rebalanced");
    out.println(
        "\t-disable-default-server  Do not add a default <cache-server>");
    out.println("\t"
        + "<server-port>  Port the server is to listen on for client connections. This overrides the port set in the <cache-server> element of the cache-xml-file");
    out.println(
        "\t<server-bind-address>  Address the server is to listen on for client connections. This overrides the bind-address set in the <cache-server> element of the cache-xml-file");
    out.println(
        "\t<critical-heap-percentage>  Sets the critical heap threshold limit of the Resource Manager. This best works with parallel young generation collector (UseParNewGC) and concurrent low pause collector (UseConcMarkSweepGC) with appropriate CMSInitiatingOccupancyFraction like 50%. This overrides the critical-heap-percentage set in the <resource-manager> element of the cache-xml-file");
    out.println(
        "\t<eviction-heap-percentage>  Sets the eviction heap threshold limit of the Resource Manager above which the eviction should begin on Regions configured for eviction by heap LRU. This overrides the eviction-heap-percentage set in the resource-manager> element of the cache-xml-file");
    out.println("\t"
        + "<critical-Off-heap-percentage>  Sets the critical off-heap threshold limit of the Resource Manager. This overrides the critical-off-heap-percentage set in the <resource-manager> element of the cache-xml-file");
    out.println("\t"
        + "<eviction-off-heap-percentage>  Sets the eviction heap threshold limit of the Resource Manager above which the eviction should begin on Regions configured for eviction by off-heap LRU. This overrides the eviction-off-heap-percentage set in the <resource-manager> element of the cache-xml-file");
    out.println("\t"
        + "-lock-memory Locks heap and off-heap memory pages into RAM, thereby preventing the operating system from swapping them out to disk.");

    out.println();
    out.println("cacheserver stop [-dir=<workingdir>]");
    out.println("\t"
        + "Stops a GemFire CacheServer VM");
    out.println("\t"
        + "<workingdir> Directory in which cacheserver runs, default is the current directory");
    out.println();
    out.println("cacheserver status [-dir=<workingdir>]");
    out.println("\tReports the status and process id of a GemFire CacheServer VM");
    out.println("\t"
        + "<workingdir> Directory in which cacheserver runs, default is the current directory");
  }

  /**
   * Prints the status of the cache server running the configured working directory.
   */
  protected void status(final String[] args) throws Exception {
    workingDir = (File) getStopOptions(args).get(DIR);
    System.out.println(getStatus());
    ExitCode.NORMAL.doSystemExit();
  }

  /**
   * Returns the <code>Status</code> of the cache server in the <code>workingDir</code>.
   */
  protected Status getStatus() throws Exception {
    Status status;

    if (new File(workingDir, statusName).exists()) {
      status = spinReadStatus(); // See bug 32456
    } else {
      // no pid since the cache server is not running
      status = createStatus(this.baseName, SHUTDOWN, 0);
    }

    return status;
  }

  /**
   * Main method that parses the command line and performs an will start, stop, or get the status of
   * a cache server. This main method is also the main method of the launched cacher server VM
   * ("server" mode).
   */
  public static void main(final String[] args) {
    final CacheServerLauncher launcher = new CacheServerLauncher("CacheServer");
    boolean inServer = false;

    try {
      if (args.length > 0) {
        if (args[0].equalsIgnoreCase("start")) {
          launcher.start(args);
        } else if (args[0].equalsIgnoreCase("server")) {
          inServer = true;
          launcher.server(args);
        } else if (args[0].equalsIgnoreCase("stop")) {
          launcher.stop(args);
        } else if (args[0].equalsIgnoreCase("status")) {
          launcher.status(args);
        } else {
          launcher.usage();
          ExitCode.FATAL.doSystemExit();
        }
      } else {
        launcher.usage();
        ExitCode.FATAL.doSystemExit();
      }

      throw new Exception("internal error.. should not reach here.");
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      t.printStackTrace();
      if (inServer) {
        launcher.setServerError(
            "Error starting server process. ",
            t);
      }
      launcher.restoreStdOut();
      if (launcher.logger != null) {
        launcher.logger.severe("Cache server error", t);

      } else {
        System.out.println(
            String.format("Error: %s", t.getMessage()));
      }
      ExitCode.FATAL.doSystemExit();
    }
  }

  protected void restoreStdOut() {
    System.setErr(oldErr);
    System.setOut(oldOut);
  }

  protected static final String DIR = "dir";
  protected static final String VMARGS = "vmargs";
  protected static final String PROPERTIES = "properties";
  protected static final String CLASSPATH = "classpath";
  protected static final String REBALANCE = "rebalance";
  protected static final String SERVER_PORT = "server-port";
  protected static final String SERVER_BIND_ADDRESS_NAME = SERVER_BIND_ADDRESS;
  protected static final String DISABLE_DEFAULT_SERVER = "disable-default-server";
  public static final String CRITICAL_HEAP_PERCENTAGE = "critical-heap-percentage";
  public static final String EVICTION_HEAP_PERCENTAGE = "eviction-heap-percentage";
  public static final String CRITICAL_OFF_HEAP_PERCENTAGE = "critical-off-heap-percentage";
  public static final String EVICTION_OFF_HEAP_PERCENTAGE = "eviction-off-heap-percentage";
  protected static final String LOCK_MEMORY = ConfigurationProperties.LOCK_MEMORY;

  protected File processDirOption(final Map<String, Object> options, final String dirValue)
      throws FileNotFoundException {
    final File inputWorkingDirectory = new File(dirValue);

    if (!inputWorkingDirectory.exists()) {
      throw new FileNotFoundException(
          String.format("The input working directory does not exist: %s",
              dirValue));
    }

    options.put(DIR, inputWorkingDirectory);

    return inputWorkingDirectory;
  }

  /**
   * Populates a map that maps the name of the start options such as {@link #DIR} to its value on
   * the command line. If no value is specified on the command line, a default one is provided.
   */
  protected Map<String, Object> getStartOptions(String[] args) throws Exception {
    final Map<String, Object> options = new HashMap<String, Object>();
    options.put(DIR, new File(System.getProperty("user.dir")));

    final List<String> vmArgs = new ArrayList<String>();
    options.put(VMARGS, vmArgs);

    final Properties props = new Properties();
    options.put(PROPERTIES, props);

    for (final String arg : args) {
      if (arg.equals("start")) {
        // expected
      } else if (arg.startsWith("-classpath=")) {
        options.put(CLASSPATH, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-dir=")) {
        processDirOption(options, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-disable-default-server")) {
        options.put(DISABLE_DEFAULT_SERVER, arg);
      } else if (arg.startsWith("-lock-memory")) {
        if (System.getProperty("os.name").indexOf("Windows") >= 0) {
          throw new IllegalArgumentException("Unable to lock memory on this operating system");
        }
        props.put(LOCK_MEMORY, "true");
      } else if (arg.startsWith("-rebalance")) {
        options.put(REBALANCE, Boolean.TRUE);
      } else if (arg.startsWith("-server-port")) {
        options.put(SERVER_PORT, arg);
      } else if (arg.startsWith("-" + CRITICAL_HEAP_PERCENTAGE)) {
        options.put(CRITICAL_HEAP_PERCENTAGE, arg);
      } else if (arg.startsWith("-" + EVICTION_HEAP_PERCENTAGE)) {
        options.put(EVICTION_HEAP_PERCENTAGE, arg);
      } else if (arg.startsWith("-" + CRITICAL_OFF_HEAP_PERCENTAGE)) {
        options.put(CRITICAL_OFF_HEAP_PERCENTAGE, arg);
      } else if (arg.startsWith("-" + EVICTION_OFF_HEAP_PERCENTAGE)) {
        options.put(EVICTION_OFF_HEAP_PERCENTAGE, arg);
      } else if (arg.startsWith("-server-bind-address")) {
        options.put(SERVER_BIND_ADDRESS_NAME, arg);
      } else if (arg.startsWith("-J")) {
        String vmArg = arg.substring(2);
        if (vmArg.startsWith("-Xmx")) {
          this.maxHeapSize = vmArg.substring(4);
        } else if (vmArg.startsWith("-Xms")) {
          this.initialHeapSize = vmArg.substring(4);
        }
        vmArgs.add(vmArg);
      }
      // moved this default block down so that "-J" like options can have '=' in them.
      // an 'indexOf' the assignment operator with greater than 0 ensures a non-empty String key
      // value
      else if (arg.indexOf("=") > 0) {
        final int assignmentIndex = arg.indexOf("=");
        final String key = arg.substring(0, assignmentIndex);
        final String value = arg.substring(assignmentIndex + 1);

        if (key.startsWith("-")) {
          processStartOption(key.substring(1), value, options, vmArgs, props);
        } else {
          processStartArg(key, value, options, vmArgs, props);
        }
      } else {
        throw new IllegalArgumentException(
            String.format("Unknown argument: %s", arg));
      }
    }

    // -J-Djava.awt.headless=true has been added for Mac platform where it
    // causes an icon to appear for launched procs
    // TODO: check which library/GemFire code causes awt to be touched
    vmArgs.add("-Djava.awt.headless=true");

    // configure commons-logging to use JDK logging
    vmArgs.add("-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.Jdk14Logger");

    options.put(VMARGS, vmArgs);
    return options;
  }

  /**
   * Process a command-line options of the form "key=value".
   */
  protected void processStartArg(final String key, final String value,
      final Map<String, Object> options, final List<String> vmArgs, final Properties props)
      throws Exception {
    props.setProperty(key, value);
  }

  /**
   * Process a command-line option of the form "-key=value".
   */
  protected void processStartOption(final String key, final String value,
      final Map<String, Object> options, final List<String> vmArgs, final Properties props)
      throws Exception {
    processUnknownStartOption(key, value, options, vmArgs, props);
  }

  /**
   * Process a command-line option of the form "-key=value" unknown to the base class.
   */
  protected void processUnknownStartOption(final String key, final String value,
      final Map<String, Object> options, final List<String> vmArgs, final Properties props) {
    throw new IllegalArgumentException(
        String.format("Unknown argument: %s", key));
  }

  /**
   * Extracts configuration information used when launching the cache server VM.
   */
  protected Map<String, Object> getServerOptions(final String[] args) throws Exception {
    final Map<String, Object> options = new HashMap<String, Object>();
    options.put(DIR, new File("."));
    workingDir = (File) options.get(DIR);

    final Properties props = new Properties();
    options.put(PROPERTIES, props);

    for (final String arg : args) {
      if (arg.equals("server")) {
        // expected
      } else if (arg.startsWith("-dir=")) {
        this.workingDir = processDirOption(options, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-rebalance")) {
        options.put(REBALANCE, Boolean.TRUE);
      } else if (arg.startsWith("-disable-default-server")) {
        options.put(DISABLE_DEFAULT_SERVER, Boolean.TRUE);
      } else if (arg.startsWith("-lock-memory")) {
        props.put(LOCK_MEMORY, "true");
      } else if (arg.startsWith("-server-port")) {
        options.put(SERVER_PORT, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-server-bind-address")) {
        options.put(SERVER_BIND_ADDRESS_NAME, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-" + CRITICAL_HEAP_PERCENTAGE)) {
        options.put(CRITICAL_HEAP_PERCENTAGE, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-" + EVICTION_HEAP_PERCENTAGE)) {
        options.put(EVICTION_HEAP_PERCENTAGE, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-" + CRITICAL_OFF_HEAP_PERCENTAGE)) {
        options.put(CRITICAL_OFF_HEAP_PERCENTAGE, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.startsWith("-" + EVICTION_OFF_HEAP_PERCENTAGE)) {
        options.put(EVICTION_OFF_HEAP_PERCENTAGE, arg.substring(arg.indexOf("=") + 1));
      } else if (arg.indexOf("=") > 1) {
        final int assignmentIndex = arg.indexOf("=");
        final String key = arg.substring(0, assignmentIndex);
        final String value = arg.substring(assignmentIndex + 1);

        if (key.startsWith("-")) {
          options.put(key.substring(1), value);
        } else {
          props.setProperty(key, value);
        }
      } else {
        throw new IllegalArgumentException(
            String.format("Unknown argument: %s", arg));
      }
    }

    return options;
  }

  /**
   * Extracts configuration information for stopping a cache server based on the contents of the
   * command-line. This method can also be used with getting the status of a cache server.
   */
  protected Map<String, Object> getStopOptions(final String[] args) throws Exception {
    final Map<String, Object> options = new HashMap<String, Object>();
    options.put(DIR, new File("."));

    for (final String arg : args) {
      if (arg.equals("stop") || arg.equals("status")) {
        // expected
      } else if (arg.startsWith("-dir=")) {
        processDirOption(options, arg.substring(arg.indexOf("=") + 1));
      } else {
        throw new IllegalArgumentException(
            String.format("Unknown argument: %s", arg));
      }
    }

    return options;
  }

  /**
   * Configures and spawns a VM that hosts a cache server. Any output generated while starting the
   * VM will be placed in a special {@linkplain #startLogFileName log file}.
   *
   * See #getStartOptions
   *
   * @see OSProcess#bgexec(String[], File, File, boolean, Map)
   */
  public void start(final String[] args) throws Exception {
    final Map<String, Object> options = getStartOptions(args);

    workingDir = (File) options.get(DIR);

    // Complain if a cache server is already running in the specified working directory.
    // See bug 32574.
    verifyAndClearStatus();

    // start the GemFire Cache Server proces...
    runCommandLine(options, buildCommandLine(options));

    // wait for status.state == RUNNING
    waitForRunning();

    if (DONT_EXIT_AFTER_LAUNCH) {
      return;
    }

    ExitCode.NORMAL.doSystemExit();
  }

  private void verifyAndClearStatus() throws Exception {
    final Status status = getStatus();

    if (status != null && status.state != SHUTDOWN) {
      throw new IllegalStateException(
          String.format("A %s is already running in directory %s %s",
              this.baseName, workingDir, status));
    }

    deleteStatus();
  }

  private String[] buildCommandLine(final Map<String, Object> options) {
    final List<String> commandLine = JavaCommandBuilder.buildCommand(this.getClass().getName(),
        (String) options.get(CLASSPATH), null, (List<String>) options.get(VMARGS));

    commandLine.add("server");
    addToServerCommand(commandLine, options);

    return commandLine.toArray(new String[commandLine.size()]);
  }

  private void printCommandLine(final String[] commandLine) {
    if (PRINT_LAUNCH_COMMAND) {
      System.out.println("Starting " + this.baseName + " with command:");
      for (final String command : commandLine) {
        System.out.print(command);
        System.out.print(" ");
      }
      System.out.println();
    }
  }

  private int runCommandLine(final Map<String, Object> options, final String[] commandLine)
      throws Exception {
    final File startLogFile = new File(workingDir, startLogFileName).getAbsoluteFile(); // see bug
                                                                                        // 32548

    if (startLogFile.exists() && !startLogFile.delete()) {
      throw new IOException(
          "Unable to delete start log file (" + startLogFile.getAbsolutePath() + ")!");
    }

    Map<String, String> env = new HashMap<String, String>();
    // read the passwords from command line
    SocketCreator.readSSLProperties(env);

    printCommandLine(commandLine);

    final int pid = OSProcess.bgexec(commandLine, workingDir, startLogFile, false, env);

    printStartMessage(options, pid);

    return pid;
  }

  protected void printStartMessage(final Map<String, Object> options, final int pid)
      throws Exception {
    System.out.println(String.format("Starting %s with pid: %s",
        this.baseName, pid));
  }

  /**
   * Sets the status of the cache server to be {@link #RUNNING}.
   */
  public void running() {
    try {
      writeStatus(createStatus(this.baseName, RUNNING, OSProcess.getId()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * The method that does the work of being a cache server. It is invoked in the VM spawned by the
   * {@link #start} method. Basically, it creates a GemFire {@link Cache} based on configuration
   * passed in from the command line. (It will also take <code>gemfire.properties</code>, etc. into
   * account, just like an application cache.)
   *
   * <P>
   *
   * After creating the cache and setting the server's status to {@link #RUNNING}, it periodically
   * monitors the status, waiting for it to change to {@link #SHUTDOWN_PENDING} (see {@link #stop}).
   * When the status does change, it closes the <code>Cache</code> and sets the status to be
   * {@link #SHUTDOWN}.
   *
   * @param args Configuration options passed in from the command line
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
  public void server(final String[] args) throws Exception {
    Integer serverPort = null;
    isDedicatedCacheServer = true;
    SystemFailure.setExitOK(true);
    workingDir = new File(System.getProperty("user.dir"));
    final Map<String, Object> options = getServerOptions(args);
    final String serverPortString = (String) options.get(SERVER_PORT);
    if (serverPortString != null)
      serverPort = Integer.parseInt(serverPortString);

    ServerLauncherParameters.INSTANCE
        .withPort(serverPort)
        .withBindAddress((String) options.get(SERVER_BIND_ADDRESS_NAME))
        .withDisableDefaultServer((Boolean) options.get(DISABLE_DEFAULT_SERVER));

    // Say that we're starting...
    Status originalStatus = createStatus(this.baseName, STARTING, OSProcess.getId());
    status = originalStatus;
    writeStatus(status);

    // Connect to the distributed system. The properties will
    // properly configure logging, the declarative caching file, etc.
    final Properties props = (Properties) options.get(PROPERTIES);

    if (props.getProperty(LOG_FILE) == null && CacheServerLauncher.isLoggingToStdOut()) {
      // Check First if the gemfire.properties set the log-file. If they do, we shouldn't override
      // that default
      final Properties gemfireProperties = new Properties();

      DistributionConfigImpl.loadGemFireProperties(gemfireProperties);

      if (gemfireProperties.get(LOG_FILE) == null) {
        // Do not allow the cache server to log to stdout, override the logger with
        // #defaultLogFileName
        props.setProperty(LOG_FILE, defaultLogFileName);
      }
    }

    InternalDistributedSystem system = this.connect(props);

    installLogListener();

    logger = system.getLogWriter();
    // redirect output to the log file
    OSProcess.redirectOutput(system.getConfig().getLogFile());

    InternalCache cache = createCache(system, options);
    cache.setIsServer(true);
    startAdditionalServices(cache, options);

    this.running();

    clearLogListener();

    if (ASSIGN_BUCKETS) {
      for (PartitionedRegion region : cache.getPartitionedRegions()) {
        PartitionRegionHelper.assignBucketsToPartitions(region);
      }
    }

    if (Boolean.TRUE.equals(options.get(REBALANCE))) {
      cache.getResourceManager().createRebalanceFactory().start();
    }

    File statusFile = new File(workingDir, statusName);
    long lastModified = 0, oldModified = statusFile.lastModified();
    // Every FORCE_STATUS_FILE_READ_ITERATION_COUNT iterations, read the status file despite the
    // modification time
    // to catch situations where the file is modified quicker than the file timestamp's resolution.
    short count = 0;
    boolean loggedWarning = false;
    while (true) {
      lastModified = statusFile.lastModified();
      if (lastModified > oldModified || count++ == FORCE_STATUS_FILE_READ_ITERATION_COUNT) {
        count = 0;
        Thread.sleep(500); // allow for it to be finished writing.
        // Sometimes the status file is partially written causing readObject to
        // fail, sleep and retry.
        try {
          status = readStatus();
        } catch (IOException ioeSecondChance) {
          Thread.sleep(1000);
          try {
            status = readStatus();
          } catch (IOException ioeThirdChance) {
            Thread.sleep(5000);
            try {
              status = readStatus();
            } catch (FileNotFoundException fnfe) {
              // See bug 44627.
              // The cache server used to just shutdown at this point. Instead,
              // recreate the status file if possible and continue.
              status = createStatus(this.baseName, RUNNING, originalStatus.pid);
              try {
                writeStatus(status);
              } catch (FileNotFoundException e) {
                if (!loggedWarning) {
                  logger.warning(String.format(
                      "The cacheserver status file could not be recreated due to the following exception: %s",
                      e.toString()));
                  loggedWarning = true;
                }
              }
            }
          }
        }
        oldModified = lastModified;
        if (status.state == SHUTDOWN_PENDING) {
          stopAdditionalServices();
          this.disconnect(cache);
          status.state = SHUTDOWN;
          writeStatus(status);
        } else {
          Thread.sleep(250);
        }

      } else {
        Thread.sleep(1000);
      }
      if (!system.isConnected()) {
        // System.out.println("System is disconnected. isReconnecting = " +
        // system.isReconnecting());
        boolean reconnected = false;
        if (system.isReconnecting()) {
          reconnected = system.waitUntilReconnected(-1, TimeUnit.SECONDS);
          if (reconnected) {
            system = (InternalDistributedSystem) system.getReconnectedSystem();
            cache = system.getCache();
          }
        }
        if (!reconnected) {
          // shutdown-all disconnected the DS
          ExitCode.NORMAL.doSystemExit();
        }
      }
    }
  }

  private void installLogListener() {
    MainLogReporter reporter = new MainLogReporter(this.status);
    StartupStatus.setListener(reporter);
    reporter.setDaemon(true);
    reporter.start();
  }

  private void clearLogListener() {
    MainLogReporter mainLogListener = (MainLogReporter) StartupStatus.getStartupListener();
    if (mainLogListener != null) {
      mainLogListener.shutdown();
      StartupStatus.clearListener();
    }
  }

  protected InternalDistributedSystem connect(Properties props) {
    return (InternalDistributedSystem) DistributedSystem.connect(props);
  }

  protected static float getCriticalHeapPercent(Map<String, Object> options) {
    if (options != null) {
      String criticalHeapThreshold = (String) options.get(CRITICAL_HEAP_PERCENTAGE);
      if (criticalHeapThreshold != null) {
        return Float
            .parseFloat(criticalHeapThreshold.substring(criticalHeapThreshold.indexOf("=") + 1));
      }
    }
    return -1.0f;
  }

  protected static float getEvictionHeapPercent(Map<String, Object> options) {
    if (options != null) {
      String evictionHeapThreshold = (String) options.get(EVICTION_HEAP_PERCENTAGE);
      if (evictionHeapThreshold != null) {
        return Float
            .parseFloat(evictionHeapThreshold.substring(evictionHeapThreshold.indexOf("=") + 1));
      }
    }
    return -1.0f;
  }

  protected static float getCriticalOffHeapPercent(Map<String, Object> options) {
    if (options != null) {
      String criticalOffHeapThreshold = (String) options.get(CRITICAL_OFF_HEAP_PERCENTAGE);
      if (criticalOffHeapThreshold != null) {
        return Float.parseFloat(
            criticalOffHeapThreshold.substring(criticalOffHeapThreshold.indexOf("=") + 1));
      }
    }
    return -1.0f;
  }

  protected static float getEvictionOffHeapPercent(Map<String, Object> options) {
    if (options != null) {
      String evictionOffHeapThreshold = (String) options.get(EVICTION_OFF_HEAP_PERCENTAGE);
      if (evictionOffHeapThreshold != null) {
        return Float.parseFloat(
            evictionOffHeapThreshold.substring(evictionOffHeapThreshold.indexOf("=") + 1));
      }
    }
    return -1.0f;
  }

  protected InternalCache createCache(InternalDistributedSystem system, Map<String, Object> options)
      throws IOException {
    InternalCache cache = (InternalCache) CacheFactory.create(system);

    float threshold = getCriticalHeapPercent(options);
    if (threshold > 0.0f) {
      cache.getResourceManager().setCriticalHeapPercentage(threshold);
    }
    threshold = getEvictionHeapPercent(options);
    if (threshold > 0.0f) {
      cache.getResourceManager().setEvictionHeapPercentage(threshold);
    }

    threshold = getCriticalOffHeapPercent(options);
    getCriticalOffHeapPercent(options);
    if (threshold > 0.0f) {
      cache.getResourceManager().setCriticalOffHeapPercentage(threshold);
    }
    threshold = getEvictionOffHeapPercent(options);
    if (threshold > 0.0f) {
      cache.getResourceManager().setEvictionOffHeapPercentage(threshold);
    }

    // Create and start a default cache server
    // If (disableDefaultServer is not set or it is set but false) AND (the number of cacheservers
    // is 0)
    Boolean disable = ServerLauncherParameters.INSTANCE.isDisableDefaultServer();
    if ((disable == null || !disable) && cache.getCacheServers().size() == 0) {
      // Create and add a cache server
      CacheServer server = cache.addCacheServer();

      CacheServerHelper.setIsDefaultServer(server);

      // Set its port if necessary
      Integer serverPort = ServerLauncherParameters.INSTANCE.getPort();
      if (serverPort != null) {
        server.setPort(serverPort);
      }

      // Set its bind address if necessary
      String serverBindAddress = ServerLauncherParameters.INSTANCE.getBindAddress();
      if (serverBindAddress != null) {
        server.setBindAddress(serverBindAddress.trim());
      }

      // Start it
      server.start();
    }

    return cache;
  }

  protected void disconnect(Cache cache) {
    DistributedSystem dsys = cache.getDistributedSystem();
    cache.close();
    dsys.disconnect();
  }

  /**
   * Stops a cache server (which is running in a different VM) by setting its status to
   * {@link #SHUTDOWN_PENDING}. Waits for the cache server to actually shut down.
   */
  public void stop(final String[] args) throws Exception {
    this.workingDir = (File) getStopOptions(args).get(DIR);

    // determine the current state of the Cache Server process...
    final File statusFile = new File(this.workingDir, this.statusName);
    ExitCode exitCode = ExitCode.FATAL;

    if (statusFile.exists()) {
      this.status = spinReadStatus();

      // upon reading the status file, request the Cache Server to shutdown if it has not already...
      if (this.status.state != SHUTDOWN) {
        // copy server PID and not use own PID; see bug #39707
        this.status = createStatus(this.baseName, SHUTDOWN_PENDING, this.status.pid);
        writeStatus(this.status);
      }

      // poll the Cache Server for a response to our shutdown request (passes through if the Cache
      // Server
      // has already shutdown)...
      pollCacheServerForShutdown();

      // after polling, determine the status of the Cache Server one last time and determine how to
      // exit...
      if (this.status.state == SHUTDOWN) {
        System.out.println(
            String.format("The %s has stopped.", this.baseName));
        deleteStatus();
        exitCode = ExitCode.NORMAL;
      } else {
        System.out.println(
            String.format("Timeout waiting for %s to shutdown, status is: %s",
                this.baseName, this.status));
      }
    } else {
      System.out.println(
          String.format("The specified working directory (%s) contains no status file",
              this.workingDir));
    }

    if (DONT_EXIT_AFTER_LAUNCH) {
      return;
    }

    exitCode.doSystemExit();
  }

  private void pollCacheServerForShutdown() throws InterruptedException {
    final int increment = 250; // unit is in milliseconds
    int clock = 0;

    // wait for a total of 20000 milliseconds (or 20 seconds)
    while (clock < SHUTDOWN_WAIT_TIME && status.state != SHUTDOWN) {
      try {
        status = readStatus();
      } catch (IOException ignore) {
      }

      try {
        Thread.sleep(increment);
      } catch (InterruptedException ie) {
        break;
      }

      clock += increment;
    }
  }

  /**
   * A class that represents the status of a cache server. Instances of this class are serialized to
   * a {@linkplain #statusName file} on disk.
   *
   * @see #SHUTDOWN
   * @see #STARTING
   * @see #RUNNING
   * @see #SHUTDOWN_PENDING
   */
  static class Status implements Serializable {

    private static final long serialVersionUID = 190943081363646485L;
    public int state = 0;
    public int pid = 0;

    private final String baseName;
    public Throwable exception;
    public String msg;
    public String dsMsg;

    public Status(String baseName) {
      this.baseName = baseName;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder();
      buffer.append(this.baseName).append(" pid: ").append(pid).append(" status: ");
      switch (state) {
        case SHUTDOWN:
          buffer.append("stopped");
          break;
        case STARTING:
          buffer.append("starting");
          break;
        case RUNNING:
          buffer.append("running");
          break;
        case SHUTDOWN_PENDING:
          buffer.append("stopping");
          break;
        default:
          buffer.append("unknown");
          break;
      }
      if (exception != null) {
        if (msg != null) {
          buffer.append("\n").append(msg).append(" - ");
        } else {
          buffer.append("\nException in ").append(this.baseName).append(" - ");
        }
        buffer.append(
            "See log file for details.");
      } else if (this.dsMsg != null) {
        buffer.append('\n').append(this.dsMsg);
      }
      return buffer.toString();
    }
  }

  /**
   * Notes that an error has occurred in the cache server and that it has shut down because of it.
   */
  protected void setServerError(final String msg, final Throwable t) {
    try {
      writeStatus(createStatus(this.baseName, SHUTDOWN, OSProcess.getId(), msg, t));
    } catch (Exception e) {
      if (logger != null) {
        logger.severe(e);
      } else {
        e.printStackTrace();
      }
      ExitCode.FATAL.doSystemExit();
    }
  }

  /**
   * Sets the status of a cache server by serializing a <code>Status</code> instance to a file in
   * the server's working directory.
   */
  public void writeStatus(final Status s) throws IOException {
    FileOutputStream fileOutput = null;
    ObjectOutputStream objectOutput = null;

    try {
      fileOutput = new FileOutputStream(new File(workingDir, statusName));
      objectOutput = new ObjectOutputStream(fileOutput);
      objectOutput.writeObject(s);
      objectOutput.flush();
    } finally {
      IOUtils.close(objectOutput);
      IOUtils.close(fileOutput);
    }
  }

  /**
   * Reads a cache server's status. If the status file cannot be read because of I/O problems, it
   * will try again.
   */
  protected Status spinReadStatus() {
    final long timeout = (System.currentTimeMillis() + 60000);
    Status status = null;

    while (status == null && System.currentTimeMillis() < timeout) {
      try {
        status = readStatus();
      } catch (Exception e) {
        // try again - the status might have been read in the middle of it being written by the
        // server resulting in
        // an EOFException here
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          status = null;
          break;
        }
      }
    }

    return status;
  }

  /**
   * Reads a cache server's status from a file in its working directory.
   */
  protected Status readStatus() throws InterruptedException, IOException {
    final File statusFile = new File(workingDir, statusName);

    FileInputStream fileInput = null;
    ObjectInputStream objectInput = null;

    try {
      fileInput = new FileInputStream(statusFile);
      objectInput = new ObjectInputStream(fileInput);

      Status status = (Status) objectInput.readObject();

      // See bug 32760
      // Note, only execute the conditional createStatus statement if we are in native mode; if we
      // are in pure Java mode
      // the the process ID identified in the Status object is assumed to exist!
      if (!isExistingProcess(status.pid)) {
        status = createStatus(this.baseName, SHUTDOWN, status.pid);
      }

      return status;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (FileNotFoundException e) {
      Thread.sleep(500);

      if (statusFile.exists()) {
        return readStatus();
      } else {
        throw e;
      }
    } finally {
      IOUtils.close(objectInput);
      IOUtils.close(fileInput);
    }
  }

  /**
   * Removes a cache server's status file
   */
  private void deleteStatus() throws IOException {
    final File statusFile = new File(workingDir, statusName);

    if (statusFile.exists() && !statusFile.delete()) {
      throw new IOException("Could not delete status file (" + statusFile.getAbsolutePath() + ")!");
    }
  }

  protected boolean isExistingProcess(final int pid) {
    return (PureJavaMode.isPure() || (pid != 0 && OSProcess.exists(pid)));
  }

  protected void waitForRunning() throws Exception {
    Status status = spinReadStatus();
    String lastReadMessage = null;
    String lastReportedMessage = null;
    long lastReadTime = System.nanoTime();
    if (status == null) {
      throw new Exception(
          "No available status.");
    } else {
      switch (status.state) {
        case STARTING:
          // re-read status for a while...
          while (status.state == STARTING) {
            Thread.sleep(500); // fix for bug 36998
            status = spinReadStatus();

            // check to see if the status message has changed
            if (status.dsMsg != null && !status.dsMsg.equals(lastReadMessage)) {
              lastReadMessage = status.dsMsg;
              lastReadTime = System.nanoTime();
            }

            // if the status message has not changed for 15 seconds, print
            // out the message.
            long elapsed = System.nanoTime() - lastReadTime;
            if (TimeUnit.NANOSECONDS.toMillis(elapsed) > STATUS_WAIT_TIME && lastReadMessage != null
                && !lastReadMessage.equals(lastReportedMessage)) {
              long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(elapsed);
              System.out.println(String.format(
                  "The server is still starting. %s seconds have elapsed since the last log message: %s",
                  elapsedSec, status.dsMsg));
              lastReportedMessage = lastReadMessage;
            }
          }
          if (status.state == SHUTDOWN) {
            System.out.println(status);
            ExitCode.FATAL.doSystemExit();
          }
          break;
        default:
          break;
      }
      System.out.println(status);
    }
  }

  /**
   * Reads {@link DistributedSystem#PROPERTY_FILE} and determines if the
   * {@link ConfigurationProperties#LOG_FILE} property is set to stdout
   *
   * @return true if the logging would go to stdout
   */
  private static boolean isLoggingToStdOut() {
    Properties gfprops = new Properties();
    URL url = DistributedSystem.getPropertyFileURL();
    if (url != null) {
      try {
        gfprops.load(url.openStream());
      } catch (IOException io) {
        // throw new GemFireIOException("Failed reading " + url, io);
        System.out.println("Failed reading " + url);
        ExitCode.FATAL.doSystemExit();
      }
      final String logFile = gfprops.getProperty(LOG_FILE);
      if (logFile == null || logFile.length() == 0) {
        return true;
      }
    } else {
      // Didnt find a property file, assuming the default is to log to stdout
      return true;
    }
    return false;
  }

  /**
   * Process information contained in the options map and add to the command line of the subprocess
   * as needed.
   */
  protected void addToServerCommand(final List<String> commandLine,
      final Map<String, Object> options) {
    final ListWrapper<String> commandLineWrapper = new ListWrapper<String>(commandLine);

    if (Boolean.TRUE.equals(options.get(REBALANCE))) {
      commandLineWrapper.add("-rebalance");
    }

    commandLineWrapper.add((String) options.get(DISABLE_DEFAULT_SERVER));
    commandLineWrapper.add((String) options.get(SERVER_PORT));
    commandLineWrapper.add((String) options.get(SERVER_BIND_ADDRESS_NAME));

    String criticalHeapThreshold = (String) options.get(CRITICAL_HEAP_PERCENTAGE);
    if (criticalHeapThreshold != null) {
      commandLineWrapper.add(criticalHeapThreshold);
    }
    String evictionHeapThreshold = (String) options.get(EVICTION_HEAP_PERCENTAGE);
    if (evictionHeapThreshold != null) {
      commandLineWrapper.add(evictionHeapThreshold);
    }

    String criticalOffHeapThreshold = (String) options.get(CRITICAL_OFF_HEAP_PERCENTAGE);
    if (criticalOffHeapThreshold != null) {
      commandLineWrapper.add(criticalOffHeapThreshold);
    }
    String evictionOffHeapThreshold = (String) options.get(EVICTION_OFF_HEAP_PERCENTAGE);
    if (evictionOffHeapThreshold != null) {
      commandLineWrapper.add(evictionOffHeapThreshold);
    }

    final Properties props = (Properties) options.get(PROPERTIES);

    for (final Object key : props.keySet()) {
      commandLineWrapper.add(key + "=" + props.getProperty(key.toString()));
    }

    if (props.getProperty(LOG_FILE) == null && CacheServerLauncher.isLoggingToStdOut()) {
      // Do not allow the cache server to log to stdout; override the logger with
      // #defaultLogFileName
      commandLineWrapper.add(LOG_FILE + "=" + defaultLogFileName);
    }
  }

  /**
   * This method is called immediately following cache creation in the spawned process, but prior to
   * setting the RUNNING flag in the status file. So the spawning process will block until this
   * method completes.
   */
  protected void startAdditionalServices(final Cache cache, final Map<String, Object> options)
      throws Exception {}

  /**
   * This method is called prior to DistributedSytstem.disconnect(). Care should be taken not to
   * take too long in this method or else #CacheServerLauncher.stop may timeout.
   */
  protected void stopAdditionalServices() throws Exception {}

  /**
   * A List implementation that disallows null values.
   *
   * @param <E> the Class type for the List elements.
   */
  protected static class ListWrapper<E> extends AbstractList<E> {

    private static final ThreadLocal<Boolean> addResult = new ThreadLocal<Boolean>();

    private final List<E> list;

    public ListWrapper(final List<E> list) {
      assert list != null : "The List cannot be null!";
      this.list = list;
    }

    @Override
    public boolean add(final E e) {
      final boolean localAddResult = super.add(e);
      return (localAddResult && addResult.get());
    }

    @Override
    public void add(final int index, final E element) {
      if (element != null) {
        list.add(index, element);
      }
      addResult.set(element != null);
    }

    @Override
    public E get(final int index) {
      return this.list.get(index);
    }

    @Override
    public E remove(final int index) {
      return list.remove(index);
    }

    @Override
    public E set(final int index, final E element) {
      return (element != null ? list.set(index, element) : list.get(index));
    }

    @Override
    public int size() {
      return list.size();
    }
  }

  private class MainLogReporter extends Thread implements StartupStatusListener {
    private String lastLogMessage;
    private final Status status;
    boolean running = true;

    public MainLogReporter(Status status) {
      this.status = status;
    }

    public synchronized void shutdown() {
      this.running = false;
      this.status.dsMsg = null;
      this.notifyAll();
    }


    @Override
    public void setStatus(String status) {
      lastLogMessage = status;
    }

    public synchronized void run() {
      while (running) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          // this should not happen.
          break;
        }
        if (running && safeEquals(lastLogMessage, status.dsMsg)) {
          status.dsMsg = lastLogMessage;
          try {
            writeStatus(status);
          } catch (IOException e) {
            // this could happen if there was a concurrent write to the file
            // eg a stop.
            continue;
          }
        }
      }
    }
  }

  protected static boolean safeEquals(String lastLogMessage, String dsMsg) {
    if (lastLogMessage == null && dsMsg == null) {
      return true;
    } else if (lastLogMessage == null || dsMsg == null) {
      return false;
    }
    return lastLogMessage.equals(dsMsg);
  }
}
