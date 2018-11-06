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
package org.apache.geode.admin.jmx.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.jmx.Agent;
import org.apache.geode.admin.jmx.AgentConfig;
import org.apache.geode.admin.jmx.AgentFactory;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.internal.util.JavaCommandBuilder;

/**
 * A command line utility inspired by the <code>CacheServerLauncher</code> that is responsible for
 * administering a stand-along GemFire JMX {@link Agent}.
 * <p/>
 *
 * @since GemFire 3.5
 */
public class AgentLauncher {

  private static final Logger logger = LogService.getLogger();

  /** Should the launch command be printed? */
  public static final boolean PRINT_LAUNCH_COMMAND =
      Boolean.getBoolean(AgentLauncher.class.getSimpleName() + ".PRINT_LAUNCH_COMMAND");

  /* constants used to define state */
  static final int SHUTDOWN = 0;
  static final int STARTING = 1;
  static final int RUNNING = 2;
  static final int SHUTDOWN_PENDING = 3;
  static final int SHUTDOWN_PENDING_AFTER_FAILED_STARTUP = 4;
  static final int UNKNOWN = 6;

  /** Agent configuration options */
  static final String AGENT_PROPS = "agent-props";

  /**
   * A flag to indicate if the current log file should be kept. Used only when 'start' is used to
   * fork off the 'server'
   */
  static final String APPENDTO_LOG_FILE = "appendto-log-file";

  /** optional and additional classpath entries */
  static final String CLASSPATH = "classpath";

  /** The directory argument */
  static final String DIR = "dir";

  /** Extra VM arguments */
  static final String VMARGS = "vmargs";

  /** The directory in which the agent's output resides */
  private File workingDirectory = null;

  /** The Status object for the agent */
  private Status status = null;

  /** base name for the agent to be launched */
  private final String basename;

  /** The name for the start up log file */
  private final String startLogFileName;

  /** The name of the status file */
  private final String statusFileName;

  /**
   * Instantiates an AgentLauncher for execution and control of the GemFire JMX Agent process. This
   * constructor is package private to prevent direct instantiation or subclassing by classes
   * outside this package, but does allow the class to be tested as needed.
   * <p/>
   *
   * @param basename base name for the application to be launched
   */
  AgentLauncher(final String basename) {
    assert basename != null : "The base name used by the AgentLauncher to create files cannot be null!";
    this.basename = basename;
    final String formattedBasename = this.basename.toLowerCase().replace(" ", "");
    this.startLogFileName = "start_" + formattedBasename + ".log";
    this.statusFileName = "." + formattedBasename + ".ser";
  }

  /**
   * Prints information about the agent configuration options
   */
  public void configHelp() {
    PrintStream out = System.out;

    Properties props = AgentConfigImpl.getDefaultValuesForAllProperties();

    out.println("\n");
    out.println("Agent configuration properties");

    SortedMap<String, String> map = new TreeMap<String, String>();

    int maxLength = 0;
    for (Iterator<Object> iter = props.keySet().iterator(); iter.hasNext();) {
      String prop = (String) iter.next();
      int length = prop.length();
      if (length > maxLength) {
        maxLength = length;
      }

      map.put(prop,
          AgentConfigImpl.getPropertyDescription(prop) + " (Default  \""
              + props.getProperty(prop) + "\")");
    }

    Iterator<Entry<String, String>> entries = map.entrySet().iterator();
    while (entries.hasNext()) {
      Entry<String, String> entry = entries.next();
      String prop = entry.getKey();
      out.print("  ");
      out.println(prop);

      String description = entry.getValue();
      StringTokenizer st = new StringTokenizer(description, " ");
      out.print("    ");
      int printed = 6;
      while (st.hasMoreTokens()) {
        String word = st.nextToken();
        if (printed + word.length() > 72) {
          out.print("\n    ");
          printed = 6;
        }
        out.print(word);
        out.print(" ");
        printed += word.length() + 1;
      }
      out.println("");
    }
    out.println("");

    ExitCode.FATAL.doSystemExit();
  }

  /**
   * Returns a map that maps the name of the start options to its value on the command line. If no
   * value is specified on the command line, a default one is provided.
   */
  protected Map<String, Object> getStartOptions(final String[] args) throws Exception {
    final Map<String, Object> options = new HashMap<String, Object>();

    options.put(APPENDTO_LOG_FILE, "false");
    options.put(DIR, IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(new File(".")));

    final List<String> vmArgs = new ArrayList<String>();
    options.put(VMARGS, vmArgs);

    final Properties agentProps = new Properties();
    options.put(AGENT_PROPS, agentProps);

    for (final String arg : args) {
      if (arg.startsWith("-classpath=")) {
        options.put(CLASSPATH, arg.substring("-classpath=".length()));
      } else if (arg.startsWith("-dir=")) {
        final File workingDirectory = processDirOption(options, arg.substring("-dir=".length()));
        System.setProperty(AgentConfigImpl.AGENT_PROPSFILE_PROPERTY_NAME,
            new File(workingDirectory, AgentConfig.DEFAULT_PROPERTY_FILE).getPath());
      } else if (arg.startsWith("-J")) {
        vmArgs.add(arg.substring(2));
      } else if (arg.contains("=")) {
        final int index = arg.indexOf("=");
        final String prop = arg.substring(0, index);
        final String value = arg.substring(index + 1);

        // if appendto-log-file is set, put it in options; it is not set as an agent prop
        if (prop.equals(APPENDTO_LOG_FILE)) {
          options.put(APPENDTO_LOG_FILE, value);
          continue;
        }

        // verify the property is valid
        AgentConfigImpl.getPropertyDescription(prop);

        // Note, the gfAgentPropertyFile System property is ultimately read in the constructor of
        // the AgentImpl class
        // in order to make any properties defined in this file not only accessible to the
        // DistributedSystem but to
        // the GemFire Agent as well.
        if (AgentConfigImpl.PROPERTY_FILE_NAME.equals(prop)) {
          System.setProperty(AgentConfigImpl.AGENT_PROPSFILE_PROPERTY_NAME, value);
        }

        // The Agent properties file (specified with the command-line key=value) is used to pass
        // configuration settings
        // to the GemFire DistributedSystem. A property file can be passed using the property-file
        // command-line switch
        // is a large number of properties are specified, or the properties maybe individually
        // specified on the
        // command-line as property=value arguments.
        agentProps.setProperty(prop, value);
      }
    }

    return options;
  }

  /**
   * After parsing the command line arguments, spawn the Java VM that will host the GemFire JMX
   * Agent.
   */
  public void start(final String[] args) throws Exception {
    final Map<String, Object> options = getStartOptions(args);

    workingDirectory = IOUtils.tryGetCanonicalFileElseGetAbsoluteFile((File) options.get(DIR));

    // verify that any GemFire JMX Agent process has been properly shutdown and delete any remaining
    // status files...
    verifyAndClearStatus();

    // start the GemFire JMX Agent process...
    runCommandLine(options, buildCommandLine(options));

    // wait for the GemFire JMX Agent process to complete startup and begin running...
    // it is also possible the Agent process may fail to start, so this should not wait indefinitely
    // unless
    // the status file was not successfully written to
    pollAgentUntilRunning();

    ExitCode.NORMAL.doSystemExit();
  }

  private void verifyAndClearStatus() throws Exception {
    final Status status = getStatus();

    if (status != null && status.state != SHUTDOWN) {
      throw new IllegalStateException(
          "JMX Agent exists but was not shutdown.");
    }

    deleteStatus();
  }

  private String[] buildCommandLine(final Map<String, Object> options) {
    final List<String> commands = JavaCommandBuilder.buildCommand(AgentLauncher.class.getName(),
        (String) options.get(CLASSPATH), null, (List<String>) options.get(VMARGS));

    commands.add("server");
    commands.add("-dir=" + workingDirectory);

    final Properties agentProps = (Properties) options.get(AGENT_PROPS);

    for (final Object key : agentProps.keySet()) {
      commands.add(key + "=" + agentProps.get(key.toString()));
    }

    return commands.toArray(new String[commands.size()]);
  }

  private void printCommandLine(final String[] commandLine) {
    if (PRINT_LAUNCH_COMMAND) {
      System.out.print("Starting " + this.basename + " with command:\n");
      for (final String command : commandLine) {
        System.out.print(command);
        System.out.print(' ');
      }
      System.out.println();
    }
  }

  private int runCommandLine(final Map<String, Object> options, final String[] commandLine)
      throws IOException {
    // initialize the startup log starting with a fresh log file (where all startup messages are
    // printed)
    final File startLogFile = IOUtils
        .tryGetCanonicalFileElseGetAbsoluteFile(new File(workingDirectory, startLogFileName));

    if (startLogFile.exists() && !startLogFile.delete()) {
      throw new IOException(String.format("Unable to delete file %s.",
          startLogFile.getAbsolutePath()));
    }

    Map<String, String> env = new HashMap<String, String>();
    // read the passwords from command line
    SocketCreator.readSSLProperties(env, true);

    printCommandLine(commandLine);

    final int pid = OSProcess.bgexec(commandLine, workingDirectory, startLogFile, false, env);

    System.out.println(
        String.format("Starting JMX Agent with pid: %s", pid));

    return pid;
  }

  private void pollAgentUntilRunning() throws Exception {
    Status status = spinReadStatus();

    // TODO this loop could recurse indefinitely if the GemFire JMX Agent's state never changes from
    // STARTING
    // to something else (like RUNNING), which could happen if server process fails to startup
    // correctly
    // and did not or could not write to the status file!
    // TODO should we really allow the InterruptedException from the Thread.sleep call to break this
    // loop (yeah, I
    // think so given the fact this could loop indefinitely)?
    while (status != null && status.state == STARTING) {
      Thread.sleep(500);
      status = spinReadStatus();
    }

    if (status == null) {
      // TODO throw a more appropriate Exception here!
      throw new Exception("No available status.");
    } else {
      System.out.println(status);
    }
  }

  /**
   * Starts the GemFire JMX Agent "server" process with the given command line arguments.
   */
  public void server(final String[] args) throws Exception {
    final Map<String, Object> options = getStartOptions(args);

    workingDirectory = IOUtils.tryGetCanonicalFileElseGetAbsoluteFile((File) options.get(DIR));

    writeStatus(createStatus(this.basename, STARTING, OSProcess.getId()));

    final Agent agent = createAgent((Properties) options.get(AGENT_PROPS));

    final Thread thread = createAgentProcessThread(agent);
    thread.start();

    // periodically check and see if the JMX Agent has been told to stop
    pollAgentForPendingShutdown(agent);
  }

  private Agent createAgent(final Properties props) throws IOException, AdminException {
    ClusterDistributionManager.setIsDedicatedAdminVM(true);
    SystemFailure.setExitOK(true);

    final AgentConfigImpl config = new AgentConfigImpl(props);

    // see bug 43760
    if (config.getLogFile() == null || "".equals(config.getLogFile().trim())) {
      config.setLogFile(AgentConfigImpl.DEFAULT_LOG_FILE);
    }

    // LOG:TODO: redirectOutput called here
    OSProcess.redirectOutput(new File(config.getLogFile())); // redirect output to the configured
                                                             // log file

    return AgentFactory.getAgent(config);
  }

  private UncaughtExceptionHandler createUncaughtExceptionHandler() {
    return (t, e) -> {
      if (e instanceof VirtualMachineError) {
        SystemFailure.setFailure((VirtualMachineError) e);
      }
      setServerError(String.format("Uncaught exception in thread %s",
          t.getName()), e);
    };
  }

  private Thread createAgentProcessThread(final Agent agent) {
    Thread thread = new LoggingThread("Start agent", createAgentProcessRunnable(agent));
    thread.setUncaughtExceptionHandler(createUncaughtExceptionHandler());
    return thread;
  }

  private Runnable createAgentProcessRunnable(final Agent agent) {
    return new Runnable() {
      public void run() {
        try {
          agent.start();
          writeStatus(createStatus(AgentLauncher.this.basename, RUNNING, OSProcess.getId()));
        } catch (IOException e) {
          e.printStackTrace();
        } catch (GemFireException e) {
          e.printStackTrace();
          handleGemFireException(e);
        }
      }

      private void handleGemFireException(final GemFireException e) {
        String message = String.format("Server failed to start: %s",
            e.getMessage());

        if (e.getCause() != null) {
          if (e.getCause().getCause() != null) {
            message += ", " + e.getCause().getCause().getMessage();
          }
        }

        setServerError(null, new Exception(message));
      }
    };
  }


  /**
   * Notes that an error has occurred in the agent and that it has shut down because of it.
   */
  private void setServerError(final String message, final Throwable cause) {
    try {
      writeStatus(createStatus(this.basename, SHUTDOWN_PENDING_AFTER_FAILED_STARTUP,
          OSProcess.getId(), message, cause));
    } catch (Exception e) {
      logger.fatal(e.getMessage(), e);
      ExitCode.FATAL.doSystemExit();
    }
  }

  private void pollAgentForPendingShutdown(final Agent agent) throws Exception {
    while (true) {
      pause(500);
      spinReadStatus();

      if (isStatus(SHUTDOWN_PENDING, SHUTDOWN_PENDING_AFTER_FAILED_STARTUP)) {
        agent.stop();
        final ExitCode exitCode =
            (isStatus(SHUTDOWN_PENDING_AFTER_FAILED_STARTUP) ? ExitCode.FATAL : ExitCode.NORMAL);
        writeStatus(createStatus(this.status, SHUTDOWN));
        exitCode.doSystemExit();
      }
    }
  }

  /**
   * Extracts configuration information for stopping a agent based on the contents of the command
   * line. This method can also be used with getting the status of a agent.
   */
  protected Map<String, Object> getStopOptions(final String[] args) throws Exception {
    final Map<String, Object> options = new HashMap<String, Object>();

    options.put(DIR, IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(new File(".")));

    for (final String arg : args) {
      if (arg.equals("stop") || arg.equals("status")) {
        // expected
      } else if (arg.startsWith("-dir=")) {
        processDirOption(options, arg.substring("-dir=".length()));
      } else {
        throw new Exception(
            String.format("Unknown argument: %s", arg));
      }
    }

    return options;
  }

  /**
   * Stops a running JMX Agent by setting the status to "shutdown pending".
   */
  public void stop(final String[] args) throws Exception {
    final Map<String, Object> options = getStopOptions(args);

    workingDirectory = IOUtils.tryGetCanonicalFileElseGetAbsoluteFile((File) options.get(DIR));

    ExitCode exitCode = ExitCode.FATAL;

    if (new File(workingDirectory, statusFileName).exists()) {
      spinReadStatus();

      if (!isStatus(SHUTDOWN)) {
        writeStatus(createStatus(this.basename, SHUTDOWN_PENDING, status.pid));
      }

      pollAgentForShutdown();

      if (isStatus(SHUTDOWN)) {
        System.out
            .println(String.format("The %s has shut down.", this.basename));
        deleteStatus();
        exitCode = ExitCode.NORMAL;
      } else {
        System.out
            .println(String.format("Timeout waiting for %s to shutdown, status is: %s",
                this.basename, status));
      }
    } else {
      System.out.println(
          String.format("The specified working directory (%s) contains no status file",
              workingDirectory));
    }

    exitCode.doSystemExit();
  }

  private void pollAgentForShutdown() throws InterruptedException {
    final long endTime = (System.currentTimeMillis() + 20000);
    long clock = 0;

    while (clock < endTime && !isStatus(SHUTDOWN)) {
      pause(500);
      spinReadStatus();
      clock = System.currentTimeMillis();
    }
  }

  /**
   * Prints the status of the GemFire JMX Agent running in the configured working directory.
   */
  public void status(final String[] args) throws Exception {
    this.workingDirectory =
        IOUtils.tryGetCanonicalFileElseGetAbsoluteFile((File) getStopOptions(args).get(DIR));
    System.out.println(getStatus());
    ExitCode.NORMAL.doSystemExit();
  }

  /**
   * Returns the <code>Status</code> of the GemFire JMX Agent in the <code>workingDirectory</code>.
   */
  protected Status getStatus() throws Exception {
    Status status;

    if (new File(workingDirectory, statusFileName).exists()) {
      status = spinReadStatus();
    } else {
      status = createStatus(this.basename, SHUTDOWN, 0,
          String.format("%s is not running in the specified working directory: (%s).",
              this.basename, this.workingDirectory),
          null);
    }

    return status;
  }

  /**
   * Determines if the Status.state is one of the specified states in the given array of states.
   * Note, the status of the Agent, as indicated in the .agent.ser status file, should never have a
   * written value of UNKNOWN.
   * <p/>
   *
   * @param states an array of possible acceptable states satisfying the condition of the Agent's
   *        status.
   * @return a boolean value indicating whether the Agent's status satisfies one of the specified
   *         states.
   */
  private boolean isStatus(final Integer... states) {
    return (this.status != null
        && Arrays.asList(defaultToUnknownStateIfNull(states)).contains(this.status.state));
  }

  /**
   * Removes an agent's status file
   */
  protected void deleteStatus() throws IOException {
    deleteStatus(workingDirectory);
  }

  void deleteStatus(final File workingDirectory) throws IOException {
    final File statusFile = new File(workingDirectory, statusFileName);

    if (statusFile.exists() && !statusFile.delete()) {
      throw new IOException("Could not delete status file (" + statusFile.getAbsolutePath() + ")");
    }
  }

  /**
   * Reads the GemFire JMX Agent's status from the status file (.agent.ser) in it's working
   * directory.
   * <p/>
   *
   * @return a Status object containing the state persisted to the .agent.ser file in the working
   *         directory and representing the status of the Agent
   * @throws IOException if the status file was unable to be read.
   * @throws RuntimeException if the class of the object written to the .agent.ser file is not of
   *         type Status.
   */
  protected Status readStatus() throws IOException {
    return readStatus(workingDirectory);
  }

  Status readStatus(final File workingDirectory) throws IOException {
    FileInputStream fileIn = null;
    ObjectInputStream objectIn = null;

    try {
      fileIn = new FileInputStream(new File(workingDirectory, statusFileName));
      objectIn = new ObjectInputStream(fileIn);
      this.status = (Status) objectIn.readObject();
      return this.status;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } finally {
      IOUtils.close(objectIn);
      IOUtils.close(fileIn);
    }
  }

  /**
   * A wrapper method for the readStatus method to make one last check for the GemFire JMX Agent
   * process if running with the native libraries.
   *
   * @return the Status object as returned from readStatus unless running in native mode and a
   *         determination is made such that the Agent process is not running.
   * @throws IOException if the state of the Agent process could not be read from the .agent.ser
   *         status file.
   * @see #readStatus()
   */
  protected Status nativeReadStatus() throws IOException {
    Status status = readStatus();

    // @see Bug #32760 - the bug is still possible in pure Java mode
    if (status != null && !PureJavaMode.isPure() && !OSProcess.exists(status.pid)) {
      status = createStatus(status, SHUTDOWN);
    }

    return status;
  }

  /**
   * Reads the JMX Agent's status from the .agent.ser status file. If the status file cannot be read
   * due to I/O problems, the method will keep attempting to read the file for up to 20 seconds.
   * <p/>
   *
   * @return the Status of the GemFire JMX Agent as determined by the .agent.ser status file, or
   *         natively based on the presence/absence of the Agent process.
   */
  protected Status spinReadStatus() {
    Status status = null;

    final long endTime = (System.currentTimeMillis() + 20000);
    long clock = 0;

    while (status == null && clock < endTime) {
      try {
        status = nativeReadStatus();
      } catch (Exception ignore) {
        // see bug 31575
        // see bug 36998
        // try again after a short delay... the status file might have been read prematurely before
        // it existed
        // or while the server was trying to write to it resulting in a possible EOFException, or
        // other IOException.
        pause(500);
      } finally {
        clock = System.currentTimeMillis();
      }
    }

    return status;
  }

  /**
   * Sets the status of the GemFire JMX Agent by serializing a <code>Status</code> object to a
   * status file in the Agent's working directory.
   * <p/>
   *
   * @param status the Status object representing the state of the Agent process to persist to disk.
   * @return the written Status object.
   * @throws IOException if the Status could not be successfully persisted to disk.
   */
  public Status writeStatus(final Status status) throws IOException {
    return writeStatus(status, workingDirectory);
  }

  Status writeStatus(final Status status, final File workingDirectory) throws IOException {
    FileOutputStream fileOut = null;
    ObjectOutputStream objectOut = null;

    try {
      fileOut = new FileOutputStream(new File(workingDirectory, statusFileName));
      objectOut = new ObjectOutputStream(fileOut);
      objectOut.writeObject(status);
      objectOut.flush();
      this.status = status;
      return this.status;
    } finally {
      IOUtils.close(objectOut);
      IOUtils.close(fileOut);
    }
  }

  protected static Status createStatus(final String basename, final int state, final int pid) {
    return createStatus(basename, state, pid, null, null);
  }

  protected static Status createStatus(final String basename, final int state, final int pid,
      final String msg, final Throwable t) {
    final Status status = new Status(basename);
    status.state = state;
    status.pid = pid;
    status.msg = msg;
    status.exception = t;
    return status;
  }

  protected static Status createStatus(final Status status, final int state) {
    assert status != null : "The status to clone cannot be null!";
    return createStatus(status.baseName, state, status.pid, status.msg, status.exception);
  }

  protected static Integer[] defaultToUnknownStateIfNull(final Integer... states) {
    return (states != null ? states : new Integer[] {UNKNOWN});
  }

  protected static boolean pause(final int milliseconds) {
    try {
      Thread.sleep(milliseconds);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  protected static File processDirOption(final Map<String, Object> options, final String dirValue)
      throws FileNotFoundException {
    final File workingDirectory = new File(dirValue);

    if (!workingDirectory.exists()) {
      throw new FileNotFoundException(
          String.format("The input working directory does not exist: %s",
              dirValue));
    }

    options.put(DIR, workingDirectory);

    return workingDirectory;
  }

  /**
   * Prints usage information for the AgentLauncher to the command line.
   * <p/>
   *
   * @param message a String to output to the command line indicating the user error.
   */
  private static void usage(final String message) {
    final PrintStream out = System.out;

    out.println("\n** " + message + "\n");

    out.println("agent start [-J<vmarg>]* [-dir=<dir>] [prop=value]*");
    out.println("Starts the GemFire JMX Agent");
    out.println("\t"
        + "<vmarg> a VM-option passed to the agent's VM, example -J-Xmx1024M for a 1 Gb heap");
    out.println("\t<dir> Directory in which agent runs, default is the current directory");
    out.println("\t<prop> A configuration property/value passed to the agent");
    out.println("\t(see help config for more details)");
    out.println();

    out.println("agent stop [-dir=<dir>]");
    out.println("Stops a GemFire JMX Agent");
    out.println("\t<dir> Directory in which agent runs, default is the current directory");
    out.println("");
    out.println("agent status [-dir=<dir>]");
    out.println(
        "Reports the status and the process id of a GemFire JMX Agent");
    out.println("\t<dir> Directory in which agent runs, default is the current directory");
    out.println();

    ExitCode.FATAL.doSystemExit();
  }

  /**
   * Bootstrap method to launch the GemFire JMX Agent process to monitor and manage a GemFire
   * Distributed System/Cache. Main will read the arguments passed on the command line and dispatch
   * the command to the appropriate handler.
   */
  public static void main(final String[] args) {
    if (args.length < 1) {
      usage("Missing command");
    }

    // TODO is this only needed on 'agent server'? 'agent {start|stop|status}' technically do no run
    // any GemFire Cache
    // or DS code inside the current process.
    SystemFailure.loadEmergencyClasses();

    final AgentLauncher launcher = new AgentLauncher("Agent");

    try {
      final String command = args[0];

      if (command.equalsIgnoreCase("start")) {
        launcher.start(args);
      } else if (command.equalsIgnoreCase("server")) {
        launcher.server(args);
      } else if (command.equalsIgnoreCase("stop")) {
        launcher.stop(args);
      } else if (command.equalsIgnoreCase("status")) {
        launcher.status(args);
      } else if (command.toLowerCase().matches("-{0,2}help")) {
        if (args.length > 1) {
          final String topic = args[1];

          if (topic.equals("config")) {
            launcher.configHelp();
          } else {
            usage(String.format("No help available for %s", topic));
          }
        }

        usage("agent help");
      } else {
        usage(String.format("Unknown command: %s", command));
      }
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      t.printStackTrace();
      System.err.println(
          String.format("Error : %s", t.getLocalizedMessage()));
      ExitCode.FATAL.doSystemExit();
    }
  }

  /**
   * A class representing the current state of the GemFire JMX Agent process. Instances of this
   * class are serialized to a {@linkplain #statusFileName file} on disk in the specified working
   * directory {@linkplain #workingDirectory}.
   * <p/>
   *
   * @see #SHUTDOWN
   * @see #STARTING
   * @see #RUNNING
   * @see #SHUTDOWN_PENDING
   * @see #SHUTDOWN_PENDING_AFTER_FAILED_STARTUP
   */
  // TODO refactor this class and internalize the state
  // TODO refactor the class and make immutable
  static class Status implements Serializable {

    private static final long serialVersionUID = -7758402454664266174L;

    int pid = 0;
    int state = 0;

    final String baseName;
    String msg;

    Throwable exception;

    public Status(final String baseName) {
      this.baseName = baseName;
    }

    @Override
    public String toString() {
      final StringBuilder buffer = new StringBuilder();

      if (pid == Integer.MIN_VALUE && state == SHUTDOWN && msg != null) {
        buffer.append(msg);
      } else {
        buffer.append(
            String.format("%s pid: %d status: ", this.baseName, pid));

        switch (state) {
          case SHUTDOWN:
            buffer.append("shutdown");
            break;
          case STARTING:
            buffer.append("starting");
            break;
          case RUNNING:
            buffer.append("running");
            break;
          case SHUTDOWN_PENDING:
            buffer.append("shutdown pending");
            break;
          case SHUTDOWN_PENDING_AFTER_FAILED_STARTUP:
            buffer.append("shutdown pending after failed startup");
            break;
          default:
            buffer.append("unknown");
            break;
        }

        if (exception != null) {
          if (msg != null) {
            buffer.append("\n").append(msg).append(" - ");
          } else {
            buffer.append("\n " + String.format("Exception in %s : %s ",
                this.baseName, exception.getMessage()) + " - ");
          }
          buffer
              .append("See log file for details.");
        }
      }

      return buffer.toString();
    }
  }

}
