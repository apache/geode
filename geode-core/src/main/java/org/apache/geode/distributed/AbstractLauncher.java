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

package org.apache.geode.distributed;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.unsafe.RegisterSignalHandlerSupport;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.lang.ClassUtils;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.util.SunAPINotFoundException;
import org.apache.geode.management.internal.cli.json.GfJsonObject;

import java.io.*;
import java.net.BindException;
import java.net.InetAddress;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * The AbstractLauncher class is a base class for implementing various launchers to construct and run different GemFire
 * processes, like Cache Servers, Locators, Managers, HTTP servers and so on.
 * 
 * @see java.lang.Comparable
 * @see java.lang.Runnable
 * @see org.apache.geode.lang.Identifiable
 * @since GemFire 7.0
 */
public abstract class AbstractLauncher<T extends Comparable<T>> implements Runnable {

  protected static final Boolean DEFAULT_FORCE = Boolean.FALSE;
  
  protected static final long READ_PID_FILE_TIMEOUT_MILLIS = 2*1000;

  // @see http://publib.boulder.ibm.com/infocenter/javasdk/v6r0/index.jsp?topic=%2Fcom.ibm.java.doc.user.lnx.60%2Fuser%2Fattachapi.html
  // @see http://docs.oracle.com/cd/E13150_01/jrockit_jvm/jrockit/geninfo/diagnos/aboutjrockit.html#wp1083571
  private static final List<String> ATTACH_API_PACKAGES = Arrays.asList(
    "com.sun.tools.attach",
    "com/sun/tools/attach",
    "com.ibm.tools.attach",
    "com/ibm/tools/attach"
  );

  public static final String DEFAULT_WORKING_DIRECTORY = SystemUtils.CURRENT_DIRECTORY;
  public static final String SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + "launcher.registerSignalHandlers";

  protected static final String OPTION_PREFIX = "-";

  private static final String IBM_ATTACH_API_CLASS_NAME = "com.ibm.tools.attach.AgentNotSupportedException";
  private static final String SUN_ATTACH_API_CLASS_NAME = "com.sun.tools.attach.AttachNotSupportedException";
  private static final String SUN_SIGNAL_API_CLASS_NAME = "sun.misc.Signal";

  private volatile boolean debug;

  protected final transient AtomicBoolean running = new AtomicBoolean(false);

  protected Logger logger = Logger.getLogger(getClass().getName()); // TODO:KIRK: does this need log4j2?

  public AbstractLauncher() {
    try {
      if (Boolean.getBoolean(SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY)) {
        ClassUtils.forName(SUN_SIGNAL_API_CLASS_NAME, new SunAPINotFoundException(
          "WARNING!!! Not running a Sun JVM.  Could not find the sun.misc.Signal class; Signal handling disabled."));
        RegisterSignalHandlerSupport.registerSignalHandlers();
      }
    }
    catch (SunAPINotFoundException e) {
      info(e.getMessage());
    }
  }

  /**
   * Asserts that the specified port is available on all network interfaces on this local system.
   *
   * @param port an integer indicating the network port to listen for client network requests.
   * @throws BindException if the network port is not available.
   * @see #assertPortAvailable
   */
  protected static void assertPortAvailable(final int port) throws BindException {
    assertPortAvailable(null, port);
  }

  /**
   * Asserts that the specified port is available on the specified network interface, indicated by it's assigned
   * IP address, on this local system.
   *
   * @param bindAddress an InetAddress indicating the bounded network interface to determine whether the service port
   * is available or not.
   * @param port an integer indicating the network port to listen for client network requests.
   * @throws BindException if the network address and port are not available.  Address defaults to localhost (or all
   * network interfaces on the local system) if null.
   * @see org.apache.geode.internal.AvailablePort
   */
  protected static void assertPortAvailable(final InetAddress bindAddress, final int port) throws BindException {
    if (!AvailablePort.isPortAvailable(port, AvailablePort.SOCKET, bindAddress)) {
      throw new BindException(String.format("Network is unreachable; port (%1$d) is not available on %2$s.", port,
        (bindAddress != null ? bindAddress.getCanonicalHostName() : "localhost")));
    }
  }

  /**
   * Determines whether the specified property with name is set to a value in the referenced Properties. The property
   * is considered "set" if the String value of the property is not non-null, non-empty and non-blank.  Therefore,
   * the Properties may "have" a property with name, but having no value as determined by this method.
   * 
   * @param properties the Properties used in determining whether the given property is set.
   * @param propertyName a String indicating the name of the property to check if set.
   * @return a boolean indicating whether the specified property with name has been given a value in the referenced
   * Properties.
   * @see java.util.Properties
   */
  protected static boolean isSet(final Properties properties, final String propertyName) {
    return !StringUtils.isBlank(properties.getProperty(propertyName));
  }

  /**
   * Loads the GemFire properties at the specified URL.
   * 
   * @param url the URL to the gemfire.properties to load.
   * @return a Properties instance populated with the gemfire.properties.
   * @see java.net.URL
   */
  protected static Properties loadGemFireProperties(final URL url) {
    final Properties properties = new Properties();

    if (url != null) {
      try {
        properties.load(new FileReader(new File(url.toURI())));
      }
      catch (Exception e) {
        try {
          // not in the file system, try the classpath
          properties.load(AbstractLauncher.class.getResourceAsStream(DistributedSystem.getPropertiesFile()));
        }
        catch (Exception ignore) {
          // not in the file system or the classpath; gemfire.properties does not exist
        }
      }
    }

    return properties;
  }

  void initLogger() {
    try {
      this.logger.addHandler(new FileHandler(SystemUtils.CURRENT_DIRECTORY.concat("debug.log")));
      this.logger.setLevel(Level.ALL);
      this.logger.setUseParentHandlers(true);
    }
    catch (IOException e) {
      e.printStackTrace(System.err);
      System.err.flush();
      throw new RuntimeException(e);
    }
  }

  /**
   * This method attempts to make a best effort determination for whether the Attach API classes are on the classpath.
   * 
   * @param t the Throwable being evaluated for missing Attach API classes.
   * @return a boolean indicating whether the Exception or Error condition is a result of the Attach API
   * missing from the classpath.
   */
  protected boolean isAttachAPINotFound(final Throwable t) {
    boolean missing = false;

    // NOTE prerequisite, Throwable must be a ClassNotFoundException or NoClassDefFoundError
    if (t instanceof ClassNotFoundException || t instanceof NoClassDefFoundError) {
      // NOTE the use of the 'testing' class member variable, yuck!
      if (!isAttachAPIOnClasspath()) {
        // NOTE ok, the Attach API is not available, however we still do not know whether an user application class
        // caused the ClassNotFoundException or NoClassDefFoundError.
        final StringWriter stackTraceWriter = new StringWriter();

        // NOTE the full stack trace includes the Throwable message, which typically indicates the Exception/Error
        // thrown and the reason (as in which class was not found).
        t.printStackTrace(new PrintWriter(stackTraceWriter));

        final String stackTrace = stackTraceWriter.toString();

        for (String attachApiPackage : ATTACH_API_PACKAGES) {
          missing |= stackTrace.contains(attachApiPackage);
        }
      }
    }

    return missing;
  }

  /**
   * Determines if the Attach API is on the classpath.
   * 
   * @return a boolean value indicating if the Attach API is on the classpath.
   */
  boolean isAttachAPIOnClasspath() {
    return (ClassUtils.isClassAvailable(SUN_ATTACH_API_CLASS_NAME)
      || ClassUtils.isClassAvailable(IBM_ATTACH_API_CLASS_NAME));
  }

  /**
   * Determines whether the Locator launcher is in debug mode.
   * 
   * @return a boolean to indicate whether the Locator launcher is in debug mode.
   * @see #setDebug(boolean)
   */
  public boolean isDebugging() {
    return this.debug;
  }

  /**
   * Sets the debug mode of the GemFire launcher class.  This mutable property of the launcher enables the user to turn
   * the debug mode on and off programmatically.
   * 
   * @param debug a boolean used to enable or disable debug mode.
   * @see #isDebugging()
   */
  public final void setDebug(final boolean debug) {
    this.debug = debug;
  }

  /**
   * Determines whether the Locator referenced by this launcher is running.
   * 
   * @return a boolean valued indicating if the referenced Locator is running.
   */
  public boolean isRunning() {
    return this.running.get();
  }

  /**
   * Creates a Properties object with configuration settings that the launcher has that should take
   * precedence over anything the user has defined in their gemfire properties file.
   *
   * @return a Properties object with GemFire properties that the launcher has defined.
   * @see #getDistributedSystemProperties(java.util.Properties)
   * @see java.util.Properties
   */
  protected Properties getDistributedSystemProperties() {
    return getDistributedSystemProperties(null);
  }

  /**
   * Creates a Properties object with configuration settings that the launcher has that should take
   * precedence over anything the user has defined in their gemfire properties file.
   *
   * @param defaults default GemFire Distributed System properties as configured in the Builder.
   * @return a Properties object with GemFire properties that the launcher has defined.
   * @see java.util.Properties
   */
  protected Properties getDistributedSystemProperties(final Properties defaults) {
    final Properties distributedSystemProperties = new Properties();

    if (defaults != null) {
      distributedSystemProperties.putAll(defaults);
    }

    if (!StringUtils.isBlank(getMemberName())) {
      distributedSystemProperties.setProperty(NAME, getMemberName());
    }

    return distributedSystemProperties;
  }

  /**
   * Gets a File reference with the path to the log file for the process.
   *
   * @return a File reference to the path of the log file for the process.
   */
  protected File getLogFile() {
    return new File(getWorkingDirectory(), getLogFileName());
  }

  /**
   * Gets the fully qualified canonical path of the log file for the process.
   *
   * @return a String value indicating the canonical path of the log file for the process.
   */
  protected String getLogFileCanonicalPath() {
    try {
      return getLogFile().getCanonicalPath();
    } catch (IOException e) {
      return getLogFileName(); // TODO: or return null?
    }
  }

  /**
   * Gets the name of the log file used to log information about this GemFire service.
   * 
   * @return a String value indicating the name of this GemFire service's log file.
   */
  public abstract String getLogFileName();

  /**
   * Gets the name or ID of the member in the GemFire distributed system.  This method prefers name if specified,
   * otherwise the ID is returned.  If name was not specified to the Builder that created this Launcher and this call
   * is not in-process, then null is returned.
   * 
   * @return a String value indicating the member's name if specified, otherwise the member's ID is returned if
   * this call is made in-process, or finally, null is returned if neither name name was specified or the call is
   * out-of-process.
   * @see #getMemberName()
   * @see #getMemberId()
   */
  public String getMember() {
    return StringUtils.defaultIfBlank(getMemberName(), getMemberId());
  }

  /**
   * Gets the ID of the member in the GemFire distributed system as determined and assigned by GemFire when the member
   * process joins the distributed system.  Note, this call only works if the API is used in-process.
   *
   * @return a String value indicating the ID of the member in the GemFire distributed system.
   */
  public String getMemberId() {
    final InternalDistributedSystem distributedSystem = InternalDistributedSystem.getConnectedInstance();
    return (distributedSystem != null ? distributedSystem.getMemberId() : null);
  }

  /**
   * Gets the name of the member in the GemFire distributed system as determined by the 'name' GemFire property.
   * Note, this call only works if the API is used in-process.
   *
   * @return a String value indicating the name of the member in the GemFire distributed system.
   */
  public String getMemberName() {
    final InternalDistributedSystem distributedSystem = InternalDistributedSystem.getConnectedInstance();
    return (distributedSystem != null ? distributedSystem.getConfig().getName() : null);
  }

  /**
   * Gets the user-specified process ID (PID) of the running GemFire service that AbstractLauncher implementations
   * can use to determine status, or stop the service.
   * 
   * @return an Integer value indicating the process ID (PID) of the running GemFire service.
   */
  public abstract Integer getPid();

  /**
   * Gets the name of the GemFire service.
   * 
   * @return a String indicating the name of the GemFire service.
   */
  public abstract String getServiceName();

  /**
   * Gets the working directory pathname in which the process will be run.
   * 
   * @return a String value indicating the pathname of the Server's working directory.
   */
  public String getWorkingDirectory() {
    return DEFAULT_WORKING_DIRECTORY;
  }

  /**
   * Prints the specified debug message to standard err, replacing any placeholder values with the specified arguments
   * on output, if debugging has been enabled.
   * 
   * @param message the String value written to standard err.
   * @param args an Object array containing arguments to replace the placeholder values in the message.
   * @see java.lang.System#err
   * @see #isDebugging()
   * @see #debug(Throwable)
   * @see #info(Object, Object...)
   */
  protected void debug(final String message, final Object... args) {
    if (isDebugging()) {
      if (args != null && args.length > 0) {
        System.err.printf(message, args);
      }
      else {
        System.err.print(message);
      }
    }
  }

  /**
   * Prints the stack trace of the given Throwable to standard err if debugging has been enabled.
   * 
   * @param t the Throwable who's stack trace is printed to standard err.
   * @see java.lang.System#err
   * @see #isDebugging()
   * @see #debug(String, Object...)
   */
  protected void debug(final Throwable t) {
    if (isDebugging()) {
      t.printStackTrace(System.err);
    }
  }

  /**
   * Prints the specified informational message to standard err, replacing any placeholder values with the specified
   * arguments on output.
   * 
   * @param message the String value written to standard err.
   * @param args an Object array containing arguments to replace the placeholder values in the message.
   * @see java.lang.System#err
   * @see #debug(String, Object...)
   */
  protected void info(final Object message, final Object... args) {
    if (args != null && args.length > 0) {
      System.err.printf(message.toString(), args);
    }
    else {
      System.err.print(message);
    }
  }

  /**
   * Redirects the standard out and standard err to the configured log file as specified in the GemFire distributed
   * system properties.
   * 
   * @param distributedSystem the GemFire model for a distributed system.
   * @throws IOException if the standard out and err redirection was unsuccessful.
   */
  protected void redirectOutput(final DistributedSystem distributedSystem) throws IOException {
    if (distributedSystem instanceof InternalDistributedSystem) {
      OSProcess.redirectOutput(((InternalDistributedSystem) distributedSystem).getConfig().getLogFile());
    }
  }

  /**
   * Gets the version of GemFire currently running.
   * 
   * @return a String representation of GemFire's version.
   */
  public String version() {
    return GemFireVersion.getGemFireVersion();
  }
  
  int identifyPid() throws PidUnavailableException {
    return ProcessUtils.identifyPid();
  }
  
  int identifyPidOrNot() {
    try {
      return identifyPid();
    } catch (PidUnavailableException e) {
      return -1;
    }
  }
  
  boolean isPidInProcess() {
    Integer pid = getPid();
    return pid != null && pid == identifyPidOrNot();
  }

  /**
   * The ServiceState is an immutable type representing the state of the specified Locator at any given moment in time.
   * The ServiceState associates the Locator with it's state at the exact moment an instance of this class
   * is constructed.
   */
  public static abstract class ServiceState<T extends Comparable<T>> {

    protected static final String JSON_CLASSPATH = "classpath";
    protected static final String JSON_GEMFIREVERSION = "gemFireVersion";
    protected static final String JSON_HOST = "bindAddress";
    protected static final String JSON_JAVAVERSION = "javaVersion";
    protected static final String JSON_JVMARGUMENTS = "jvmArguments";
    protected static final String JSON_LOCATION = "location";
    protected static final String JSON_LOGFILE = "logFileName";
    protected static final String JSON_MEMBERNAME = "memberName";
    protected static final String JSON_PID = "pid";
    protected static final String JSON_PORT = "port";
    protected static final String JSON_STATUS = "status";
    protected static final String JSON_STATUSMESSAGE = "statusMessage";
    protected static final String JSON_TIMESTAMP = "timestamp";
    protected static final String JSON_UPTIME = "uptime";
    protected static final String JSON_WORKINGDIRECTORY = "workingDirectory";

    private static final String DATE_TIME_FORMAT_PATTERN = "MM/dd/yyyy hh:mm a";

    private final Integer pid;

    // NOTE the mutable non-Thread safe List is guarded by a call to Collections.unmodifiableList on initialization
    private final List<String> jvmArguments;

    private final Long uptime;

    private final Status status;

    private final String classpath;
    private final String gemfireVersion;
    private final String host;
    private final String javaVersion;
    private final String logFile;
    private final String memberName;
    private final String port;
    private final String serviceLocation;
    private final String statusMessage;
    private final String workingDirectory;

    private final Timestamp timestamp;

    // TODO refactor the logic in this method into a DateTimeFormatUtils class
    protected static String format(final Date timestamp) {
      return (timestamp == null ? "" : new SimpleDateFormat(DATE_TIME_FORMAT_PATTERN).format(timestamp));
    }

    protected static Integer identifyPid() {
      try {
        return ProcessUtils.identifyPid();
      }
      catch (PidUnavailableException ignore) {
        return null;
      }
    }

    // TODO refactor the logic in this method into a DateTimeFormatUtils class
    protected static String toDaysHoursMinutesSeconds(final Long milliseconds) {
      final StringBuilder buffer = new StringBuilder(StringUtils.EMPTY_STRING);

      if (milliseconds != null) {
        long millisecondsRemaining = milliseconds;

        final long days = TimeUnit.MILLISECONDS.toDays(millisecondsRemaining);

        millisecondsRemaining -= TimeUnit.DAYS.toMillis(days);

        final long hours = TimeUnit.MILLISECONDS.toHours(millisecondsRemaining);

        millisecondsRemaining -= TimeUnit.HOURS.toMillis(hours);

        final long minutes = TimeUnit.MILLISECONDS.toMinutes(millisecondsRemaining);

        millisecondsRemaining -= TimeUnit.MINUTES.toMillis(minutes);

        final long seconds = TimeUnit.MILLISECONDS.toSeconds(millisecondsRemaining);

        if (days > 0) {
          buffer.append(days).append(days > 1 ? " days " : " day ");
        }

        if (hours > 0) {
          buffer.append(hours).append(hours> 1 ? " hours " : " hour ");
        }

        if (minutes > 0) {
          buffer.append(minutes).append(minutes > 1 ? " minutes " : " minute ");
        }

        buffer.append(seconds).append(seconds == 0 || seconds > 1 ? " seconds" : " second");
      }

      return buffer.toString();
    }

    @SuppressWarnings("unchecked")
    protected ServiceState(final Status status,
                           final String statusMessage,
                           final long timestamp,
                           final String serviceLocation,
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
      assert status != null : "The status of the GemFire service cannot be null!";
      this.status = status;
      this.statusMessage = statusMessage;
      this.timestamp = new Timestamp(timestamp);
      this.serviceLocation = serviceLocation;
      this.pid = pid;
      this.uptime = uptime;
      this.workingDirectory = workingDirectory;
      this.jvmArguments = ObjectUtils.defaultIfNull(Collections.unmodifiableList(jvmArguments),
        Collections.<String>emptyList());
      this.classpath = classpath;
      this.gemfireVersion = gemfireVersion;
      this.javaVersion = javaVersion;
      this.logFile = logFile;
      this.host = host;
      this.port = port;
      this.memberName = memberName;
    }

    /**
     * Marshals this state object into a JSON String.
     *
     * @return a String value containing the JSON representation of this state object.
     */
    public String toJson() {
      final Map<String, Object> map = new HashMap<String, Object>();
      map.put(JSON_CLASSPATH, getClasspath());
      map.put(JSON_GEMFIREVERSION, getGemFireVersion());
      map.put(JSON_HOST, getHost());
      map.put(JSON_JAVAVERSION, getJavaVersion());
      map.put(JSON_JVMARGUMENTS, getJvmArguments());
      map.put(JSON_LOCATION, getServiceLocation());
      map.put(JSON_LOGFILE, getLogFile());
      map.put(JSON_MEMBERNAME, getMemberName());
      map.put(JSON_PID, getPid());
      map.put(JSON_PORT, getPort());
      map.put(JSON_STATUS, getStatus().getDescription());
      map.put(JSON_STATUSMESSAGE, getStatusMessage());
      map.put(JSON_TIMESTAMP, getTimestamp().getTime());
      map.put(JSON_UPTIME, getUptime());
      map.put(JSON_WORKINGDIRECTORY, getWorkingDirectory());
      return new GfJsonObject(map).toString();
    }

    /**
     * Gets the Java classpath used when launching the GemFire service.
     * 
     * @return a String value indicating the Java classpath used when launching the GemFire service.
     * @see java.lang.System#getProperty(String) with 'java.class.path'
     */
    public String getClasspath() {
      return classpath;
    }

    /**
     * Gets the version of GemFire used to launch and run the GemFire service.
     * 
     * @return a String indicating the version of GemFire used in the running GemFire service.
     */
    public String getGemFireVersion() {
      return gemfireVersion;
    }

    /**
     * Gets the version of Java used to launch and run the GemFire service.
     * 
     * @return a String indicating the version of the Java runtime used in the running GemFire service.
     * @see java.lang.System#getProperty(String) with 'java.verson'
     */
    public String getJavaVersion() {
      return javaVersion;
    }

    /**
     * Gets the arguments passed to the JVM process that is running the GemFire service.
     * 
     * @return a List of String value each representing an argument passed to the JVM of the GemFire service.
     * @see java.lang.management.RuntimeMXBean#getInputArguments()
     */
    public List<String> getJvmArguments() {
      return jvmArguments;
    }

    /**
     * Gets GemFire member's name for the process.
     * 
     * @return a String indicating the GemFire member's name for the process.
     */
    public String getMemberName() {
      return this.memberName;
    }

    /**
     * Gets the process ID of the running GemFire service if known, otherwise returns null.
     * 
     * @return a integer value indicating the process ID (PID) of the running GemFire service, or null if the PID
     * cannot be determined.
     */
    public Integer getPid() {
      return pid;
    }

    /**
     * Gets the location of the GemFire service (usually the host in combination with the port).
     * 
     * @return a String indication the location (such as host/port) of the GemFire service.
     */
    public String getServiceLocation() {
      return this.serviceLocation;
    }

    /**
     * Gets the name of the GemFire service.
     * 
     * @return a String indicating the name of the GemFire service.
     */
    protected abstract String getServiceName();

    /**
     * Gets the state of the GemFire service.
     * 
     * @return a Status enumerated type representing the state of the GemFire service.
     * @see org.apache.geode.distributed.AbstractLauncher.Status
     */
    public Status getStatus() {
      return status;
    }

    /**
     * Gets description of the the service's current state.
     * 
     * @return a String describing the service's current state.
     */
    public String getStatusMessage() {
      return statusMessage;
    }

    /**
     * The date and time the GemFire service was last in this state.
     * 
     * @return a Timestamp signifying the last date and time the GemFire service was in this state.
     * @see java.sql.Timestamp
     */
    public Timestamp getTimestamp() {
      return (Timestamp) timestamp.clone();
    }

    /**
     * Gets the amount of time in milliseconds that the JVM process with the GemFire service has been running.
     * 
     * @return a long value indicating the number of milliseconds that the GemFire service JVM has been running.
     * @see java.lang.management.RuntimeMXBean#getUptime()
     */
    public Long getUptime() {
      return uptime;
    }

    /**
     * Gets the directory in which the GemFire service is running.  This is also the location where all GemFire service
     * files (log files, the PID file, and so on) are written.
     * 
     * @return a String value indicating the GemFire service's working (running) directory.
     */
    public String getWorkingDirectory() {
      return ObjectUtils.defaultIfNull(workingDirectory, DEFAULT_WORKING_DIRECTORY);
    }

    /**
     * Gets the path of the log file for the process.
     * 
     * @return a String value indicating the path of the log file for the process.
     */
    public String getLogFile() {
      return this.logFile;
    }

    /**
     * Gets the host or IP address for the process and its service.
     * 
     * @return a String value representing the host or IP address for the process and its service.
     */
    public String getHost() {
      return this.host;
    }

    /**
     * Gets the port for the process and its service.
     * 
     * @return an Integer value indicating the port for the process and its service.
     */
    public String getPort() {
      return this.port;
    }

    /**
     * Gets a String describing the state of the GemFire service.
     * 
     * @return a String describing the state of the GemFire service.
     */
    @Override
    public String toString() {
      switch (getStatus()) {
        case STARTING:
          return LocalizedStrings.Launcher_ServiceStatus_STARTING_MESSAGE.toLocalizedString(
            getServiceName(),
            getWorkingDirectory(),
            getServiceLocation(),
            getMemberName(),
            toString(getTimestamp()),
            toString(getPid()),
            toString(getGemFireVersion()),
            toString(getJavaVersion()),
            getLogFile(),
            toString(getJvmArguments().toArray()),
            toString(getClasspath()));
        case ONLINE:
          return LocalizedStrings.Launcher_ServiceStatus_RUNNING_MESSAGE.toLocalizedString(
            getServiceName(),
            getWorkingDirectory(),
            getServiceLocation(),
            getMemberName(),
            getStatus(),
            toString(getPid()),
            toDaysHoursMinutesSeconds(getUptime()),
            toString(getGemFireVersion()),
            toString(getJavaVersion()),
            getLogFile(),
            toString(getJvmArguments().toArray()),
            toString(getClasspath()));
        case STOPPED:
          return LocalizedStrings.Launcher_ServiceStatus_STOPPED_MESSAGE.toLocalizedString(
            getServiceName(),
            getWorkingDirectory(),
            getServiceLocation());
        default: // NOT_RESPONDING
          return LocalizedStrings.Launcher_ServiceStatus_MESSAGE.toLocalizedString(
            getServiceName(),
            getWorkingDirectory(),
            getServiceLocation(),
            getStatus());
      }
    }

    // a timestamp (date/time) formatted as MM/dd/yyyy hh:mm a
    protected String toString(final Date dateTime) {
      return format(dateTime);
    }

    // the value of a Number as a String, or "" if null
    protected String toString(final Number value) {
      return StringUtils.valueOf(value, "");
    }

    // a String concatenation of all values separated by " "
    protected String toString (final Object... values) {
      return StringUtils.concat(values, " ");
    }

    // the value of the String, or "" if value is null
    protected String toString(final String value) {
      return ObjectUtils.defaultIfNull(value, "");
    }
  }

  /**
   * The Status enumerated type represents the various lifecycle states of a GemFire service (such as a Cache Server,
   * a Locator or a Manager).
   */
  public static enum Status {
    NOT_RESPONDING(LocalizedStrings.Launcher_Status_NOT_RESPONDING.toLocalizedString()),
    ONLINE(LocalizedStrings.Launcher_Status_ONLINE.toLocalizedString()),
    STARTING(LocalizedStrings.Launcher_Status_STARTING.toLocalizedString()),
    STOPPED(LocalizedStrings.Launcher_Status_STOPPED.toLocalizedString());

    private final String description;

    Status(final String description) {
      assert !StringUtils.isBlank(description) : "The Status description must be specified!";
      this.description = StringUtils.toLowerCase(description);
    }

    /**
     * Looks up the Status enum type by description.  The lookup operation is case-insensitive.
     * 
     * @param description a String value describing the Locator's status.
     * @return a Status enumerated type matching the description.
     */
    public static Status valueOfDescription(final String description) {
      for (Status status : values()) {
        if (status.getDescription().equalsIgnoreCase(description)) {
          return status;
        }
      }

      return null;
    }

    /**
     * Gets the description of the Status enum type.
     * 
     * @return a String describing the Status enum type.
     */
    public String getDescription() {
      return description;
    }

    /**
     * Gets a String representation of the Status enum type.
     * 
     * @return a String representing the Status enum type.
     * @see #getDescription()
     */
    @Override
    public String toString() {
      return getDescription();
    }
  }

}
