/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.launcher;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.geode.internal.process.ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX;
import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.apache.logging.log4j.util.Strings.isBlank;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.process.ConnectionFailedException;
import org.apache.geode.internal.process.ControlNotificationHandler;
import org.apache.geode.internal.process.ControllableProcess;
import org.apache.geode.internal.process.FileAlreadyExistsException;
import org.apache.geode.internal.process.MBeanInvocationFailedException;
import org.apache.geode.internal.process.PidControllableProcess;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessController;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.internal.process.ProcessControllerParameters;
import org.apache.geode.internal.process.ProcessLauncherContext;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.process.UnableToControlProcessException;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.management.internal.util.JsonUtil;
import org.apache.geode.services.bootstrapping.BootstrappingService;
import org.apache.geode.services.bootstrapping.internal.impl.BootstrappingServiceImpl;
import org.apache.geode.services.management.ManagementService;
import org.apache.geode.services.management.impl.ComponentIdentifier;
import org.apache.geode.services.management.impl.ManagementServiceImpl;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.module.impl.JBossModuleServiceImpl;
import org.apache.geode.services.result.ServiceResult;

public class ModuleServerLauncher implements ServerLauncherConfig {

  private static final String DEFAULT_SERVER_LOG_EXT = ".log";
  private static final String DEFAULT_SERVER_LOG_NAME = "gemfire";
  private static final String GEODE_CORE_DEPENDENCIES_PATH =
      "/Users/patrickjohnson/projects/geode/geode-assembly/build/install/apache-geode/lib/geode-core-dependencies.jar";
  // System.getProperty("user.dir")
  // + "/geode-assembly/build/install/apache-geode/lib/geode-core-dependencies.jar";

  private final boolean assignBuckets;
  private final boolean debug;
  private final boolean deletePidFileOnStop;
  private final boolean disableDefaultServer;
  private final boolean force;
  private final Integer pid;
  private final boolean rebalance;
  private final boolean redirectOutput;
  private final String hostNameForClients;
  private final String memberName;
  private final String springXmlLocation;
  private final InetAddress serverBindAddress;
  private final Integer serverPort;
  private final String workingDirectory;
  private final Float criticalHeapPercentage;
  private final Float evictionHeapPercentage;
  private final Float criticalOffHeapPercentage;
  private final Float evictionOffHeapPercentage;
  private final Integer maxConnections;
  private final Integer maxMessageCount;
  private final Integer maxThreads;
  private final Integer messageTimeToLive;
  private final Integer socketBufferSize;
  private final Properties properties;
  private final AtomicBoolean starting = new AtomicBoolean(false);
  private final transient AtomicBoolean running = new AtomicBoolean(false);
  private ManagementService managementService;
  private ControllableProcess process;
  private String status;

  private ModuleServerLauncher(Builder builder) {

    assignBuckets = builder.getAssignBuckets();
    disableDefaultServer = builder.getDisableDefaultServer();
    force = builder.getForce();
    rebalance = builder.getRebalance();
    redirectOutput = builder.getRedirectOutput();
    hostNameForClients = builder.getHostNameForClients();
    memberName = builder.getMemberName();
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
    // help = builder.getHelp();
    pid = builder.getPid();
    deletePidFileOnStop = builder.getDeletePidFileOnStop();
    debug = builder.getDebug();

    properties = builder.getProperties();
    properties.setProperty("log-file", getLogFileCanonicalPath());
  }

  public static void main(String[] args) {
    new Builder(args).build().start();
  }

  public static ModuleServerLauncher getInstance() {
    ServerLauncherConfig instance = ServerLauncherManager.getInstance();
    if (instance instanceof ModuleServerLauncher) {
      return (ModuleServerLauncher) instance;
    }
    return null;
  }

  private String processPackagesIntoJBossPackagesNames(Set<String> packages) {
    StringBuilder stringBuilder = new StringBuilder();
    packages.forEach(packageName -> stringBuilder.append(packageName + ","));
    if (stringBuilder.length() > 0) {
      stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    }
    return stringBuilder.toString();
  }

  public void start() {
    if (!running.get() && starting.compareAndSet(false, true)) {

      ServerLauncherManager.compareAndSet(null, this);

      try {
        process = getControllableProcess();

        ProcessLauncherContext.set(isRedirectingOutput(), getOverriddenDefaults(),
            (String statusMessage) -> this.status = statusMessage);

        Set<String> packages =
            // GeodeJDKPaths.getListPackagesFromJars(property);
            new TreeSet<>();
        packages.add("javax.management");
        packages.add("java.lang.management");

        System.setProperty("jboss.modules.system.pkgs",
            processPackagesIntoJBossPackagesNames(packages));

        Logger logger = LogManager.getLogger();
        ModuleService moduleService = new JBossModuleServiceImpl(logger);

        BootstrappingService bootstrappingService = new BootstrappingServiceImpl();
        bootstrappingService.init(moduleService, logger);

        managementService = new ManagementServiceImpl(bootstrappingService, logger);

        // properties.setProperty("mcast-port", "0");
        // properties.setProperty("start-locator", "localhost[10334]");
        // properties.setProperty("jmx-manager-start", "true");
        // properties.setProperty("jmx-manager", "true");

        ServiceResult<Boolean> cache =
            managementService.createComponent(
                new ComponentIdentifier("Cache", GEODE_CORE_DEPENDENCIES_PATH), properties);

        if (cache.isSuccessful()) {
          running.set(true);
        } else {
          logger.error(cache.getErrorMessage());
        }
        starting.set(false);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new IllegalStateException(
          String.format("A server is already running in %s on %s.",
              getWorkingDirectory(), getServerBindAddress() + "[" + getServerPort() + "]"));
    }
  }

  public void stop() {
    if (running.get()) {

      managementService
          .closeComponent(new ComponentIdentifier("Cache", GEODE_CORE_DEPENDENCIES_PATH));

      Thread t = new LoggingThread("ModuleServerLauncherStopper", false,
          () -> process.stop(deletePidFileOnStop));
      t.start();

      try {
        t.join();
      } catch (InterruptedException ignore) {
        // no matter, we're shutting down...
      }
      running.set(false);
      ServerLauncherManager.compareAndSet(this, null);
    }
  }

  protected void debug(final String message, final Object... args) {
    if (isDebugging()) {
      if (args != null && args.length > 0) {
        System.err.printf(message, args);
      } else {
        System.err.print(message);
      }
    }
  }

  private Properties getOverriddenDefaults() {
    final Properties overriddenDefaults = new Properties();

    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith(OVERRIDDEN_DEFAULTS_PREFIX)) {
        overriddenDefaults.setProperty(key, System.getProperty(key));
      }
    }

    return overriddenDefaults;
  }

  @Override
  public ServiceInfo status() {

    ModuleServerLauncher launcher = getInstance();

    if (starting.get() || running.get()) {
      return new ServerState(this, running.get() ? Status.ONLINE : Status.STARTING);
    }
    if (isPidInProcess() && launcher != null) {
      return launcher.statusInProcess();
    }
    if (pid != null) {
      return statusWithPid();
    }
    // attempt to get status using workingDirectory
    if (getWorkingDirectory() != null) {
      return statusWithWorkingDirectory();
    }

    return new ServerState(this, Status.NOT_RESPONDING);
  }

  private ServerState statusWithPid() {
    try {
      final ProcessController controller = new ProcessControllerFactory()
          .createProcessController(new ServerControllerParameters(), pid);
      controller.checkPidSupport();
      final String statusJson = controller.status();
      return ServerState.fromJson(statusJson);
    } catch (ConnectionFailedException handled) {
      // failed to attach to server JVM
      return createNoResponseState(handled,
          "Failed to connect to server with process id " + pid);
    } catch (IOException | MBeanInvocationFailedException | UnableToControlProcessException
        | InterruptedException | TimeoutException handled) {
      return createNoResponseState(handled,
          "Failed to communicate with server with process id " + pid);
    }
  }

  private ServerState statusWithWorkingDirectory() {
    int parsedPid = 0;
    try {
      final ProcessController controller =
          new ProcessControllerFactory().createProcessController(new ServerControllerParameters(),
              new File(getWorkingDirectory()), ProcessType.SERVER.getPidFileName());
      parsedPid = controller.getProcessId();
      // note: in-process request will go infinite loop unless we do the following
      if (parsedPid == identifyPid()) {
        final ModuleServerLauncher runningLauncher = getInstance();
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

  public String getLogFileName() {
    return defaultIfBlank(getMemberName(), DEFAULT_SERVER_LOG_NAME).concat(DEFAULT_SERVER_LOG_EXT);
  }

  private ControllableProcess getControllableProcess()
      throws IOException, PidUnavailableException, FileAlreadyExistsException {
    return new PidControllableProcess(new File(getWorkingDirectory()),
        ProcessType.SERVER, isForcing(), new ControlNotificationHandler<ServiceInfo>() {
          @Override
          public void handleStop() {

        }

          @Override
          public ServiceInfo handleStatus() {
            return statusInProcess();
          }
        });
  }

  private ServerState createNoResponseState(Exception cause, String errorMessage) {
    debug(ExceptionUtils.getStackTrace(cause) + errorMessage);
    return new ServerState(this, Status.NOT_RESPONDING, errorMessage);
  }

  private ServerState statusInProcess() {
    if (starting.get() || running.get()) {
      return new ServerState(this, running.get() ? Status.ONLINE : Status.STARTING);
    }
    return new ServerState(this, Status.NOT_RESPONDING);
  }

  int identifyPidOrNot() {
    try {
      return ProcessUtils.identifyPid();
    } catch (PidUnavailableException handled) {
      return -1;
    }
  }

  public String getLogFileCanonicalPath() {
    try {
      return new File(getWorkingDirectory(), getLogFileName()).getCanonicalPath();
    } catch (IOException e) {
      return getLogFileName();
    }
  }

  boolean isPidInProcess() {
    return pid != null && pid == identifyPidOrNot();
  }

  @Override
  public boolean isAssignBuckets() {
    return assignBuckets;
  }

  @Override
  public boolean isDebugging() {
    return debug;
  }

  @Override
  public boolean isDisableDefaultServer() {
    return disableDefaultServer;
  }

  @Override
  public boolean isForcing() {
    return force;
  }

  @Override
  public boolean isRebalancing() {
    return rebalance;
  }

  @Override
  public boolean isRedirectingOutput() {
    return redirectOutput;
  }

  @Override
  public String getHostNameForClients() {
    return hostNameForClients;
  }

  @Override
  public String getMemberName() {
    return memberName;
  }

  @Override
  public InetAddress getServerBindAddress() {
    return serverBindAddress;
  }

  @Override
  public Integer getServerPort() {
    return serverPort;
  }

  @Override
  public String getSpringXmlLocation() {
    return springXmlLocation;
  }

  @Override
  public Float getCriticalHeapPercentage() {
    return criticalHeapPercentage;
  }

  @Override
  public Float getEvictionHeapPercentage() {
    return evictionHeapPercentage;
  }

  @Override
  public Float getCriticalOffHeapPercentage() {
    return criticalOffHeapPercentage;
  }

  @Override
  public Float getEvictionOffHeapPercentage() {
    return evictionOffHeapPercentage;
  }

  @Override
  public Integer getMaxConnections() {
    return maxConnections;
  }

  @Override
  public Integer getMaxMessageCount() {
    return maxMessageCount;
  }

  @Override
  public Integer getMaxThreads() {
    return maxThreads;
  }

  @Override
  public Integer getMessageTimeToLive() {
    return messageTimeToLive;
  }

  @Override
  public Integer getSocketBufferSize() {
    return socketBufferSize;
  }

  @Override
  public boolean isSpringXmlLocationSpecified() {
    return isNotBlank(springXmlLocation);
  }

  @Override
  public String getWorkingDirectory() {
    return workingDirectory;
  }

  public static class ServerState implements ServiceInfo {

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

    static final String TO_STRING_PROCESS_ID = "Process ID: ";
    static final String TO_STRING_JAVA_VERSION = "Java Version: ";
    static final String TO_STRING_LOG_FILE = "Log File: ";
    static final String TO_STRING_JVM_ARGUMENTS = "JVM Arguments: ";
    static final String TO_STRING_CLASS_PATH = "Class-Path: ";
    static final String TO_STRING_UPTIME = "Uptime: ";
    static final String TO_STRING_GEODE_VERSION = "Geode Version: ";

    private final String serviceLocation;
    private final Integer pid;
    private final Long uptime;
    private final String workingDirectory;
    private final List<String> jvmArguments;
    private final String classpath;
    private final String gemfireVersion;
    private final String javaVersion;
    private final String memberName;
    private final String port;
    private final String host;
    private final String logFile;
    private final Timestamp timestamp;
    private final Status status;
    private final String statusMessage;

    public ServerState(ModuleServerLauncher launcher, Status status) {
      this(launcher, status, launcher.status);
    }

    public ServerState(ModuleServerLauncher launcher, Status status, String statusMessage) {
      this(status,
          statusMessage,
          System.currentTimeMillis(),
          null,
          launcher.identifyPidOrNot(),
          ManagementFactory.getRuntimeMXBean().getUptime(),
          launcher.getWorkingDirectory(),
          ManagementFactory.getRuntimeMXBean().getInputArguments(),
          System.getProperty("java.class.path"),
          GemFireVersion.getGemFireVersion(),
          System.getProperty("java.version"),
          launcher.getLogFileCanonicalPath(),
          launcher.getHostNameForClients(),
          String.valueOf(launcher.getServerPort()),
          launcher.getMemberName());
    }

    protected ServerState(final Status status, final String statusMessage, final long timestamp,
        final String serviceLocation, final Integer pid, final Long uptime,
        final String workingDirectory, final List<String> jvmArguments, final String classpath,
        final String gemfireVersion, final String javaVersion, final String logFile,
        final String host, final String port, final String memberName) {
      assert status != null : "The status of the GemFire service cannot be null!";
      this.status = status;
      this.statusMessage = statusMessage;
      this.timestamp = new Timestamp(timestamp);
      this.serviceLocation = serviceLocation;
      this.pid = pid;
      this.uptime = uptime;
      this.workingDirectory = workingDirectory;
      this.jvmArguments = Collections.unmodifiableList(jvmArguments);
      this.classpath = classpath;
      this.gemfireVersion = gemfireVersion;
      this.javaVersion = javaVersion;
      this.logFile = logFile;
      this.host = host;
      this.port = port;
      this.memberName = memberName;
    }

    public static ServerState fromJson(final String json) {
      try {
        final JsonNode jsonObject = new ObjectMapper().readTree(json);

        final Status status = Status.valueOfDescription(jsonObject.get(JSON_STATUS).asText());
        final List<String> jvmArguments = JsonUtil.toStringList(jsonObject.get("jvmArguments"));

        return new ServerState(status, jsonObject.get(JSON_STATUSMESSAGE).asText(),
            jsonObject.get(JSON_TIMESTAMP).asLong(), jsonObject.get(JSON_LOCATION).asText(),
            jsonObject.get(JSON_PID).asInt(), jsonObject.get(JSON_UPTIME).asLong(),
            jsonObject.get(JSON_WORKINGDIRECTORY).asText(), jvmArguments,
            jsonObject.get(JSON_CLASSPATH).asText(), jsonObject.get(JSON_GEMFIREVERSION).asText(),
            jsonObject.get(JSON_JAVAVERSION).asText(), jsonObject.get(JSON_LOGFILE).asText(),
            jsonObject.get(JSON_HOST).asText(), jsonObject.get(JSON_PORT).asText(),
            jsonObject.get(JSON_MEMBERNAME).asText());
      } catch (Exception e) {
        e.printStackTrace();
        throw new IllegalArgumentException("Unable to create ServerStatus from JSON: " + json, e);
      }
    }

    protected static String toDaysHoursMinutesSeconds(final Long milliseconds) {
      final StringBuilder buffer = new StringBuilder();

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
          buffer.append(hours).append(hours > 1 ? " hours " : " hour ");
        }

        if (minutes > 0) {
          buffer.append(minutes).append(minutes > 1 ? " minutes " : " minute ");
        }

        buffer.append(seconds).append(seconds == 0 || seconds > 1 ? " seconds" : " second");
      }

      return buffer.toString();
    }

    @Override
    public boolean isStartingOrNotResponding() {
      return status == Status.STARTING || status == Status.NOT_RESPONDING;
    }

    @Override
    public String getStatusMessage() {
      return statusMessage;
    }

    public String toJson() {
      final Map<String, Object> map = new HashMap<>();
      map.put(JSON_CLASSPATH, classpath);
      map.put(JSON_GEMFIREVERSION, gemfireVersion);
      map.put(JSON_HOST, host);
      map.put(JSON_JAVAVERSION, javaVersion);
      map.put(JSON_JVMARGUMENTS, jvmArguments);
      map.put(JSON_LOCATION, serviceLocation);
      map.put(JSON_LOGFILE, logFile);
      map.put(JSON_MEMBERNAME, memberName);
      map.put(JSON_PID, pid);
      map.put(JSON_PORT, port);
      map.put(JSON_STATUS, status.getDescription());
      map.put(JSON_STATUSMESSAGE, getStatusMessage());
      map.put(JSON_TIMESTAMP, timestamp.getTime());
      map.put(JSON_UPTIME, uptime);
      map.put(JSON_WORKINGDIRECTORY, workingDirectory);

      String jsonStatus = null;
      try {
        jsonStatus = new ObjectMapper().writeValueAsString(map);
      } catch (JsonProcessingException e) {
        // Ignored
      }

      return jsonStatus;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      switch (status) {
        case STARTING:
          sb.append("Starting %s in %s on %s as %s at %s").append(System.lineSeparator());
          sb.append(TO_STRING_PROCESS_ID).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_JAVA_VERSION).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_LOG_FILE).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_JVM_ARGUMENTS).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_CLASS_PATH).append("%s");
          return String.format(sb.toString(),
              "Server", workingDirectory, serviceLocation, memberName, timestamp, pid,
              gemfireVersion, javaVersion, logFile, String.join(" ", jvmArguments), classpath);

        case ONLINE:
          sb.append("%s in %s on %s as %s is currently %s.").append(System.lineSeparator());
          sb.append(TO_STRING_PROCESS_ID).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_UPTIME).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_GEODE_VERSION).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_JAVA_VERSION).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_LOG_FILE).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_JVM_ARGUMENTS).append("%s").append(System.lineSeparator());
          sb.append(TO_STRING_CLASS_PATH).append("%s");
          return String.format(sb.toString(),
              "Server", workingDirectory, serviceLocation, memberName, status, pid,
              toDaysHoursMinutesSeconds(uptime), gemfireVersion, javaVersion, logFile,
              String.join(" ", jvmArguments), classpath);

        case STOPPED:
          sb.append("%s in %s on %s has been requested to stop.");
          return String.format(sb.toString(),
              "Server", workingDirectory, serviceLocation);

        default: // NOT_RESPONDING
          sb.append("%s in %s on %s is currently %s.");
          return String.format(sb.toString(), "Server",
              workingDirectory, serviceLocation, status);
      }
    }
  }

  public static class Builder implements ServerLauncherConfig.Builder {

    private final Properties properties = new Properties();
    private boolean assignBuckets;
    private boolean debug;
    private boolean disableDefaultServer;
    private boolean force;
    private boolean rebalance;
    private boolean redirectOutput;
    private InetAddress serverBindAddress;
    private Integer serverPort;
    private String springXmlLocation;
    private String workingDirectory;
    private Float criticalHeapPercentage;
    private Float evictionHeapPercentage;
    private Float criticalOffHeapPercentage;
    private Float evictionOffHeapPercentage;
    private Integer maxConnections;
    private Integer maxMessageCount;
    private Integer maxThreads;
    private Integer messageTimeToLive;
    private Integer socketBufferSize;
    private String hostNameForClients;
    private String memberName;
    private boolean deletePidFileOnStop;
    private Integer pid;

    public Builder() {

    }

    public Builder(String... args) {
      parseArguments(args);
    }

    private void parseArguments(final String... args) {
      try {
        OptionSet options = getParser().parse(args);

        // parseCommand(args);

        setMemberName(System.getProperty("gemfire.name"));

        setAssignBuckets(options.has("assign-buckets"));
        setDebug(options.has("debug"));
        setDeletePidFileOnStop(options.has("delete-pid-file-on-stop"));
        setDisableDefaultServer(options.has("disable-default-server"));
        setForce(options.has("force"));

        // setHelp(options.has("help"));
        setRebalance(options.has("rebalance"));
        setRedirectOutput(options.has("redirect-output"));

        if (options.hasArgument("critical-heap-percentage")) {
          setCriticalHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf("critical-heap-percentage"))));
        }

        if (options.hasArgument("eviction-heap-percentage")) {
          setEvictionHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf("eviction-heap-percentage"))));
        }

        if (options.hasArgument("critical-off-heap-percentage")) {
          setCriticalOffHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf("critical-off-heap-percentage"))));
        }

        if (options.hasArgument("eviction-off-heap-percentage")) {
          setEvictionOffHeapPercentage(Float.parseFloat(ObjectUtils
              .toString(options.valueOf("eviction-off-heap-percentage"))));
        }

        if (options.hasArgument("max-connections")) {
          setMaxConnections(Integer.parseInt(
              ObjectUtils.toString(options.valueOf("max-connections"))));
        }

        if (options.hasArgument("max-message-count")) {
          setMaxMessageCount(Integer.parseInt(
              ObjectUtils.toString(options.valueOf("max-message-count"))));
        }

        if (options.hasArgument("message-time-to-live")) {
          setMessageTimeToLive(Integer.parseInt(ObjectUtils
              .toString(options.valueOf("message-time-to-live"))));
        }

        if (options.hasArgument("socket-buffer-size")) {
          setSocketBufferSize(Integer.parseInt(ObjectUtils
              .toString(options.valueOf("socket-buffer-size"))));
        }

        if (options.hasArgument("max-threads")) {
          setMaxThreads(Integer.parseInt(
              ObjectUtils.toString(options.valueOf("max-threads"))));
        }

        // if (!isHelping()) {
        // if (options.has("dir")) {
        // setWorkingDirectory(ObjectUtils.toString(options.valueOf("dir")));
        // }
        //
        // if (options.has("pid")) {
        // setPid((Integer) options.valueOf("pid"));
        // }
        //
        // if (options.has("server-bind-address")) {
        // setServerBindAddress(ObjectUtils.toString(options.valueOf("server-bind-address")));
        // }
        //
        // if (options.has("server-port")) {
        // setServerPort((Integer) options.valueOf("server-port"));
        // }
        //
        // if (options.has("spring-xml-location")) {
        // setSpringXmlLocation(ObjectUtils.toString(options.valueOf("spring-xml-location")));
        // }
        //
        // if (options.has("version")) {
        // setCommand(Command.VERSION);
        // }
        //
        // if (options.hasArgument("critical-heap-percentage")) {
        // setCriticalHeapPercentage(Float.parseFloat(ObjectUtils
        // .toString(options.valueOf("critical-heap-percentage"))));
        // }
        //
        // if (options.hasArgument("eviction-heap-percentage")) {
        // setEvictionHeapPercentage(Float.parseFloat(ObjectUtils
        // .toString(options.valueOf("eviction-heap-percentage"))));
        // }
        //
        // if (options.hasArgument("max-connections")) {
        // setMaxConnections(Integer.parseInt(
        // ObjectUtils.toString(options.valueOf("max-connections"))));
        // }
        //
        // if (options.hasArgument("max-message-count")) {
        // setMaxMessageCount(Integer.parseInt(
        // ObjectUtils.toString(options.valueOf("max-message-count"))));
        // }
        //
        // if (options.hasArgument("max-threads")) {
        // setMaxThreads(Integer.parseInt(
        // ObjectUtils.toString(options.valueOf("max-threads"))));
        // }
        //
        // if (options.hasArgument("message-time-to-live")) {
        // setMessageTimeToLive(Integer.parseInt(ObjectUtils
        // .toString(options.valueOf("message-time-to-live"))));
        // }
        //
        // if (options.hasArgument("socket-buffer-size")) {
        // setSocketBufferSize(Integer.parseInt(ObjectUtils
        // .toString(options.valueOf("socket-buffer-size"))));
        // }
        //
        // if (options.hasArgument("hostname-for-clients")) {
        // setHostNameForClients(ObjectUtils
        // .toString(options.valueOf("hostname-for-clients")));
        // }
        // }
      } catch (OptionException e) {
        throw new IllegalArgumentException(
            String.format("An error occurred while parsing command-line arguments for the %s: %s",
                "Server", e.getMessage()),
            e);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }

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
      parser.accepts("critical-heap-percentage").withRequiredArg()
          .ofType(Float.class);
      parser.accepts("eviction-heap-percentage").withRequiredArg()
          .ofType(Float.class);
      parser.accepts("critical-off-heap-percentage").withRequiredArg()
          .ofType(Float.class);
      parser.accepts("eviction-off-heap-percentage").withRequiredArg()
          .ofType(Float.class);
      parser.accepts("max-connections").withRequiredArg()
          .ofType(Integer.class);
      parser.accepts("max-message-count").withRequiredArg()
          .ofType(Integer.class);
      parser.accepts("max-threads").withRequiredArg().ofType(Integer.class);
      parser.accepts("message-time-to-live").withRequiredArg()
          .ofType(Integer.class);
      parser.accepts("socket-buffer-size").withRequiredArg()
          .ofType(Integer.class);
      parser.accepts("hostname-for-clients").withRequiredArg()
          .ofType(String.class);

      return parser;
    }

    protected Properties getProperties() {
      return properties;
    }

    @Override
    public Boolean getAssignBuckets() {
      return assignBuckets;
    }

    @Override
    public Builder setAssignBuckets(Boolean assignBuckets) {
      this.assignBuckets = assignBuckets;
      return this;
    }

    @Override
    public Boolean getDisableDefaultServer() {
      return disableDefaultServer;
    }

    @Override
    public Builder setDisableDefaultServer(Boolean disableDefaultServer) {
      this.disableDefaultServer = disableDefaultServer;
      return this;
    }

    @Override
    public Boolean getForce() {
      return force;
    }

    @Override
    public Builder setForce(Boolean force) {
      this.force = force;
      return this;
    }

    @Override
    public Integer getPid() {
      return pid;
    }

    @Override
    public Builder setPid(Integer pid) {
      this.pid = pid;
      return this;
    }

    @Override
    public Boolean getRebalance() {
      return rebalance;
    }

    @Override
    public Builder setRebalance(Boolean rebalance) {
      this.rebalance = rebalance;
      return this;
    }

    @Override
    public Boolean getRedirectOutput() {
      return redirectOutput;
    }

    @Override
    public Builder setRedirectOutput(Boolean redirectOutput) {
      this.redirectOutput = redirectOutput;
      return this;
    }

    @Override
    public InetAddress getServerBindAddress() {
      return serverBindAddress;
    }

    @Override
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
          properties.setProperty("bind-address", bindAddress.toString());
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

    @Override
    public Integer getServerPort() {
      return serverPort;
    }

    @Override
    public Builder setServerPort(Integer serverPort) {
      this.serverPort = serverPort;
      return this;
    }

    @Override
    public String getSpringXmlLocation() {
      return springXmlLocation;
    }

    @Override
    public Builder setSpringXmlLocation(String springXmlLocation) {
      this.springXmlLocation = springXmlLocation;
      // properties.setProperty("spring-xml-location", springXmlLocation);
      return this;
    }

    @Override
    public String getWorkingDirectory() {
      File workingDir = new File(defaultIfBlank(workingDirectory, System.getProperty("user.dir")));
      try {
        return workingDir.getCanonicalPath();
      } catch (Exception e) {
        return workingDir.getAbsolutePath();
      }
    }

    @Override
    public Builder setWorkingDirectory(String workingDirectory) {
      if (!new File(defaultIfBlank(workingDirectory, System.getProperty("user.dir")))
          .isDirectory()) {
        throw new IllegalArgumentException("Working directory is invalid",
            new FileNotFoundException(workingDirectory));
      }
      this.workingDirectory = workingDirectory;
      return this;
    }

    @Override
    public Float getCriticalHeapPercentage() {
      return criticalHeapPercentage;
    }

    @Override
    public Builder setCriticalHeapPercentage(Float criticalHeapPercentage) {
      this.criticalHeapPercentage = criticalHeapPercentage;
      return this;
    }

    @Override
    public Float getEvictionHeapPercentage() {
      return evictionHeapPercentage;
    }

    @Override
    public Builder setEvictionHeapPercentage(Float evictionHeapPercentage) {
      this.evictionHeapPercentage = evictionHeapPercentage;
      return this;
    }

    @Override
    public Float getCriticalOffHeapPercentage() {
      return criticalOffHeapPercentage;
    }

    @Override
    public Builder setCriticalOffHeapPercentage(Float criticalOffHeapPercentage) {
      this.criticalOffHeapPercentage = criticalOffHeapPercentage;
      return this;
    }

    @Override
    public Float getEvictionOffHeapPercentage() {
      return evictionOffHeapPercentage;
    }

    @Override
    public Builder setEvictionOffHeapPercentage(Float evictionOffHeapPercentage) {
      this.evictionOffHeapPercentage = evictionOffHeapPercentage;
      return this;
    }

    @Override
    public Integer getMaxConnections() {
      return maxConnections;
    }

    @Override
    public Builder setMaxConnections(Integer maxConnections) {
      this.maxConnections = maxConnections;
      return this;
    }

    @Override
    public Integer getMaxMessageCount() {
      return maxMessageCount;
    }

    @Override
    public Builder setMaxMessageCount(Integer maxMessageCount) {
      this.maxMessageCount = maxMessageCount;
      return this;
    }

    @Override
    public Integer getMaxThreads() {
      return maxThreads;
    }

    @Override
    public Builder setMaxThreads(Integer maxThreads) {
      this.maxThreads = maxThreads;
      return this;
    }

    @Override
    public Integer getMessageTimeToLive() {
      return messageTimeToLive;
    }

    @Override
    public Builder setMessageTimeToLive(Integer messageTimeToLive) {
      this.messageTimeToLive = messageTimeToLive;
      return this;
    }

    @Override
    public Integer getSocketBufferSize() {
      return socketBufferSize;
    }

    @Override
    public Builder setSocketBufferSize(Integer socketBufferSize) {
      this.socketBufferSize = socketBufferSize;
      if (socketBufferSize != null) {
        properties.setProperty("socket-buffer-size", socketBufferSize.toString());
      }
      return this;
    }

    @Override
    public String getHostNameForClients() {
      return hostNameForClients;
    }

    @Override
    public Builder setHostNameForClients(String hostNameForClients) {
      this.hostNameForClients = hostNameForClients;
      return this;
    }

    @Override
    public String getMemberName() {
      return memberName;
    }

    @Override
    public Builder setMemberName(String memberName) {
      this.memberName = memberName;
      if (memberName != null) {
        properties.setProperty("name", memberName);
      }
      return this;
    }

    @Override
    public ModuleServerLauncher build() {
      return new ModuleServerLauncher(this);
    }

    @Override
    public Boolean getDebug() {
      return debug;
    }

    @Override
    public Builder setDebug(Boolean debug) {
      this.debug = debug;
      return this;
    }

    @Override
    public Boolean getDeletePidFileOnStop() {
      return deletePidFileOnStop;
    }

    @Override
    public Builder setDeletePidFileOnStop(Boolean deletePidFileOnStop) {
      this.deletePidFileOnStop = deletePidFileOnStop;
      return this;
    }
  }

  private class ServerControllerParameters implements ProcessControllerParameters {
    @Override
    public File getPidFile() {
      return new File(getWorkingDirectory(), ProcessType.SERVER.getPidFileName());
    }

    @Override
    public File getDirectory() {
      return new File(getWorkingDirectory());
    }

    @Override
    public int getProcessId() {
      return pid;
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
}
