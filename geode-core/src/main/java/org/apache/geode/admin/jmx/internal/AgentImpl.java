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
import java.io.IOException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.OperationsException;
import javax.management.ReflectionException;
import javax.management.modelmbean.ModelMBean;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import mx4j.tools.adaptor.http.HttpAdaptor;
import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.LogWriter;
import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.jmx.Agent;
import org.apache.geode.admin.jmx.AgentConfig;
import org.apache.geode.admin.jmx.AgentFactory;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.admin.remote.TailLogResponse;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogWriterFactory;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.logging.internal.LoggingSession;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigListener;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;

/**
 * The GemFire JMX Agent provides the ability to administrate one GemFire distributed system via
 * JMX.
 *
 * @since GemFire 3.5
 */
public class AgentImpl implements org.apache.geode.admin.jmx.Agent,
    org.apache.geode.admin.jmx.internal.ManagedResource, LogConfigSupplier {

  private static final Logger logger = LogService.getLogger();

  /**
   * MX4J HttpAdaptor only supports "basic" as an authentication method. Enabling HttpAdaptor
   * authentication ({@link AgentConfig#HTTP_AUTHENTICATION_ENABLED_NAME}) causes the browser to
   * require a login with username ({@link AgentConfig#HTTP_AUTHENTICATION_USER_NAME}) and password
   * ({@link AgentConfig#HTTP_AUTHENTICATION_PASSWORD_NAME}).
   */
  private static final String MX4J_HTTPADAPTOR_BASIC_AUTHENTICATION = "basic";

  /** JMX Service URL template for JMX/RMI Connector Server */
  private static final String JMX_SERVICE_URL = "service:jmx:rmi://{0}:{1}/jndi/rmi://{2}:{3}{4}";

  /*
   * Set third-party logging configration: MX4J, Jakarta Commons-Logging.
   */
  static {
    checkDebug();
    String commonsLog = System.getProperty("org.apache.commons.logging.log");
    if (commonsLog == null || commonsLog.length() == 0) {
      System.setProperty("org.apache.commons.logging.log",
          "org.apache.commons.logging.impl.SimpleLog");
    }
  }

  /** Enables mx4j tracing if Agent debugging is enabled. */
  private static void checkDebug() {
    try {
      if (Boolean.getBoolean("gfAgentDebug")) {
        mx4j.log.Log.setDefaultPriority(mx4j.log.Logger.TRACE); // .DEBUG
      }
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
      /* ignore */
    }
  }

  // -------------------------------------------------------------------------
  // Member variables
  // -------------------------------------------------------------------------

  /** This Agent's log writer */
  private InternalLogWriter logWriter;

  /** This Agent's JMX http adaptor from MX4J */
  private HttpAdaptor httpAdaptor;

  /** This Agent's RMI Connector Server from MX4J */
  private JMXConnectorServer rmiConnector;

  /** The name of the MBean manages this resource */
  private final String mbeanName;

  /** The ObjectName of the MBean that manages this resource */
  private final ObjectName objectName;

  /** The actual ModelMBean that manages this resource */
  private ModelMBean modelMBean;

  /** The configuration for this Agent */
  private final AgentConfigImpl agentConfig;

  /**
   * The <code>AdminDistributedSystem</code> this Agent is currently connected to or
   * <code>null</code>
   */
  private AdminDistributedSystem system;

  /** The agent's configuration file */
  private String propertyFile;

  /**
   * A lock object to guard the Connect and Disconnect calls being made on the agent for connections
   * to the DS
   **/
  private Object CONN_SYNC = new Object();

  protected MemberInfoWithStatsMBean memberInfoWithStatsMBean;

  private MBeanServer mBeanServer;

  private final LoggingSession loggingSession;
  private final Set<LogConfigListener> logConfigListeners = new HashSet<>();

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs a new Agent using the specified configuration.
   *
   * @param agentConfig instance of configuration for Agent
   * @throws org.apache.geode.admin.AdminException TODO-javadocs
   * @throws IllegalArgumentException if agentConfig is null
   */
  public AgentImpl(AgentConfigImpl agentConfig) throws AdminException, IllegalArgumentException {
    loggingSession = LoggingSession.create();

    shutdownHook = new LoggingThread("Shutdown", false, () -> disconnectFromSystem());
    addShutdownHook();
    if (agentConfig == null) {
      throw new IllegalArgumentException(
          "AgentConfig must not be null");
    }
    this.agentConfig = (AgentConfigImpl) agentConfig;
    this.mbeanName = MBEAN_NAME_PREFIX + MBeanUtil.makeCompliantMBeanNameProperty("Agent");

    try {
      this.objectName = new ObjectName(this.mbeanName);
    } catch (MalformedObjectNameException ex) {
      String s = String.format("While creating ObjectName: %s",
          new Object[] {this.mbeanName});
      throw new AdminException(s, ex);
    }

    this.propertyFile = this.agentConfig.getPropertyFile().getAbsolutePath();

    // bind address only affects how the Agent VM connects to the system...
    // It should be set only once in the agent lifecycle
    this.agentConfig.setBindAddress(getBindAddress());

    // LOG: create LogWriterAppender and LogWriterLogger
    initLogWriter();

    mBeanServer = MBeanUtil.start();

    MBeanUtil.createMBean(this);

    initializeHelperMbean();
  }

  private void initializeHelperMbean() {
    try {
      memberInfoWithStatsMBean = new MemberInfoWithStatsMBean(this);

      MBeanServer mbs = getMBeanServer();
      mbs.registerMBean(memberInfoWithStatsMBean, memberInfoWithStatsMBean.getObjectName());
      /*
       * We are not re-throwing these exceptions as failure create/register the GemFireTypesWrapper
       * will not stop the Agent from working. But we are logging it as it could be an indication of
       * some problem. Also not creating Localized String for the exception.
       */
    } catch (OperationsException e) {
      logger.info("Failed to initialize MemberInfoWithStatsMBean.", e);
    } catch (MBeanRegistrationException e) {
      logger.info("Failed to initialize MemberInfoWithStatsMBean.", e);
    } catch (AdminException e) {
      logger.info("Failed to initialize MemberInfoWithStatsMBean.", e);
    }
  }

  // -------------------------------------------------------------------------
  // Public operations
  // -------------------------------------------------------------------------

  @Override
  public AgentConfig getConfig() {
    return this.agentConfig;
  }

  @Override
  public AdminDistributedSystem getDistributedSystem() {
    return this.system;
  }

  /**
   * Persists the current Agent configuration to its property file.
   *
   * @throws GemFireIOException if unable to persist the configuration to props
   * @see #getPropertyFile
   */
  @Override
  public void saveProperties() {
    throw new GemFireIOException("saveProperties is no longer supported for security reasons");
  }

  /**
   * Starts the jmx agent
   */
  @Override
  public void start() {
    checkDebug();

    this.agentConfig.validate();

    if (mBeanServer == null) {
      mBeanServer = MBeanUtil.start();
    }

    try {
      startHttpAdaptor();
    } catch (StartupException e) {
      loggingSession.stopSession();
      loggingSession.shutdown();
      throw e;
    }

    try {
      startRMIConnectorServer();
    } catch (StartupException e) {
      stopHttpAdaptor();
      loggingSession.stopSession();
      loggingSession.shutdown();
      throw e;
    }

    try {
      startSnmpAdaptor();
    } catch (StartupException e) {
      stopRMIConnectorServer();
      stopHttpAdaptor();
      loggingSession.stopSession();
      loggingSession.shutdown();
      throw e;
    }

    if (this.agentConfig.getAutoConnect()) {
      try {
        connectToSystem();
        /*
         * Call Agent.stop() if connectToSystem() fails. This should clean up agent-DS connection &
         * stop all the HTTP/RMI/SNMP adapters started earlier.
         */
      } catch (AdminException ex) {
        logger.error("auto connect failed:  {}",
            ex.getMessage());
        this.stop();
        throw new StartupException(ex);
      } catch (MalformedObjectNameException ex) {
        String autoConnectFailed = "auto connect failed:  {}";
        logger.error(autoConnectFailed, ex.getMessage());
        this.stop();
        throw new StartupException(new AdminException(
            String.format("auto connect failed: %s", ex.getMessage()), ex));
      }
    } // getAutoConnect

    logger.info("GemFire JMX Agent is running...");

    if (memberInfoWithStatsMBean == null) {
      initializeHelperMbean();
    }
  }

  /**
   * Deregisters everything this Agent registered and releases the MBeanServer.
   */
  @Override
  public void stop() {
    try {
      logger.info("Stopping JMX agent");

      loggingSession.stopSession();

      // stop the GemFire Distributed System
      stopDistributedSystem();

      // stop all JMX Adaptors and Connectors...
      stopHttpAdaptor();
      stopRMIConnectorServer();
      memberInfoWithStatsMBean = null;
      stopSnmpAdaptor();

      // release the MBeanServer for cleanup...
      MBeanUtil.stop();
      mBeanServer = null;

      // remove the register shutdown hook which disconnects the Agent from the Distributed System
      // upon JVM shutdown
      removeShutdownHook();

      logger.info("Agent has stopped");
    } finally {
      loggingSession.shutdown();
    }

  }

  private void stopDistributedSystem() {
    // disconnect from the distributed system...
    try {
      disconnectFromSystem();
    } catch (Exception e) {
      // disconnectFromSystem already prints any Exceptions
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      throw err;
    } catch (Error e) {
      // Whenever you catch Error or Throwable, you must also catch VirtualMachineError (see above).
      // However, there is _still_ a possibility that you are dealing with a cascading error
      // condition,
      // so you also need to check to see if the JVM is still usable:
      SystemFailure.checkFailure();
    }
  }

  @Override
  public ObjectName manageDistributedSystem() throws MalformedObjectNameException {
    synchronized (CONN_SYNC) {
      if (isConnected()) {
        return ((AdminDistributedSystemJmxImpl) this.system).getObjectName();
      }
      return null;
    }
  }

  /**
   * Connects to the DistributedSystem currently described by this Agent's attributes for
   * administration and monitoring.
   *
   * @return the object name of the system that the Agent is now connected to
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "This is only a style warning.")
  public ObjectName connectToSystem() throws AdminException, MalformedObjectNameException {
    synchronized (CONN_SYNC) {
      try {
        if (isConnected()) {
          return ((AdminDistributedSystemJmxImpl) this.system).getObjectName();
        }

        ClusterDistributionManager.setIsDedicatedAdminVM(true);

        AdminDistributedSystemJmxImpl systemJmx = (AdminDistributedSystemJmxImpl) this.system;
        if (systemJmx == null) {
          systemJmx = (AdminDistributedSystemJmxImpl) createDistributedSystem(this.agentConfig);
          this.system = systemJmx;
        }
        systemJmx.connect(this.logWriter);

        return new ObjectName(systemJmx.getMBeanName());
      } catch (AdminException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (RuntimeException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.error(e.getMessage(), e);
        throw e;
      }
    }
  }

  /**
   * Disconnects from the current DistributedSystem (if connected to one).
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD",
      justification = "This is only a style warning.")
  public void disconnectFromSystem() {
    synchronized (CONN_SYNC) {
      try {
        if (this.system == null || !this.system.isConnected()) {
          return;
        }
        ((AdminDistributedSystemJmxImpl) this.system).disconnect();
      } catch (RuntimeException e) {
        logger.warn(e.getMessage(), e);
        throw e;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Error e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        logger.warn(e.getMessage(), e);
        throw e;
      } finally {
        ClusterDistributionManager.setIsDedicatedAdminVM(false);
      }
    }
  }

  /**
   * Retrieves a displayable snapshot of this Agent's log.
   *
   * @return snapshot of the current log
   */
  public String getLog() {
    File childLogFile = loggingSession.getLogFile().isPresent()
        ? loggingSession.getLogFile().get().getChildLogFile() : null;
    String childTail = tailFile(childLogFile);
    String mainTail = tailFile(new File(this.agentConfig.getLogFile()));
    if (childTail == null && mainTail == null) {
      return "No log file configured, log messages will be directed to stdout.";
    } else {
      StringBuffer result = new StringBuffer();
      if (mainTail != null) {
        result.append(mainTail);
      }
      if (childTail != null) {
        result
            .append("\n" + "-------------------- tail of child log --------------------" + "\n");
        result.append(childTail);
      }
      return result.toString();
    }
  }

  /**
   * Retrieves display-friendly GemFire version information.
   */
  public String getVersion() {
    return GemFireVersion.asString();
  }

  // -------------------------------------------------------------------------
  // Public attribute accessors/mutators
  // -------------------------------------------------------------------------

  /** Returns true if this Agent is currently connected to a system. */
  @Override
  public boolean isConnected() {
    boolean result = false;
    synchronized (CONN_SYNC) {
      result = ((this.system != null) && this.system.isConnected());
    }
    return result;
  }

  /**
   * Gets the agent's property file. This is the file it will use when saving its configuration. It
   * was also used when the agent started to initialize its configuration.
   *
   * @return the agent's property file
   */
  public String getPropertyFile() {
    return this.propertyFile;
  }

  /**
   * Sets the agent's property file.
   *
   * @param value the name of the file to save the agent properties in.
   * @throws IllegalArgumentException if the specified file is a directory.
   * @throws IllegalArgumentException if the specified file's parent is not an existing directory.
   */
  public void setPropertyFile(String value) {
    File f = (new File(value)).getAbsoluteFile();
    if (f.isDirectory()) {
      throw new IllegalArgumentException(
          String.format("The file %s is a directory.", f));
    }
    File parent = f.getParentFile();
    if (parent != null) {
      if (!parent.isDirectory()) {
        throw new IllegalArgumentException(
            String.format("The directory %s does not exist.", parent));
      }
    }
    this.propertyFile = f.getPath();
  }

  /**
   * Gets the mcastAddress of the distributed system that this Agent is managing.
   *
   * @return The mcastAddress value
   */
  public String getMcastAddress() {
    return this.agentConfig.getMcastAddress();
  }

  /**
   * Sets the mcastAddress of the distributed system that this Agent is managing.
   *
   * @param mcastAddress The new mcastAddress value
   */
  public void setMcastAddress(String mcastAddress) {
    this.agentConfig.setMcastAddress(mcastAddress);
  }

  /**
   * Gets the mcastPort of the distributed system that this Agent is managing.
   *
   * @return The mcastPort value
   */
  public int getMcastPort() {
    return this.agentConfig.getMcastPort();
  }

  /**
   * Sets the mcastPort of the distributed system that this Agent is managing.
   *
   * @param mcastPort The new mcastPort value
   */
  public void setMcastPort(int mcastPort) {
    this.agentConfig.setMcastPort(mcastPort);
  }

  /**
   * Gets the locators of the distributed system that this Agent is managing.
   * <p>
   * Format is a comma-delimited list of "host[port]" entries.
   *
   * @return The locators value
   */
  public String getLocators() {
    return this.agentConfig.getLocators();
  }

  /**
   * Sets the locators of the distributed system that this Agent is managing.
   * <p>
   * Format is a comma-delimited list of "host[port]" entries.
   *
   * @param locators The new locators value
   */
  public void setLocators(String locators) {
    this.agentConfig.setLocators(locators);
  }

  /**
   * Gets the membership UDP port range in the distributed system that this Agent is monitoring.
   * <p>
   * This range is given as two numbers separated by a minus sign like "min-max"
   *
   * @return membership UDP port range
   */
  public String getMembershipPortRange() {
    return this.agentConfig.getMembershipPortRange();
  }

  /**
   * Sets the membership UDP port range in the distributed system that this Agent is monitoring.
   * <p>
   * This range is given as two numbers separated by a minus sign like "min-max"
   *
   * @param membershipPortRange membership UDP port range
   */
  public void setMembershipPortRange(String membershipPortRange) {
    this.agentConfig.setMembershipPortRange(membershipPortRange);
  }

  /**
   * Gets the bindAddress of the distributed system that this Agent is managing.
   *
   * @return The bindAddress value
   */
  public String getBindAddress() {
    return this.agentConfig.getBindAddress();
  }

  /**
   * Sets the bindAddress of the distributed system that this Agent is managing.
   *
   * @param bindAddress The new bindAddress value
   */
  public void setBindAddress(String bindAddress) {
    this.agentConfig.setBindAddress(bindAddress);
  }

  /**
   * Retrieves the command that the DistributedSystem will use to perform remote manipulation of
   * config files and log files.
   *
   * @return the remote command for DistributedSystem
   */
  public String getRemoteCommand() {
    return this.agentConfig.getRemoteCommand();
  }

  /**
   * Sets the command that the DistributedSystem will use to perform remote manipulation of config
   * files and log files.
   *
   * @param remoteCommand the remote command for DistributedSystem
   */
  public void setRemoteCommand(String remoteCommand) {
    this.agentConfig.setRemoteCommand(remoteCommand);
  }

  /** Returns the system identity for the DistributedSystem */
  public String getSystemId() {
    return this.agentConfig.getSystemId();
  }

  /** Sets the system identity for the DistributedSystem */
  public void setSystemId(String systemId) {
    this.agentConfig.setSystemId(systemId);
  }

  /**
   * Gets the logFileSizeLimit in megabytes of this Agent. Zero indicates no limit.
   *
   * @return The logFileSizeLimit value
   */
  public int getLogFileSizeLimit() {
    return this.agentConfig.getLogFileSizeLimit();
  }

  /**
   * Sets the logFileSizeLimit in megabytes of this Agent. Zero indicates no limit.
   *
   * @param logFileSizeLimit The new logFileSizeLimit value
   */
  public void setLogFileSizeLimit(int logFileSizeLimit) {
    this.agentConfig.setLogFileSizeLimit(logFileSizeLimit);
    logConfigChanged();
  }

  /**
   * Gets the logDiskSpaceLimit in megabytes of this Agent. Zero indicates no limit.
   *
   * @return The logDiskSpaceLimit value
   */
  public int getLogDiskSpaceLimit() {
    return this.agentConfig.getLogDiskSpaceLimit();
  }

  /**
   * Sets the logDiskSpaceLimit in megabytes of this Agent. Zero indicates no limit.
   *
   * @param logDiskSpaceLimit The new logDiskSpaceLimit value
   */
  public void setLogDiskSpaceLimit(int logDiskSpaceLimit) {
    this.agentConfig.setLogDiskSpaceLimit(logDiskSpaceLimit);
    logConfigChanged();
  }

  /**
   * Gets the logFile name for this Agent to log to.
   *
   * @return The logFile value
   */
  public String getLogFile() {
    return this.agentConfig.getLogFile();
  }

  /**
   * Sets the logFile name for this Agent to log to.
   *
   * @param logFile The new logFile value
   */
  public void setLogFile(String logFile) {
    this.agentConfig.setLogFile(logFile);
    logConfigChanged();
  }

  /**
   * Gets the logLevel of this Agent.
   *
   * @return The logLevel value
   */
  public String getLogLevel() {
    return this.agentConfig.getLogLevel();
  }

  /**
   * Sets the logLevel of this Agent.
   *
   * @param logLevel The new logLevel value
   */
  public void setLogLevel(String logLevel) {
    this.agentConfig.setLogLevel(logLevel);
    logConfigChanged();
  }

  /** Returns true if the Agent is set to auto connect to a system. */
  public boolean getAutoConnect() {
    return this.agentConfig.getAutoConnect();
  }

  /** Returns true if the Agent is set to auto connect to a system. */
  public boolean isAutoConnect() {
    return this.agentConfig.getAutoConnect();
  }

  /** Sets or unsets the option to auto connect to a system. */
  public void setAutoConnect(boolean v) {
    this.agentConfig.setAutoConnect(v);
  }

  /**
   * Returns the address (URL) on which the RMI connector server runs or <code>null</code> if the
   * RMI connector server has not been started. This method is used primarily for testing purposes.
   *
   * @see JMXConnectorServer#getAddress()
   */
  public JMXServiceURL getRMIAddress() {
    if (this.rmiConnector != null) {
      return this.rmiConnector.getAddress();

    } else {
      return null;
    }
  }

  /**
   * Gets the configuration for this Agent.
   *
   * @return the configuration for this Agent
   */
  protected AgentConfig getAgentConfig() {
    return this.agentConfig;
  }

  // -------------------------------------------------------------------------
  // Internal implementation methods
  // -------------------------------------------------------------------------

  /** Returns the tail of the system log specified by <code>File</code>. */
  private String tailFile(File f) {
    try {
      return TailLogResponse.tailSystemLog(f);
    } catch (IOException ex) {
      return String.format("Could not tail %s because: %s",
          new Object[] {f, ex});
    }
  }

  /**
   * Returns the active MBeanServer which has any GemFire MBeans registered.
   *
   * @return the GemFire mbeanServer
   */
  @Override
  public MBeanServer getMBeanServer() {
    return mBeanServer;
  }

  /**
   * Gets the current instance of LogWriter for logging
   *
   * @return the logWriter
   */
  @Override
  public LogWriter getLogWriter() {
    return this.logWriter;
  }

  private final Thread shutdownHook;

  /**
   * Adds a ShutdownHook to the Agent for cleaning up any resources
   */
  private void addShutdownHook() {
    if (!Boolean.getBoolean(
        org.apache.geode.distributed.internal.InternalDistributedSystem.DISABLE_SHUTDOWN_HOOK_PROPERTY)) {
      Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
  }

  private void removeShutdownHook() {
    if (!Boolean.getBoolean(
        org.apache.geode.distributed.internal.InternalDistributedSystem.DISABLE_SHUTDOWN_HOOK_PROPERTY)) {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }
  }

  /**
   * Creates a LogWriterI18n for this Agent to use in logging.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
      justification = "Return value for file delete is not important here.")
  private void initLogWriter() throws org.apache.geode.admin.AdminException {
    loggingSession.createSession(this);

    final LogConfig logConfig = this.agentConfig.createLogConfig();

    // LOG: create logWriterAppender here
    loggingSession.startSession();

    // LOG: look in AgentConfigImpl for existing LogWriter to use
    InternalLogWriter existingLogWriter = this.agentConfig.getInternalLogWriter();
    if (existingLogWriter != null) {
      this.logWriter = existingLogWriter;
    } else {
      // LOG: create LogWriterLogger
      this.logWriter = LogWriterFactory.createLogWriterLogger(logConfig, false);
      // Set this log writer in AgentConfigImpl
      this.agentConfig.setInternalLogWriter(this.logWriter);
    }

    // LOG: create logWriter here
    this.logWriter = LogWriterFactory.createLogWriterLogger(logConfig, false);

    // Set this log writer in AgentConfig
    this.agentConfig.setInternalLogWriter(this.logWriter);

    // LOG:CONFIG: changed next three statements from config to info
    logger.info(LogMarker.CONFIG_MARKER,
        String.format("Agent config property file name: %s",
            AgentConfigImpl.retrievePropertyFile()));
    logger.info(LogMarker.CONFIG_MARKER, this.agentConfig.getPropertyFileDescription());
    logger.info(LogMarker.CONFIG_MARKER, this.agentConfig.toPropertiesAsString());
  }

  /**
   * Stops the HttpAdaptor and its XsltProcessor. Unregisters the associated MBeans.
   */
  private void stopHttpAdaptor() {
    if (!this.agentConfig.isHttpEnabled())
      return;

    // stop the adaptor...
    try {
      this.httpAdaptor.stop();
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
    }

    try {
      MBeanUtil.unregisterMBean(getHttpAdaptorName());
      MBeanUtil.unregisterMBean(getXsltProcessorName());
    } catch (MalformedObjectNameException e) {
      logger.warn(e.getMessage(), e);
    }
  }

  /** Stops the RMIConnectorServer and unregisters its MBean. */
  private void stopRMIConnectorServer() {
    if (!this.agentConfig.isRmiEnabled())
      return;

    // stop the RMI Connector server...
    try {
      this.rmiConnector.stop();
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
    }

    try {
      ObjectName rmiRegistryNamingName = getRMIRegistryNamingName();
      if (this.agentConfig.isRmiRegistryEnabled()
          && mBeanServer.isRegistered(rmiRegistryNamingName)) {
        String[] empty = new String[0];
        mBeanServer.invoke(rmiRegistryNamingName, "stop", empty, empty);
        MBeanUtil.unregisterMBean(rmiRegistryNamingName);
      }
    } catch (MalformedObjectNameException e) {
      logger.warn(e.getMessage(), e);
    } catch (InstanceNotFoundException e) {
      logger.warn(e.getMessage(), e);
    } catch (ReflectionException e) {
      logger.warn(e.getMessage(), e);
    } catch (MBeanException e) {
      logger.warn(e.getMessage(), e);
    }

    try {
      ObjectName rmiConnectorServerName = getRMIConnectorServerName();
      if (mBeanServer.isRegistered(rmiConnectorServerName)) {
        MBeanUtil.unregisterMBean(rmiConnectorServerName);
      }
    } catch (MalformedObjectNameException e) {
      logger.warn(e.getMessage(), e);
    }
  }

  /** Stops the SnmpAdaptor and unregisters its MBean. */
  private void stopSnmpAdaptor() {
    if (!this.agentConfig.isSnmpEnabled())
      return;

    // stop the SnmpAdaptor...
    try {
      getMBeanServer().invoke(getSnmpAdaptorName(), "unbind", new Object[0], new String[0]);
    } catch (Exception e) {
      logger.warn(e.getMessage(), e);
    }

    try {
      MBeanUtil.unregisterMBean(getSnmpAdaptorName());
    } catch (MalformedObjectNameException e) {
      logger.warn(e.getMessage(), e);
    }
  }

  /** Returns the JMX ObjectName for the RMI registry Naming MBean. */
  private ObjectName getRMIRegistryNamingName()
      throws javax.management.MalformedObjectNameException {
    return ObjectName.getInstance("naming:type=rmiregistry");
  }

  /** Returns the JMX ObjectName for the HttpAdaptor. */
  private ObjectName getHttpAdaptorName() throws javax.management.MalformedObjectNameException {
    return new ObjectName("Server:name=HttpAdaptor");
  }

  /** Returns the JMX ObjectName for the RMIConnectorServer. */
  private ObjectName getRMIConnectorServerName()
      throws javax.management.MalformedObjectNameException {
    return new ObjectName("connectors:protocol=rmi");
  }

  /** Returns the JMX ObjectName for the SnmpAdaptor. */
  private ObjectName getSnmpAdaptorName() throws javax.management.MalformedObjectNameException {
    return new ObjectName("Adaptors:protocol=SNMP");
  }

  /** Returns the JMX ObjectName for the HttpAdaptor's XsltProcessor. */
  private ObjectName getXsltProcessorName() throws javax.management.MalformedObjectNameException {
    return new ObjectName("Server:name=XSLTProcessor");
  }

  // -------------------------------------------------------------------------
  // Factory method for creating DistributedSystem
  // -------------------------------------------------------------------------

  /**
   * Creates and connects to a <code>DistributedSystem</code>.
   *
   */
  private AdminDistributedSystem createDistributedSystem(AgentConfigImpl config)
      throws org.apache.geode.admin.AdminException {
    return new AdminDistributedSystemJmxImpl(config);
  }

  // -------------------------------------------------------------------------
  // Agent main
  // -------------------------------------------------------------------------

  /**
   * Command-line main for running the GemFire Management Agent.
   * <p>
   * Accepts command-line arguments matching the options in {@link AgentConfig} and
   * {@link org.apache.geode.admin.DistributedSystemConfig}.
   * <p>
   * <code>AgentConfig</code> will convert -Jarguments to System properties.
   */
  public static void main(String[] args) {
    SystemFailure.loadEmergencyClasses();

    AgentConfigImpl ac;
    try {
      ac = new AgentConfigImpl(args);
    } catch (RuntimeException ex) {
      System.err
          .println(String.format("Failed reading configuration: %s", ex));
      ExitCode.FATAL.doSystemExit();
      return;
    }

    try {
      Agent agent = AgentFactory.getAgent(ac);
      agent.start();

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
      ExitCode.FATAL.doSystemExit();
    }
  }

  // -------------------------------------------------------------------------
  // MX4J Connectors/Adaptors
  // -------------------------------------------------------------------------

  private void createRMIRegistry() throws Exception {
    if (!this.agentConfig.isRmiRegistryEnabled()) {
      return;
    }
    MBeanServer mbs = getMBeanServer();
    String host = this.agentConfig.getRmiBindAddress();
    int port = this.agentConfig.getRmiPort();

    /*
     * Register and start the rmi-registry naming MBean, which is needed by JSR 160
     * RMIConnectorServer
     */
    ObjectName registryName = getRMIRegistryNamingName();
    try {
      RMIRegistryService registryNamingService = null;
      if (host != null && !("".equals(host.trim()))) {
        registryNamingService = new RMIRegistryService(host, port);
      } else {
        registryNamingService = new RMIRegistryService(port);
      }
      mbs.registerMBean(registryNamingService, registryName);
    } catch (javax.management.InstanceAlreadyExistsException e) {
      logger.info("{}  is already registered.",
          registryName);
    }
    mbs.invoke(registryName, "start", null, null);
  }

  /**
   * Defines and starts the JMX RMIConnector and service.
   * <p>
   * If {@link AgentConfig#isRmiEnabled} returns false, then this adaptor will not be started.
   */
  private void startRMIConnectorServer() {
    if (!this.agentConfig.isRmiEnabled())
      return;

    String rmiBindAddress = this.agentConfig.getRmiBindAddress();

    // Set RMI Stubs to use the given RMI Bind Address
    // Default bindAddress is "", if none is set - ignore if not set
    // If java.rmi.server.hostname property is specified then
    // that override is not changed
    String rmiStubServerNameKey = "java.rmi.server.hostname";
    String overrideHostName = System.getProperty(rmiStubServerNameKey);
    if ((overrideHostName == null || overrideHostName.trim().length() == 0)
        && (rmiBindAddress != null && rmiBindAddress.trim().length() != 0)) {
      System.setProperty(rmiStubServerNameKey, rmiBindAddress);
      logger.info((new StringBuilder("Setting ").append(rmiStubServerNameKey).append(" = ")
          .append(rmiBindAddress).toString()));
    }

    try {
      createRMIRegistry();
      ObjectName objName = getRMIConnectorServerName();

      // make sure this adaptor is not already registered...
      if (getMBeanServer().isRegistered(objName)) {
        // dunno how we got here...
        logger.info("RMIConnectorServer already registered as {}", objName);
        return;
      }

      /*
       * url defined as: service:jmx:protocol:sap where 1. protocol: rmi 2. sap is:
       * [host[:port]][url-path] where host: rmi-binding-address port: rmi-server-port url-path:
       * /jndi/rmi://<rmi-binding-address>:<rmi-port><JNDI_NAME>
       */
      String urlString = null;
      String connectorServerHost = "";
      int connectorServerPort = this.agentConfig.getRmiServerPort();
      String rmiRegistryHost = "";
      int rmiRegistryPort = this.agentConfig.getRmiPort();

      // Set registryHost to localhost if not specified
      // RMI stubs would use a default IP if namingHost is left empty
      if (rmiBindAddress == null || rmiBindAddress.trim().length() == 0) {
        connectorServerHost = "localhost";
        rmiRegistryHost = "";
      } else {
        connectorServerHost = applyRFC2732(rmiBindAddress);
        rmiRegistryHost = connectorServerHost;
      }

      urlString = MessageFormat.format(AgentImpl.JMX_SERVICE_URL, connectorServerHost,
          String.valueOf(connectorServerPort), rmiRegistryHost, String.valueOf(rmiRegistryPort),
          JNDI_NAME);

      logger.debug("JMX Service URL string is : \"{}\"", urlString);

      // The address of the connector
      JMXServiceURL url = new JMXServiceURL(urlString);

      Map<String, Object> env = new HashMap<String, Object>();

      RMIServerSocketFactory ssf = new MX4JServerSocketFactory(this.agentConfig.isAgentSSLEnabled(), // true,
          this.agentConfig.isAgentSSLRequireAuth(), // true,
          this.agentConfig.getAgentSSLProtocols(), // "any",
          this.agentConfig.getAgentSSLCiphers(), // "any",
          this.agentConfig.getRmiBindAddress(), 10, // backlog
          this.agentConfig.getGfSecurityProperties());
      env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, ssf);

      if (this.agentConfig.isAgentSSLEnabled()) {
        RMIClientSocketFactory csf = new SslRMIClientSocketFactory();
        env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, csf);
      }

      MBeanServer mbs = null; // will be set by registering w/ mbeanServer
      this.rmiConnector = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);

      // for cleanup
      this.rmiConnector.addNotificationListener(new ConnectionNotificationAdapter(),
          new ConnectionNotificationFilterImpl(), this);

      // Register the JMXConnectorServer in the MBeanServer
      getMBeanServer().registerMBean(this.rmiConnector, objName);

      // Start the JMXConnectorServer
      this.rmiConnector.start();
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
      logger.error("Failed to start RMIConnectorServer:", t);
      throw new StartupException(
          "Failed to start RMI service, verify RMI configuration properties", t);
    }
  }

  /**
   * Starts the optional third-party AdventNet SNMP Adaptor.
   * <p>
   * If {@link AgentConfig#isSnmpEnabled} returns false, then this adaptor will not be started.
   */
  private void startSnmpAdaptor() {
    if (!this.agentConfig.isSnmpEnabled())
      return;
    try {
      ObjectName objName = getSnmpAdaptorName();

      // make sure this adaptor is not already registered...
      if (getMBeanServer().isRegistered(objName)) {
        // dunno how we got here...
        logger.info("SnmpAdaptor already registered as  {}", objName);
        return;
      }

      String className = "com.adventnet.adaptors.snmp.snmpsupport.SmartSnmpAdaptor";
      String snmpDir = this.agentConfig.getSnmpDirectory();
      // ex:/merry2/users/klund/agent

      // validate the directory...
      if (snmpDir == null || snmpDir.length() == 0) {
        throw new IllegalArgumentException(
            "snmp-directory must be specified because SNMP is enabled");
      }
      File root = new File(snmpDir);
      if (!root.exists()) {
        throw new IllegalArgumentException(
            "snmp-directory does not exist");
      }

      // create the adaptor...
      String[] sigs = new String[] {"java.lang.String"};
      Object[] args = new Object[] {snmpDir};

      String bindAddress = this.agentConfig.getSnmpBindAddress();
      if (bindAddress != null && bindAddress.length() > 0) {
        sigs = new String[] {"java.lang.String", sigs[0]};
        args = new Object[] {bindAddress, args[0]};
      }

      // go...
      getMBeanServer().createMBean(className, objName, args, sigs);
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
      logger.error("Failed to start SnmpAdaptor:  {}", t.getMessage());
      throw new StartupException(String.format("Failed to start SnmpAdaptor: %s",
          t.getMessage()), t);
    }
  }

  /**
   * Defines and starts the JMX Http Adaptor service from MX4J.
   * <p>
   * If {@link AgentConfig#isHttpEnabled} returns false, then this adaptor will not be started.
   */
  private void startHttpAdaptor() {
    if (!this.agentConfig.isHttpEnabled())
      return;
    try {
      ObjectName objName = getHttpAdaptorName();

      // make sure this adaptor is not already registered...
      if (getMBeanServer().isRegistered(objName)) {
        // dunno how we got here...
        logger.info("HttpAdaptor already registered as  {}", objName);
        return;
      }

      this.httpAdaptor = new HttpAdaptor();

      // validate and set host and port values...
      if (this.agentConfig.getHttpPort() > 0) {
        this.httpAdaptor.setPort(this.agentConfig.getHttpPort());
        logger.info(LogMarker.CONFIG_MARKER,
            "HTTP adaptor listening on port: {}",
            this.agentConfig.getHttpPort());
      } else {
        logger.error("Incorrect port value  {}",
            this.agentConfig.getHttpPort());
      }

      if (this.agentConfig.getHttpBindAddress() != null) {
        String host = this.agentConfig.getHttpBindAddress();
        logger.info(LogMarker.CONFIG_MARKER, "HTTP adaptor listening on address:  {}", host);
        this.httpAdaptor.setHost(host);
      } else {
        logger.error("Incorrect null hostname");
      }

      // SSL support...
      MX4JServerSocketFactory socketFactory =
          new MX4JServerSocketFactory(this.agentConfig.isAgentSSLEnabled(),
              this.agentConfig.isHttpSSLRequireAuth(), this.agentConfig.getAgentSSLProtocols(),
              this.agentConfig.getAgentSSLCiphers(), this.agentConfig.getGfSecurityProperties());
      this.httpAdaptor.setSocketFactory(socketFactory);

      // authentication (user login) support...
      if (this.agentConfig.isHttpAuthEnabled()) {
        // this pops up a login dialog from the browser...
        this.httpAdaptor.setAuthenticationMethod(MX4J_HTTPADAPTOR_BASIC_AUTHENTICATION); // only
                                                                                         // basic
                                                                                         // works

        this.httpAdaptor.addAuthorization(this.agentConfig.getHttpAuthUser(),
            this.agentConfig.getHttpAuthPassword());
      }

      // add the XsltProcessor...
      this.httpAdaptor.setProcessorName(createXsltProcessor());

      // register the HttpAdaptor and snap on the XsltProcessor...
      getMBeanServer().registerMBean(this.httpAdaptor, objName);
      this.httpAdaptor.start();
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
      logger.error("Failed to start HttpAdaptor:  {}", t.getMessage());
      throw new StartupException(String.format("Failed to start HttpAdaptor: %s",
          t.getMessage()), t);
    }
  }

  /**
   * Defines and starts the Xslt Processor helper service for the Http Adaptor.
   */
  private ObjectName createXsltProcessor() throws javax.management.JMException {
    ObjectName objName = getXsltProcessorName();

    // make sure this mbean is not already registered...
    if (getMBeanServer().isRegistered(objName)) {
      // dunno how we got here...
      logger.info("XsltProcessor already registered as  {}", objName);
      return objName;
    }

    getMBeanServer().registerMBean(new mx4j.tools.adaptor.http.XSLTProcessor(), objName);
    return objName;
  }

  // -------------------------------------------------------------------------
  // SSL configuration for GemFire
  // -------------------------------------------------------------------------
  public boolean isSSLEnabled() {
    return this.agentConfig.isSSLEnabled();
  }

  public void setSSLEnabled(boolean enabled) {
    this.agentConfig.setSSLEnabled(enabled);
  }

  public String getSSLProtocols() {
    return this.agentConfig.getSSLProtocols();
  }

  public void setSSLProtocols(String protocols) {
    this.agentConfig.setSSLProtocols(protocols);
  }

  public String getSSLCiphers() {
    return this.agentConfig.getSSLCiphers();
  }

  public void setSSLCiphers(String ciphers) {
    this.agentConfig.setSSLCiphers(ciphers);
  }

  public boolean isSSLAuthenticationRequired() {
    return this.agentConfig.isSSLAuthenticationRequired();
  }

  public void setSSLAuthenticationRequired(boolean authRequired) {
    this.agentConfig.setSSLAuthenticationRequired(authRequired);
  }

  public Properties getSSLProperties() {
    return this.agentConfig.getSSLProperties();
  }

  public void setSSLProperties(Properties sslProperties) {
    this.agentConfig.setSSLProperties(sslProperties);
  }

  public void addSSLProperty(String key, String value) {
    this.agentConfig.addSSLProperty(key, value);
  }

  public void removeSSLProperty(String key) {
    this.agentConfig.removeSSLProperty(key);
  }

  // -------------------------------------------------------------------------
  // ManagedResource implementation
  // -------------------------------------------------------------------------

  @Override
  public String getMBeanName() {
    return this.mbeanName;
  }

  @Override
  public ModelMBean getModelMBean() {
    return this.modelMBean;
  }

  @Override
  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  @Override
  public ObjectName getObjectName() {
    return this.objectName;
  }

  @Override
  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.AGENT;
  }

  @Override
  public void cleanupResource() {}

  @Override
  public LogConfig getLogConfig() {
    return agentConfig.createLogConfig();
  }

  @Override
  public StatisticsConfig getStatisticsConfig() {
    return agentConfig.createStatisticsConfig();
  }

  @Override
  public void addLogConfigListener(LogConfigListener logConfigListener) {
    logConfigListeners.add(logConfigListener);
  }

  @Override
  public void removeLogConfigListener(LogConfigListener logConfigListener) {
    logConfigListeners.remove(logConfigListener);
  }

  void logConfigChanged() {
    for (LogConfigListener listener : logConfigListeners) {
      listener.configChanged();
    }
  }

  static class StartupException extends GemFireException {
    private static final long serialVersionUID = 6614145962199330348L;

    StartupException(Throwable cause) {
      super(cause);
    }

    StartupException(String reason, Throwable cause) {
      super(reason, cause);
    }
  }

  // -------------------------------------------------------------------------
  // Other Support methods
  // -------------------------------------------------------------------------
  /**
   * Checks the no. of active RMI clients and updates a flag in the Admin Distributed System.
   *
   * @see AdminDistributedSystemJmxImpl#setRmiClientCountZero(boolean)
   * @since GemFire 6.0
   */
  void updateRmiClientsCount() {
    int noOfClientsConnected = 0;

    String[] connectionIds = this.rmiConnector.getConnectionIds();

    if (connectionIds != null) {
      noOfClientsConnected = connectionIds.length;
    }

    logger.info("No. of RMI clients connected :: {}", noOfClientsConnected);

    AdminDistributedSystemJmxImpl adminDSJmx = (AdminDistributedSystemJmxImpl) this.system;

    adminDSJmx.setRmiClientCountZero(noOfClientsConnected == 0);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("AgentImpl[");
    sb.append("config=" + agentConfig.toProperties().toString());
    sb.append("; mbeanName=" + mbeanName);
    sb.append("; modelMBean=" + modelMBean);
    sb.append("; objectName=" + objectName);
    sb.append("; propertyFile=" + propertyFile);
    sb.append(": rmiConnector=" + rmiConnector);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Process the String form of a hostname to make it comply with Jmx URL restrictions. Namely wrap
   * IPv6 literal address with "[", "]"
   *
   * @param hostname the name to safeguard.
   * @return a string representation suitable for use in a Jmx connection URL
   */
  private static String applyRFC2732(String hostname) {
    if (hostname.indexOf(":") != -1) {
      // Assuming an IPv6 literal because of the ':'
      return "[" + hostname + "]";
    }
    return hostname;
  }
}


/**
 * Adapter class for NotificationListener that listens to notifications of type
 * javax.management.remote.JMXConnectionNotification
 *
 * @since GemFire 6.0
 */
class ConnectionNotificationAdapter implements NotificationListener {
  private static final Logger logger = LogService.getLogger();

  /**
   * If the handback object passed is an AgentImpl, updates the JMX client count
   *
   * @param notification JMXConnectionNotification for change in client connection status
   * @param handback An opaque object which helps the listener to associate information regarding
   *        the MBean emitter. This object is passed to the MBean during the addListener call and
   *        resent, without modification, to the listener. The MBean object should not use or modify
   *        the object. (NOTE: copied from javax.management.NotificationListener)
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "BC_UNCONFIRMED_CAST",
      justification = "Only JMXConnectionNotification instances are used.")
  public void handleNotification(Notification notification, Object handback) {
    if (handback instanceof AgentImpl) {
      AgentImpl agent = (AgentImpl) handback;

      JMXConnectionNotification jmxNotifn = (JMXConnectionNotification) notification;

      if (logger.isDebugEnabled()) {
        logger.debug("Connection notification for connection id : '{}'",
            jmxNotifn.getConnectionId());
      }

      agent.updateRmiClientsCount();
    }
  }
}


/**
 * Filters out the notifications of the type JMXConnectionNotification.OPENED,
 * JMXConnectionNotification.CLOSED and JMXConnectionNotification.FAILED.
 *
 * @since GemFire 6.0
 */
class ConnectionNotificationFilterImpl implements NotificationFilter {

  /**
   * Default serialVersionUID
   */
  private static final long serialVersionUID = 1L;

  /**
   * Invoked before sending the specified notification to the listener. Returns whether the given
   * notification is to be sent to the listener.
   *
   * @param notification The notification to be sent.
   * @return true if the notification has to be sent to the listener, false otherwise.
   */
  @Override
  public boolean isNotificationEnabled(Notification notification) {
    boolean isThisNotificationEnabled = false;
    if (notification.getType().equals(JMXConnectionNotification.OPENED)
        || notification.getType().equals(JMXConnectionNotification.CLOSED)
        || notification.getType().equals(JMXConnectionNotification.FAILED)) {
      isThisNotificationEnabled = true;
    }
    return isThisNotificationEnabled;
  }
}
