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
package org.apache.geode.admin.internal;

import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.TCP_PORT;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.AdminXmlException;
import org.apache.geode.admin.CacheServerConfig;
import org.apache.geode.admin.CacheVmConfig;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.admin.DistributionLocator;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.logging.log4j.LogLevel;

/**
 * An implementation of the configuration object for an <code>AdminDistributedSystem</code>. After a
 * config has been used to create an <code>AdminDistributedSystem</code> most of the configuration
 * attributes cannot be changed. However, some operations (such as getting information about GemFire
 * managers and distribution locators) are "passed through" to the
 * <code>AdminDistributedSystem</code> associated with this configuration object.
 *
 * @since GemFire 3.5
 */
public class DistributedSystemConfigImpl implements DistributedSystemConfig {

  private static final Logger logger = LogService.getLogger();

  private String entityConfigXMLFile = DEFAULT_ENTITY_CONFIG_XML_FILE;
  private String systemId = DEFAULT_SYSTEM_ID;
  private String mcastAddress = DEFAULT_MCAST_ADDRESS;
  private int mcastPort = DEFAULT_MCAST_PORT;
  private int ackWaitThreshold = DEFAULT_ACK_WAIT_THRESHOLD;
  private int ackSevereAlertThreshold = DEFAULT_ACK_SEVERE_ALERT_THRESHOLD;
  private String locators = DEFAULT_LOCATORS;
  private String bindAddress = DEFAULT_BIND_ADDRESS;
  private String serverBindAddress = DEFAULT_BIND_ADDRESS;
  private String remoteCommand = DEFAULT_REMOTE_COMMAND;
  private boolean disableTcp = DEFAULT_DISABLE_TCP;
  private boolean enableNetworkPartitionDetection = DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION;
  private boolean disableAutoReconnect = DEFAULT_DISABLE_AUTO_RECONNECT;
  private int memberTimeout = DEFAULT_MEMBER_TIMEOUT;
  private String membershipPortRange = getMembershipPortRangeString(DEFAULT_MEMBERSHIP_PORT_RANGE);
  private int tcpPort = DEFAULT_TCP_PORT;

  private String logFile = DEFAULT_LOG_FILE;
  private String logLevel = DEFAULT_LOG_LEVEL;
  private int logDiskSpaceLimit = DEFAULT_LOG_DISK_SPACE_LIMIT;
  private int logFileSizeLimit = DEFAULT_LOG_FILE_SIZE_LIMIT;
  private int refreshInterval = DEFAULT_REFRESH_INTERVAL;
  private Properties gfSecurityProperties = new Properties();

  /**
   * Listeners to notify when this DistributedSystemConfig changes
   */
  private Set listeners = new HashSet();

  /**
   * Configs for CacheServers that this system config is aware of
   */
  private Set cacheServerConfigs = new HashSet();

  /**
   * Configs for the managed distribution locators in the distributed system
   */
  private Set locatorConfigs = new HashSet();

  /**
   * The display name of this distributed system
   */
  private String systemName = DEFAULT_NAME;

  /**
   * The admin distributed system object that is configured by this config object.
   *
   * @since GemFire 4.0
   */
  private AdminDistributedSystemImpl system;

  /**
   * The GemFire log writer used by the distributed system
   */
  private InternalLogWriter logWriter;

  /////////////////////// Static Methods ///////////////////////

  /**
   * Filters out all properties that are unique to the admin <code>DistributedSystemConfig</code>
   * that are not present in the internal <code>DistributionConfig</code>.
   *
   * @since GemFire 4.0
   */
  private static Properties filterOutAdminProperties(Properties props) {

    Properties props2 = new Properties();
    for (Enumeration names = props.propertyNames(); names.hasMoreElements();) {
      String name = (String) names.nextElement();
      if (!(ENTITY_CONFIG_XML_FILE_NAME.equals(name) || REFRESH_INTERVAL_NAME.equals(name)
          || REMOTE_COMMAND_NAME.equals(name))) {
        String value = props.getProperty(name);
        if ((name != null) && (value != null)) {
          props2.setProperty(name, value);
        }
      }
    }

    return props2;
  }

  //////////////////////// Constructors ////////////////////////

  /**
   * Creates a new <code>DistributedSystemConfigImpl</code> based on the configuration stored in a
   * <code>DistributedSystem</code>'s <code>DistributionConfig</code>.
   */
  public DistributedSystemConfigImpl(DistributionConfig distConfig, String remoteCommand) {
    if (distConfig == null) {
      throw new IllegalArgumentException(
          "DistributionConfig must not be null.");
    }

    this.mcastAddress = InetAddressUtil.toString(distConfig.getMcastAddress());
    this.mcastPort = distConfig.getMcastPort();
    this.locators = distConfig.getLocators();
    this.membershipPortRange = getMembershipPortRangeString(distConfig.getMembershipPortRange());

    this.systemName = distConfig.getName();

    this.sslEnabled = distConfig.getClusterSSLEnabled();
    this.sslCiphers = distConfig.getClusterSSLCiphers();
    this.sslProtocols = distConfig.getClusterSSLProtocols();
    this.sslAuthenticationRequired = distConfig.getClusterSSLRequireAuthentication();

    this.logFile = distConfig.getLogFile().getPath();
    this.logLevel = LogWriterImpl.levelToString(distConfig.getLogLevel());
    this.logDiskSpaceLimit = distConfig.getLogDiskSpaceLimit();
    this.logFileSizeLimit = distConfig.getLogFileSizeLimit();

    basicSetBindAddress(distConfig.getBindAddress());
    this.tcpPort = distConfig.getTcpPort();

    this.disableTcp = distConfig.getDisableTcp();

    this.remoteCommand = remoteCommand;
    this.serverBindAddress = distConfig.getServerBindAddress();
    this.enableNetworkPartitionDetection = distConfig.getEnableNetworkPartitionDetection();
    this.memberTimeout = distConfig.getMemberTimeout();
    this.refreshInterval = DistributedSystemConfig.DEFAULT_REFRESH_INTERVAL;
    this.gfSecurityProperties = (Properties) distConfig.getSSLProperties().clone();
  }

  /**
   * Zero-argument constructor to be used only by subclasses.
   *
   * @since GemFire 4.0
   */
  protected DistributedSystemConfigImpl() {

  }

  /**
   * Creates a new <code>DistributedSystemConifgImpl</code> whose configuration is specified by the
   * given <code>Properties</code> object.
   */
  protected DistributedSystemConfigImpl(Properties props) {
    this(props, false);
  }

  /**
   * Creates a new <code>DistributedSystemConifgImpl</code> whose configuration is specified by the
   * given <code>Properties</code> object.
   *
   * @param props The configuration properties specified by the caller
   * @param ignoreGemFirePropsFile whether to skip loading distributed system properties from
   *        gemfire.properties file
   *
   * @since GemFire 6.5
   */
  protected DistributedSystemConfigImpl(Properties props, boolean ignoreGemFirePropsFile) {
    this(new DistributionConfigImpl(filterOutAdminProperties(props), ignoreGemFirePropsFile),
        DEFAULT_REMOTE_COMMAND);
    String remoteCommand = props.getProperty(REMOTE_COMMAND_NAME);
    if (remoteCommand != null) {
      this.remoteCommand = remoteCommand;
    }

    String entityConfigXMLFile = props.getProperty(ENTITY_CONFIG_XML_FILE_NAME);
    if (entityConfigXMLFile != null) {
      this.entityConfigXMLFile = entityConfigXMLFile;
    }

    String refreshInterval = props.getProperty(REFRESH_INTERVAL_NAME);
    if (refreshInterval != null) {
      try {
        this.refreshInterval = Integer.parseInt(refreshInterval);
      } catch (NumberFormatException nfEx) {
        throw new IllegalArgumentException(
            String.format("%s is not a valid integer for %s",
                new Object[] {refreshInterval, REFRESH_INTERVAL_NAME}));
      }
    }
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Returns the <code>LogWriterI18n</code> to be used when administering the distributed system.
   * Returns null if nothing has been provided via <code>setInternalLogWriter</code>.
   *
   * @since GemFire 4.0
   */
  public InternalLogWriter getInternalLogWriter() {
    // LOG: used only for sharing between IDS, AdminDSImpl and AgentImpl -- to prevent multiple
    // banners, etc.
    synchronized (this) {
      return this.logWriter;
    }
  }

  /**
   * Sets the <code>LogWriterI18n</code> to be used when administering the distributed system.
   */
  public void setInternalLogWriter(InternalLogWriter logWriter) {
    // LOG: used only for sharing between IDS, AdminDSImpl and AgentImpl -- to prevent multiple
    // banners, etc.
    synchronized (this) {
      this.logWriter = logWriter;
    }
  }

  public LogConfig createLogConfig() {
    return new LogConfig() {
      @Override
      public int getLogLevel() {
        return LogLevel.getLogWriterLevel(DistributedSystemConfigImpl.this.getLogLevel());
      }

      @Override
      public File getLogFile() {
        return new File(DistributedSystemConfigImpl.this.getLogFile());
      }

      @Override
      public int getLogFileSizeLimit() {
        return DistributedSystemConfigImpl.this.getLogFileSizeLimit();
      }

      @Override
      public int getLogDiskSpaceLimit() {
        return DistributedSystemConfigImpl.this.getLogDiskSpaceLimit();
      }

      @Override
      public String getName() {
        return DistributedSystemConfigImpl.this.getSystemName();
      }

      @Override
      public String toLoggerString() {
        return DistributedSystemConfigImpl.this.toString();
      }
    };
  }

  /**
   * Marks this config object as "read only". Attempts to modify a config object will result in a
   * {@link IllegalStateException} being thrown.
   *
   * @since GemFire 4.0
   */
  void setDistributedSystem(AdminDistributedSystemImpl system) {
    this.system = system;
  }

  /**
   * Checks to see if this config object is "read only". If it is, then an
   * {@link IllegalStateException} is thrown.
   *
   * @since GemFire 4.0
   */
  protected void checkReadOnly() {
    if (this.system != null) {
      throw new IllegalStateException(
          "A DistributedSystemConfig object cannot be modified after it has been used to create an AdminDistributedSystem.");
    }
  }

  public String getEntityConfigXMLFile() {
    return this.entityConfigXMLFile;
  }

  public void setEntityConfigXMLFile(String xmlFile) {
    checkReadOnly();
    this.entityConfigXMLFile = xmlFile;
    configChanged();
  }

  /**
   * Parses the XML configuration file that describes managed entities.
   *
   * @throws AdminXmlException If a problem is encountered while parsing the XML file.
   */
  private void parseEntityConfigXMLFile() {
    String fileName = this.entityConfigXMLFile;
    File xmlFile = new File(fileName);
    if (!xmlFile.exists()) {
      if (DEFAULT_ENTITY_CONFIG_XML_FILE.equals(fileName)) {
        // Default doesn't exist, no big deal
        return;
      } else {
        throw new AdminXmlException(
            String.format("Entity configuration XML file %s does not exist",
                fileName));
      }
    }

    try {
      InputStream is = new FileInputStream(xmlFile);
      try {
        ManagedEntityConfigXmlParser.parse(is, this);
      } finally {
        is.close();
      }
    } catch (IOException ex) {
      throw new AdminXmlException(
          String.format("While parsing %s", fileName),
          ex);
    }
  }

  public String getSystemId() {
    return this.systemId;
  }

  public void setSystemId(String systemId) {
    checkReadOnly();
    this.systemId = systemId;
    configChanged();
  }

  /**
   * Returns the multicast address for the system
   */
  public String getMcastAddress() {
    return this.mcastAddress;
  }

  public void setMcastAddress(String mcastAddress) {
    checkReadOnly();
    this.mcastAddress = mcastAddress;
    configChanged();
  }

  /**
   * Returns the multicast port for the system
   */
  public int getMcastPort() {
    return this.mcastPort;
  }

  public void setMcastPort(int mcastPort) {
    checkReadOnly();
    this.mcastPort = mcastPort;
    configChanged();
  }

  public int getAckWaitThreshold() {
    return this.ackWaitThreshold;
  }

  public void setAckWaitThreshold(int seconds) {
    checkReadOnly();
    this.ackWaitThreshold = seconds;
    configChanged();
  }

  public int getAckSevereAlertThreshold() {
    return this.ackSevereAlertThreshold;
  }

  public void setAckSevereAlertThreshold(int seconds) {
    checkReadOnly();
    this.ackSevereAlertThreshold = seconds;
    configChanged();
  }

  /**
   * Returns the comma-delimited list of locators for the system
   */
  public String getLocators() {
    return this.locators;
  }

  public void setLocators(String locators) {
    checkReadOnly();
    if (locators == null) {
      this.locators = "";
    } else {
      this.locators = locators;
    }
    configChanged();
  }

  /**
   * Returns the value for membership-port-range
   *
   * @return the value for the Distributed System property membership-port-range
   */
  public String getMembershipPortRange() {
    return this.membershipPortRange;
  }

  /**
   * Sets the Distributed System property membership-port-range
   *
   * @param membershipPortRangeStr the value for membership-port-range given as two numbers
   *        separated by a minus sign.
   */
  public void setMembershipPortRange(String membershipPortRangeStr) {
    /*
     * FIXME: Setting attributes in DistributedSystemConfig has no effect on DistributionConfig
     * which is actually used for connection with DS. This is true for all such attributes. Should
     * be addressed in the Admin Revamp if we want these 'set' calls to affect anything. Then we can
     * use the validation code in DistributionConfigImpl code.
     */
    checkReadOnly();
    if (membershipPortRangeStr == null) {
      this.membershipPortRange = getMembershipPortRangeString(DEFAULT_MEMBERSHIP_PORT_RANGE);
    } else {
      try {
        if (validateMembershipRange(membershipPortRangeStr)) {
          this.membershipPortRange = membershipPortRangeStr;
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "The value specified %s is invalid for the property : %s. This range should be specified as min-max.",

                  new Object[] {membershipPortRangeStr, MEMBERSHIP_PORT_RANGE_NAME}));
        }
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug(e.getMessage(), e);
        }
      }
    }
  }

  public void setTcpPort(int port) {
    checkReadOnly();
    this.tcpPort = port;
    configChanged();
  }

  public int getTcpPort() {
    return this.tcpPort;
  }

  /**
   * Validates the given string - which is expected in the format as two numbers separated by a
   * minus sign - in to an integer array of length 2 with first element as lower end & second
   * element as upper end of the range.
   *
   * @param membershipPortRange membership-port-range given as two numbers separated by a minus
   *        sign.
   * @return true if the membership-port-range string is valid, false otherwise
   */
  private boolean validateMembershipRange(String membershipPortRange) {
    int[] range = null;
    if (membershipPortRange != null && membershipPortRange.trim().length() > 0) {
      String[] splitted = membershipPortRange.split("-");
      range = new int[2];
      range[0] = Integer.parseInt(splitted[0].trim());
      range[1] = Integer.parseInt(splitted[1].trim());
      // NumberFormatException if any could be thrown

      if (range[0] < 0 || range[0] >= range[1] || range[1] < 0 || range[1] > 65535) {
        range = null;
      }
    }
    return range != null;
  }

  /**
   * @return the String representation of membershipPortRange with lower & upper limits of the port
   *         range separated by '-' e.g. 1-65535
   */
  private static String getMembershipPortRangeString(int[] membershipPortRange) {
    String membershipPortRangeString = "";
    if (membershipPortRange != null && membershipPortRange.length == 2) {
      membershipPortRangeString = membershipPortRange[0] + "-" + membershipPortRange[1];
    }

    return membershipPortRangeString;
  }

  public String getBindAddress() {
    return this.bindAddress;
  }

  public void setBindAddress(String bindAddress) {
    checkReadOnly();
    basicSetBindAddress(bindAddress);
    configChanged();
  }

  public String getServerBindAddress() {
    return this.serverBindAddress;
  }

  public void setServerBindAddress(String bindAddress) {
    checkReadOnly();
    basicSetServerBindAddress(bindAddress);
    configChanged();
  }

  public boolean getDisableTcp() {
    return this.disableTcp;
  }

  public void setDisableTcp(boolean flag) {
    checkReadOnly();
    disableTcp = flag;
    configChanged();
  }

  public void setEnableNetworkPartitionDetection(boolean newValue) {
    checkReadOnly();
    this.enableNetworkPartitionDetection = newValue;
    configChanged();
  }

  public boolean getEnableNetworkPartitionDetection() {
    return this.enableNetworkPartitionDetection;
  }

  public void setDisableAutoReconnect(boolean newValue) {
    checkReadOnly();
    this.disableAutoReconnect = newValue;
    configChanged();
  }

  public boolean getDisableAutoReconnect() {
    return this.disableAutoReconnect;
  }

  public int getMemberTimeout() {
    return this.memberTimeout;
  }

  public void setMemberTimeout(int value) {
    checkReadOnly();
    this.memberTimeout = value;
    configChanged();
  }

  private void basicSetBindAddress(String bindAddress) {
    if (!validateBindAddress(bindAddress)) {
      throw new IllegalArgumentException(
          String.format("Invalid bind address: %s",
              bindAddress));
    }
    this.bindAddress = bindAddress;
  }

  private void basicSetServerBindAddress(String bindAddress) {
    if (!validateBindAddress(bindAddress)) {
      throw new IllegalArgumentException(
          String.format("Invalid bind address: %s",
              bindAddress));
    }
    this.serverBindAddress = bindAddress;
  }

  /**
   * Returns the remote command setting to use for remote administration
   */
  public String getRemoteCommand() {
    return this.remoteCommand;
  }

  /**
   * Sets the remote command for this config object. This attribute may be modified after this
   * config object has been used to create an admin distributed system.
   */
  public void setRemoteCommand(String remoteCommand) {
    if (!ALLOW_ALL_REMOTE_COMMANDS) {
      checkRemoteCommand(remoteCommand);
    }
    this.remoteCommand = remoteCommand;
    configChanged();
  }

  private static final boolean ALLOW_ALL_REMOTE_COMMANDS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "admin.ALLOW_ALL_REMOTE_COMMANDS");
  private static final String[] LEGAL_REMOTE_COMMANDS = {"rsh", "ssh"};
  private static final String ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH =
      "Allowed remote commands include \"rsh {HOST} {CMD}\" or \"ssh {HOST} {CMD}\" with valid rsh or ssh switches. Invalid: ";

  private void checkRemoteCommand(final String remoteCommand) {
    if (remoteCommand == null || remoteCommand.isEmpty()) {
      return;
    }
    final String command = remoteCommand.toLowerCase().trim();
    if (!command.contains("{host}") || !command.contains("{cmd}")) {
      throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
    }

    final StringTokenizer tokenizer = new StringTokenizer(command, " ");
    final ArrayList<String> array = new ArrayList<String>();
    for (int i = 0; tokenizer.hasMoreTokens(); i++) {
      String string = tokenizer.nextToken();
      if (i == 0) {
        // first element must be rsh or ssh
        boolean found = false;
        for (int j = 0; j < LEGAL_REMOTE_COMMANDS.length; j++) {
          if (string.contains(LEGAL_REMOTE_COMMANDS[j])) {
            // verify command is at end of string
            if (!(string.endsWith(LEGAL_REMOTE_COMMANDS[j])
                || string.endsWith(LEGAL_REMOTE_COMMANDS[j] + ".exe"))) {
              throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
            }
            found = true;
          }
        }
        if (!found) {
          throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
        }
      } else {
        final boolean isSwitch = string.startsWith("-");
        final boolean isHostOrCmd = string.equals("{host}") || string.equals("{cmd}");

        // additional elements must be switches or values-for-switches or {host} or user@{host} or
        // {cmd}
        if (!isSwitch && !isHostOrCmd) {
          final String previous =
              (array == null || array.isEmpty()) ? null : array.get(array.size() - 1);
          final boolean isValueForSwitch = previous != null && previous.startsWith("-");
          final boolean isHostWithUser = string.contains("@") && string.endsWith("{host}");

          if (!(isValueForSwitch || isHostWithUser)) {
            throw new IllegalArgumentException(ILLEGAL_REMOTE_COMMAND_RSH_OR_SSH + remoteCommand);
          }
        }
      }
      array.add(string);
    }
  }

  public String getSystemName() {
    return this.systemName;
  }

  public void setSystemName(final String systemName) {
    checkReadOnly();
    this.systemName = systemName;
    configChanged();
  }

  /**
   * Returns an array of configurations for statically known CacheServers
   *
   * @since GemFire 4.0
   */
  public CacheServerConfig[] getCacheServerConfigs() {
    return (CacheServerConfig[]) this.cacheServerConfigs
        .toArray(new CacheServerConfig[this.cacheServerConfigs.size()]);
  }

  public CacheVmConfig[] getCacheVmConfigs() {
    return (CacheVmConfig[]) this.cacheServerConfigs
        .toArray(new CacheVmConfig[this.cacheServerConfigs.size()]);
  }

  /**
   * Creates the configuration for a CacheServer
   *
   * @since GemFire 4.0
   */
  public CacheServerConfig createCacheServerConfig() {
    CacheServerConfig config = new CacheServerConfigImpl();
    addCacheServerConfig(config);
    return config;
  }

  public CacheVmConfig createCacheVmConfig() {
    return (CacheVmConfig) createCacheServerConfig();
  }

  /**
   * Adds the configuration for a CacheServer
   *
   * @since GemFire 4.0
   */
  private void addCacheServerConfig(CacheServerConfig managerConfig) {
    checkReadOnly();

    if (managerConfig == null)
      return;
    for (Iterator iter = this.cacheServerConfigs.iterator(); iter.hasNext();) {
      CacheServerConfigImpl impl = (CacheServerConfigImpl) iter.next();
      if (impl.equals(managerConfig)) {
        return;
      }
    }
    this.cacheServerConfigs.add(managerConfig);
    configChanged();
  }

  /**
   * Removes the configuration for a CacheServer
   *
   * @since GemFire 4.0
   */
  public void removeCacheServerConfig(CacheServerConfig managerConfig) {
    removeCacheVmConfig((CacheVmConfig) managerConfig);
  }

  public void removeCacheVmConfig(CacheVmConfig managerConfig) {
    checkReadOnly();
    this.cacheServerConfigs.remove(managerConfig);
    configChanged();
  }

  /**
   * Returns the configurations of all managed distribution locators
   */
  public DistributionLocatorConfig[] getDistributionLocatorConfigs() {
    if (this.system != null) {
      DistributionLocator[] locators = this.system.getDistributionLocators();
      DistributionLocatorConfig[] configs = new DistributionLocatorConfig[locators.length];
      for (int i = 0; i < locators.length; i++) {
        configs[i] = locators[i].getConfig();
      }
      return configs;

    } else {
      Object[] array = new DistributionLocatorConfig[this.locatorConfigs.size()];
      return (DistributionLocatorConfig[]) this.locatorConfigs.toArray(array);
    }
  }

  /**
   * Creates the configuration for a DistributionLocator
   */
  public DistributionLocatorConfig createDistributionLocatorConfig() {
    checkReadOnly();
    DistributionLocatorConfig config = new DistributionLocatorConfigImpl();
    addDistributionLocatorConfig(config);
    return config;
  }

  /**
   * Adds the configuration for a DistributionLocator
   */
  private void addDistributionLocatorConfig(DistributionLocatorConfig config) {
    checkReadOnly();
    this.locatorConfigs.add(config);
    configChanged();
  }

  /**
   * Removes the configuration for a DistributionLocator
   */
  public void removeDistributionLocatorConfig(DistributionLocatorConfig config) {
    checkReadOnly();
    this.locatorConfigs.remove(config);
    configChanged();
  }

  /**
   * Validates the bind address. The address may be a host name or IP address, but it must not be
   * empty and must be usable for creating an InetAddress. Cannot have a leading '/' (which
   * InetAddress.toString() produces).
   *
   * @param bindAddress host name or IP address to validate
   */
  public static boolean validateBindAddress(String bindAddress) {
    if (bindAddress == null || bindAddress.length() == 0)
      return true;
    if (InetAddressUtil.validateHost(bindAddress) == null)
      return false;
    return true;
  }

  public synchronized void configChanged() {
    ConfigListener[] clients = null;
    synchronized (this.listeners) {
      clients = (ConfigListener[]) listeners.toArray(new ConfigListener[this.listeners.size()]);
    }
    for (int i = 0; i < clients.length; i++) {
      try {
        clients[i].configChanged(this);
      } catch (Exception e) {
        logger.warn(e.getMessage(), e);
      }
    }
  }

  /**
   * Registers listener for notification of changes in this config.
   */
  public void addListener(ConfigListener listener) {
    synchronized (this.listeners) {
      this.listeners.add(listener);
    }
  }

  /**
   * Removes previously registered listener of this config.
   */
  public void removeListener(ConfigListener listener) {
    synchronized (this.listeners) {
      this.listeners.remove(listener);
    }
  }

  // -------------------------------------------------------------------------
  // SSL support...
  // -------------------------------------------------------------------------
  private boolean sslEnabled = DistributionConfig.DEFAULT_SSL_ENABLED;
  private String sslProtocols = DistributionConfig.DEFAULT_SSL_PROTOCOLS;
  private String sslCiphers = DistributionConfig.DEFAULT_SSL_CIPHERS;
  private boolean sslAuthenticationRequired = DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION;
  private Properties sslProperties = new Properties();

  public boolean isSSLEnabled() {
    return this.sslEnabled;
  }

  public void setSSLEnabled(boolean enabled) {
    checkReadOnly();
    this.sslEnabled = enabled;
    configChanged();
  }

  public String getSSLProtocols() {
    return this.sslProtocols;
  }

  public void setSSLProtocols(String protocols) {
    checkReadOnly();
    this.sslProtocols = protocols;
    configChanged();
  }

  public String getSSLCiphers() {
    return this.sslCiphers;
  }

  public void setSSLCiphers(String ciphers) {
    checkReadOnly();
    this.sslCiphers = ciphers;
    configChanged();
  }

  public boolean isSSLAuthenticationRequired() {
    return this.sslAuthenticationRequired;
  }

  public void setSSLAuthenticationRequired(boolean authRequired) {
    checkReadOnly();
    this.sslAuthenticationRequired = authRequired;
    configChanged();
  }

  public Properties getSSLProperties() {
    return this.sslProperties;
  }

  public void setSSLProperties(Properties sslProperties) {
    checkReadOnly();
    this.sslProperties = sslProperties;
    if (this.sslProperties == null) {
      this.sslProperties = new Properties();
    }
    configChanged();
  }

  public void addSSLProperty(String key, String value) {
    checkReadOnly();
    this.sslProperties.put(key, value);
    configChanged();
  }

  public void removeSSLProperty(String key) {
    checkReadOnly();
    this.sslProperties.remove(key);
    configChanged();
  }

  /**
   * @return the gfSecurityProperties
   * @since GemFire 6.6.3
   */
  public Properties getGfSecurityProperties() {
    return gfSecurityProperties;
  }

  public String getLogFile() {
    return this.logFile;
  }

  public void setLogFile(String logFile) {
    checkReadOnly();
    this.logFile = logFile;
    configChanged();
  }

  public String getLogLevel() {
    return this.logLevel;
  }

  public void setLogLevel(String logLevel) {
    checkReadOnly();
    this.logLevel = logLevel;
    configChanged();
  }

  public int getLogDiskSpaceLimit() {
    return this.logDiskSpaceLimit;
  }

  public void setLogDiskSpaceLimit(int limit) {
    checkReadOnly();
    this.logDiskSpaceLimit = limit;
    configChanged();
  }

  public int getLogFileSizeLimit() {
    return this.logFileSizeLimit;
  }

  public void setLogFileSizeLimit(int limit) {
    checkReadOnly();
    this.logFileSizeLimit = limit;
    configChanged();
  }

  /**
   * Returns the refreshInterval in seconds
   */
  public int getRefreshInterval() {
    return this.refreshInterval;
  }

  /**
   * Sets the refreshInterval in seconds
   */
  public void setRefreshInterval(int timeInSecs) {
    checkReadOnly();
    this.refreshInterval = timeInSecs;
    configChanged();
  }

  /**
   * Makes sure that the mcast port and locators are correct and consistent.
   *
   * @throws IllegalArgumentException If configuration is not valid
   */
  public void validate() {
    if (this.getMcastPort() < MIN_MCAST_PORT || this.getMcastPort() > MAX_MCAST_PORT) {
      throw new IllegalArgumentException(
          String.format("mcastPort must be an integer inclusively between %s and %s",

              new Object[] {Integer.valueOf(MIN_MCAST_PORT), Integer.valueOf(MAX_MCAST_PORT)}));
    }

    LogLevel.getLogWriterLevel(this.logLevel);

    if (this.logFileSizeLimit < MIN_LOG_FILE_SIZE_LIMIT
        || this.logFileSizeLimit > MAX_LOG_FILE_SIZE_LIMIT) {
      throw new IllegalArgumentException(
          String.format("LogFileSizeLimit must be an integer between %s and %s",
              new Object[] {Integer.valueOf(MIN_LOG_FILE_SIZE_LIMIT),
                  Integer.valueOf(MAX_LOG_FILE_SIZE_LIMIT)}));
    }

    if (this.logDiskSpaceLimit < MIN_LOG_DISK_SPACE_LIMIT
        || this.logDiskSpaceLimit > MAX_LOG_DISK_SPACE_LIMIT) {
      throw new IllegalArgumentException(
          String.format("LogDiskSpaceLimit must be an integer between %s and %s",
              new Object[] {Integer.valueOf(MIN_LOG_DISK_SPACE_LIMIT),
                  Integer.valueOf(MAX_LOG_DISK_SPACE_LIMIT)}));
    }

    parseEntityConfigXMLFile();
  }

  /**
   * Makes a deep copy of this config object.
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    DistributedSystemConfigImpl other = (DistributedSystemConfigImpl) super.clone();
    other.system = null;
    other.cacheServerConfigs = new HashSet();
    other.locatorConfigs = new HashSet();

    DistributionLocatorConfig[] myLocators = this.getDistributionLocatorConfigs();
    for (int i = 0; i < myLocators.length; i++) {
      DistributionLocatorConfig locator = myLocators[i];
      other.addDistributionLocatorConfig((DistributionLocatorConfig) locator.clone());
    }

    CacheServerConfig[] myCacheServers = this.getCacheServerConfigs();
    for (int i = 0; i < myCacheServers.length; i++) {
      CacheServerConfig locator = myCacheServers[i];
      other.addCacheServerConfig((CacheServerConfig) locator.clone());
    }

    return other;
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer(1000);
    String lf = System.getProperty("line.separator");
    if (lf == null)
      lf = ",";

    buf.append("DistributedSystemConfig(");
    buf.append(lf);
    buf.append("  system-name=");
    buf.append(String.valueOf(this.systemName));
    buf.append(lf);
    buf.append("  " + MCAST_ADDRESS + "=");
    buf.append(String.valueOf(this.mcastAddress));
    buf.append(lf);
    buf.append("  " + MCAST_PORT + "=");
    buf.append(String.valueOf(this.mcastPort));
    buf.append(lf);
    buf.append("  " + LOCATORS + "=");
    buf.append(String.valueOf(this.locators));
    buf.append(lf);
    buf.append("  " + MEMBERSHIP_PORT_RANGE_NAME + "=");
    buf.append(getMembershipPortRange());
    buf.append(lf);
    buf.append("  " + BIND_ADDRESS + "=");
    buf.append(String.valueOf(this.bindAddress));
    buf.append(lf);
    buf.append("  " + TCP_PORT + "=" + this.tcpPort);
    buf.append(lf);
    buf.append("  " + DISABLE_TCP + "=");
    buf.append(String.valueOf(this.disableTcp));
    buf.append(lf);
    buf.append("  " + DISABLE_AUTO_RECONNECT + "=");
    buf.append(String.valueOf(this.disableAutoReconnect));
    buf.append(lf);
    buf.append("  " + REMOTE_COMMAND_NAME + "=");
    buf.append(String.valueOf(this.remoteCommand));
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_ENABLED + "=");
    buf.append(String.valueOf(this.sslEnabled));
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_CIPHERS + "=");
    buf.append(String.valueOf(this.sslCiphers));
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_PROTOCOLS + "=");
    buf.append(String.valueOf(this.sslProtocols));
    buf.append(lf);
    buf.append("  " + CLUSTER_SSL_REQUIRE_AUTHENTICATION + "=");
    buf.append(String.valueOf(this.sslAuthenticationRequired));
    buf.append(lf);
    buf.append("  " + LOG_FILE_NAME + "=");
    buf.append(String.valueOf(this.logFile));
    buf.append(lf);
    buf.append("  " + LOG_LEVEL_NAME + "=");
    buf.append(String.valueOf(this.logLevel));
    buf.append(lf);
    buf.append("  " + LOG_DISK_SPACE_LIMIT_NAME + "=");
    buf.append(String.valueOf(this.logDiskSpaceLimit));
    buf.append(lf);
    buf.append("  " + LOG_FILE_SIZE_LIMIT_NAME + "=");
    buf.append(String.valueOf(this.logFileSizeLimit));
    buf.append(lf);
    buf.append("  " + REFRESH_INTERVAL_NAME + "=");
    buf.append(String.valueOf(this.refreshInterval));
    buf.append(")");
    return buf.toString();
  }
}
