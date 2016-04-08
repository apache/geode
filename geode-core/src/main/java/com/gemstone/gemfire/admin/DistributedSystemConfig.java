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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.admin.internal.InetAddressUtil;
//import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

//import java.net.InetAddress;
import java.util.Properties;

/**
 * Configuration for defining a GemFire distributed system to
 * administrate.  This configuration includes information about the
 * discovery mechanism used to find members of the distributed system
 * and information about {@linkplain ManagedEntity managed entities}
 * such as {@linkplain DistributionLocator distribution locators}
 * and {@linkplain CacheVm GemFire cache vms}
 * that can be {@linkplain AdminDistributedSystem#start started}.  
 *
 * <P>
 *
 * Detailed descriptions of many of these configuration attributes can
 * be found in the {@link
 * com.gemstone.gemfire.distributed.DistributedSystem
 * DistributedSystem} class.  Note that the default values of these
 * configuration attributes can be specified using Java system
 * properties.
 *
 * <P>
 *
 * A <code>DistributedSystemConfig</code> can be modified using a
 * number of mutator methods until the
 * <code>AdminDistributedSystem</code> that it configures {@linkplain
 * AdminDistributedSystem#connect connects} to the distributed system.
 * After that, attempts to modify most attributes in the
 * <code>DistributedSystemConfig</code> will result in an {@link
 * IllegalStateException} being thrown.  If you wish to use the same
 * <code>DistributedSystemConfig</code> to configure multiple
 * <code>AdminDistributedSystem</code>s, a copy of the
 * <code>DistributedSystemConfig</code> object can be made by invoking
 * the {@link #clone} method.
 *
 * @since 3.5 
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
*/
public interface DistributedSystemConfig extends Cloneable {

  /** The name of an XML file that specifies the configuration for the
   * {@linkplain ManagedEntity managed entities} administered by the
   * <code>DistributedSystem</code>.  The XML file must conform to a
   * <a href="doc-files/ds5_0.dtd">dtd</a>. */
  public static final String ENTITY_CONFIG_XML_FILE_NAME =
    "entity-config-xml-file";

  /** The default value of the "entity-config-xml-file" property
   * ("distributed-system.xml"). */
  public static final String DEFAULT_ENTITY_CONFIG_XML_FILE =
    "distributed-system.xml";

  /** The name of the "system-id" property */
  public static final String SYSTEM_ID_NAME = "system-id";

  /** The default value of the "system-id" property ("") */
  public static final String DEFAULT_SYSTEM_ID = "Default System";

  /** The name of the "name" property. See {@link #getSystemName()}. */
  public static final String NAME_NAME = DistributionConfig.NAME_NAME;

  /** The default value of the "name" property (""). See {@link #getSystemName()}. */
  public static final String DEFAULT_NAME = "";

  /** The name of the "mcastPort" property */
  public static final String MCAST_PORT_NAME = 
    DistributionConfig.MCAST_PORT_NAME;

  /** The default value of the "mcastPort" property (10334) */
  public static final int DEFAULT_MCAST_PORT =
    DistributionConfig.DEFAULT_MCAST_PORT;

  /** The minimum mcastPort (0) */
  public static final int MIN_MCAST_PORT =
    DistributionConfig.MIN_MCAST_PORT;

  /** The maximum mcastPort (65535) */
  public static final int MAX_MCAST_PORT =
    DistributionConfig.MAX_MCAST_PORT;
  
  /** The name of the "mcastAddress" property */
  public static final String MCAST_ADDRESS_NAME = 
    DistributionConfig.MCAST_ADDRESS_NAME;

  /** The default value of the "mcastAddress" property (239.192.81.1). */
  public static final String DEFAULT_MCAST_ADDRESS = 
      InetAddressUtil.toString(DistributionConfig.DEFAULT_MCAST_ADDRESS);
  
  /** The name of the "membership-port-range" property
   * @since 6.5
   */
  public static final String MEMBERSHIP_PORT_RANGE_NAME = 
    DistributionConfig.MEMBERSHIP_PORT_RANGE_NAME;

  /**
   * The default membership-port-range.
   * <p> Actual value is <code>[1024,65535]</code>.
   * @since 6.5
   */
  public static final int[] DEFAULT_MEMBERSHIP_PORT_RANGE = 
    DistributionConfig.DEFAULT_MEMBERSHIP_PORT_RANGE;
  
  /** settings for tcp-port
   * @since 6.5
   */
  public static final String TCP_PORT_NAME = DistributionConfig.TCP_PORT_NAME;
  /** The default value of the "tcpPort" property.
   * <p> Actual value is <code>0</code>.
   * @since 6.5
   */
  public static final int DEFAULT_TCP_PORT = DistributionConfig.DEFAULT_TCP_PORT;

  /**
   * The default AckWaitThreshold.
   * <p> Actual value of this constant is <code>15</code> seconds.
   */
  public static final int DEFAULT_ACK_WAIT_THRESHOLD =
    DistributionConfig.DEFAULT_ACK_WAIT_THRESHOLD;
  /**
   * The minimum AckWaitThreshold.
   * <p> Actual value of this constant is <code>1</code> second.
   */
  public static final int MIN_ACK_WAIT_THRESHOLD =
    DistributionConfig.MIN_ACK_WAIT_THRESHOLD;
  /**
   * The maximum AckWaitThreshold.
   * <p> Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  public static final int MAX_ACK_WAIT_THRESHOLD =
    DistributionConfig.MIN_ACK_WAIT_THRESHOLD;
  
  /**
   * The default ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>0</code> seconds, which
   * turns off forced disconnects based on ack wait periods.
   */
  public static final int DEFAULT_ACK_SEVERE_ALERT_THRESHOLD =
    DistributionConfig.DEFAULT_ACK_SEVERE_ALERT_THRESHOLD;
  /**
   * The minimum ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>0</code> second,
   * which turns off forced disconnects based on ack wait periods.
   */
  public static final int MIN_ACK_SEVERE_ALERT_THRESHOLD =
    DistributionConfig.MIN_ACK_SEVERE_ALERT_THRESHOLD;
  /**
   * The maximum ackSevereAlertThreshold.
   * <p> Actual value of this constant is <code>MAX_INT</code> seconds.
   */
  public static final int MAX_ACK_SEVERE_ALERT_THRESHOLD =
    DistributionConfig.MAX_ACK_SEVERE_ALERT_THRESHOLD;

  /** The name of the "locators" property (comma-delimited host[port] list) */
  public static final String LOCATORS_NAME = 
    DistributionConfig.LOCATORS_NAME;

  /** The default value of the "locators" property ("") */
  public static final String DEFAULT_LOCATORS =
    DistributionConfig.DEFAULT_LOCATORS;

  /** The name of the "bindAddress" property */
  public static final String BIND_ADDRESS_NAME =
    DistributionConfig.BIND_ADDRESS_NAME;

  /** The default value of the "bindAddress" property */
  public static final String DEFAULT_BIND_ADDRESS =
    DistributionConfig.DEFAULT_BIND_ADDRESS;

  /** The name of the remote-command property */
  public static final String REMOTE_COMMAND_NAME = "remote-command";

  /** The default value of the remote-command property */
  public static final String DEFAULT_REMOTE_COMMAND =
    "rsh -n {HOST} {CMD}";

  /** The name of the "SSLEnabled" property */
  public static final String SSL_ENABLED_NAME = 
    DistributionConfig.SSL_ENABLED_NAME;

  /** The default ssl-enabled state (<code>false</code>) */
  public static final boolean DEFAULT_SSL_ENABLED =
    DistributionConfig.DEFAULT_SSL_ENABLED;
 
  /** The name of the "SSLProtocols" property */
  public static final String SSL_PROTOCOLS_NAME =
    DistributionConfig.SSL_PROTOCOLS_NAME;

  /** The default ssl-protocols value (<code>any</code>) */
  public static final String DEFAULT_SSL_PROTOCOLS =
    DistributionConfig.DEFAULT_SSL_PROTOCOLS;
   
  /** The name of the "SSLCiphers" property */
  public static final String SSL_CIPHERS_NAME = 
    DistributionConfig.SSL_CIPHERS_NAME;

  /** The default ssl-ciphers value. (<code>any</code>) */
  public static final String DEFAULT_SSL_CIPHERS =
    DistributionConfig.DEFAULT_SSL_CIPHERS; 
  
  /** The name of the "SSLRequireAuthentication" property */
  public static final String SSL_REQUIRE_AUTHENTICATION_NAME =
    DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME;

  /** The default ssl-require-authentication value (<code>true</code>) */
  public static final boolean DEFAULT_SSL_REQUIRE_AUTHENTICATION =
    DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION; 

  /** The default disable-tcp value (<code>false</code>) */
  public static final boolean DEFAULT_DISABLE_TCP = DistributionConfig.DEFAULT_DISABLE_TCP;
  
  /** The default enable-network-partition-detection setting (<code>false</code>) */
  public static final boolean DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION = DistributionConfig.DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION;
  
  /** The default disable-auto-reconnect setting (<code>false</code>) */
  public static final boolean DEFAULT_DISABLE_AUTO_RECONNECT = DistributionConfig.DEFAULT_DISABLE_AUTO_RECONNECT;

  /** The default failure-detection timeout period for member heart-beat responses */
  public static final int DEFAULT_MEMBER_TIMEOUT = DistributionConfig.DEFAULT_MEMBER_TIMEOUT;
  
  /** The name of the "logFile" property */
  public static final String LOG_FILE_NAME = "log-file";

  /** The default log-file value ("" which directs logging to standard
   * output) */
  public static final String DEFAULT_LOG_FILE = "";

  /** The name of the "logLevel" property */
  public static final String LOG_LEVEL_NAME = "log-level";

  /** The default log level ("config") */
  public static final String DEFAULT_LOG_LEVEL = "config";

  /** The name of the "LogDiskSpaceLimit" property */
  public static final String LOG_DISK_SPACE_LIMIT_NAME =
    "log-disk-space-limit";

  /** The default log disk space limit in megabytes (0) */
  public static final int DEFAULT_LOG_DISK_SPACE_LIMIT =
    DistributionConfig.DEFAULT_LOG_DISK_SPACE_LIMIT;

  /** The minimum log disk space limit in megabytes (0) */
  public static final int MIN_LOG_DISK_SPACE_LIMIT = 
    DistributionConfig.MIN_LOG_DISK_SPACE_LIMIT;

  /** The minimum log disk space limit in megabytes (1000000) */
  public static final int MAX_LOG_DISK_SPACE_LIMIT =
    DistributionConfig.MAX_LOG_DISK_SPACE_LIMIT;
    
  /** The name of the "LogFileSizeLimit" property */
  public static final String LOG_FILE_SIZE_LIMIT_NAME =
    "log-file-size-limit";

  /** The default log file size limit in megabytes (0) */
  public static final int DEFAULT_LOG_FILE_SIZE_LIMIT =
    DistributionConfig.DEFAULT_LOG_FILE_SIZE_LIMIT;

  /** The minimum log file size limit in megabytes (0) */
  public static final int MIN_LOG_FILE_SIZE_LIMIT =
    DistributionConfig.MIN_LOG_FILE_SIZE_LIMIT;

  /** The minimum log file size limit in megabytes (1000000) */
  public static final int MAX_LOG_FILE_SIZE_LIMIT =
    DistributionConfig.MAX_LOG_FILE_SIZE_LIMIT;

  /**
   * The name of the "refreshInterval" property which will apply to
   * SystemMember, SystemMemberCache and StatisticResource refresh. This interval
   * (in seconds) is used for auto-polling and updating AdminDistributedSystem
   * constituents including SystemMember, CacheServer, SystemMemberCache and
   * StatisticResource. This interval is read-only and retains the value set
   * when the config is created. Note that the resource MBeans actually refresh
   * and hit the DS only if there is an RMI client connected
   * */
  public static final String REFRESH_INTERVAL_NAME =
    "refresh-interval";

  /**
   * The default "refreshInterval" in seconds which will apply to
   * REFRESH_INTERVAL_NAME property. The default value is 15 secs
   * */
  public static final int DEFAULT_REFRESH_INTERVAL = 15;

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns the name of the XML file that specifies the configuration
   * of the {@linkplain com.gemstone.gemfire.admin.ManagedEntity
   * managed entities} administered by the
   * <code>DistributedSystem</code>.  The XML file must conform to a
   * <a href="doc-files/ds5_0.dtd">dtd</a>.
   *
   * @since 4.0
   */
  public String getEntityConfigXMLFile();

  /**
   * Sets the name of the XML file that specifies the configuration of
   * managed entities administered by the
   * <code>DistributedSystem</code>. 
   */
  public void setEntityConfigXMLFile(String xmlFile);

  /** Returns the string identity for the system */
  public String getSystemId();

  /** Sets the string identity for the system */
  public void setSystemId(String systemId);

  /** Returns the optional non-unique name for the system */
  public String getSystemName();

  /** Sets the optional non-unique name for the system */
  public void setSystemName(final String name);

  /** Returns the multicast address for the system */
  public String getMcastAddress();

  /** Sets the multicast address for the system */
  public void setMcastAddress(String mcastAddress);

  /** Returns the multicast port for the system */
  public int getMcastPort();
  
  /** Sets the multicast port for the system */
  public void setMcastPort(int mcastPort);

  /** Returns the ack-wait-threshold for the system */
  public int getAckWaitThreshold();
  
  /** Sets the ack-wait-threshold for the system */
  public void setAckWaitThreshold(int seconds);

  /** Returns the ack-severe-alert-threshold for the system */
  public int getAckSevereAlertThreshold();
  
  /** Sets the ack-severe-alert-threshold for the system */
  public void setAckSevereAlertThreshold(int seconds);

  /** Returns a comma-delimited list of locators for the system */
  public String getLocators();

  /** Sets the comma-delimited list of locators for the system */
  public void setLocators(String locators);

  /**
   * Returns the membership-port-range property of the Distributed System. This 
   * range is given as two numbers separated by a minus sign.
   * @since 6.5
   */
  public String getMembershipPortRange();
  
  /**
   * Sets the membership-port-range property of the Distributed System. This 
   * range is given as two numbers separated by a minus sign.
   * @since 6.5
   */
  public void setMembershipPortRange(String membershipPortRange);
  
  
  /**
   * Sets the primary communication port number for the Distributed System.
   * @since 6.5
   */
  public void setTcpPort(int port);

  /**
   * Returns the primary communication port number for the Distributed System.
   * @since 6.5
   */
  public int getTcpPort();


  /** Sets the disable-tcp property for the system.  When tcp is disabled,
      the cache uses udp for unicast messaging.   This must be consistent
      across all members of the distributed system. The default is to enable
      tcp. */
  public void setDisableTcp(boolean flag);
  
  /** Returns the disable-tcp property for the system.  When tcp is
      disabled, the cache uses udp for unicast messaging.  This must be
      consistent across all members of the distributed system.  The default
      is to enable tcp.
   */
  public boolean getDisableTcp();


  /**
   * Turns on network partition detection
   */
  public void setEnableNetworkPartitionDetection(boolean newValue);
  /**
   * Returns true if network partition detection is enabled.
   */
  public boolean getEnableNetworkPartitionDetection();
  
  /**
   * Disables auto reconnect after being forced out of the distributed system
   */
  public void setDisableAutoReconnect(boolean newValue);
  
  /**
   * Returns true if auto reconnect is disabled
   */
  public boolean getDisableAutoReconnect();
  
  

  /**
   * Returns the member-timeout millisecond value used in failure-detection
   * protocols
   */
  public int getMemberTimeout();

  /**
   * Set the millisecond value of the member-timeout used in failure-detection
   * protocols.  This timeout determines how long a member has to respond to
   * a heartbeat request. The member is given three chances before being
   * kicked out of the distributed system with a SystemConnectException.
   */
  public void setMemberTimeout(int value);

  /**
   * Returns the IP address to which the distributed system's server
   * sockets are bound.
   *
   * @since 4.0
   */
  public String getBindAddress();

  /**
   * Sets the IP address to which the distributed system's server
   * sockets are bound.
   *
   * @since 4.0
   */
  public void setBindAddress(String bindAddress);
  
  
  /**
   * Returns the IP address to which client/server server sockets are
   * bound
   */
  public String getServerBindAddress();
  
  /**
   * Sets the IP address to which a server cache will bind when listening
   * for client cache connections. 
   */
  public void setServerBindAddress(String bindAddress);
  

  /** Returns the remote command setting to use for remote administration */
  public String getRemoteCommand();

  /** 
   * Sets the remote command setting to use for remote administration.
   * This attribute may be modified after this
   * <code>DistributedSystemConfig</code> has been used to create an
   * <codE>AdminDistributedSystem</code>.
   */
  public void setRemoteCommand(String command);

  /** Returns the value of the "ssl-enabled" property. */
  public boolean isSSLEnabled();

  /** Sets the value of the "ssl-enabled" property. */
  public void setSSLEnabled(boolean enabled);

  /** Returns the value of the "ssl-protocols" property. */
  public String getSSLProtocols();

  /** Sets the value of the "ssl-protocols" property. */
  public void setSSLProtocols(String protocols);

  /** Returns the value of the "ssl-ciphers" property. */
  public String getSSLCiphers();

  /** Sets the value of the "ssl-ciphers" property. */
  public void setSSLCiphers(String ciphers);

  /** Returns the value of the "ssl-require-authentication" property. */
  public boolean isSSLAuthenticationRequired();

  /** Sets the value of the "ssl-require-authentication" property. */
  public void setSSLAuthenticationRequired(boolean authRequired);
  
  /** Returns the provider-specific properties for SSL. */
  public Properties getSSLProperties();

  /** Sets the provider-specific properties for SSL. */
  public void setSSLProperties(Properties sslProperties);

  /** Adds an SSL property */
  public void addSSLProperty(String key, String value);

  /** Removes an SSL property */
  public void removeSSLProperty(String key);
  
  /**
   * Returns the name of the log file to which informational messages
   * are written.
   *
   * @see com.gemstone.gemfire.i18n.LogWriterI18n
   */
  public String getLogFile();

  /**
   * Sets the name of the log file to which informational messages
   * are written.
   *
   * @see com.gemstone.gemfire.i18n.LogWriterI18n
   */
  public void setLogFile(String logFile);

  /**
   * Returns the level at which informational messages are logged.
   */
  public String getLogLevel();

  /**
   * Sets the level at which information messages are logged.
   */
  public void setLogLevel(String logLevel);

  /**
   * Returns the log disk space limit in megabytes
   */
  public int getLogDiskSpaceLimit();

  /**
   * Sets the log disk space limit in megabytes
   */
  public void setLogDiskSpaceLimit(int limit);

  /**
   * Returns the log file size limit in megabytes
   */
  public int getLogFileSizeLimit();

  /**
   * Sets the log file size limit in megabytes
   */
  public void setLogFileSizeLimit(int limit);

  /**
   * Returns the refreshInterval in seconds used for auto-polling and updating
   * AdminDistributedSystem constituents including SystemMember, CacheServer,
   * SystemMemberCache and StatisticResource
   * @since 6.0
   */
  public int getRefreshInterval();

  /**
   * Sets the refreshInterval in seconds
   * @since 6.0
   */
  public void setRefreshInterval(int timeInSecs);

  /** 
   * Returns an array of configurations for statically known
   * <code>CacheServers</code>.
   * @deprecated as of 5.7 use {@link #getCacheVmConfigs} instead.
   */ 
  @Deprecated
  public CacheServerConfig[] getCacheServerConfigs();

  /** 
   * Creates the configuration for a CacheServer
   * @deprecated as of 5.7 use {@link #createCacheVmConfig} instead.
   */
  @Deprecated
  public CacheServerConfig createCacheServerConfig();

  /** 
   * Removes the configuration for a CacheServer
   * @deprecated as of 5.7 use {@link #removeCacheVmConfig} instead.
   */
  @Deprecated
  public void removeCacheServerConfig(CacheServerConfig managerConfig);

  /** 
   * Returns an array of configurations for statically known
   * {@link CacheVm}s.
   * @since 5.7
   */ 
  public CacheVmConfig[] getCacheVmConfigs();

  /** 
   * Creates the configuration for a {@link CacheVm}.
   * @since 5.7
   */
  public CacheVmConfig createCacheVmConfig();

  /** 
   * Removes the configuration for a {@link CacheVm}
   * @since 5.7
   */
  public void removeCacheVmConfig(CacheVmConfig existing);

  /**
   * Returns configuration information about {@link
   * DistributionLocator}s that are managed by an
   * <code>AdminDistributedSystem</code>.
   */
  public DistributionLocatorConfig[] getDistributionLocatorConfigs();
  
  /**
   * Creates a new <code>DistributionLocatorConfig</code> for a
   * distribution locator that is managed in this distributed system.
   * The default locator config is set to not use multicast
   */
  public DistributionLocatorConfig createDistributionLocatorConfig();

  /**
   * Removes a <code>DistributionLocatorConfig</code> from the
   * distributed system.
   */
  public void removeDistributionLocatorConfig(DistributionLocatorConfig config);

  /** Registers listener for notification of changes in this config. */
  public void addListener(ConfigListener listener);

  /** Removes previously registered listener of this config. */
  public void removeListener(ConfigListener listener);

  /**
   * Validates that this distributed system configuration is correct
   * and consistent.
   *
   * @throws IllegalStateException
   *         If this config is not valid
   * @throws AdminXmlException
   *         If the {@linkplain #getEntityConfigXMLFile entity config
   *         XML file} is not valid 
   */
  public void validate();

  /**
   * Returns a copy of this <code>DistributedSystemConfig</code>
   * object whose configuration can be modified.  Note that this
   * {@link DistributedSystemConfig.ConfigListener ConfigListener}s
   * that are registered on this config object are not cloned.
   *
   * @since 4.0
   */
  public Object clone() throws CloneNotSupportedException;

  ////////////////////// Inner Classes  //////////////////////

  /** A listener whose callback methods are invoked when this config
   * changes. */
  public interface ConfigListener extends java.util.EventListener {

    /** Invoked when this configurated is changed. */
    public void configChanged(DistributedSystemConfig config);
  }

}

