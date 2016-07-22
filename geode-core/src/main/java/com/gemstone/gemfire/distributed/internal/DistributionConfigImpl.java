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

package com.gemstone.gemfire.distributed.internal;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.redis.GeodeRedisServer;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.net.SocketCreator;
import com.gemstone.gemfire.internal.process.ProcessLauncherContext;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer;

/**
 * Provides an implementation of <code>DistributionConfig</code> that
 * knows how to read the configuration file.
 * <p>
 * <p>
 * <p>
 * Note that if you add a property to this interface, should should
 * update  the {@link
 * #DistributionConfigImpl(DistributionConfig) copy constructor}.
 * @see InternalDistributedSystem
 * @since GemFire 2.1
 */
public class DistributionConfigImpl extends AbstractDistributionConfig implements Serializable {

  private static final long serialVersionUID = 4096393792893825167L;

  /**
   * The name of the distribution manager/shared memory connection
   */
  private String name = DEFAULT_NAME;

  /**
   * The tcp/ip port used for distribution
   */
  private int tcpPort = DEFAULT_TCP_PORT;

  /**
   * The multicast port used for distribution
   */
  private int mcastPort = DEFAULT_MCAST_PORT;

  /**
   * The multicast ttl used for distribution
   */
  private int mcastTtl = DEFAULT_MCAST_TTL;

  private int socketLeaseTime = DEFAULT_SOCKET_LEASE_TIME;
  private int socketBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
  private boolean conserveSockets = DEFAULT_CONSERVE_SOCKETS;

  /**
   * Comma-delimited list of the application roles performed by this member.
   */
  private String roles = DEFAULT_ROLES;

  /**
   * The multicast address used for distribution
   */
  private InetAddress mcastAddress = DEFAULT_MCAST_ADDRESS;

  /**
   * The address server socket's should listen on
   */
  private String bindAddress = DEFAULT_BIND_ADDRESS;

  /**
   * The address server socket's in a  client-server topology should listen on
   */
  private String serverBindAddress = DEFAULT_SERVER_BIND_ADDRESS;

  /**
   * The locations of the distribution locators
   */
  private String locators = DEFAULT_LOCATORS;

  /**
   * The amount of time to wait for a locator to appear when starting up
   */
  private int locatorWaitTime;

  /**
   * The name of the log file
   */
  private File logFile = DEFAULT_LOG_FILE;

  protected File deployWorkingDir = DEFAULT_DEPLOY_WORKING_DIR;

  /**
   * The level at which log messages are logged
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl#levelNameToCode(String)
   */
  protected int logLevel = DEFAULT_LOG_LEVEL;

  /**
   * bind-address and host of locator to start
   */
  private String startLocator = DEFAULT_START_LOCATOR;

  /**
   * port of locator to start.  use bind-address as host name
   */
  private int startLocatorPort;

  /**
   * Is statistic sampling enabled?
   */
  protected boolean statisticSamplingEnabled = DEFAULT_STATISTIC_SAMPLING_ENABLED;

  /**
   * The rate (in milliseconds) at which statistics are sampled
   */
  protected int statisticSampleRate = DEFAULT_STATISTIC_SAMPLE_RATE;

  /**
   * The name of the file to which statistics should be archived
   */
  protected File statisticArchiveFile = DEFAULT_STATISTIC_ARCHIVE_FILE;

  /**
   * The amount of time to wait for a ACK message
   */
  private int ackWaitThreshold = DEFAULT_ACK_WAIT_THRESHOLD;

  /**
   * The amount of time to wait for a ACK message after the ackWaitThreshold
   * before shunning members that haven't responded.  If zero, this feature
   * is disabled.
   */
  private int ackForceDisconnectThreshold = DEFAULT_ACK_SEVERE_ALERT_THRESHOLD;

  /**
   * The name of an XML file used to initialize the cache
   */
  private File cacheXmlFile = Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE) ? new File("") : DEFAULT_CACHE_XML_FILE;

  protected int archiveDiskSpaceLimit = DEFAULT_ARCHIVE_DISK_SPACE_LIMIT;
  protected int archiveFileSizeLimit = DEFAULT_ARCHIVE_FILE_SIZE_LIMIT;
  protected int logDiskSpaceLimit = DEFAULT_LOG_DISK_SPACE_LIMIT;
  protected int logFileSizeLimit = DEFAULT_LOG_FILE_SIZE_LIMIT;

  private boolean clusterSSLEnabled = DEFAULT_CLUSTER_SSL_ENABLED;
  private String clusterSSLProtocols = DEFAULT_CLUSTER_SSL_PROTOCOLS;
  private String clusterSSLCiphers = DEFAULT_CLUSTER_SSL_CIPHERS;
  private boolean clusterSSLRequireAuthentication = DEFAULT_CLUSTER_SSL_REQUIRE_AUTHENTICATION;
  private String clusterSSLKeyStore = DEFAULT_CLUSTER_SSL_KEYSTORE;
  private String clusterSSLKeyStoreType = DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE;
  private String clusterSSLKeyStorePassword = DEFAULT_CLUSTER_SSL_KEYSTORE_PASSWORD;
  private String clusterSSLTrustStore = DEFAULT_CLUSTER_SSL_TRUSTSTORE;
  private String clusterSSLTrustStorePassword = DEFAULT_CLUSTER_SSL_TRUSTSTORE_PASSWORD;
  private String clusterSSLAlias = DEFAULT_CLUSTER_SSL_ALIAS;

  /**
   * multicast send buffer size, in bytes
   */
  protected int mcastSendBufferSize = DEFAULT_MCAST_SEND_BUFFER_SIZE;
  /**
   * multicast receive buffer size, in bytes
   */
  protected int mcastRecvBufferSize = DEFAULT_MCAST_RECV_BUFFER_SIZE;
  /**
   * flow-of-control parameters for multicast messaging
   */
  protected FlowControlParams mcastFlowControl = DEFAULT_MCAST_FLOW_CONTROL;

  /**
   * datagram socket send buffer size, in bytes
   */
  protected int udpSendBufferSize = DEFAULT_UDP_SEND_BUFFER_SIZE;
  /**
   * datagram socket receive buffer size, in bytes
   */
  protected int udpRecvBufferSize = DEFAULT_UDP_RECV_BUFFER_SIZE;
  /**
   * max datagram message size, in bytes.  This should be < 64k
   */
  protected int udpFragmentSize = DEFAULT_UDP_FRAGMENT_SIZE;

  /**
   * whether tcp/ip sockets should be disabled
   */
  protected boolean disableTcp = DEFAULT_DISABLE_TCP;

  /**
   * whether time statistics should be enabled for the distributed system
   */
  protected boolean enableTimeStatistics = DEFAULT_ENABLE_TIME_STATISTICS;

  /**
   * member contact timeout, in milliseconds, for failure detection
   */
  protected int memberTimeout = DEFAULT_MEMBER_TIMEOUT;

  /**
   * the Jgroups port ranges allowed
   */
  private int[] membershipPortRange = DEFAULT_MEMBERSHIP_PORT_RANGE;

  /**
   * Max wait time for the member before reconnecting to the DS in case of
   * required role loss.
   */
  private int maxWaitTimeForReconnect = DEFAULT_MAX_WAIT_TIME_FOR_RECONNECT;
  /**
   * Max number of tries allowed for reconnect in case of required role loss.
   */
  private int maxNumReconnectTries = DEFAULT_MAX_NUM_RECONNECT_TRIES;


  private int asyncDistributionTimeout = DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT;
  private int asyncQueueTimeout = DEFAULT_ASYNC_QUEUE_TIMEOUT;
  private int asyncMaxQueueSize = DEFAULT_ASYNC_MAX_QUEUE_SIZE;

  /**
   * @since GemFire 5.7
   */
  private String clientConflation = CLIENT_CONFLATION_PROP_VALUE_DEFAULT;

  /**
   * The id of the durable client
   */
  private String durableClientId = DEFAULT_DURABLE_CLIENT_ID;

  /**
   * The timeout of the durable client
   */
  private int durableClientTimeout = DEFAULT_DURABLE_CLIENT_TIMEOUT;

  /**
   * The client authentication initialization method name
   */
  private String securityClientAuthInit = DEFAULT_SECURITY_CLIENT_AUTH_INIT;

  /**
   * The client authenticating method name
   */
  private String securityClientAuthenticator = DEFAULT_SECURITY_CLIENT_AUTHENTICATOR;

  /**
   * The security manager class name
   */
  private String securityManager = DEFAULT_SECURITY_MANAGER;

  /**
   * The post processor class name
   */
  private String postProcessor = DEFAULT_SECURITY_POST_PROCESSOR;

  /**
   * The client Diffie-Hellman method name
   */
  private String securityClientDHAlgo = DEFAULT_SECURITY_CLIENT_DHALGO;

  /**
   * The peer authentication initialization method name
   */
  private String securityPeerAuthInit = DEFAULT_SECURITY_PEER_AUTH_INIT;

  /**
   * The peer authenticating method name
   */
  private String securityPeerAuthenticator = DEFAULT_SECURITY_PEER_AUTHENTICATOR;

  /**
   * The client authorization method name
   */
  private String securityClientAccessor = DEFAULT_SECURITY_CLIENT_ACCESSOR;

  /**
   * The post-processing client authorization method name
   */
  private String securityClientAccessorPP = DEFAULT_SECURITY_CLIENT_ACCESSOR_PP;

  /**
   * The level at which security related log messages are logged
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl#levelNameToCode(String)
   */
  protected int securityLogLevel = DEFAULT_LOG_LEVEL;

  /**
   * whether network partition detection algorithms are enabled
   */
  private boolean enableNetworkPartitionDetection = DEFAULT_ENABLE_NETWORK_PARTITION_DETECTION;

  /**
   * whether auto reconnect after network partition is disabled
   */
  private boolean disableAutoReconnect = DEFAULT_DISABLE_AUTO_RECONNECT;

  /**
   * The security log file
   */
  private File securityLogFile = DEFAULT_SECURITY_LOG_FILE;

  /**
   * The p2p membership check timeout
   */
  private int securityPeerMembershipTimeout = DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT;

  /**
   * The member security credentials
   */
  private Properties security = new Properties();

  /**
   * The User defined properties to be used for cache.xml replacements
   */
  private Properties userDefinedProps = new Properties();
  /**
   * Prefix to use for properties that are put as JVM java properties for use
   * with layers (e.g. jgroups membership) that do not have a
   * <code>DistributionConfig</code> object.
   */
  public static final String SECURITY_SYSTEM_PREFIX = GEMFIRE_PREFIX + "sys.";

  /**
   * whether to remove unresponsive client or not
   */
  private boolean removeUnresponsiveClient = DEFAULT_REMOVE_UNRESPONSIVE_CLIENT;

  /**
   * Is delta propagation enabled or not
   **/
  private boolean deltaPropagation = DEFAULT_DELTA_PROPAGATION;

  private Map props;

  private int distributedSystemId = DistributionConfig.DEFAULT_DISTRIBUTED_SYSTEM_ID;

  /**
   * The locations of the remote distribution locators
   */
  private String remoteLocators = DEFAULT_REMOTE_LOCATORS;

  private boolean enforceUniqueHost = DistributionConfig.DEFAULT_ENFORCE_UNIQUE_HOST;

  private String redundancyZone = DistributionConfig.DEFAULT_REDUNDANCY_ZONE;

  /**
   * holds the ssl properties specified in gfsecurity.properties
   */
  private Properties sslProperties = new Properties();

  /**
   * holds the ssl properties specified in gfsecurity.properties
   */
  private Properties clusterSSLProperties = new Properties();

  private String groups = DEFAULT_GROUPS;

  protected boolean enableSharedConfiguration = DistributionConfig.DEFAULT_ENABLE_CLUSTER_CONFIGURATION;
  protected boolean useSharedConfiguration = DistributionConfig.DEFAULT_USE_CLUSTER_CONFIGURATION;
  protected boolean loadSharedConfigurationFromDir = DistributionConfig.DEFAULT_LOAD_CLUSTER_CONFIG_FROM_DIR;
  protected String clusterConfigDir = "";


  private int httpServicePort = DEFAULT_HTTP_SERVICE_PORT;

  private String httpServiceBindAddress = DEFAULT_HTTP_SERVICE_BIND_ADDRESS;

  private boolean startDevRestApi = DEFAULT_START_DEV_REST_API;
  /**
   * port on which {@link GemFireMemcachedServer} server is started
   */
  private int memcachedPort;

  /**
   * protocol for GemFireMemcachedServer
   */
  private String memcachedProtocol = DEFAULT_MEMCACHED_PROTOCOL;

  /**
   * Bind address for GemFireMemcachedServer
   */
  private String memcachedBindAddress = DEFAULT_MEMCACHED_BIND_ADDRESS;

  /**
   * Are distributed transactions enabled or not
   */
  private boolean distributedTransactions = DEFAULT_DISTRIBUTED_TRANSACTIONS;


  /**
   * port on which {@link GeodeRedisServer} is started
   */
  private int redisPort = DEFAULT_REDIS_PORT;

  /**
   * Bind address for GeodeRedisServer
   */
  private String redisBindAddress = DEFAULT_REDIS_BIND_ADDRESS;

  private String redisPassword = DEFAULT_REDIS_PASSWORD;

  private boolean jmxManager = Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE) ? true : DEFAULT_JMX_MANAGER;
  private boolean jmxManagerStart = DEFAULT_JMX_MANAGER_START;

  private int jmxManagerPort = DEFAULT_JMX_MANAGER_PORT;
  private String jmxManagerBindAddress = DEFAULT_JMX_MANAGER_BIND_ADDRESS;
  private String jmxManagerHostnameForClients = DEFAULT_JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
  private String jmxManagerPasswordFile = DEFAULT_JMX_MANAGER_PASSWORD_FILE;
  private String jmxManagerAccessFile = DEFAULT_JMX_MANAGER_ACCESS_FILE;
  private int jmxManagerHttpPort = DEFAULT_HTTP_SERVICE_PORT;
  private int jmxManagerUpdateRate = DEFAULT_JMX_MANAGER_UPDATE_RATE;

  private boolean jmxManagerSSLEnabled = DEFAULT_JMX_MANAGER_SSL_ENABLED;
  private boolean jmxManagerSslRequireAuthentication = DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION;
  private String jmxManagerSslProtocols = DEFAULT_JMX_MANAGER_SSL_PROTOCOLS;
  private String jmxManagerSslCiphers = DEFAULT_JMX_MANAGER_SSL_CIPHERS;
  private Properties jmxManagerSslProperties = new Properties();

  private String jmxManagerSSLKeyStore = DEFAULT_JMX_MANAGER_SSL_KEYSTORE;
  private String jmxManagerSSLKeyStoreType = DEFAULT_JMX_MANAGER_SSL_KEYSTORE_TYPE;
  private String jmxManagerSSLKeyStorePassword = DEFAULT_JMX_MANAGER_SSL_KEYSTORE_PASSWORD;
  private String jmxManagerSSLTrustStore = DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE;
  private String jmxManagerSSLTrustStorePassword = DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD;

  private String jmxManagerSSLAlias = clusterSSLAlias;

  private boolean serverSSLEnabled = DEFAULT_SERVER_SSL_ENABLED;
  private boolean serverSslRequireAuthentication = DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION;
  private String serverSslProtocols = DEFAULT_SERVER_SSL_PROTOCOLS;
  private String serverSslCiphers = DEFAULT_SERVER_SSL_CIPHERS;
  private Properties serverSslProperties = new Properties();

  private String serverSSLKeyStore = DEFAULT_SERVER_SSL_KEYSTORE;
  private String serverSSLKeyStoreType = DEFAULT_SERVER_SSL_KEYSTORE_TYPE;
  private String serverSSLKeyStorePassword = DEFAULT_SERVER_SSL_KEYSTORE_PASSWORD;
  private String serverSSLTrustStore = DEFAULT_SERVER_SSL_TRUSTSTORE;
  private String serverSSLTrustStorePassword = DEFAULT_SERVER_SSL_TRUSTSTORE_PASSWORD;

  private String serverSSLAlias = clusterSSLAlias;

  private boolean gatewaySSLEnabled = DEFAULT_GATEWAY_SSL_ENABLED;
  private boolean gatewaySslRequireAuthentication = DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION;
  private String gatewaySslProtocols = DEFAULT_GATEWAY_SSL_PROTOCOLS;
  private String gatewaySslCiphers = DEFAULT_GATEWAY_SSL_CIPHERS;
  private Properties gatewaySslProperties = new Properties();

  private String gatewaySSLKeyStore = DEFAULT_GATEWAY_SSL_KEYSTORE;
  private String gatewaySSLKeyStoreType = DEFAULT_GATEWAY_SSL_KEYSTORE_TYPE;
  private String gatewaySSLKeyStorePassword = DEFAULT_GATEWAY_SSL_KEYSTORE_PASSWORD;
  private String gatewaySSLTrustStore = DEFAULT_GATEWAY_SSL_TRUSTSTORE;
  private String gatewaySSLTrustStorePassword = DEFAULT_GATEWAY_SSL_TRUSTSTORE_PASSWORD;

  private String gatewaySSLAlias = clusterSSLAlias;


  private boolean httpServiceSSLEnabled = DEFAULT_HTTP_SERVICE_SSL_ENABLED;
  private boolean httpServiceSSLRequireAuthentication = DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION;
  private String httpServiceSSLProtocols = DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS;
  private String httpServiceSSLCiphers = DEFAULT_HTTP_SERVICE_SSL_CIPHERS;
  private Properties httpServiceSSLProperties = new Properties();

  private String httpServiceSSLKeyStore = DEFAULT_HTTP_SERVICE_SSL_KEYSTORE;
  private String httpServiceSSLKeyStoreType = DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_TYPE;
  private String httpServiceSSLKeyStorePassword = DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;
  private String httpServiceSSLTrustStore = DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE;
  private String httpServiceSSLTrustStorePassword = DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;

  private String httpServiceSSLAlias = clusterSSLAlias;

  private String[] sslEnabledComponents = DEFAULT_SSL_ENABLED_COMPONENTS;

  private Map<String, ConfigSource> sourceMap = Collections.synchronizedMap(new HashMap<String, ConfigSource>());

  protected String userCommandPackages = DEFAULT_USER_COMMAND_PACKAGES;

  /**
   * "off-heap-memory-size" with value of "" or "<size>[g|m]"
   */
  protected String offHeapMemorySize = DEFAULT_OFF_HEAP_MEMORY_SIZE;

  /**
   * Whether pages should be locked into memory or allowed to swap to disk
   */
  private boolean lockMemory = DEFAULT_LOCK_MEMORY;

  private String shiroInit = "";

  //////////////////////  Constructors  //////////////////////

  /**
   * Create a new <code>DistributionConfigImpl</code> from the
   * contents of another <code>DistributionConfig</code>.
   */
  public DistributionConfigImpl(DistributionConfig other) {
    this.name = other.getName();
    this.tcpPort = other.getTcpPort();
    this.mcastPort = other.getMcastPort();
    this.mcastTtl = other.getMcastTtl();
    this.socketLeaseTime = other.getSocketLeaseTime();
    this.socketBufferSize = other.getSocketBufferSize();
    this.conserveSockets = other.getConserveSockets();
    this.roles = other.getRoles();
    this.mcastAddress = other.getMcastAddress();
    this.bindAddress = other.getBindAddress();
    this.serverBindAddress = other.getServerBindAddress();
    this.locators = ((DistributionConfigImpl) other).locators;
    this.locatorWaitTime = other.getLocatorWaitTime();
    this.remoteLocators = other.getRemoteLocators();
    this.startLocator = other.getStartLocator();
    this.startLocatorPort = ((DistributionConfigImpl) other).startLocatorPort;
    this.deployWorkingDir = other.getDeployWorkingDir();
    this.logFile = other.getLogFile();
    this.logLevel = other.getLogLevel();
    this.statisticSamplingEnabled = other.getStatisticSamplingEnabled();
    this.statisticSampleRate = other.getStatisticSampleRate();
    this.statisticArchiveFile = other.getStatisticArchiveFile();
    this.ackWaitThreshold = other.getAckWaitThreshold();
    this.ackForceDisconnectThreshold = other.getAckSevereAlertThreshold();
    this.cacheXmlFile = other.getCacheXmlFile();
    this.archiveDiskSpaceLimit = other.getArchiveDiskSpaceLimit();
    this.archiveFileSizeLimit = other.getArchiveFileSizeLimit();
    this.logDiskSpaceLimit = other.getLogDiskSpaceLimit();
    this.logFileSizeLimit = other.getLogFileSizeLimit();
    this.clusterSSLEnabled = other.getClusterSSLEnabled();
    this.clusterSSLProtocols = other.getClusterSSLProtocols();
    this.clusterSSLCiphers = other.getClusterSSLCiphers();
    this.clusterSSLRequireAuthentication = other.getClusterSSLRequireAuthentication();
    this.clusterSSLKeyStore = other.getClusterSSLKeyStore();
    this.clusterSSLKeyStoreType = other.getClusterSSLKeyStoreType();
    this.clusterSSLKeyStorePassword = other.getClusterSSLKeyStorePassword();
    this.clusterSSLTrustStore = other.getClusterSSLTrustStore();
    this.clusterSSLTrustStorePassword = other.getClusterSSLTrustStorePassword();
    this.asyncDistributionTimeout = other.getAsyncDistributionTimeout();
    this.asyncQueueTimeout = other.getAsyncQueueTimeout();
    this.asyncMaxQueueSize = other.getAsyncMaxQueueSize();
    this.modifiable = true;
    // the following were added after version 4.1.2
    this.mcastSendBufferSize = other.getMcastSendBufferSize();
    this.mcastRecvBufferSize = other.getMcastRecvBufferSize();
    this.mcastFlowControl = other.getMcastFlowControl();
    this.udpSendBufferSize = other.getUdpSendBufferSize();
    this.udpRecvBufferSize = other.getUdpRecvBufferSize();
    this.udpFragmentSize = other.getUdpFragmentSize();
    this.disableTcp = other.getDisableTcp();
    this.enableTimeStatistics = other.getEnableTimeStatistics();
    this.memberTimeout = other.getMemberTimeout();
    this.membershipPortRange = other.getMembershipPortRange();
    this.maxWaitTimeForReconnect = other.getMaxWaitTimeForReconnect();
    this.maxNumReconnectTries = other.getMaxNumReconnectTries();
    this.clientConflation = other.getClientConflation();
    this.durableClientId = other.getDurableClientId();
    this.durableClientTimeout = other.getDurableClientTimeout();

    this.enableNetworkPartitionDetection = other.getEnableNetworkPartitionDetection();
    this.disableAutoReconnect = other.getDisableAutoReconnect();

    this.securityClientAuthInit = other.getSecurityClientAuthInit();
    this.securityClientAuthenticator = other.getSecurityClientAuthenticator();
    this.securityClientDHAlgo = other.getSecurityClientDHAlgo();
    this.securityPeerAuthInit = other.getSecurityPeerAuthInit();
    this.securityPeerAuthenticator = other.getSecurityPeerAuthenticator();
    this.securityClientAccessor = other.getSecurityClientAccessor();
    this.securityClientAccessorPP = other.getSecurityClientAccessorPP();
    this.securityPeerMembershipTimeout = other.getSecurityPeerMembershipTimeout();
    this.securityLogLevel = other.getSecurityLogLevel();
    this.securityLogFile = other.getSecurityLogFile();
    this.security.putAll(other.getSecurityProps());
    this.removeUnresponsiveClient = other.getRemoveUnresponsiveClient();
    this.deltaPropagation = other.getDeltaPropagation();
    this.distributedSystemId = other.getDistributedSystemId();
    this.redundancyZone = other.getRedundancyZone();
    this.enforceUniqueHost = other.getEnforceUniqueHost();
    this.sslProperties = other.getSSLProperties();
    this.clusterSSLProperties = other.getClusterSSLProperties();
    this.jmxManagerSslProperties = other.getJmxSSLProperties();
    //Similar to this.security, assigning userDefinedProps
    this.userDefinedProps.putAll(other.getUserDefinedProps());

    // following added for 7.0
    this.groups = other.getGroups();
    this.jmxManager = other.getJmxManager();
    this.jmxManagerStart = other.getJmxManagerStart();
    this.jmxManagerSSLEnabled = other.getJmxManagerSSLEnabled();
    this.jmxManagerSslRequireAuthentication = other.getJmxManagerSSLRequireAuthentication();
    this.jmxManagerSslProtocols = other.getJmxManagerSSLProtocols();
    this.jmxManagerSslCiphers = other.getJmxManagerSSLCiphers();
    this.jmxManagerSSLKeyStore = other.getJmxManagerSSLKeyStore();
    this.jmxManagerSSLKeyStoreType = other.getJmxManagerSSLKeyStoreType();
    this.jmxManagerSSLKeyStorePassword = other.getJmxManagerSSLKeyStorePassword();
    this.jmxManagerSSLTrustStore = other.getJmxManagerSSLTrustStore();
    this.jmxManagerSSLTrustStorePassword = other.getJmxManagerSSLTrustStorePassword();
    this.jmxManagerSslProperties = other.getJmxSSLProperties();
    this.jmxManagerPort = other.getJmxManagerPort();
    this.jmxManagerBindAddress = other.getJmxManagerBindAddress();
    this.jmxManagerHostnameForClients = other.getJmxManagerHostnameForClients();
    this.jmxManagerPasswordFile = other.getJmxManagerPasswordFile();
    this.jmxManagerAccessFile = other.getJmxManagerAccessFile();
    this.jmxManagerHttpPort = other.getJmxManagerHttpPort();
    this.jmxManagerUpdateRate = other.getJmxManagerUpdateRate();
    this.memcachedPort = other.getMemcachedPort();
    this.memcachedProtocol = other.getMemcachedProtocol();
    this.memcachedBindAddress = other.getMemcachedBindAddress();
    this.redisPort = other.getRedisPort();
    this.redisBindAddress = other.getRedisBindAddress();
    this.redisPassword = other.getRedisPassword();
    this.userCommandPackages = other.getUserCommandPackages();

    // following added for 8.0
    this.enableSharedConfiguration = other.getEnableClusterConfiguration();
    this.loadSharedConfigurationFromDir = other.getLoadClusterConfigFromDir();
    this.clusterConfigDir = other.getClusterConfigDir();
    this.useSharedConfiguration = other.getUseSharedConfiguration();
    this.serverSSLEnabled = other.getServerSSLEnabled();
    this.serverSslRequireAuthentication = other.getServerSSLRequireAuthentication();
    this.serverSslProtocols = other.getServerSSLProtocols();
    this.serverSslCiphers = other.getServerSSLCiphers();
    this.serverSSLKeyStore = other.getServerSSLKeyStore();
    this.serverSSLKeyStoreType = other.getServerSSLKeyStoreType();
    this.serverSSLKeyStorePassword = other.getServerSSLKeyStorePassword();
    this.serverSSLTrustStore = other.getServerSSLTrustStore();
    this.serverSSLTrustStorePassword = other.getServerSSLTrustStorePassword();
    this.serverSslProperties = other.getServerSSLProperties();

    this.gatewaySSLEnabled = other.getGatewaySSLEnabled();
    this.gatewaySslRequireAuthentication = other.getGatewaySSLRequireAuthentication();
    this.gatewaySslProtocols = other.getGatewaySSLProtocols();
    this.gatewaySslCiphers = other.getGatewaySSLCiphers();
    this.gatewaySSLKeyStore = other.getGatewaySSLKeyStore();
    this.gatewaySSLKeyStoreType = other.getGatewaySSLKeyStoreType();
    this.gatewaySSLKeyStorePassword = other.getGatewaySSLKeyStorePassword();
    this.gatewaySSLTrustStore = other.getGatewaySSLTrustStore();
    this.gatewaySSLTrustStorePassword = other.getGatewaySSLTrustStorePassword();
    this.gatewaySslProperties = other.getGatewaySSLProperties();

    this.httpServicePort = other.getHttpServicePort();
    this.httpServiceBindAddress = other.getHttpServiceBindAddress();

    this.httpServiceSSLEnabled = other.getHttpServiceSSLEnabled();
    this.httpServiceSSLCiphers = other.getHttpServiceSSLCiphers();
    this.httpServiceSSLProtocols = other.getHttpServiceSSLProtocols();
    this.httpServiceSSLRequireAuthentication = other.getHttpServiceSSLRequireAuthentication();
    this.httpServiceSSLKeyStore = other.getHttpServiceSSLKeyStore();
    this.httpServiceSSLKeyStorePassword = other.getHttpServiceSSLKeyStorePassword();
    this.httpServiceSSLKeyStoreType = other.getHttpServiceSSLKeyStoreType();
    this.httpServiceSSLTrustStore = other.getHttpServiceSSLTrustStore();
    this.httpServiceSSLTrustStorePassword = other.getHttpServiceSSLTrustStorePassword();
    this.httpServiceSSLProperties = other.getHttpServiceSSLProperties();

    this.startDevRestApi = other.getStartDevRestApi();

    // following added for 9.0
    this.offHeapMemorySize = other.getOffHeapMemorySize();

    Map<String, ConfigSource> otherSources = ((DistributionConfigImpl) other).sourceMap;
    if (otherSources != null) {
      this.sourceMap = new HashMap<String, ConfigSource>(otherSources);
    }

    this.lockMemory = other.getLockMemory();
    this.distributedTransactions = other.getDistributedTransactions();
    this.shiroInit = other.getShiroInit();
    this.securityManager = other.getSecurityManager();
    this.postProcessor = other.getPostProcessor();
    this.clusterSSLAlias = other.getClusterSSLAlias();
    this.gatewaySSLAlias = other.getGatewaySSLAlias();
    this.httpServiceSSLAlias = other.getHTTPServiceSSLAlias();
    this.jmxManagerSSLAlias = other.getJMXManagerSSLAlias();
    this.serverSSLAlias = other.getServerSSLAlias();
    this.sslEnabledComponents = ((DistributionConfigImpl) other).sslEnabledComponents;
  }

  /**
   * Set to true to make attributes writable.
   * Set to false to make attributes read only.
   * By default they are read only.
   */
  protected boolean modifiable = false;

  @Override
  protected boolean _modifiableDefault() {
    return modifiable;
  }

  /**
   * Creates a default application config. Does not read any
   * properties. Currently only used by DistributionConfigImpl.main.
   */
  private DistributionConfigImpl() {
    // do nothing. We just want a default config
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given
   * non-default configuration properties.  See {@link
   * com.gemstone.gemfire.distributed.DistributedSystem#connect} for a
   * list of exceptions that may be thrown.
   * @param nonDefault The configuration properties specified by the caller
   */
  public DistributionConfigImpl(Properties nonDefault) {
    this(nonDefault, false, false);
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given
   * non-default configuration properties. See
   * {@link com.gemstone.gemfire.distributed.DistributedSystem#connect} for a
   * list of exceptions that may be thrown.
   * @param nonDefault The configuration properties specified by the caller
   * @param ignoreGemFirePropsFile whether to skip loading distributed system properties from
   * gemfire.properties file
   *
   * @since GemFire 6.5
   */

  public DistributionConfigImpl(Properties nonDefault, boolean ignoreGemFirePropsFile) {
    this(nonDefault, ignoreGemFirePropsFile, false);
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given
   * non-default configuration properties. See
   * {@link com.gemstone.gemfire.distributed.DistributedSystem#connect} for a
   * list of exceptions that may be thrown.
   * @param nonDefault The configuration properties specified by the caller
   * @param ignoreGemFirePropsFile whether to skip loading distributed system properties from
   * gemfire.properties file
   * @param isConnected whether to skip Validation for SSL properties and copy of ssl
   * properties to other ssl properties. This parameter will be used
   * till we provide support for ssl-* properties.
   *
   * @since GemFire 8.0
   */
  public DistributionConfigImpl(Properties nonDefault, boolean ignoreGemFirePropsFile, boolean isConnected) {
    HashMap props = new HashMap();
    if (!ignoreGemFirePropsFile) {//For admin bug #40434
      props.putAll(loadPropertiesFromURL(DistributedSystem.getPropertyFileURL(), false));
    }
    props.putAll(loadPropertiesFromURL(DistributedSystem.getSecurityPropertiesFileURL(), true));

    // Now override values picked up from the file with values passed
    // in from the caller's code
    if (nonDefault != null) {
      props.putAll(nonDefault);
      setSource(nonDefault, ConfigSource.api());
    }
    //Now remove all user defined properties from props.
    for (Object entry : props.entrySet()) {
      Map.Entry<String, String> ent = (Map.Entry<String, String>) entry;
      if (((String) ent.getKey()).startsWith(USERDEFINED_PREFIX_NAME)) {
        userDefinedProps.put(ent.getKey(), ent.getValue());
      }
    }
    // Now override values picked up from the file or code with values
    // from the system properties.
    String[] attNames = getAttributeNames();

    // For gemfire.security-* properties, we will need to look at
    // all the system properties instead of looping through attNames
    Set attNameSet = new HashSet();
    for (int index = 0; index < attNames.length; ++index) {
      attNameSet.add(GEMFIRE_PREFIX + attNames[index]);
    }

    /* clone() is a synchronized method for Properties (actually in Hashtable) */
    Properties sysProps = (Properties) System.getProperties().clone();
    Iterator<?> sysPropsIter = sysProps.entrySet().iterator();
    while (sysPropsIter.hasNext()) {
      Map.Entry sysEntry = (Map.Entry) sysPropsIter.next();
      String sysName = (String) sysEntry.getKey();
      if (attNameSet.contains(sysName) || sysName.startsWith(GEMFIRE_PREFIX + SECURITY_PREFIX_NAME) || sysName.startsWith(GEMFIRE_PREFIX + SSL_SYSTEM_PROPS_NAME)) {
        String sysValue = (String) sysEntry.getValue();
        if (sysValue != null) {
          String attName = sysName.substring(GEMFIRE_PREFIX.length());
          props.put(attName, sysValue);
          this.sourceMap.put(attName, ConfigSource.sysprop());
        }
      }
    }
    sysProps.clear(); //clearing cloned SysProps

    final Properties overriddenDefaults = ProcessLauncherContext.getOverriddenDefaults();
    if (!overriddenDefaults.isEmpty()) {
      for (String key : overriddenDefaults.stringPropertyNames()) {
        // only apply the overridden default if it's not already specified in props
        final String property = key.substring(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX.length());
        if (!props.containsKey((property))) {
          props.put(property, overriddenDefaults.getProperty(key));
          this.sourceMap.put(property, ConfigSource.launcher());
        }
      }
    }

    initialize(props);

    if (securityPeerAuthInit != null && securityPeerAuthInit.length() > 0) {
      System.setProperty(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTH_INIT, securityPeerAuthInit);
    }
    if (securityPeerAuthenticator != null && securityPeerAuthenticator.length() > 0) {
      System.setProperty(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTHENTICATOR, securityPeerAuthenticator);
    }

    Iterator iter = security.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      System.setProperty(SECURITY_SYSTEM_PREFIX + (String) entry.getKey(), (String) entry.getValue());
    }
    if (!isConnected) {
      copySSLPropsToServerSSLProps();
      copySSLPropsToJMXSSLProps();
      copyClusterSSLPropsToGatewaySSLProps();
      copySSLPropsToHTTPSSLProps();
    }

    // Make attributes writeable only
    this.modifiable = true;
    validateConfigurationProperties(props);
    // Make attributes read only
    this.modifiable = false;

  }

  /**
   * Here we will validate the correctness of the set properties as per the CheckAttributeChecker annotations defined in #AbstractDistributionConfig
   * @param props
   */
  private void validateConfigurationProperties(final HashMap props) {
    for (Object o : props.keySet()) {
      String propertyName = (String) o;
      Object value = null;
      try {
        Method method = getters.get(propertyName);
        if (method != null) {
          value = method.invoke(this, new Object[] {});
        }
      } catch (Exception e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        }
        if (e.getCause() instanceof RuntimeException) {
          throw (RuntimeException) e.getCause();
        } else {
          throw new InternalGemFireException("error invoking getter for property" + propertyName);
        }
      }
      checkAttribute(propertyName, value);
    }
  }

  /*
   * if jmx-manager-ssl is true and jmx-manager-ssl-enabled is false then override jmx-manager-ssl-enabled with jmx-manager-ssl
   * if jmx-manager-ssl-enabled is false, then use the properties from cluster-ssl-* properties
   * if jmx-manager-ssl-*properties are given then use them, and copy the unspecified jmx-manager properties from cluster-properties 
   */
  private void copySSLPropsToJMXSSLProps() {
    boolean jmxSSLEnabledOverriden = this.sourceMap.get(JMX_MANAGER_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = this.sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = this.sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !jmxSSLEnabledOverriden && !hasSSLComponents) {
      this.jmxManagerSSLEnabled = this.clusterSSLEnabled;
      this.sourceMap.put(JMX_MANAGER_SSL_ENABLED, this.sourceMap.get(CLUSTER_SSL_ENABLED));
      if (this.sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        this.jmxManagerSslCiphers = this.clusterSSLCiphers;
        this.sourceMap.put(JMX_MANAGER_SSL_CIPHERS, this.sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        this.jmxManagerSslProtocols = this.clusterSSLProtocols;
        this.sourceMap.put(JMX_MANAGER_SSL_PROTOCOLS, this.sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        this.jmxManagerSslRequireAuthentication = this.clusterSSLRequireAuthentication;
        this.sourceMap.put(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION, this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        this.jmxManagerSSLKeyStore = this.clusterSSLKeyStore;
        this.sourceMap.put(JMX_MANAGER_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        this.jmxManagerSSLKeyStoreType = this.clusterSSLKeyStoreType;
        this.sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        this.jmxManagerSSLKeyStorePassword = this.clusterSSLKeyStorePassword;
        this.sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        this.jmxManagerSSLTrustStore = this.clusterSSLTrustStore;
        this.sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        this.jmxManagerSSLTrustStorePassword = this.clusterSSLTrustStorePassword;
        this.sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      this.jmxManagerSslProperties.putAll(this.clusterSSLProperties);
    }

    if (jmxSSLEnabledOverriden) {
      if (this.sourceMap.get(JMX_MANAGER_SSL_KEYSTORE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        this.jmxManagerSSLKeyStore = this.clusterSSLKeyStore;
        this.sourceMap.put(JMX_MANAGER_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(JMX_MANAGER_SSL_KEYSTORE_TYPE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        this.jmxManagerSSLKeyStoreType = this.clusterSSLKeyStoreType;
        this.sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(JMX_MANAGER_SSL_KEYSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        this.jmxManagerSSLKeyStorePassword = this.clusterSSLKeyStorePassword;
        this.sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(JMX_MANAGER_SSL_TRUSTSTORE) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        this.jmxManagerSSLTrustStore = this.clusterSSLTrustStore;
        this.sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        this.jmxManagerSSLTrustStorePassword = this.clusterSSLTrustStorePassword;
        this.sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
    }
  }

  /*
   * if http-service-ssl-enabled is false, then use the properties from cluster-ssl-* properties
   * if http-service-ssl-*properties are given then use them, and copy the unspecified http-service properties from cluster-properties 
   */
  private void copySSLPropsToHTTPSSLProps() {
    boolean httpServiceSSLEnabledOverriden = this.sourceMap.get(HTTP_SERVICE_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = this.sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = this.sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !httpServiceSSLEnabledOverriden && !hasSSLComponents) {
      this.httpServiceSSLEnabled = this.clusterSSLEnabled;
      this.sourceMap.put(HTTP_SERVICE_SSL_ENABLED, this.sourceMap.get(CLUSTER_SSL_ENABLED));

      if (this.sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        this.httpServiceSSLCiphers = this.clusterSSLCiphers;
        this.sourceMap.put(HTTP_SERVICE_SSL_CIPHERS, this.sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        this.httpServiceSSLProtocols = this.clusterSSLProtocols;
        this.sourceMap.put(HTTP_SERVICE_SSL_PROTOCOLS, this.sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        this.httpServiceSSLRequireAuthentication = this.clusterSSLRequireAuthentication;
        this.sourceMap.put(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION, this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        setHttpServiceSSLKeyStore(this.clusterSSLKeyStore);
        this.sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        setHttpServiceSSLKeyStoreType(this.clusterSSLKeyStoreType);
        this.sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        setHttpServiceSSLKeyStorePassword(this.clusterSSLKeyStorePassword);
        this.sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        setHttpServiceSSLTrustStore(this.clusterSSLTrustStore);
        this.sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        setHttpServiceSSLTrustStorePassword(this.clusterSSLTrustStorePassword);
        this.sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      this.httpServiceSSLProperties.putAll(this.clusterSSLProperties);
    }

    if (httpServiceSSLEnabledOverriden) {
      if (this.sourceMap.get(HTTP_SERVICE_SSL_KEYSTORE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        this.httpServiceSSLKeyStore = this.clusterSSLKeyStore;
        this.sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(HTTP_SERVICE_SSL_KEYSTORE_TYPE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        this.httpServiceSSLKeyStoreType = this.clusterSSLKeyStoreType;
        this.sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        this.httpServiceSSLKeyStorePassword = this.clusterSSLKeyStorePassword;
        this.sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(HTTP_SERVICE_SSL_TRUSTSTORE) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        this.httpServiceSSLTrustStore = this.clusterSSLTrustStore;
        this.sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        this.httpServiceSSLTrustStorePassword = this.clusterSSLTrustStorePassword;
        this.sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
    }

  }

  /*
   * if server-ssl-enabled is false, then use the properties from cluster-ssl-* properties
   * if server-ssl-*properties are given then use them, and copy the unspecified server properties from cluster-properties 
   */
  private void copySSLPropsToServerSSLProps() {
    boolean cacheServerSSLOverriden = this.sourceMap.get(SERVER_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = this.sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = this.sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !cacheServerSSLOverriden && !hasSSLComponents) {
      this.serverSSLEnabled = this.clusterSSLEnabled;
      this.sourceMap.put(SERVER_SSL_ENABLED, this.sourceMap.get(CLUSTER_SSL_ENABLED));
      if (this.sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        this.serverSslCiphers = this.clusterSSLCiphers;
        this.sourceMap.put(SERVER_SSL_CIPHERS, this.sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        this.serverSslProtocols = this.clusterSSLProtocols;
        this.sourceMap.put(SERVER_SSL_PROTOCOLS, this.sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        this.serverSslRequireAuthentication = this.clusterSSLRequireAuthentication;
        this.sourceMap.put(SERVER_SSL_REQUIRE_AUTHENTICATION, this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        this.serverSSLKeyStore = this.clusterSSLKeyStore;
        this.sourceMap.put(SERVER_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        this.serverSSLKeyStoreType = this.clusterSSLKeyStoreType;
        this.sourceMap.put(SERVER_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        this.serverSSLKeyStorePassword = this.clusterSSLKeyStorePassword;
        this.sourceMap.put(SERVER_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        this.serverSSLTrustStore = this.clusterSSLTrustStore;
        this.sourceMap.put(SERVER_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        this.serverSSLTrustStorePassword = this.clusterSSLTrustStorePassword;
        this.sourceMap.put(SERVER_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      this.serverSslProperties.putAll(this.clusterSSLProperties);
    }

    if (cacheServerSSLOverriden) {
      if (this.sourceMap.get(SERVER_SSL_KEYSTORE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        this.serverSSLKeyStore = this.clusterSSLKeyStore;
        this.sourceMap.put(SERVER_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(SERVER_SSL_KEYSTORE_TYPE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        this.serverSSLKeyStoreType = this.clusterSSLKeyStoreType;
        this.sourceMap.put(SERVER_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(SERVER_SSL_KEYSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        this.serverSSLKeyStorePassword = this.clusterSSLKeyStorePassword;
        this.sourceMap.put(SERVER_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(SERVER_SSL_TRUSTSTORE) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        this.serverSSLTrustStore = this.clusterSSLTrustStore;
        this.sourceMap.put(SERVER_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(SERVER_SSL_TRUSTSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        this.serverSSLTrustStorePassword = this.clusterSSLTrustStorePassword;
        this.sourceMap.put(SERVER_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
    }
  }

  /*
   * if gateway-ssl-enabled is false, then use the properties from cluster-ssl-* properties
   * if gateway-ssl-*properties are given then use them, and copy the unspecified gateway properties from cluster-properties 
   */
  private void copyClusterSSLPropsToGatewaySSLProps() {
    boolean gatewaySSLOverriden = this.sourceMap.get(GATEWAY_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = this.sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = this.sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !gatewaySSLOverriden && !hasSSLComponents) {
      this.gatewaySSLEnabled = this.clusterSSLEnabled;
      this.sourceMap.put(GATEWAY_SSL_ENABLED, this.sourceMap.get(CLUSTER_SSL_ENABLED));
      if (this.sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        this.gatewaySslCiphers = this.clusterSSLCiphers;
        this.sourceMap.put(GATEWAY_SSL_CIPHERS, this.sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        this.gatewaySslProtocols = this.clusterSSLProtocols;
        this.sourceMap.put(GATEWAY_SSL_PROTOCOLS, this.sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        this.gatewaySslRequireAuthentication = this.clusterSSLRequireAuthentication;
        this.sourceMap.put(GATEWAY_SSL_REQUIRE_AUTHENTICATION, this.sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        this.gatewaySSLKeyStore = this.clusterSSLKeyStore;
        this.sourceMap.put(GATEWAY_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        this.gatewaySSLKeyStoreType = this.clusterSSLKeyStoreType;
        this.sourceMap.put(GATEWAY_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        this.gatewaySSLKeyStorePassword = this.clusterSSLKeyStorePassword;
        this.sourceMap.put(GATEWAY_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        this.gatewaySSLTrustStore = this.clusterSSLTrustStore;
        this.sourceMap.put(GATEWAY_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        this.gatewaySSLTrustStorePassword = this.clusterSSLTrustStorePassword;
        this.sourceMap.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      this.gatewaySslProperties.putAll(this.clusterSSLProperties);
    }

    if (gatewaySSLOverriden) {
      if (this.sourceMap.get(GATEWAY_SSL_KEYSTORE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        this.gatewaySSLKeyStore = this.clusterSSLKeyStore;
        this.sourceMap.put(GATEWAY_SSL_KEYSTORE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (this.sourceMap.get(GATEWAY_SSL_KEYSTORE_TYPE) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        this.gatewaySSLKeyStoreType = this.clusterSSLKeyStoreType;
        this.sourceMap.put(GATEWAY_SSL_KEYSTORE_TYPE, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (this.sourceMap.get(GATEWAY_SSL_KEYSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        this.gatewaySSLKeyStorePassword = this.clusterSSLKeyStorePassword;
        this.sourceMap.put(GATEWAY_SSL_KEYSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (this.sourceMap.get(GATEWAY_SSL_TRUSTSTORE) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        this.gatewaySSLTrustStore = this.clusterSSLTrustStore;
        this.sourceMap.put(GATEWAY_SSL_TRUSTSTORE, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (this.sourceMap.get(GATEWAY_SSL_TRUSTSTORE_PASSWORD) == null && this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        this.gatewaySSLTrustStorePassword = this.clusterSSLTrustStorePassword;
        this.sourceMap.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD, this.sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
    }
  }


  /**
   * Produce a DistributionConfigImpl for the given properties and return it.
   */
  public static DistributionConfigImpl produce(Properties props) {
    if (props != null) {
      Object o = props.get(DS_CONFIG_NAME);
      if (o instanceof DistributionConfigImpl) {
        return (DistributionConfigImpl) o;
      }
    }
    return new DistributionConfigImpl(props, false, false);
  }

  /**
   * Produce a DistributionConfigImpl for the given properties and return it.
   */
  public static DistributionConfigImpl produce(Properties props, boolean isConnected) {
    if (props != null) {
      Object o = props.get(DS_CONFIG_NAME);
      if (o instanceof DistributionConfigImpl) {
        return (DistributionConfigImpl) o;
      }
    }
    return new DistributionConfigImpl(props, false, isConnected);
  }

  public void setApiProps(Properties apiProps) {
    if (apiProps != null) {
      setSource(apiProps, ConfigSource.api());
      this.modifiable = true;
      Iterator it = apiProps.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry) it.next();
        String propName = (String) me.getKey();
        this.props.put(propName, me.getValue());
        if (specialPropName(propName)) {
          continue;
        }
        String propVal = (String) me.getValue();
        if (propVal != null) {
          this.setAttribute(propName, propVal.trim(), this.sourceMap.get(propName));
        }
      }
      // Make attributes read only
      this.modifiable = false;
    }
  }

  public static boolean specialPropName(String propName) {
    return propName.equalsIgnoreCase(CLUSTER_SSL_ENABLED) ||
           propName.equals(SECURITY_PEER_AUTH_INIT) ||
           propName.equals(SECURITY_PEER_AUTHENTICATOR) ||
           propName.equals(LOG_WRITER_NAME) ||
           propName.equals(DS_CONFIG_NAME) ||
           propName.equals(SECURITY_LOG_WRITER_NAME) ||
           propName.equals(LOG_OUTPUTSTREAM_NAME) ||
           propName.equals(SECURITY_LOG_OUTPUTSTREAM_NAME);
  }

  @Override
  protected Map<String, ConfigSource> getAttSourceMap() {
    return this.sourceMap;
  }

  public Properties getUserDefinedProps() {
    return userDefinedProps;
  }

  /**
   * Loads the properties from gemfire.properties & gfsecurity.properties files
   * into given Properties object.
   * @param p the Properties to fill in
   *
   * @throws GemFireIOException when error occurs while reading properties file
   */
  public static void loadGemFireProperties(Properties p) throws GemFireIOException {
    loadGemFireProperties(p, false);
  }

  /**
   * Loads the properties from gemfire.properties & gfsecurity.properties files
   * into given Properties object. if <code>ignoreGemFirePropsFile</code> is
   * <code>true</code>, properties are not read from gemfire.properties.
   * @param p the Properties to fill in
   * @param ignoreGemFirePropsFile whether to ignore properties from gemfire.properties
   *
   * @throws GemFireIOException when error occurs while reading properties file
   */
  // Fix for #44924
  public static void loadGemFireProperties(Properties p, boolean ignoreGemFirePropsFile) throws GemFireIOException {
    if (!ignoreGemFirePropsFile) {
      loadPropertiesFromURL(p, DistributedSystem.getPropertyFileURL());
    }
    // load the security properties file
    loadPropertiesFromURL(p, DistributedSystem.getSecurityPropertiesFileURL());
  }

  /**
   * For every key in p mark it as being from the given source.
   */
  private void setSource(Properties p, ConfigSource source) {
    if (source == null) {
      throw new IllegalArgumentException("Valid ConfigSource must be specified instead of null.");
    }
    for (Object k : p.keySet()) {
      this.sourceMap.put((String) k, source);
    }
  }

  private Properties loadPropertiesFromURL(URL url, boolean secure) {
    Properties result = new Properties();
    loadPropertiesFromURL(result, url);
    if (!result.isEmpty()) {
      setSource(result, ConfigSource.file(url.toString(), secure));
    }
    return result;
  }

  private static void loadPropertiesFromURL(Properties p, URL url) {
    if (url != null) {
      try {
        p.load(url.openStream());
      } catch (IOException io) {
        throw new GemFireIOException(LocalizedStrings.DistributionConfigImpl_FAILED_READING_0.toLocalizedString(url), io);
      }
    }
  }

  private void initialize(Map props) {
    // Allow attributes to be modified
    this.modifiable = true;
    this.props = props;
    Iterator it = props.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry me = (Map.Entry) it.next();
      String propName = (String) me.getKey();
      // if ssl-enabled is set to true before the mcast port is set to 0, then it will error.
      // security should not be enabled before the mcast port is set to 0.
      if (specialPropName(propName)) {
        continue;
      }
      Object propVal = me.getValue();
      if (propVal != null && (propVal instanceof String)) { // weed out extraneous non-string properties
        this.setAttribute(propName, ((String) propVal).trim(), this.sourceMap.get(propName));
      }
    }
    if (props.containsKey(CLUSTER_SSL_ENABLED)) {
      this.setAttribute(CLUSTER_SSL_ENABLED, (String) props.get(CLUSTER_SSL_ENABLED), this.sourceMap.get(CLUSTER_SSL_ENABLED));
    }
    // now set the security authInit if needed
    if (props.containsKey(SECURITY_PEER_AUTH_INIT)) {
      this.setAttribute(SECURITY_PEER_AUTH_INIT, (String) props.get(SECURITY_PEER_AUTH_INIT), this.sourceMap.get(SECURITY_PEER_AUTH_INIT));
    }
    // and security authenticator if needed
    if (props.containsKey(SECURITY_PEER_AUTHENTICATOR)) {
      this.setAttribute(SECURITY_PEER_AUTHENTICATOR, (String) props.get(SECURITY_PEER_AUTHENTICATOR), this.sourceMap.get(SECURITY_PEER_AUTHENTICATOR));
    }

    // Make attributes read only
    this.modifiable = false;
  }

  public void close() {
    // Clear the extra stuff from System properties
    Properties props = System.getProperties();
    props.remove(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTH_INIT);
    props.remove(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTHENTICATOR);

    Iterator iter = security.keySet().iterator();
    while (iter.hasNext()) {
      props.remove(SECURITY_SYSTEM_PREFIX + (String) iter.next());
    }
    System.setProperties(props);
  }

  ////////////////////  Configuration Methods  ////////////////////

  public String getName() {
    return this.name;
  }

  public int getTcpPort() {
    return this.tcpPort;
  }

  public int getMcastPort() {
    return this.mcastPort;
  }

  public int getMcastTtl() {
    return this.mcastTtl;
  }

  public int getSocketLeaseTime() {
    return this.socketLeaseTime;
  }

  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public boolean getConserveSockets() {
    return this.conserveSockets;
  }

  public String getRoles() {
    return this.roles;
  }

  public int getMaxWaitTimeForReconnect() {
    return this.maxWaitTimeForReconnect;
  }

  public int getMaxNumReconnectTries() {
    return this.maxNumReconnectTries;
  }

  public InetAddress getMcastAddress() {
    try {
      return this.mcastAddress;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public String getBindAddress() {
    return this.bindAddress;
  }

  public String getServerBindAddress() {
    return this.serverBindAddress;
  }

  public String getLocators() {
    if (this.startLocator != null && this.startLocator.length() > 0) {
      String locs = this.locators;
      String startL = getStartLocator();
      int comma = startL.indexOf(',');
      if (comma >= 0) {
        startL = startL.substring(0, comma);
      }
      if (locs.length() > 0) {
        if (locs.contains(startL)) {
          return locs; // fix for bug 43929
        }
        return locs + "," + startL;
      } else {
        return startL;
      }
    }
    return this.locators;
  }

  public String getStartLocator() {
    if (this.startLocatorPort > 0) {
      if (this.bindAddress != null) {
        return this.bindAddress + "[" + this.startLocatorPort + "]";
      }
      try {
        return SocketCreator.getHostName(SocketCreator.getLocalHost()) + "[" + this.startLocatorPort + "]";
      } catch (UnknownHostException e) {
        // punt and use this.startLocator instead
      }
    }
    return this.startLocator;
  }

  public File getDeployWorkingDir() {
    return this.deployWorkingDir;
  }

  public File getLogFile() {
    return this.logFile;
  }

  public int getLogLevel() {
    return this.logLevel;
  }

  public boolean getStatisticSamplingEnabled() {
    return this.statisticSamplingEnabled;
  }

  public int getStatisticSampleRate() {
    return this.statisticSampleRate;
  }

  public File getStatisticArchiveFile() {
    return this.statisticArchiveFile;
  }

  public int getAckWaitThreshold() {
    return this.ackWaitThreshold;
  }

  public int getAckSevereAlertThreshold() {
    return this.ackForceDisconnectThreshold;
  }

  public File getCacheXmlFile() {
    return this.cacheXmlFile;
  }

  public boolean getClusterSSLEnabled() {
    return this.clusterSSLEnabled;
  }

  public String getClusterSSLProtocols() {
    return this.clusterSSLProtocols;
  }

  public String getClusterSSLCiphers() {
    return this.clusterSSLCiphers;
  }

  public boolean getClusterSSLRequireAuthentication() {
    return this.clusterSSLRequireAuthentication;
  }

  public String getClusterSSLKeyStore() {
    return this.clusterSSLKeyStore;
  }

  public String getClusterSSLKeyStoreType() {
    return this.clusterSSLKeyStoreType;
  }

  public String getClusterSSLKeyStorePassword() {
    return this.clusterSSLKeyStorePassword;
  }

  public String getClusterSSLTrustStore() {
    return this.clusterSSLTrustStore;
  }

  public String getClusterSSLTrustStorePassword() {
    return this.clusterSSLTrustStorePassword;
  }

  public int getAsyncDistributionTimeout() {
    return this.asyncDistributionTimeout;
  }

  public int getAsyncQueueTimeout() {
    return this.asyncQueueTimeout;
  }

  public int getAsyncMaxQueueSize() {
    return this.asyncMaxQueueSize;
  }

  public String getUserCommandPackages() {
    return this.userCommandPackages;
  }

  public int getHttpServicePort() {
    return this.httpServicePort;
  }

  public void setHttpServicePort(int value) {
    this.httpServicePort = value;
  }

  public String getHttpServiceBindAddress() {
    return this.httpServiceBindAddress;
  }

  public void setHttpServiceBindAddress(String value) {
    this.httpServiceBindAddress = value;
  }

  public boolean getStartDevRestApi() {
    return this.startDevRestApi;
  }

  public void setStartDevRestApi(boolean value) {
    this.startDevRestApi = value;
  }

  public void setUserCommandPackages(String value) {
    this.userCommandPackages = value;
  }

  public boolean getDeltaPropagation() {
    return this.deltaPropagation;
  }

  public void setDeltaPropagation(boolean value) {
    this.deltaPropagation = (Boolean) value;
  }

  public void setName(String value) {
    if (value == null) {
      value = DEFAULT_NAME;
    }
    this.name = (String) value;
  }

  public void setTcpPort(int value) {
    this.tcpPort = (Integer) value;
  }

  public void setMcastPort(int value) {
    this.mcastPort = (Integer) value;
  }

  public void setMcastTtl(int value) {
    this.mcastTtl = (Integer) value;
  }

  public void setSocketLeaseTime(int value) {
    this.socketLeaseTime = (Integer) value;
  }

  public void setSocketBufferSize(int value) {
    this.socketBufferSize = (Integer) value;
  }

  public void setConserveSockets(boolean value) {
    this.conserveSockets = (Boolean) value;
  }

  public void setRoles(String value) {
    this.roles = (String) value;
  }

  public void setMaxWaitTimeForReconnect(int value) {
    this.maxWaitTimeForReconnect = value;
  }

  public void setMaxNumReconnectTries(int value) {
    this.maxNumReconnectTries = value;
  }

  public void setMcastAddress(InetAddress value) {
    this.mcastAddress = (InetAddress) value;
  }

  public void setBindAddress(String value) {
    this.bindAddress = (String) value;
  }

  public void setServerBindAddress(String value) {
    this.serverBindAddress = (String) value;
  }

  public void setLocators(String value) {
    if (value == null) {
      value = DEFAULT_LOCATORS;
    }
    this.locators = (String) value;
  }

  public void setLocatorWaitTime(int value) {
    this.locatorWaitTime = value;
  }

  public int getLocatorWaitTime() {
    return this.locatorWaitTime;
  }

  public void setDeployWorkingDir(File value) {
    this.deployWorkingDir = (File) value;
  }

  public void setLogFile(File value) {
    this.logFile = (File) value;
  }

  public void setLogLevel(int value) {
    this.logLevel = (Integer) value;
  }

  /**
   * the locator startup code must be able to modify the locator log file in order
   * to establish a default log file if one hasn't been specified by the user.
   * This method will change the log file, but only in the configuration settings -
   * it won't affect a running distributed system's log file
   */
  public void unsafeSetLogFile(File value) {
    this.logFile = value;
  }

  public void setStartLocator(String value) {
    startLocatorPort = 0;
    if (value == null) {
      value = DEFAULT_START_LOCATOR;
    } else {
      // bug 37938 - allow just a port
      boolean alldigits = true;
      for (int i = 0; i < value.length(); i++) {
        char c = value.charAt(i);
        if (!Character.isDigit(c)) {
          alldigits = false;
          break;
        }
      }
      if (value.length() > 0 && alldigits) {
        try {
          int port = Integer.parseInt(value);
          if (port < 0 || port > 65535) {
            throw new GemFireConfigException("Illegal port specified for start-locator");
          }
          startLocatorPort = port;
        } catch (NumberFormatException e) {
          throw new GemFireConfigException("Illegal port specified for start-locator", e);
        }
      } else {

      }
    }
    this.startLocator = value;
  }

  public void setStatisticSamplingEnabled(boolean value) {
    this.statisticSamplingEnabled = (Boolean) value;
  }

  public void setStatisticSampleRate(int value) {
    value = (Integer) value;
    if (value < DEFAULT_STATISTIC_SAMPLE_RATE) {
      // fix 48228
      InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
      if (ids != null) {
        ids.getLogWriter()
           .info("Setting statistic-sample-rate to " + DEFAULT_STATISTIC_SAMPLE_RATE + " instead of the requested " + value + " because VSD does not work with sub-second sampling.");
      }
      value = DEFAULT_STATISTIC_SAMPLE_RATE;
    }
    this.statisticSampleRate = value;
  }

  public void setStatisticArchiveFile(File value) {
    if (value == null) {
      value = new File("");
    }
    this.statisticArchiveFile = (File) value;
  }

  public void setCacheXmlFile(File value) {
    this.cacheXmlFile = (File) value;
  }

  public void setAckWaitThreshold(int value) {
    this.ackWaitThreshold = (Integer) value;
  }

  public void setAckSevereAlertThreshold(int value) {
    this.ackForceDisconnectThreshold = (Integer) value;
  }

  public int getArchiveDiskSpaceLimit() {
    return this.archiveDiskSpaceLimit;
  }

  public void setArchiveDiskSpaceLimit(int value) {
    this.archiveDiskSpaceLimit = (Integer) value;
  }

  public int getArchiveFileSizeLimit() {
    return this.archiveFileSizeLimit;
  }

  public void setArchiveFileSizeLimit(int value) {
    this.archiveFileSizeLimit = (Integer) value;
  }

  public int getLogDiskSpaceLimit() {
    return this.logDiskSpaceLimit;
  }

  public void setLogDiskSpaceLimit(int value) {
    this.logDiskSpaceLimit = (Integer) value;
  }

  public int getLogFileSizeLimit() {
    return this.logFileSizeLimit;
  }

  public void setLogFileSizeLimit(int value) {
    this.logFileSizeLimit = (Integer) value;
  }

  public void setClusterSSLEnabled(boolean value) {
    this.clusterSSLEnabled = (Boolean) value;
  }

  public void setClusterSSLProtocols(String value) {
    this.clusterSSLProtocols = (String) value;
  }

  public void setClusterSSLCiphers(String value) {
    this.clusterSSLCiphers = (String) value;
  }

  public void setClusterSSLRequireAuthentication(boolean value) {
    this.clusterSSLRequireAuthentication = (Boolean) value;
  }

  public void setClusterSSLKeyStore(String value) {
    this.getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, value);
    this.clusterSSLKeyStore = value;
  }

  public void setClusterSSLKeyStoreType(String value) {
    this.getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME, value);
    this.clusterSSLKeyStoreType = value;
  }

  public void setClusterSSLKeyStorePassword(String value) {
    this.getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME, value);
    this.clusterSSLKeyStorePassword = value;
  }

  public void setClusterSSLTrustStore(String value) {
    this.getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, value);
    this.clusterSSLTrustStore = value;
  }

  public void setClusterSSLTrustStorePassword(String value) {
    this.getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME, value);
    this.clusterSSLTrustStorePassword = value;
  }

  public int getMcastSendBufferSize() {
    return mcastSendBufferSize;
  }

  public void setMcastSendBufferSize(int value) {
    mcastSendBufferSize = (Integer) value;
  }

  public int getMcastRecvBufferSize() {
    return mcastRecvBufferSize;
  }

  public void setMcastRecvBufferSize(int value) {
    mcastRecvBufferSize = (Integer) value;
  }

  public void setAsyncDistributionTimeout(int value) {
    this.asyncDistributionTimeout = (Integer) value;
  }

  public void setAsyncQueueTimeout(int value) {
    this.asyncQueueTimeout = (Integer) value;
  }

  public void setAsyncMaxQueueSize(int value) {
    this.asyncMaxQueueSize = (Integer) value;
  }

  public FlowControlParams getMcastFlowControl() {
    return mcastFlowControl;
  }

  public void setMcastFlowControl(FlowControlParams values) {
    mcastFlowControl = (FlowControlParams) values;
  }

  public int getUdpFragmentSize() {
    return udpFragmentSize;
  }

  public void setUdpFragmentSize(int value) {
    udpFragmentSize = (Integer) value;
  }

  public int getUdpSendBufferSize() {
    return udpSendBufferSize;
  }

  public void setUdpSendBufferSize(int value) {
    udpSendBufferSize = (Integer) value;
  }

  public int getUdpRecvBufferSize() {
    return udpRecvBufferSize;
  }

  public void setUdpRecvBufferSize(int value) {
    udpRecvBufferSize = (Integer) value;
  }

  public boolean getDisableTcp() {
    return disableTcp;
  }

  public void setDisableTcp(boolean newValue) {
    disableTcp = newValue;
  }

  public boolean getEnableTimeStatistics() {
    return enableTimeStatistics;
  }

  public void setEnableTimeStatistics(boolean newValue) {
    enableTimeStatistics = newValue;
  }

  public int getMemberTimeout() {
    return memberTimeout;
  }

  public void setMemberTimeout(int value) {
    memberTimeout = (Integer) value;
  }

  /**
   * @since GemFire 5.7
   */
  public String getClientConflation() {
    return this.clientConflation;
  }

  /**
   * @since GemFire 5.7
   */
  public void setClientConflation(String value) {
    this.clientConflation = (String) value;
  }

  public String getDurableClientId() {
    return durableClientId;
  }

  public void setDurableClientId(String value) {
    durableClientId = (String) value;
  }

  public int getDurableClientTimeout() {
    return durableClientTimeout;
  }

  public void setDurableClientTimeout(int value) {
    durableClientTimeout = (Integer) value;
  }

  public String getSecurityClientAuthInit() {
    return securityClientAuthInit;
  }

  public void setSecurityClientAuthInit(String value) {
    securityClientAuthInit = (String) value;
  }

  public String getSecurityClientAuthenticator() {
    return securityClientAuthenticator;
  }

  public String getSecurityManager() {
    return securityManager;
  }

  public String getPostProcessor() {
    return postProcessor;
  }

  public boolean getEnableNetworkPartitionDetection() {
    return this.enableNetworkPartitionDetection;
  }

  public void setEnableNetworkPartitionDetection(boolean value) {
    this.enableNetworkPartitionDetection = value;
  }

  public boolean getDisableAutoReconnect() {
    return this.disableAutoReconnect;
  }

  public void setDisableAutoReconnect(boolean value) {
    this.disableAutoReconnect = value;
  }

  public void setSecurityClientAuthenticator(String value) {
    securityClientAuthenticator = value;
  }

  public void setSecurityManager(String value) {
    securityManager = value;
  }

  public void setPostProcessor(String value) {
    postProcessor = value;
  }

  public String getSecurityClientDHAlgo() {
    return securityClientDHAlgo;
  }

  public void setSecurityClientDHAlgo(String value) {
    securityClientDHAlgo = (String) value;
  }

  public String getSecurityPeerAuthInit() {
    return securityPeerAuthInit;
  }

  public void setSecurityPeerAuthInit(String value) {
    securityPeerAuthInit = (String) value;
  }

  public String getSecurityPeerAuthenticator() {
    return securityPeerAuthenticator;
  }

  public void setSecurityPeerAuthenticator(String value) {
    securityPeerAuthenticator = (String) value;
  }

  public String getSecurityClientAccessor() {
    return securityClientAccessor;
  }

  public void setSecurityClientAccessor(String value) {
    securityClientAccessor = (String) value;
  }

  public String getSecurityClientAccessorPP() {
    return securityClientAccessorPP;
  }

  public void setSecurityClientAccessorPP(String value) {
    securityClientAccessorPP = (String) value;
  }

  public int getSecurityLogLevel() {
    return securityLogLevel;
  }

  public void setSecurityLogLevel(int value) {
    securityLogLevel = (Integer) value;
  }

  public File getSecurityLogFile() {
    return securityLogFile;
  }

  public void setSecurityLogFile(File value) {
    securityLogFile = (File) value;
  }

  public int getSecurityPeerMembershipTimeout() {
    return securityPeerMembershipTimeout;
  }

  public void setSecurityPeerMembershipTimeout(int value) {
    securityPeerMembershipTimeout = (Integer) value;
  }

  public Properties getSecurityProps() {
    return security;
  }

  public String getSecurity(String attName) {

    String attValue = security.getProperty(attName);
    return (attValue == null ? "" : attValue);
  }

  public void setSecurity(String attName, String attValue) {
    security.setProperty(attName, attValue);
  }

  public boolean getRemoveUnresponsiveClient() {
    return removeUnresponsiveClient;
  }

  public void setRemoveUnresponsiveClient(boolean value) {
    removeUnresponsiveClient = value;
  }

  public int getDistributedSystemId() {
    return distributedSystemId;
  }

  public void setDistributedSystemId(int distributedSystemId) {
    this.distributedSystemId = (Integer) distributedSystemId;

  }

  public boolean getEnforceUniqueHost() {
    return enforceUniqueHost;
  }

  public String getRedundancyZone() {
    // TODO Auto-generated method stub
    return redundancyZone;
  }

  public void setEnforceUniqueHost(boolean enforceUniqueHost) {
    this.enforceUniqueHost = (Boolean) enforceUniqueHost;

  }

  public void setRedundancyZone(String redundancyZone) {
    this.redundancyZone = (String) redundancyZone;

  }

  public void setSSLProperty(String attName, String attValue) {
    if (attName.startsWith(SYS_PROP_NAME)) {
      attName = attName.substring(SYS_PROP_NAME.length());
    }
    if (attName.endsWith(JMX_SSL_PROPS_SUFFIX)) {
      this.jmxManagerSslProperties.setProperty(attName.substring(0, attName.length() - JMX_SSL_PROPS_SUFFIX.length()), attValue);
    } else {
      this.sslProperties.setProperty(attName, attValue);

      if (!this.jmxManagerSslProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix JMX_SSL_PROPS_SUFFIX override that base
        this.jmxManagerSslProperties.setProperty(attName, attValue);
      }

      if (!this.serverSslProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix CACHESERVER_SSL_PROPS_SUFFIX override that base
        this.serverSslProperties.setProperty(attName, attValue);
      }
      if (!this.gatewaySslProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix GATEWAY_SSL_PROPS_SUFFIX override that base
        this.gatewaySslProperties.setProperty(attName, attValue);
      }
      if (!this.httpServiceSSLProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix GATEWAY_SSL_PROPS_SUFFIX override that base
        this.httpServiceSSLProperties.setProperty(attName, attValue);
      }
      if (!this.clusterSSLProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix GATEWAY_SSL_PROPS_SUFFIX override that base
        this.clusterSSLProperties.setProperty(attName, attValue);
      }
    }
  }

  public Properties getSSLProperties() {
    return this.sslProperties;
  }

  public Properties getClusterSSLProperties() {
    return this.clusterSSLProperties;
  }

  public Properties getJmxSSLProperties() {
    return this.jmxManagerSslProperties;
  }

  public String getGroups() {
    return this.groups;
  }

  public void setGroups(String value) {
    if (value == null) {
      value = DEFAULT_GROUPS;
    }
    this.groups = (String) value;
  }

  @Override
  public boolean getJmxManager() {
    return this.jmxManager;
  }

  @Override
  public void setJmxManager(boolean value) {
    this.jmxManager = value;
  }

  @Override
  public boolean getJmxManagerStart() {
    return this.jmxManagerStart;
  }

  @Override
  public void setJmxManagerStart(boolean value) {
    this.jmxManagerStart = value;
  }

  @Override
  public boolean getJmxManagerSSLEnabled() {
    return this.jmxManagerSSLEnabled;
  }

  @Override
  public void setJmxManagerSSLEnabled(boolean value) {
    this.jmxManagerSSLEnabled = value;
  }

  @Override
  public boolean getJmxManagerSSLRequireAuthentication() {
    return this.jmxManagerSslRequireAuthentication;
  }

  @Override
  public void setJmxManagerSSLRequireAuthentication(boolean jmxManagerSslRequireAuthentication) {
    this.jmxManagerSslRequireAuthentication = jmxManagerSslRequireAuthentication;
  }

  @Override
  public String getJmxManagerSSLProtocols() {
    return this.jmxManagerSslProtocols;
  }

  @Override
  public void setJmxManagerSSLProtocols(String protocols) {
    this.jmxManagerSslProtocols = protocols;
  }

  @Override
  public String getJmxManagerSSLCiphers() {
    return this.jmxManagerSslCiphers;
  }

  @Override
  public void setJmxManagerSSLCiphers(String ciphers) {
    this.jmxManagerSslCiphers = ciphers;
  }

  public void setJmxManagerSSLKeyStore(String value) {

    this.getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, value);
    this.jmxManagerSSLKeyStore = value;
  }

  public void setJmxManagerSSLKeyStoreType(String value) {

    this.getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME, value);
    this.jmxManagerSSLKeyStoreType = value;
  }

  public void setJmxManagerSSLKeyStorePassword(String value) {

    this.getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME, value);
    this.jmxManagerSSLKeyStorePassword = value;
  }

  public void setJmxManagerSSLTrustStore(String value) {

    this.getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, value);
    this.jmxManagerSSLTrustStore = value;
  }

  public void setJmxManagerSSLTrustStorePassword(String value) {

    this.getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME, value);
    this.jmxManagerSSLTrustStorePassword = value;
  }

  public String getJmxManagerSSLKeyStore() {
    return this.jmxManagerSSLKeyStore;
  }

  public String getJmxManagerSSLKeyStoreType() {
    return this.jmxManagerSSLKeyStoreType;
  }

  public String getJmxManagerSSLKeyStorePassword() {
    return this.jmxManagerSSLKeyStorePassword;
  }

  public String getJmxManagerSSLTrustStore() {
    return this.jmxManagerSSLTrustStore;
  }

  public String getJmxManagerSSLTrustStorePassword() {
    return this.jmxManagerSSLTrustStorePassword;
  }

  @Override
  public int getJmxManagerPort() {
    return this.jmxManagerPort;
  }

  @Override
  public void setJmxManagerPort(int value) {
    this.jmxManagerPort = (Integer) value;
  }

  @Override
  public String getJmxManagerBindAddress() {
    return this.jmxManagerBindAddress;
  }

  @Override
  public void setJmxManagerBindAddress(String value) {
    if (value == null) {
      value = "";
    }
    this.jmxManagerBindAddress = (String) value;
  }

  @Override
  public String getJmxManagerHostnameForClients() {
    return this.jmxManagerHostnameForClients;
  }

  @Override
  public void setJmxManagerHostnameForClients(String value) {
    if (value == null) {
      value = "";
    }
    this.jmxManagerHostnameForClients = (String) value;
  }

  @Override
  public String getJmxManagerPasswordFile() {
    return this.jmxManagerPasswordFile;
  }

  @Override
  public void setJmxManagerPasswordFile(String value) {
    if (value == null) {
      value = "";
    }
    this.jmxManagerPasswordFile = (String) value;
  }

  @Override
  public String getJmxManagerAccessFile() {
    return this.jmxManagerAccessFile;
  }

  @Override
  public void setJmxManagerAccessFile(String value) {
    if (value == null) {
      value = "";
    }
    this.jmxManagerAccessFile = (String) value;
  }

  @Override
  public int getJmxManagerHttpPort() {
    return getHttpServicePort();
  }

  @Override
  public void setJmxManagerHttpPort(int value) {
    setHttpServicePort(value);
  }

  @Override
  public int getJmxManagerUpdateRate() {
    return this.jmxManagerUpdateRate;
  }

  @Override
  public void setJmxManagerUpdateRate(int value) {
    this.jmxManagerUpdateRate = (Integer) value;
  }

  @Override
  public boolean getLockMemory() {
    return this.lockMemory;
  }

  @Override
  public void setLockMemory(final boolean value) {
    this.lockMemory = value;
  }

  @Override
  public void setShiroInit(String value) {
    this.shiroInit = value;
  }

  @Override
  public String getShiroInit() {
    return this.shiroInit;
  }

  @Override
  public String getClusterSSLAlias() {
    return clusterSSLAlias;
  }

  @Override
  public void setClusterSSLAlias(final String alias) {
    clusterSSLAlias = alias;
  }

  @Override
  public String getGatewaySSLAlias() {
    return gatewaySSLAlias;
  }

  @Override
  public void setGatewaySSLAlias(final String alias) {
    gatewaySSLAlias = alias;
  }

  @Override
  public String getHTTPServiceSSLAlias() {
    return httpServiceSSLAlias;
  }

  @Override
  public void setHTTPServiceSSLAlias(final String alias) {
    httpServiceSSLAlias = alias;
  }

  @Override
  public String getJMXManagerSSLAlias() {
    return jmxManagerSSLAlias;
  }

  @Override
  public void setJMXManagerSSLAlias(final String alias) {
    jmxManagerSSLAlias = alias;
  }

  @Override
  public String getServerSSLAlias() {
    return serverSSLAlias;
  }

  @Override
  public void setServerSSLAlias(final String alias) {
    serverSSLAlias = alias;
  }

  @Override
  public String[] getSSLEnabledComponents() {
    return sslEnabledComponents;
  }

  @Override
  public void setSSLEnabledComponents(final String[] sslEnabledComponents) {
    this.sslEnabledComponents = sslEnabledComponents;
  }

  ///////////////////////  Utility Methods  ///////////////////////

  /**
   * Two instances of <code>DistributedConfigImpl</code> are equal if all of
   * their configuration properties are the same. Be careful if you need to
   * remove final and override this. See bug #50939.
   */
  @Override
  public final boolean equals(Object obj) {
    // this was auto-generated using Eclipse
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DistributionConfigImpl other = (DistributionConfigImpl) obj;
    if (ackForceDisconnectThreshold != other.ackForceDisconnectThreshold) {
      return false;
    }
    if (ackWaitThreshold != other.ackWaitThreshold) {
      return false;
    }
    if (archiveDiskSpaceLimit != other.archiveDiskSpaceLimit) {
      return false;
    }
    if (archiveFileSizeLimit != other.archiveFileSizeLimit) {
      return false;
    }
    if (asyncDistributionTimeout != other.asyncDistributionTimeout) {
      return false;
    }
    if (asyncMaxQueueSize != other.asyncMaxQueueSize) {
      return false;
    }
    if (asyncQueueTimeout != other.asyncQueueTimeout) {
      return false;
    }
    if (bindAddress == null) {
      if (other.bindAddress != null) {
        return false;
      }
    } else if (!bindAddress.equals(other.bindAddress)) {
      return false;
    }
    if (cacheXmlFile == null) {
      if (other.cacheXmlFile != null) {
        return false;
      }
    } else if (!cacheXmlFile.equals(other.cacheXmlFile)) {
      return false;
    }
    if (clientConflation == null) {
      if (other.clientConflation != null) {
        return false;
      }
    } else if (!clientConflation.equals(other.clientConflation)) {
      return false;
    }
    if (clusterSSLCiphers == null) {
      if (other.clusterSSLCiphers != null) {
        return false;
      }
    } else if (!clusterSSLCiphers.equals(other.clusterSSLCiphers)) {
      return false;
    }
    if (clusterSSLEnabled != other.clusterSSLEnabled) {
      return false;
    }
    if (clusterSSLKeyStore == null) {
      if (other.clusterSSLKeyStore != null) {
        return false;
      }
    } else if (!clusterSSLKeyStore.equals(other.clusterSSLKeyStore)) {
      return false;
    }
    if (clusterSSLKeyStorePassword == null) {
      if (other.clusterSSLKeyStorePassword != null) {
        return false;
      }
    } else if (!clusterSSLKeyStorePassword.equals(other.clusterSSLKeyStorePassword)) {
      return false;
    }
    if (clusterSSLKeyStoreType == null) {
      if (other.clusterSSLKeyStoreType != null) {
        return false;
      }
    } else if (!clusterSSLKeyStoreType.equals(other.clusterSSLKeyStoreType)) {
      return false;
    }
    if (clusterSSLProperties == null) {
      if (other.clusterSSLProperties != null) {
        return false;
      }
    } else if (!clusterSSLProperties.equals(other.clusterSSLProperties)) {
      return false;
    }
    if (clusterSSLProtocols == null) {
      if (other.clusterSSLProtocols != null) {
        return false;
      }
    } else if (!clusterSSLProtocols.equals(other.clusterSSLProtocols)) {
      return false;
    }
    if (clusterSSLRequireAuthentication != other.clusterSSLRequireAuthentication) {
      return false;
    }
    if (clusterSSLTrustStore == null) {
      if (other.clusterSSLTrustStore != null) {
        return false;
      }
    } else if (!clusterSSLTrustStore.equals(other.clusterSSLTrustStore)) {
      return false;
    }
    if (clusterSSLTrustStorePassword == null) {
      if (other.clusterSSLTrustStorePassword != null) {
        return false;
      }
    } else if (!clusterSSLTrustStorePassword.equals(other.clusterSSLTrustStorePassword)) {
      return false;
    }
    if (conserveSockets != other.conserveSockets) {
      return false;
    }
    if (deltaPropagation != other.deltaPropagation) {
      return false;
    }
    if (deployWorkingDir == null) {
      if (other.deployWorkingDir != null) {
        return false;
      }
    } else if (!deployWorkingDir.equals(other.deployWorkingDir)) {
      return false;
    }
    if (disableAutoReconnect != other.disableAutoReconnect) {
      return false;
    }
    if (disableTcp != other.disableTcp) {
      return false;
    }
    if (distributedSystemId != other.distributedSystemId) {
      return false;
    }
    if (durableClientId == null) {
      if (other.durableClientId != null) {
        return false;
      }
    } else if (!durableClientId.equals(other.durableClientId)) {
      return false;
    }
    if (durableClientTimeout != other.durableClientTimeout) {
      return false;
    }
    if (enableNetworkPartitionDetection != other.enableNetworkPartitionDetection) {
      return false;
    }
    if (enableSharedConfiguration != other.enableSharedConfiguration) {
      return false;
    }
    if (enableTimeStatistics != other.enableTimeStatistics) {
      return false;
    }
    if (enforceUniqueHost != other.enforceUniqueHost) {
      return false;
    }
    if (gatewaySSLEnabled != other.gatewaySSLEnabled) {
      return false;
    }
    if (gatewaySSLKeyStore == null) {
      if (other.gatewaySSLKeyStore != null) {
        return false;
      }
    } else if (!gatewaySSLKeyStore.equals(other.gatewaySSLKeyStore)) {
      return false;
    }
    if (gatewaySSLKeyStorePassword == null) {
      if (other.gatewaySSLKeyStorePassword != null) {
        return false;
      }
    } else if (!gatewaySSLKeyStorePassword.equals(other.gatewaySSLKeyStorePassword)) {
      return false;
    }
    if (gatewaySSLKeyStoreType == null) {
      if (other.gatewaySSLKeyStoreType != null) {
        return false;
      }
    } else if (!gatewaySSLKeyStoreType.equals(other.gatewaySSLKeyStoreType)) {
      return false;
    }
    if (gatewaySSLTrustStore == null) {
      if (other.gatewaySSLTrustStore != null) {
        return false;
      }
    } else if (!gatewaySSLTrustStore.equals(other.gatewaySSLTrustStore)) {
      return false;
    }
    if (gatewaySSLTrustStorePassword == null) {
      if (other.gatewaySSLTrustStorePassword != null) {
        return false;
      }
    } else if (!gatewaySSLTrustStorePassword.equals(other.gatewaySSLTrustStorePassword)) {
      return false;
    }
    if (gatewaySslCiphers == null) {
      if (other.gatewaySslCiphers != null) {
        return false;
      }
    } else if (!gatewaySslCiphers.equals(other.gatewaySslCiphers)) {
      return false;
    }
    if (gatewaySslProperties == null) {
      if (other.gatewaySslProperties != null) {
        return false;
      }
    } else if (!gatewaySslProperties.equals(other.gatewaySslProperties)) {
      return false;
    }
    if (gatewaySslProtocols == null) {
      if (other.gatewaySslProtocols != null) {
        return false;
      }
    } else if (!gatewaySslProtocols.equals(other.gatewaySslProtocols)) {
      return false;
    }
    if (gatewaySslRequireAuthentication != other.gatewaySslRequireAuthentication) {
      return false;
    }
    if (groups == null) {
      if (other.groups != null) {
        return false;
      }
    } else if (!groups.equals(other.groups)) {
      return false;
    }
    if (httpServiceBindAddress == null) {
      if (other.httpServiceBindAddress != null) {
        return false;
      }
    } else if (!httpServiceBindAddress.equals(other.httpServiceBindAddress)) {
      return false;
    }
    if (httpServicePort != other.httpServicePort) {
      return false;
    }
    if (jmxManager != other.jmxManager) {
      return false;
    }
    if (jmxManagerAccessFile == null) {
      if (other.jmxManagerAccessFile != null) {
        return false;
      }
    } else if (!jmxManagerAccessFile.equals(other.jmxManagerAccessFile)) {
      return false;
    }
    if (jmxManagerBindAddress == null) {
      if (other.jmxManagerBindAddress != null) {
        return false;
      }
    } else if (!jmxManagerBindAddress.equals(other.jmxManagerBindAddress)) {
      return false;
    }
    if (jmxManagerHostnameForClients == null) {
      if (other.jmxManagerHostnameForClients != null) {
        return false;
      }
    } else if (!jmxManagerHostnameForClients.equals(other.jmxManagerHostnameForClients)) {
      return false;
    }
    if (jmxManagerHttpPort != other.jmxManagerHttpPort) {
      return false;
    }
    if (jmxManagerPasswordFile == null) {
      if (other.jmxManagerPasswordFile != null) {
        return false;
      }
    } else if (!jmxManagerPasswordFile.equals(other.jmxManagerPasswordFile)) {
      return false;
    }
    if (jmxManagerPort != other.jmxManagerPort) {
      return false;
    }
    if (jmxManagerSSLEnabled != other.jmxManagerSSLEnabled) {
      return false;
    }
    if (jmxManagerSSLKeyStore == null) {
      if (other.jmxManagerSSLKeyStore != null) {
        return false;
      }
    } else if (!jmxManagerSSLKeyStore.equals(other.jmxManagerSSLKeyStore)) {
      return false;
    }
    if (jmxManagerSSLKeyStorePassword == null) {
      if (other.jmxManagerSSLKeyStorePassword != null) {
        return false;
      }
    } else if (!jmxManagerSSLKeyStorePassword.equals(other.jmxManagerSSLKeyStorePassword)) {
      return false;
    }
    if (jmxManagerSSLKeyStoreType == null) {
      if (other.jmxManagerSSLKeyStoreType != null) {
        return false;
      }
    } else if (!jmxManagerSSLKeyStoreType.equals(other.jmxManagerSSLKeyStoreType)) {
      return false;
    }
    if (jmxManagerSSLTrustStore == null) {
      if (other.jmxManagerSSLTrustStore != null) {
        return false;
      }
    } else if (!jmxManagerSSLTrustStore.equals(other.jmxManagerSSLTrustStore)) {
      return false;
    }
    if (jmxManagerSSLTrustStorePassword == null) {
      if (other.jmxManagerSSLTrustStorePassword != null) {
        return false;
      }
    } else if (!jmxManagerSSLTrustStorePassword.equals(other.jmxManagerSSLTrustStorePassword)) {
      return false;
    }
    if (jmxManagerSslCiphers == null) {
      if (other.jmxManagerSslCiphers != null) {
        return false;
      }
    } else if (!jmxManagerSslCiphers.equals(other.jmxManagerSslCiphers)) {
      return false;
    }
    if (jmxManagerSslProperties == null) {
      if (other.jmxManagerSslProperties != null) {
        return false;
      }
    } else if (!jmxManagerSslProperties.equals(other.jmxManagerSslProperties)) {
      return false;
    }
    if (jmxManagerSslProtocols == null) {
      if (other.jmxManagerSslProtocols != null) {
        return false;
      }
    } else if (!jmxManagerSslProtocols.equals(other.jmxManagerSslProtocols)) {
      return false;
    }
    if (jmxManagerSslRequireAuthentication != other.jmxManagerSslRequireAuthentication) {
      return false;
    }
    if (jmxManagerStart != other.jmxManagerStart) {
      return false;
    }
    if (jmxManagerUpdateRate != other.jmxManagerUpdateRate) {
      return false;
    }
    if (loadSharedConfigurationFromDir != other.loadSharedConfigurationFromDir) {
      return false;
    }
    if (locators == null) {
      if (other.locators != null) {
        return false;
      }
    } else if (!locators.equals(other.locators)) {
      return false;
    }
    if (locatorWaitTime != other.locatorWaitTime) {
      return false;
    }
    if (logDiskSpaceLimit != other.logDiskSpaceLimit) {
      return false;
    }
    if (logFile == null) {
      if (other.logFile != null) {
        return false;
      }
    } else if (!logFile.equals(other.logFile)) {
      return false;
    }
    if (logFileSizeLimit != other.logFileSizeLimit) {
      return false;
    }
    if (logLevel != other.logLevel) {
      return false;
    }
    if (maxNumReconnectTries != other.maxNumReconnectTries) {
      return false;
    }
    if (maxWaitTimeForReconnect != other.maxWaitTimeForReconnect) {
      return false;
    }
    if (mcastAddress == null) {
      if (other.mcastAddress != null) {
        return false;
      }
    } else if (!mcastAddress.equals(other.mcastAddress)) {
      return false;
    }
    if (mcastFlowControl == null) {
      if (other.mcastFlowControl != null) {
        return false;
      }
    } else if (!mcastFlowControl.equals(other.mcastFlowControl)) {
      return false;
    }
    if (mcastPort != other.mcastPort) {
      return false;
    }
    if (mcastRecvBufferSize != other.mcastRecvBufferSize) {
      return false;
    }
    if (mcastSendBufferSize != other.mcastSendBufferSize) {
      return false;
    }
    if (mcastTtl != other.mcastTtl) {
      return false;
    }
    if (memberTimeout != other.memberTimeout) {
      return false;
    }
    if (!Arrays.equals(membershipPortRange, other.membershipPortRange)) {
      return false;
    }
    if (memcachedPort != other.memcachedPort) {
      return false;
    }
    if (memcachedProtocol == null) {
      if (other.memcachedProtocol != null) {
        return false;
      }
    } else if (!memcachedProtocol.equals(other.memcachedProtocol)) {
      return false;
    }
    if (modifiable != other.modifiable) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (props == null) {
      if (other.props != null) {
        return false;
      }
    } else if (!props.equals(other.props)) {
      return false;
    }
    if (redundancyZone == null) {
      if (other.redundancyZone != null) {
        return false;
      }
    } else if (!redundancyZone.equals(other.redundancyZone)) {
      return false;
    }
    if (remoteLocators == null) {
      if (other.remoteLocators != null) {
        return false;
      }
    } else if (!remoteLocators.equals(other.remoteLocators)) {
      return false;
    }
    if (removeUnresponsiveClient != other.removeUnresponsiveClient) {
      return false;
    }
    if (roles == null) {
      if (other.roles != null) {
        return false;
      }
    } else if (!roles.equals(other.roles)) {
      return false;
    }
    if (security == null) {
      if (other.security != null) {
        return false;
      }
    } else if (!security.equals(other.security)) {
      return false;
    }
    if (securityClientAccessor == null) {
      if (other.securityClientAccessor != null) {
        return false;
      }
    } else if (!securityClientAccessor.equals(other.securityClientAccessor)) {
      return false;
    }
    if (securityClientAccessorPP == null) {
      if (other.securityClientAccessorPP != null) {
        return false;
      }
    } else if (!securityClientAccessorPP.equals(other.securityClientAccessorPP)) {
      return false;
    }
    if (securityClientAuthInit == null) {
      if (other.securityClientAuthInit != null) {
        return false;
      }
    } else if (!securityClientAuthInit.equals(other.securityClientAuthInit)) {
      return false;
    }
    if (securityClientAuthenticator == null) {
      if (other.securityClientAuthenticator != null) {
        return false;
      }
    } else if (!securityClientAuthenticator.equals(other.securityClientAuthenticator)) {
      if (securityManager == null) {
        if (other.securityManager != null) {
          return false;
        }
      } else if (!securityManager.equals(other.securityManager)) {
        return false;
      }
      if (postProcessor == null) {
        if (other.postProcessor != null) {
          return false;
        }
      } else if (!postProcessor.equals(other.postProcessor)) {
        return false;
      }
      if (shiroInit == null) {
        if (other.shiroInit != null) {
          return false;
        }
      } else if (!shiroInit.equals(other.shiroInit)) {
        return false;
      }
    }
    if (securityClientDHAlgo == null) {
      if (other.securityClientDHAlgo != null) {
        return false;
      }
    } else if (!securityClientDHAlgo.equals(other.securityClientDHAlgo)) {
      return false;
    }
    if (securityLogFile == null) {
      if (other.securityLogFile != null) {
        return false;
      }
    } else if (!securityLogFile.equals(other.securityLogFile)) {
      return false;
    }
    if (securityLogLevel != other.securityLogLevel) {
      return false;
    }
    if (securityPeerAuthInit == null) {
      if (other.securityPeerAuthInit != null) {
        return false;
      }
    } else if (!securityPeerAuthInit.equals(other.securityPeerAuthInit)) {
      return false;
    }
    if (securityPeerAuthenticator == null) {
      if (other.securityPeerAuthenticator != null) {
        return false;
      }
    } else if (!securityPeerAuthenticator.equals(other.securityPeerAuthenticator)) {
      return false;
    }
    if (securityPeerMembershipTimeout != other.securityPeerMembershipTimeout) {
      return false;
    }
    if (serverBindAddress == null) {
      if (other.serverBindAddress != null) {
        return false;
      }
    } else if (!serverBindAddress.equals(other.serverBindAddress)) {
      return false;
    }
    if (serverSSLEnabled != other.serverSSLEnabled) {
      return false;
    }
    if (serverSSLKeyStore == null) {
      if (other.serverSSLKeyStore != null) {
        return false;
      }
    } else if (!serverSSLKeyStore.equals(other.serverSSLKeyStore)) {
      return false;
    }
    if (serverSSLKeyStorePassword == null) {
      if (other.serverSSLKeyStorePassword != null) {
        return false;
      }
    } else if (!serverSSLKeyStorePassword.equals(other.serverSSLKeyStorePassword)) {
      return false;
    }
    if (serverSSLKeyStoreType == null) {
      if (other.serverSSLKeyStoreType != null) {
        return false;
      }
    } else if (!serverSSLKeyStoreType.equals(other.serverSSLKeyStoreType)) {
      return false;
    }
    if (serverSSLTrustStore == null) {
      if (other.serverSSLTrustStore != null) {
        return false;
      }
    } else if (!serverSSLTrustStore.equals(other.serverSSLTrustStore)) {
      return false;
    }
    if (serverSSLTrustStorePassword == null) {
      if (other.serverSSLTrustStorePassword != null) {
        return false;
      }
    } else if (!serverSSLTrustStorePassword.equals(other.serverSSLTrustStorePassword)) {
      return false;
    }
    if (serverSslCiphers == null) {
      if (other.serverSslCiphers != null) {
        return false;
      }
    } else if (!serverSslCiphers.equals(other.serverSslCiphers)) {
      return false;
    }
    if (serverSslProperties == null) {
      if (other.serverSslProperties != null) {
        return false;
      }
    } else if (!serverSslProperties.equals(other.serverSslProperties)) {
      return false;
    }
    if (serverSslProtocols == null) {
      if (other.serverSslProtocols != null) {
        return false;
      }
    } else if (!serverSslProtocols.equals(other.serverSslProtocols)) {
      return false;
    }
    if (serverSslRequireAuthentication != other.serverSslRequireAuthentication) {
      return false;
    }
    if (socketBufferSize != other.socketBufferSize) {
      return false;
    }
    if (socketLeaseTime != other.socketLeaseTime) {
      return false;
    }
    if (sourceMap == null) {
      if (other.sourceMap != null) {
        return false;
      }
    } else if (!sourceMap.equals(other.sourceMap)) {
      return false;
    }
    if (sslProperties == null) {
      if (other.sslProperties != null) {
        return false;
      }
    } else if (!sslProperties.equals(other.sslProperties)) {
      return false;
    }
    if (startDevRestApi != other.startDevRestApi) {
      return false;
    }
    if (startLocator == null) {
      if (other.startLocator != null) {
        return false;
      }
    } else if (!startLocator.equals(other.startLocator)) {
      return false;
    }
    if (startLocatorPort != other.startLocatorPort) {
      return false;
    }
    if (statisticArchiveFile == null) {
      if (other.statisticArchiveFile != null) {
        return false;
      }
    } else if (!statisticArchiveFile.equals(other.statisticArchiveFile)) {
      return false;
    }
    if (statisticSampleRate != other.statisticSampleRate) {
      return false;
    }
    if (statisticSamplingEnabled != other.statisticSamplingEnabled) {
      return false;
    }
    if (tcpPort != other.tcpPort) {
      return false;
    }
    if (udpFragmentSize != other.udpFragmentSize) {
      return false;
    }
    if (udpRecvBufferSize != other.udpRecvBufferSize) {
      return false;
    }
    if (udpSendBufferSize != other.udpSendBufferSize) {
      return false;
    }
    if (useSharedConfiguration != other.useSharedConfiguration) {
      return false;
    }
    if (userCommandPackages == null) {
      if (other.userCommandPackages != null) {
        return false;
      }
    } else if (!userCommandPackages.equals(other.userCommandPackages)) {
      return false;
    }
    if (userDefinedProps == null) {
      if (other.userDefinedProps != null) {
        return false;
      }
    } else if (!userDefinedProps.equals(other.userDefinedProps)) {
      return false;
    }
    if (clusterSSLAlias != other.clusterSSLAlias) {
      return false;
    }
    if (serverSSLAlias != other.serverSSLAlias) {
      return false;
    }
    if (httpServiceSSLAlias != other.httpServiceSSLAlias) {
      return false;
    }
    if (jmxManagerSSLAlias != other.jmxManagerSSLAlias) {
      return false;
    }
    if (gatewaySSLAlias != other.gatewaySSLAlias) {
      return false;
    }
    if (sslEnabledComponents != other.sslEnabledComponents) {
      return false;
    }
    return true;
  }

  /**
   * The hash code of a <code>DistributionConfigImpl</code> is based on the
   * value of all of its configuration properties. Be careful if you need to
   * remove final and override this. See bug #50939.
   */
  @Override
  public final int hashCode() {
    // this was auto-generated using Eclipse
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ackForceDisconnectThreshold;
    result = prime * result + ackWaitThreshold;
    result = prime * result + archiveDiskSpaceLimit;
    result = prime * result + archiveFileSizeLimit;
    result = prime * result + asyncDistributionTimeout;
    result = prime * result + asyncMaxQueueSize;
    result = prime * result + asyncQueueTimeout;
    result = prime * result + ((bindAddress == null) ? 0 : bindAddress.hashCode());
    result = prime * result + ((cacheXmlFile == null) ? 0 : cacheXmlFile.hashCode());
    result = prime * result + ((clientConflation == null) ? 0 : clientConflation.hashCode());
    result = prime * result + ((clusterSSLCiphers == null) ? 0 : clusterSSLCiphers.hashCode());
    result = prime * result + (clusterSSLEnabled ? 1231 : 1237);
    result = prime * result + ((clusterSSLKeyStore == null) ? 0 : clusterSSLKeyStore.hashCode());
    result = prime * result + ((clusterSSLKeyStorePassword == null) ? 0 : clusterSSLKeyStorePassword.hashCode());
    result = prime * result + ((clusterSSLKeyStoreType == null) ? 0 : clusterSSLKeyStoreType.hashCode());
    result = prime * result + ((clusterSSLProperties == null) ? 0 : clusterSSLProperties.hashCode());
    result = prime * result + ((clusterSSLProtocols == null) ? 0 : clusterSSLProtocols.hashCode());
    result = prime * result + (clusterSSLRequireAuthentication ? 1231 : 1237);
    result = prime * result + ((clusterSSLTrustStore == null) ? 0 : clusterSSLTrustStore.hashCode());
    result = prime * result + ((clusterSSLTrustStorePassword == null) ? 0 : clusterSSLTrustStorePassword.hashCode());
    result = prime * result + (conserveSockets ? 1231 : 1237);
    result = prime * result + (deltaPropagation ? 1231 : 1237);
    result = prime * result + ((deployWorkingDir == null) ? 0 : deployWorkingDir.hashCode());
    result = prime * result + (disableAutoReconnect ? 1231 : 1237);
    result = prime * result + (disableTcp ? 1231 : 1237);
    result = prime * result + distributedSystemId;
    result = prime * result + ((durableClientId == null) ? 0 : durableClientId.hashCode());
    result = prime * result + durableClientTimeout;
    result = prime * result + (enableNetworkPartitionDetection ? 1231 : 1237);
    result = prime * result + (enableSharedConfiguration ? 1231 : 1237);
    result = prime * result + (enableTimeStatistics ? 1231 : 1237);
    result = prime * result + (enforceUniqueHost ? 1231 : 1237);
    result = prime * result + (gatewaySSLEnabled ? 1231 : 1237);
    result = prime * result + ((gatewaySSLKeyStore == null) ? 0 : gatewaySSLKeyStore.hashCode());
    result = prime * result + ((gatewaySSLKeyStorePassword == null) ? 0 : gatewaySSLKeyStorePassword.hashCode());
    result = prime * result + ((gatewaySSLKeyStoreType == null) ? 0 : gatewaySSLKeyStoreType.hashCode());
    result = prime * result + ((gatewaySSLTrustStore == null) ? 0 : gatewaySSLTrustStore.hashCode());
    result = prime * result + ((gatewaySSLTrustStorePassword == null) ? 0 : gatewaySSLTrustStorePassword.hashCode());
    result = prime * result + ((gatewaySslCiphers == null) ? 0 : gatewaySslCiphers.hashCode());
    result = prime * result + ((gatewaySslProperties == null) ? 0 : gatewaySslProperties.hashCode());
    result = prime * result + ((gatewaySslProtocols == null) ? 0 : gatewaySslProtocols.hashCode());
    result = prime * result + (gatewaySslRequireAuthentication ? 1231 : 1237);
    result = prime * result + ((groups == null) ? 0 : groups.hashCode());
    result = prime * result + ((httpServiceBindAddress == null) ? 0 : httpServiceBindAddress.hashCode());
    result = prime * result + httpServicePort;
    result = prime * result + (jmxManager ? 1231 : 1237);
    result = prime * result + ((jmxManagerAccessFile == null) ? 0 : jmxManagerAccessFile.hashCode());
    result = prime * result + ((jmxManagerBindAddress == null) ? 0 : jmxManagerBindAddress.hashCode());
    result = prime * result + ((jmxManagerHostnameForClients == null) ? 0 : jmxManagerHostnameForClients.hashCode());
    result = prime * result + jmxManagerHttpPort;
    result = prime * result + ((jmxManagerPasswordFile == null) ? 0 : jmxManagerPasswordFile.hashCode());
    result = prime * result + jmxManagerPort;
    result = prime * result + (jmxManagerSSLEnabled ? 1231 : 1237);
    result = prime * result + ((jmxManagerSSLKeyStore == null) ? 0 : jmxManagerSSLKeyStore.hashCode());
    result = prime * result + ((jmxManagerSSLKeyStorePassword == null) ? 0 : jmxManagerSSLKeyStorePassword.hashCode());
    result = prime * result + ((jmxManagerSSLKeyStoreType == null) ? 0 : jmxManagerSSLKeyStoreType.hashCode());
    result = prime * result + ((jmxManagerSSLTrustStore == null) ? 0 : jmxManagerSSLTrustStore.hashCode());
    result = prime * result + ((jmxManagerSSLTrustStorePassword == null) ? 0 : jmxManagerSSLTrustStorePassword.hashCode());
    result = prime * result + ((jmxManagerSslCiphers == null) ? 0 : jmxManagerSslCiphers.hashCode());
    result = prime * result + ((jmxManagerSslProperties == null) ? 0 : jmxManagerSslProperties.hashCode());
    result = prime * result + ((jmxManagerSslProtocols == null) ? 0 : jmxManagerSslProtocols.hashCode());
    result = prime * result + (jmxManagerSslRequireAuthentication ? 1231 : 1237);
    result = prime * result + (jmxManagerStart ? 1231 : 1237);
    result = prime * result + jmxManagerUpdateRate;
    result = prime * result + (loadSharedConfigurationFromDir ? 1231 : 1237);
    result = prime * result + ((locators == null) ? 0 : locators.hashCode());
    result = prime * result + logDiskSpaceLimit;
    result = prime * result + ((logFile == null) ? 0 : logFile.hashCode());
    result = prime * result + logFileSizeLimit;
    result = prime * result + logLevel;
    result = prime * result + maxNumReconnectTries;
    result = prime * result + maxWaitTimeForReconnect;
    result = prime * result + ((mcastAddress == null) ? 0 : mcastAddress.hashCode());
    result = prime * result + ((mcastFlowControl == null) ? 0 : mcastFlowControl.hashCode());
    result = prime * result + mcastPort;
    result = prime * result + mcastRecvBufferSize;
    result = prime * result + mcastSendBufferSize;
    result = prime * result + mcastTtl;
    result = prime * result + memberTimeout;
    result = prime * result + Arrays.hashCode(membershipPortRange);
    result = prime * result + memcachedPort;
    result = prime * result + ((memcachedProtocol == null) ? 0 : memcachedProtocol.hashCode());
    result = prime * result + (modifiable ? 1231 : 1237);
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((props == null) ? 0 : props.hashCode());
    result = prime * result + ((redundancyZone == null) ? 0 : redundancyZone.hashCode());
    result = prime * result + ((remoteLocators == null) ? 0 : remoteLocators.hashCode());
    result = prime * result + (removeUnresponsiveClient ? 1231 : 1237);
    result = prime * result + ((roles == null) ? 0 : roles.hashCode());
    result = prime * result + ((security == null) ? 0 : security.hashCode());
    result = prime * result + ((securityClientAccessor == null) ? 0 : securityClientAccessor.hashCode());
    result = prime * result + ((securityClientAccessorPP == null) ? 0 : securityClientAccessorPP.hashCode());
    result = prime * result + ((securityClientAuthInit == null) ? 0 : securityClientAuthInit.hashCode());
    result = prime * result + ((securityClientAuthenticator == null) ? 0 : securityClientAuthenticator.hashCode());
    result = prime * result + ((securityManager == null) ? 0 : securityManager.hashCode());
    result = prime * result + ((postProcessor == null) ? 0 : postProcessor.hashCode());
    result = prime * result + ((shiroInit == null) ? 0 : shiroInit.hashCode());
    result = prime * result + ((securityClientDHAlgo == null) ? 0 : securityClientDHAlgo.hashCode());
    result = prime * result + ((securityLogFile == null) ? 0 : securityLogFile.hashCode());
    result = prime * result + securityLogLevel;
    result = prime * result + ((securityPeerAuthInit == null) ? 0 : securityPeerAuthInit.hashCode());
    result = prime * result + ((securityPeerAuthenticator == null) ? 0 : securityPeerAuthenticator.hashCode());
    result = prime * result + securityPeerMembershipTimeout;
    result = prime * result + ((serverBindAddress == null) ? 0 : serverBindAddress.hashCode());
    result = prime * result + (serverSSLEnabled ? 1231 : 1237);
    result = prime * result + ((serverSSLKeyStore == null) ? 0 : serverSSLKeyStore.hashCode());
    result = prime * result + ((serverSSLKeyStorePassword == null) ? 0 : serverSSLKeyStorePassword.hashCode());
    result = prime * result + ((serverSSLKeyStoreType == null) ? 0 : serverSSLKeyStoreType.hashCode());
    result = prime * result + ((serverSSLTrustStore == null) ? 0 : serverSSLTrustStore.hashCode());
    result = prime * result + ((serverSSLTrustStorePassword == null) ? 0 : serverSSLTrustStorePassword.hashCode());
    result = prime * result + ((serverSslCiphers == null) ? 0 : serverSslCiphers.hashCode());
    result = prime * result + ((serverSslProperties == null) ? 0 : serverSslProperties.hashCode());
    result = prime * result + ((serverSslProtocols == null) ? 0 : serverSslProtocols.hashCode());
    result = prime * result + (serverSslRequireAuthentication ? 1231 : 1237);
    result = prime * result + socketBufferSize;
    result = prime * result + socketLeaseTime;
    result = prime * result + ((sourceMap == null) ? 0 : sourceMap.hashCode());
    result = prime * result + ((sslProperties == null) ? 0 : sslProperties.hashCode());
    result = prime * result + (startDevRestApi ? 1231 : 1237);
    result = prime * result + ((startLocator == null) ? 0 : startLocator.hashCode());
    result = prime * result + startLocatorPort;
    result = prime * result + ((statisticArchiveFile == null) ? 0 : statisticArchiveFile.hashCode());
    result = prime * result + statisticSampleRate;
    result = prime * result + (statisticSamplingEnabled ? 1231 : 1237);
    result = prime * result + tcpPort;
    result = prime * result + udpFragmentSize;
    result = prime * result + udpRecvBufferSize;
    result = prime * result + udpSendBufferSize;
    result = prime * result + (useSharedConfiguration ? 1231 : 1237);
    result = prime * result + ((userCommandPackages == null) ? 0 : userCommandPackages.hashCode());
    result = prime * result + ((userDefinedProps == null) ? 0 : userDefinedProps.hashCode());
    result = prime * result + ((clusterSSLAlias == null) ? 0 : clusterSSLAlias.hashCode());
    result = prime * result + ((serverSSLAlias == null) ? 0 : serverSSLAlias.hashCode());
    result = prime * result + ((jmxManagerSSLAlias == null) ? 0 : jmxManagerSSLAlias.hashCode());
    result = prime * result + ((gatewaySSLAlias == null) ? 0 : gatewaySSLAlias.hashCode());
    result = prime * result + ((httpServiceSSLAlias == null) ? 0 : httpServiceSSLAlias.hashCode());
    result = prime * result + ((sslEnabledComponents == null) ? 0 : sslEnabledComponents.hashCode());
    return result;
  }

  /**
   * Used by gemfire build.xml to generate a default gemfire.properties
   * for use by applications. See bug 30995 for the feature request.
   */
  public static void main(String args[]) throws IOException {
    DistributionConfigImpl cfg = new DistributionConfigImpl();
    String fileName = DistributionConfig.GEMFIRE_PREFIX + "properties";
    if (args != null && args.length > 0) {
      String temp = args[0].trim();
      fileName = "".equals(temp) ? fileName : temp;
    }
    cfg.toFile(new File(fileName));
  }


  /**
   * For dunit tests we do not allow use of the default multicast address/port.
   * Please use AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)
   * to obtain a free port for your test.
   */
  public void checkForDisallowedDefaults() {
    if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disallowMcastDefaults")) {
      if (getMcastPort() != 0) { // it is not disabled
        if (getMcastAddress().equals(DistributionConfig.DEFAULT_MCAST_ADDRESS) && getMcastPort() == DistributionConfig.DEFAULT_MCAST_PORT) {
          throw new IllegalStateException(DistributionConfig.GEMFIRE_PREFIX + "disallowMcastDefaults set and default address and port are being used");
        }
      }
    }
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DistributionConfig#getMembershipPortRange()
   */
  public int[] getMembershipPortRange() {
    return membershipPortRange;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DistributionConfig#setMembershipPortRange(int[])
   */
  public void setMembershipPortRange(int[] range) {
    membershipPortRange = (int[]) range;
  }

  /**
   * Set the host-port information of remote site locator
   */
  public void setRemoteLocators(String value) {
    this.remoteLocators = (String) value;
  }

  /**
   * get the host-port information of remote site locator
   */
  public String getRemoteLocators() {
    return this.remoteLocators;
  }

  public Map getProps() {
    return props;
  }

  @Override
  public int getMemcachedPort() {
    return this.memcachedPort;
  }

  @Override
  public void setMemcachedPort(int value) {
    this.memcachedPort = (Integer) value;
  }

  @Override
  public String getMemcachedProtocol() {
    return this.memcachedProtocol;
  }

  @Override
  public void setMemcachedProtocol(String protocol) {
    this.memcachedProtocol = (String) protocol;
  }

  @Override
  public int getRedisPort() {
    return this.redisPort;
  }

  @Override
  public void setRedisPort(int value) {
    this.redisPort = (Integer) value;
  }

  @Override
  public String getRedisBindAddress() {
    return this.redisBindAddress;
  }

  @Override
  public void setRedisBindAddress(String bindAddress) {
    this.redisBindAddress = (String) bindAddress;
  }

  @Override
  public String getRedisPassword() {
    return this.redisPassword;
  }

  @Override
  public void setRedisPassword(String password) {
    this.redisPassword = password;
  }

  @Override
  public String getOffHeapMemorySize() {
    return this.offHeapMemorySize;
  }

  @Override
  public void setOffHeapMemorySize(String value) {
    this.offHeapMemorySize = (String) value;
  }

  @Override
  public String getMemcachedBindAddress() {
    return this.memcachedBindAddress;
  }

  @Override
  public void setMemcachedBindAddress(String bindAddress) {
    this.memcachedBindAddress = (String) bindAddress;
  }

  @Override
  public void setEnableClusterConfiguration(boolean value) {
    this.enableSharedConfiguration = (Boolean) value;
  }

  @Override
  public boolean getEnableClusterConfiguration() {
    return this.enableSharedConfiguration;
  }


  @Override
  public void setUseSharedConfiguration(boolean newValue) {
    this.useSharedConfiguration = (Boolean) newValue;
  }

  @Override
  public boolean getUseSharedConfiguration() {
    return this.useSharedConfiguration;
  }

  @Override
  public void setLoadClusterConfigFromDir(boolean newValue) {
    this.loadSharedConfigurationFromDir = (Boolean) newValue;
  }

  @Override
  public boolean getLoadClusterConfigFromDir() {
    return this.loadSharedConfigurationFromDir;
  }

  @Override
  public void setClusterConfigDir(String clusterConfigDir) {
    this.clusterConfigDir = (String) clusterConfigDir;
  }

  @Override
  public String getClusterConfigDir() {
    return this.clusterConfigDir;
  }

  @Override
  public boolean getServerSSLEnabled() {
    return serverSSLEnabled;
  }

  @Override
  public void setServerSSLEnabled(boolean value) {
    this.serverSSLEnabled = (Boolean) value;

  }

  @Override
  public boolean getServerSSLRequireAuthentication() {
    return serverSslRequireAuthentication;
  }

  @Override
  public void setServerSSLRequireAuthentication(boolean value) {
    this.serverSslRequireAuthentication = (Boolean) value;
  }

  @Override
  public String getServerSSLProtocols() {
    return this.serverSslProtocols;
  }

  @Override
  public void setServerSSLProtocols(String protocols) {
    this.serverSslProtocols = (String) protocols;
  }

  @Override
  public String getServerSSLCiphers() {
    return this.serverSslCiphers;
  }

  @Override
  public void setServerSSLCiphers(String ciphers) {
    this.serverSslCiphers = (String) ciphers;
  }

  public void setServerSSLKeyStore(String value) {
    this.getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, value);
    this.serverSSLKeyStore = value;
  }

  public void setServerSSLKeyStoreType(String value) {
    this.getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME, value);
    this.serverSSLKeyStoreType = value;
  }

  public void setServerSSLKeyStorePassword(String value) {
    this.getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME, value);
    this.serverSSLKeyStorePassword = value;
  }

  public void setServerSSLTrustStore(String value) {
    this.getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, value);
    this.serverSSLTrustStore = value;
  }

  public void setServerSSLTrustStorePassword(String value) {
    this.getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME, value);
    this.serverSSLTrustStorePassword = value;
  }

  public String getServerSSLKeyStore() {
    return this.serverSSLKeyStore;
  }

  public String getServerSSLKeyStoreType() {
    return this.serverSSLKeyStoreType;
  }

  public String getServerSSLKeyStorePassword() {
    return this.serverSSLKeyStorePassword;
  }

  public String getServerSSLTrustStore() {
    return this.serverSSLTrustStore;
  }

  public String getServerSSLTrustStorePassword() {
    return this.serverSSLTrustStorePassword;
  }

  @Override
  public Properties getServerSSLProperties() {
    return this.serverSslProperties;
  }

  @Override
  public boolean getGatewaySSLEnabled() {
    return gatewaySSLEnabled;
  }

  @Override
  public void setGatewaySSLEnabled(boolean value) {
    this.gatewaySSLEnabled = (Boolean) value;

  }

  @Override
  public boolean getGatewaySSLRequireAuthentication() {
    return gatewaySslRequireAuthentication;
  }

  @Override
  public void setGatewaySSLRequireAuthentication(boolean value) {
    this.gatewaySslRequireAuthentication = (Boolean) value;
  }

  @Override
  public String getGatewaySSLProtocols() {
    return this.gatewaySslProtocols;
  }

  @Override
  public void setGatewaySSLProtocols(String protocols) {
    this.gatewaySslProtocols = (String) protocols;
  }

  @Override
  public String getGatewaySSLCiphers() {
    return this.gatewaySslCiphers;
  }

  @Override
  public void setGatewaySSLCiphers(String ciphers) {
    this.gatewaySslCiphers = (String) ciphers;
  }

  public void setGatewaySSLKeyStore(String value) {
    this.getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, value);
    this.gatewaySSLKeyStore = value;
  }

  public void setGatewaySSLKeyStoreType(String value) {
    this.getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME, value);
    this.gatewaySSLKeyStoreType = value;
  }

  public void setGatewaySSLKeyStorePassword(String value) {
    this.getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME, value);
    this.gatewaySSLKeyStorePassword = value;
  }

  public void setGatewaySSLTrustStore(String value) {
    this.getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, value);
    this.gatewaySSLTrustStore = value;
  }

  public void setGatewaySSLTrustStorePassword(String value) {
    this.getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME, value);
    this.gatewaySSLTrustStorePassword = value;
  }

  public String getGatewaySSLKeyStore() {
    return this.gatewaySSLKeyStore;
  }

  public String getGatewaySSLKeyStoreType() {
    return this.gatewaySSLKeyStoreType;
  }

  public String getGatewaySSLKeyStorePassword() {
    return this.gatewaySSLKeyStorePassword;
  }

  public String getGatewaySSLTrustStore() {
    return this.gatewaySSLTrustStore;
  }

  public String getGatewaySSLTrustStorePassword() {
    return this.gatewaySSLTrustStorePassword;
  }

  @Override
  public Properties getGatewaySSLProperties() {
    return this.gatewaySslProperties;
  }

  //Adding HTTP Service SSL properties
  @Override
  public boolean getHttpServiceSSLEnabled() {
    return httpServiceSSLEnabled;
  }

  @Override
  public void setHttpServiceSSLEnabled(boolean httpServiceSSLEnabled) {
    this.httpServiceSSLEnabled = httpServiceSSLEnabled;
  }

  @Override
  public boolean getHttpServiceSSLRequireAuthentication() {
    return httpServiceSSLRequireAuthentication;
  }

  @Override
  public void setHttpServiceSSLRequireAuthentication(boolean httpServiceSSLRequireAuthentication) {
    this.httpServiceSSLRequireAuthentication = httpServiceSSLRequireAuthentication;
  }

  @Override
  public String getHttpServiceSSLProtocols() {
    return httpServiceSSLProtocols;
  }

  @Override
  public void setHttpServiceSSLProtocols(String httpServiceSSLProtocols) {
    this.httpServiceSSLProtocols = httpServiceSSLProtocols;
  }

  @Override
  public String getHttpServiceSSLCiphers() {
    return httpServiceSSLCiphers;
  }

  @Override
  public void setHttpServiceSSLCiphers(String httpServiceSSLCiphers) {
    this.httpServiceSSLCiphers = httpServiceSSLCiphers;
  }


  @Override
  public String getHttpServiceSSLKeyStore() {
    return httpServiceSSLKeyStore;
  }

  @Override
  public void setHttpServiceSSLKeyStore(String httpServiceSSLKeyStore) {
    this.getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, httpServiceSSLKeyStore);
    this.httpServiceSSLKeyStore = httpServiceSSLKeyStore;
  }

  @Override
  public String getHttpServiceSSLKeyStoreType() {
    return httpServiceSSLKeyStoreType;
  }

  @Override
  public void setHttpServiceSSLKeyStoreType(String httpServiceSSLKeyStoreType) {
    this.getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME, httpServiceSSLKeyStoreType);
    this.httpServiceSSLKeyStoreType = httpServiceSSLKeyStoreType;
  }

  @Override
  public String getHttpServiceSSLKeyStorePassword() {
    return httpServiceSSLKeyStorePassword;
  }

  @Override
  public void setHttpServiceSSLKeyStorePassword(String httpServiceSSLKeyStorePassword) {
    this.getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME, httpServiceSSLKeyStorePassword);
    this.httpServiceSSLKeyStorePassword = httpServiceSSLKeyStorePassword;
  }

  @Override
  public String getHttpServiceSSLTrustStore() {
    return httpServiceSSLTrustStore;
  }

  @Override
  public void setHttpServiceSSLTrustStore(String httpServiceSSLTrustStore) {
    this.getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, httpServiceSSLTrustStore);
    this.httpServiceSSLTrustStore = httpServiceSSLTrustStore;
  }

  @Override
  public String getHttpServiceSSLTrustStorePassword() {
    return httpServiceSSLTrustStorePassword;
  }

  @Override
  public void setHttpServiceSSLTrustStorePassword(String httpServiceSSLTrustStorePassword) {
    this.getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME, httpServiceSSLTrustStorePassword);
    this.httpServiceSSLTrustStorePassword = httpServiceSSLTrustStorePassword;
  }

  public Properties getHttpServiceSSLProperties() {
    return httpServiceSSLProperties;
  }

  public ConfigSource getConfigSource(String attName) {
    return this.sourceMap.get(attName);
  }

  public boolean getDistributedTransactions() {
    return this.distributedTransactions;
  }

  public void setDistributedTransactions(boolean value) {
    this.distributedTransactions = value;
  }


}
