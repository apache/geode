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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_UDP_DHALGO;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.GemFireIOException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.process.ProcessLauncherContext;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.security.AuthTokenEnabledComponents;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Provides an implementation of <code>DistributionConfig</code> that knows how to read the
 * configuration file.
 * <p>
 * Note that if you add a property to this interface, should should update the
 * {@link #DistributionConfigImpl(DistributionConfig) copy constructor}.
 *
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
   * The address server socket's in a client-server topology should listen on
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

  private File deployWorkingDir = new File(System.getProperty("user.dir"));

  /**
   * The level at which log messages are logged
   *
   * @see LogWriterImpl#levelNameToCode(String)
   */
  protected int logLevel = DEFAULT_LOG_LEVEL;

  /**
   * bind-address and host of locator to start
   */
  private String startLocator = DEFAULT_START_LOCATOR;

  /**
   * port of locator to start. use bind-address as host name
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
  File statisticArchiveFile = DEFAULT_STATISTIC_ARCHIVE_FILE;

  /**
   * The amount of time to wait for a ACK message
   */
  private int ackWaitThreshold = DEFAULT_ACK_WAIT_THRESHOLD;

  /**
   * The amount of time to wait for a ACK message after the ackWaitThreshold before shunning members
   * that haven't responded. If zero, this feature is disabled.
   */
  private int ackForceDisconnectThreshold = DEFAULT_ACK_SEVERE_ALERT_THRESHOLD;

  /**
   * The name of an XML file used to initialize the cache
   */
  private File cacheXmlFile = Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE)
      ? new File("") : DEFAULT_CACHE_XML_FILE;

  protected int archiveDiskSpaceLimit = DEFAULT_ARCHIVE_DISK_SPACE_LIMIT;
  protected int archiveFileSizeLimit = DEFAULT_ARCHIVE_FILE_SIZE_LIMIT;
  protected int logDiskSpaceLimit = DEFAULT_LOG_DISK_SPACE_LIMIT;
  protected int logFileSizeLimit = DEFAULT_LOG_FILE_SIZE_LIMIT;

  @Deprecated
  private boolean clusterSSLEnabled = DEFAULT_SSL_ENABLED;
  @Deprecated
  private String clusterSSLProtocols = DEFAULT_SSL_PROTOCOLS;
  @Deprecated
  private String clusterSSLCiphers = DEFAULT_SSL_CIPHERS;
  @Deprecated
  private boolean clusterSSLRequireAuthentication = DEFAULT_SSL_REQUIRE_AUTHENTICATION;
  @Deprecated
  private String clusterSSLKeyStore = DEFAULT_SSL_KEYSTORE;
  @Deprecated
  private String clusterSSLKeyStoreType = DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE;
  @Deprecated
  private String clusterSSLKeyStorePassword = DEFAULT_SSL_KEYSTORE_PASSWORD;
  @Deprecated
  private String clusterSSLTrustStore = DEFAULT_SSL_TRUSTSTORE;
  @Deprecated
  private String clusterSSLTrustStorePassword = DEFAULT_SSL_TRUSTSTORE_PASSWORD;

  private String clusterSSLAlias = DEFAULT_SSL_ALIAS;

  /**
   * multicast send buffer size, in bytes
   */
  private int mcastSendBufferSize = DEFAULT_MCAST_SEND_BUFFER_SIZE;
  /**
   * multicast receive buffer size, in bytes
   */
  private int mcastRecvBufferSize = DEFAULT_MCAST_RECV_BUFFER_SIZE;
  /**
   * flow-of-control parameters for multicast messaging
   */
  private FlowControlParams mcastFlowControl = DEFAULT_MCAST_FLOW_CONTROL;

  /**
   * datagram socket send buffer size, in bytes
   */
  private int udpSendBufferSize = DEFAULT_UDP_SEND_BUFFER_SIZE;
  /**
   * datagram socket receive buffer size, in bytes
   */
  private int udpRecvBufferSize = DEFAULT_UDP_RECV_BUFFER_SIZE;
  /**
   * max datagram message size, in bytes. This should be < 64k
   */
  private int udpFragmentSize = DEFAULT_UDP_FRAGMENT_SIZE;

  /**
   * whether tcp/ip sockets should be disabled
   */
  protected boolean disableTcp = DEFAULT_DISABLE_TCP;

  /**
   * whether JMX should be disabled
   */
  protected boolean disableJmx = DEFAULT_DISABLE_JMX;

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
   * Max wait time for the member before reconnecting to the DS in case of required role loss.
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
   * The udp Diffie-Hellman method name
   */
  private String securityUDPDHAlgo = DEFAULT_SECURITY_UDP_DHALGO;

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
   *
   * @see LogWriterImpl#levelNameToCode(String)
   */
  private int securityLogLevel = DEFAULT_LOG_LEVEL;

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
  private final Properties security = new Properties();

  /**
   * The User defined properties to be used for cache.xml replacements
   */
  private final Properties userDefinedProps = new Properties();
  /**
   * Prefix to use for properties that are put as JVM java properties for use with layers (e.g.
   * jgroups membership) that do not have a <code>DistributionConfig</code> object.
   */
  public static final String SECURITY_SYSTEM_PREFIX = GeodeGlossary.GEMFIRE_PREFIX + "sys.";

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

  private boolean enableSharedConfiguration =
      DistributionConfig.DEFAULT_ENABLE_CLUSTER_CONFIGURATION;
  private boolean enableManagementRestService =
      DistributionConfig.DEFAULT_ENABLE_MANAGEMENT_REST_SERVICE;
  private boolean useSharedConfiguration = DistributionConfig.DEFAULT_USE_CLUSTER_CONFIGURATION;
  private boolean loadSharedConfigurationFromDir =
      DistributionConfig.DEFAULT_LOAD_CLUSTER_CONFIG_FROM_DIR;
  private String clusterConfigDir = "";


  private int httpServicePort = DEFAULT_HTTP_SERVICE_PORT;

  private String httpServiceBindAddress = DEFAULT_HTTP_SERVICE_BIND_ADDRESS;

  private boolean startDevRestApi = DEFAULT_START_DEV_REST_API;

  /**
   * port on which GemFireMemcachedServer server is started
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
   * port on which GeodeRedisServer is started
   */
  private int redisPort = DEFAULT_REDIS_PORT;

  /**
   * Bind address for GeodeRedisServer
   */
  private String redisBindAddress = DEFAULT_REDIS_BIND_ADDRESS;

  private String redisPassword = DEFAULT_REDIS_PASSWORD;

  private boolean jmxManager =
      Boolean.getBoolean(InternalLocator.FORCE_LOCATOR_DM_TYPE) || DEFAULT_JMX_MANAGER;
  private boolean jmxManagerStart = DEFAULT_JMX_MANAGER_START;
  private int jmxManagerPort = DEFAULT_JMX_MANAGER_PORT;
  private String jmxManagerBindAddress = DEFAULT_JMX_MANAGER_BIND_ADDRESS;
  private String jmxManagerHostnameForClients = DEFAULT_JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
  private String jmxManagerPasswordFile = DEFAULT_JMX_MANAGER_PASSWORD_FILE;
  private String jmxManagerAccessFile = DEFAULT_JMX_MANAGER_ACCESS_FILE;
  private int jmxManagerHttpPort = DEFAULT_HTTP_SERVICE_PORT;
  private int jmxManagerUpdateRate = DEFAULT_JMX_MANAGER_UPDATE_RATE;

  @Deprecated
  private boolean jmxManagerSSLEnabled = DEFAULT_JMX_MANAGER_SSL_ENABLED;
  @Deprecated
  private boolean jmxManagerSslRequireAuthentication =
      DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION;
  @Deprecated
  private String jmxManagerSslProtocols = DEFAULT_JMX_MANAGER_SSL_PROTOCOLS;
  @Deprecated
  private String jmxManagerSslCiphers = DEFAULT_JMX_MANAGER_SSL_CIPHERS;
  @Deprecated
  private Properties jmxManagerSslProperties = new Properties();
  @Deprecated
  private String jmxManagerSSLKeyStore = DEFAULT_JMX_MANAGER_SSL_KEYSTORE;
  @Deprecated
  private String jmxManagerSSLKeyStoreType = DEFAULT_JMX_MANAGER_SSL_KEYSTORE_TYPE;
  @Deprecated
  private String jmxManagerSSLKeyStorePassword = DEFAULT_JMX_MANAGER_SSL_KEYSTORE_PASSWORD;
  @Deprecated
  private String jmxManagerSSLTrustStore = DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE;
  @Deprecated
  private String jmxManagerSSLTrustStorePassword = DEFAULT_JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD;

  private String jmxManagerSSLAlias = DEFAULT_SSL_ALIAS;

  @Deprecated
  private boolean serverSSLEnabled = DEFAULT_SERVER_SSL_ENABLED;
  @Deprecated
  private boolean serverSslRequireAuthentication = DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION;
  @Deprecated
  private String serverSslProtocols = DEFAULT_SERVER_SSL_PROTOCOLS;
  @Deprecated
  private String serverSslCiphers = DEFAULT_SERVER_SSL_CIPHERS;
  @Deprecated
  private Properties serverSslProperties = new Properties();
  @Deprecated
  private String serverSSLKeyStore = DEFAULT_SERVER_SSL_KEYSTORE;
  @Deprecated
  private String serverSSLKeyStoreType = DEFAULT_SERVER_SSL_KEYSTORE_TYPE;
  @Deprecated
  private String serverSSLKeyStorePassword = DEFAULT_SERVER_SSL_KEYSTORE_PASSWORD;
  @Deprecated
  private String serverSSLTrustStore = DEFAULT_SERVER_SSL_TRUSTSTORE;
  @Deprecated
  private String serverSSLTrustStorePassword = DEFAULT_SERVER_SSL_TRUSTSTORE_PASSWORD;

  private String serverSSLAlias = DEFAULT_SSL_ALIAS;

  @Deprecated
  private boolean gatewaySSLEnabled = DEFAULT_GATEWAY_SSL_ENABLED;
  @Deprecated
  private boolean gatewaySslRequireAuthentication = DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION;
  @Deprecated
  private String gatewaySslProtocols = DEFAULT_GATEWAY_SSL_PROTOCOLS;
  @Deprecated
  private String gatewaySslCiphers = DEFAULT_GATEWAY_SSL_CIPHERS;
  @Deprecated
  private Properties gatewaySslProperties = new Properties();
  @Deprecated
  private String gatewaySSLKeyStore = DEFAULT_GATEWAY_SSL_KEYSTORE;
  @Deprecated
  private String gatewaySSLKeyStoreType = DEFAULT_GATEWAY_SSL_KEYSTORE_TYPE;
  @Deprecated
  private String gatewaySSLKeyStorePassword = DEFAULT_GATEWAY_SSL_KEYSTORE_PASSWORD;
  @Deprecated
  private String gatewaySSLTrustStore = DEFAULT_GATEWAY_SSL_TRUSTSTORE;
  @Deprecated
  private String gatewaySSLTrustStorePassword = DEFAULT_GATEWAY_SSL_TRUSTSTORE_PASSWORD;

  private String gatewaySSLAlias = DEFAULT_SSL_ALIAS;

  @Deprecated
  private boolean httpServiceSSLEnabled = DEFAULT_HTTP_SERVICE_SSL_ENABLED;
  @Deprecated
  private boolean httpServiceSSLRequireAuthentication =
      DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION;
  @Deprecated
  private String httpServiceSSLProtocols = DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS;
  @Deprecated
  private String httpServiceSSLCiphers = DEFAULT_HTTP_SERVICE_SSL_CIPHERS;
  @Deprecated
  private Properties httpServiceSSLProperties = new Properties();
  @Deprecated
  private String httpServiceSSLKeyStore = DEFAULT_HTTP_SERVICE_SSL_KEYSTORE;
  @Deprecated
  private String httpServiceSSLKeyStoreType = DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_TYPE;
  @Deprecated
  private String httpServiceSSLKeyStorePassword = DEFAULT_HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;
  @Deprecated
  private String httpServiceSSLTrustStore = DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE;
  @Deprecated
  private String httpServiceSSLTrustStorePassword = DEFAULT_HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;

  private String httpServiceSSLAlias = DEFAULT_SSL_ALIAS;

  private Boolean sslEndPointIdentificationEnabled = null;

  private SecurableCommunicationChannel[] securableCommunicationChannels =
      DEFAULT_SSL_ENABLED_COMPONENTS;
  private String[] securityAuthTokenEnabledComponents =
      DEFAULT_SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS;

  private boolean sslUseDefaultSSLContext = DEFAULT_SSL_USE_DEFAULT_CONTEXT;
  private String sslProtocols = DEFAULT_SSL_PROTOCOLS;
  private String sslCiphers = DEFAULT_SSL_CIPHERS;
  private boolean sslRequireAuthentication = DEFAULT_SSL_REQUIRE_AUTHENTICATION;
  private String sslKeyStore = DEFAULT_SSL_KEYSTORE;
  private String sslKeyStoreType = DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE;
  private String sslKeyStorePassword = DEFAULT_SSL_KEYSTORE_PASSWORD;
  private String sslTrustStore = DEFAULT_SSL_TRUSTSTORE;
  private String sslTrustStorePassword = DEFAULT_SSL_TRUSTSTORE_PASSWORD;
  private String sslTrustStoreType = DEFAULT_CLUSTER_SSL_KEYSTORE_TYPE;
  private boolean sslWebServiceRequireAuthentication =
      DEFAULT_SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION;

  private String locatorSSLAlias = DEFAULT_SSL_ALIAS;

  private String sslDefaultAlias = DEFAULT_SSL_ALIAS;

  /**
   * The SSL Parameter Extension class name
   */
  private String sslParameterExtension = DEFAULT_SSL_PARAMETER_EXTENSION;

  private Map<String, ConfigSource> sourceMap =
      Collections.synchronizedMap(new HashMap<>());

  private String userCommandPackages = DEFAULT_USER_COMMAND_PACKAGES;

  private boolean validateSerializableObjects = DEFAULT_VALIDATE_SERIALIZABLE_OBJECTS;
  private String serializableObjectFilter = DEFAULT_SERIALIZABLE_OBJECT_FILTER;

  /**
   * "off-heap-memory-size" with value of "" or "<size>[g|m]"
   */
  protected String offHeapMemorySize = DEFAULT_OFF_HEAP_MEMORY_SIZE;

  /**
   * Whether pages should be locked into memory or allowed to swap to disk
   */
  private boolean lockMemory = DEFAULT_LOCK_MEMORY;

  private String shiroInit = "";

  /**
   * Is thread monitoring enabled
   */
  private boolean threadMonitorEnabled = DEFAULT_THREAD_MONITOR_ENABLED;

  /**
   * the thread monitoring interval
   */
  private int threadMonitorInterval = DEFAULT_THREAD_MONITOR_INTERVAL;

  /**
   * the thread monitoring time limit after which the monitored thread is considered stuck
   */
  private int threadMonitorTimeLimit = DEFAULT_THREAD_MONITOR_TIME_LIMIT;

  /**
   * Create a new <code>DistributionConfigImpl</code> from the contents of another
   * <code>DistributionConfig</code>.
   */
  public DistributionConfigImpl(DistributionConfig other) {
    name = other.getName();
    tcpPort = other.getTcpPort();
    mcastPort = other.getMcastPort();
    mcastTtl = other.getMcastTtl();
    socketLeaseTime = other.getSocketLeaseTime();
    socketBufferSize = other.getSocketBufferSize();
    conserveSockets = other.getConserveSockets();
    roles = other.getRoles();
    mcastAddress = other.getMcastAddress();
    bindAddress = other.getBindAddress();
    serverBindAddress = other.getServerBindAddress();
    locators = ((DistributionConfigImpl) other).locators;
    locatorWaitTime = other.getLocatorWaitTime();
    remoteLocators = other.getRemoteLocators();
    startLocator = other.getStartLocator();
    startLocatorPort = ((DistributionConfigImpl) other).startLocatorPort;
    deployWorkingDir = other.getDeployWorkingDir();
    logFile = other.getLogFile();
    logLevel = other.getLogLevel();
    statisticSamplingEnabled = other.getStatisticSamplingEnabled();
    threadMonitorEnabled = other.getThreadMonitorEnabled();
    threadMonitorInterval = other.getThreadMonitorInterval();
    threadMonitorTimeLimit = other.getThreadMonitorTimeLimit();
    statisticSampleRate = other.getStatisticSampleRate();
    statisticArchiveFile = other.getStatisticArchiveFile();
    ackWaitThreshold = other.getAckWaitThreshold();
    ackForceDisconnectThreshold = other.getAckSevereAlertThreshold();
    cacheXmlFile = other.getCacheXmlFile();
    archiveDiskSpaceLimit = other.getArchiveDiskSpaceLimit();
    archiveFileSizeLimit = other.getArchiveFileSizeLimit();
    logDiskSpaceLimit = other.getLogDiskSpaceLimit();
    logFileSizeLimit = other.getLogFileSizeLimit();
    clusterSSLEnabled = other.getClusterSSLEnabled();
    clusterSSLProtocols = other.getClusterSSLProtocols();
    clusterSSLCiphers = other.getClusterSSLCiphers();
    clusterSSLRequireAuthentication = other.getClusterSSLRequireAuthentication();
    clusterSSLKeyStore = other.getClusterSSLKeyStore();
    clusterSSLKeyStoreType = other.getClusterSSLKeyStoreType();
    clusterSSLKeyStorePassword = other.getClusterSSLKeyStorePassword();
    clusterSSLTrustStore = other.getClusterSSLTrustStore();
    clusterSSLTrustStorePassword = other.getClusterSSLTrustStorePassword();
    asyncDistributionTimeout = other.getAsyncDistributionTimeout();
    asyncQueueTimeout = other.getAsyncQueueTimeout();
    asyncMaxQueueSize = other.getAsyncMaxQueueSize();
    modifiable = true;
    // the following were added after version 4.1.2
    mcastSendBufferSize = other.getMcastSendBufferSize();
    mcastRecvBufferSize = other.getMcastRecvBufferSize();
    mcastFlowControl = other.getMcastFlowControl();
    udpSendBufferSize = other.getUdpSendBufferSize();
    udpRecvBufferSize = other.getUdpRecvBufferSize();
    udpFragmentSize = other.getUdpFragmentSize();
    disableTcp = other.getDisableTcp();
    disableJmx = other.getDisableJmx();
    enableTimeStatistics = other.getEnableTimeStatistics();
    memberTimeout = other.getMemberTimeout();
    membershipPortRange = other.getMembershipPortRange();
    maxWaitTimeForReconnect = other.getMaxWaitTimeForReconnect();
    maxNumReconnectTries = other.getMaxNumReconnectTries();
    clientConflation = other.getClientConflation();
    durableClientId = other.getDurableClientId();
    durableClientTimeout = other.getDurableClientTimeout();

    enableNetworkPartitionDetection = other.getEnableNetworkPartitionDetection();
    disableAutoReconnect = other.getDisableAutoReconnect();

    securityClientAuthInit = other.getSecurityClientAuthInit();
    securityClientAuthenticator = other.getSecurityClientAuthenticator();
    securityClientDHAlgo = other.getSecurityClientDHAlgo();
    securityUDPDHAlgo = other.getSecurityUDPDHAlgo();
    securityPeerAuthInit = other.getSecurityPeerAuthInit();
    securityPeerAuthenticator = other.getSecurityPeerAuthenticator();
    securityClientAccessor = other.getSecurityClientAccessor();
    securityClientAccessorPP = other.getSecurityClientAccessorPP();
    securityPeerMembershipTimeout = other.getSecurityPeerMembershipTimeout();
    securityLogLevel = other.getSecurityLogLevel();
    securityLogFile = other.getSecurityLogFile();
    security.putAll(other.getSecurityProps());
    removeUnresponsiveClient = other.getRemoveUnresponsiveClient();
    deltaPropagation = other.getDeltaPropagation();
    distributedSystemId = other.getDistributedSystemId();
    redundancyZone = other.getRedundancyZone();
    enforceUniqueHost = other.getEnforceUniqueHost();
    sslProperties = other.getSSLProperties();
    clusterSSLProperties = other.getClusterSSLProperties();
    jmxManagerSslProperties = other.getJmxSSLProperties();
    // Similar to this.security, assigning userDefinedProps
    userDefinedProps.putAll(other.getUserDefinedProps());

    // following added for 7.0
    groups = other.getGroups();
    jmxManager = other.getJmxManager();
    jmxManagerStart = other.getJmxManagerStart();
    jmxManagerSSLEnabled = other.getJmxManagerSSLEnabled();
    jmxManagerSslRequireAuthentication = other.getJmxManagerSSLRequireAuthentication();
    jmxManagerSslProtocols = other.getJmxManagerSSLProtocols();
    jmxManagerSslCiphers = other.getJmxManagerSSLCiphers();
    jmxManagerSSLKeyStore = other.getJmxManagerSSLKeyStore();
    jmxManagerSSLKeyStoreType = other.getJmxManagerSSLKeyStoreType();
    jmxManagerSSLKeyStorePassword = other.getJmxManagerSSLKeyStorePassword();
    jmxManagerSSLTrustStore = other.getJmxManagerSSLTrustStore();
    jmxManagerSSLTrustStorePassword = other.getJmxManagerSSLTrustStorePassword();
    jmxManagerSslProperties = other.getJmxSSLProperties();
    jmxManagerPort = other.getJmxManagerPort();
    jmxManagerBindAddress = other.getJmxManagerBindAddress();
    jmxManagerHostnameForClients = other.getJmxManagerHostnameForClients();
    jmxManagerPasswordFile = other.getJmxManagerPasswordFile();
    jmxManagerAccessFile = other.getJmxManagerAccessFile();
    jmxManagerHttpPort = other.getJmxManagerHttpPort();
    jmxManagerUpdateRate = other.getJmxManagerUpdateRate();
    memcachedPort = other.getMemcachedPort();
    memcachedProtocol = other.getMemcachedProtocol();
    memcachedBindAddress = other.getMemcachedBindAddress();
    redisPort = other.getRedisPort();
    redisBindAddress = other.getRedisBindAddress();
    redisPassword = other.getRedisPassword();
    userCommandPackages = other.getUserCommandPackages();

    // following added for 8.0
    enableSharedConfiguration = other.getEnableClusterConfiguration();
    loadSharedConfigurationFromDir = other.getLoadClusterConfigFromDir();
    clusterConfigDir = other.getClusterConfigDir();
    useSharedConfiguration = other.getUseSharedConfiguration();
    serverSSLEnabled = other.getServerSSLEnabled();
    serverSslRequireAuthentication = other.getServerSSLRequireAuthentication();
    serverSslProtocols = other.getServerSSLProtocols();
    serverSslCiphers = other.getServerSSLCiphers();
    serverSSLKeyStore = other.getServerSSLKeyStore();
    serverSSLKeyStoreType = other.getServerSSLKeyStoreType();
    serverSSLKeyStorePassword = other.getServerSSLKeyStorePassword();
    serverSSLTrustStore = other.getServerSSLTrustStore();
    serverSSLTrustStorePassword = other.getServerSSLTrustStorePassword();
    serverSslProperties = other.getServerSSLProperties();

    gatewaySSLEnabled = other.getGatewaySSLEnabled();
    gatewaySslRequireAuthentication = other.getGatewaySSLRequireAuthentication();
    gatewaySslProtocols = other.getGatewaySSLProtocols();
    gatewaySslCiphers = other.getGatewaySSLCiphers();
    gatewaySSLKeyStore = other.getGatewaySSLKeyStore();
    gatewaySSLKeyStoreType = other.getGatewaySSLKeyStoreType();
    gatewaySSLKeyStorePassword = other.getGatewaySSLKeyStorePassword();
    gatewaySSLTrustStore = other.getGatewaySSLTrustStore();
    gatewaySSLTrustStorePassword = other.getGatewaySSLTrustStorePassword();
    gatewaySslProperties = other.getGatewaySSLProperties();

    httpServicePort = other.getHttpServicePort();
    httpServiceBindAddress = other.getHttpServiceBindAddress();

    httpServiceSSLEnabled = other.getHttpServiceSSLEnabled();
    httpServiceSSLCiphers = other.getHttpServiceSSLCiphers();
    httpServiceSSLProtocols = other.getHttpServiceSSLProtocols();
    httpServiceSSLRequireAuthentication = other.getHttpServiceSSLRequireAuthentication();
    httpServiceSSLKeyStore = other.getHttpServiceSSLKeyStore();
    httpServiceSSLKeyStorePassword = other.getHttpServiceSSLKeyStorePassword();
    httpServiceSSLKeyStoreType = other.getHttpServiceSSLKeyStoreType();
    httpServiceSSLTrustStore = other.getHttpServiceSSLTrustStore();
    httpServiceSSLTrustStorePassword = other.getHttpServiceSSLTrustStorePassword();
    httpServiceSSLProperties = other.getHttpServiceSSLProperties();

    startDevRestApi = other.getStartDevRestApi();

    // following added for 9.0
    offHeapMemorySize = other.getOffHeapMemorySize();

    Map<String, ConfigSource> otherSources = ((DistributionConfigImpl) other).sourceMap;
    if (otherSources != null) {
      sourceMap = new HashMap<>(otherSources);
    }

    lockMemory = other.getLockMemory();
    distributedTransactions = other.getDistributedTransactions();
    shiroInit = other.getShiroInit();
    securityManager = other.getSecurityManager();
    postProcessor = other.getPostProcessor();

    clusterSSLAlias = other.getClusterSSLAlias();
    gatewaySSLAlias = other.getGatewaySSLAlias();
    httpServiceSSLAlias = other.getHTTPServiceSSLAlias();
    jmxManagerSSLAlias = other.getJMXSSLAlias();
    serverSSLAlias = other.getServerSSLAlias();
    locatorSSLAlias = other.getLocatorSSLAlias();

    this.sslEndPointIdentificationEnabled = other.getSSLEndPointIdentificationEnabled();
    this.securableCommunicationChannels =
        ((DistributionConfigImpl) other).securableCommunicationChannels;

    this.sslUseDefaultSSLContext = other.getSSLUseDefaultContext();
    this.sslCiphers = other.getSSLCiphers();
    this.sslProtocols = other.getSSLProtocols();
    this.sslRequireAuthentication = other.getSSLRequireAuthentication();
    this.sslKeyStore = other.getSSLKeyStore();
    this.sslKeyStorePassword = other.getSSLKeyStorePassword();
    this.sslKeyStoreType = other.getSSLKeyStoreType();
    this.sslTrustStore = other.getSSLTrustStore();
    this.sslTrustStorePassword = other.getSSLTrustStorePassword();
    this.sslTrustStoreType = other.getSSLTrustStoreType();
    this.sslProperties = other.getSSLProperties();
    this.sslDefaultAlias = other.getSSLDefaultAlias();
    this.sslWebServiceRequireAuthentication = other.getSSLWebRequireAuthentication();
    this.sslParameterExtension = other.getSSLParameterExtension();

    validateSerializableObjects = other.getValidateSerializableObjects();
    serializableObjectFilter = other.getSerializableObjectFilter();

    enableManagementRestService = other.getEnableManagementRestService();
    securityAuthTokenEnabledComponents = other.getSecurityAuthTokenEnabledComponents();
  }

  /**
   * Set to true to make attributes writable. Set to false to make attributes read only. By default
   * they are read only.
   */
  protected boolean modifiable;

  @Override
  protected boolean _modifiableDefault() {
    return modifiable;
  }

  /**
   * Creates a default application config. Does not read any properties. Currently only used by
   * DistributionConfigImpl.main.
   */
  private DistributionConfigImpl() {
    // do nothing. We just want a default config
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given non-default configuration
   * properties. See {@link DistributedSystem#connect} for a list of
   * exceptions that may be thrown.
   *
   * @param nonDefault The configuration properties specified by the caller
   */
  public DistributionConfigImpl(Properties nonDefault) {
    this(nonDefault, false, false);
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given non-default configuration
   * properties. See {@link DistributedSystem#connect} for a list of
   * exceptions that may be thrown.
   *
   * @param nonDefault The configuration properties specified by the caller
   * @param ignoreGemFirePropsFile whether to skip loading distributed system properties from
   *        gemfire.properties file
   *
   * @since GemFire 6.5
   */

  public DistributionConfigImpl(Properties nonDefault, boolean ignoreGemFirePropsFile) {
    this(nonDefault, ignoreGemFirePropsFile, false);
  }

  /**
   * Creates a new <code>DistributionConfigImpl</code> with the given non-default configuration
   * properties. See {@link DistributedSystem#connect} for a list of
   * exceptions that may be thrown.
   *
   * @param nonDefault The configuration properties specified by the caller
   * @param ignoreGemFirePropsFile whether to skip loading distributed system properties from
   *        gemfire.properties file
   * @param isConnected whether to skip Validation for SSL properties and copy of ssl properties to
   *        other ssl properties. This parameter will be used till we provide support for ssl-*
   *        properties.
   *
   * @since GemFire 8.0
   */
  public DistributionConfigImpl(Properties nonDefault, boolean ignoreGemFirePropsFile,
      boolean isConnected) {
    Map<Object, Object> props = new HashMap<>();
    if (!ignoreGemFirePropsFile) {// For admin bug #40434
      props.putAll(loadPropertiesFromURL(DistributedSystem.getPropertyFileURL(), false));
    }
    props.putAll(loadPropertiesFromURL(DistributedSystem.getSecurityPropertiesFileURL(), true));

    // Now override values picked up from the file with values passed
    // in from the caller's code
    if (nonDefault != null) {
      props.putAll(nonDefault);
      setSource(nonDefault, ConfigSource.api());
    }
    // Now remove all user defined properties from props.
    for (Object entry : props.entrySet()) {
      Map.Entry<String, String> ent = (Map.Entry<String, String>) entry;
      if (ent.getKey().startsWith(USERDEFINED_PREFIX_NAME)) {
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
      attNameSet.add(GeodeGlossary.GEMFIRE_PREFIX + attNames[index]);
    }

    // Ensure that we're also iterating over the default properties - see GEODE-4690.
    for (String key : System.getProperties().stringPropertyNames()) {
      if (attNameSet.contains(key)
          || key.startsWith(GeodeGlossary.GEMFIRE_PREFIX + SECURITY_PREFIX_NAME)
          || key.startsWith(GeodeGlossary.GEMFIRE_PREFIX + SSL_SYSTEM_PROPS_NAME)) {
        String sysValue = System.getProperty(key);
        if (sysValue != null) {
          String attName = key.substring(GeodeGlossary.GEMFIRE_PREFIX.length());
          props.put(attName, sysValue);
          sourceMap.put(attName, ConfigSource.sysprop());
        }
      }
    }

    final Properties overriddenDefaults = ProcessLauncherContext.getOverriddenDefaults();
    if (!overriddenDefaults.isEmpty()) {
      for (String key : overriddenDefaults.stringPropertyNames()) {
        // only apply the overridden default if it's not already specified in props
        final String property =
            key.substring(ProcessLauncherContext.OVERRIDDEN_DEFAULTS_PREFIX.length());
        if (!props.containsKey(property)) {
          props.put(property, overriddenDefaults.getProperty(key));
          sourceMap.put(property, ConfigSource.launcher());
        }
      }
    }

    initialize(props);

    if (securityPeerAuthInit != null && securityPeerAuthInit.length() > 0) {
      System.setProperty(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTH_INIT, securityPeerAuthInit);
    }
    if (securityPeerAuthenticator != null && securityPeerAuthenticator.length() > 0) {
      System.setProperty(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTHENTICATOR,
          securityPeerAuthenticator);
    }

    if (!isConnected) {
      copySSLPropsToServerSSLProps();
      copySSLPropsToJMXSSLProps();
      copyClusterSSLPropsToGatewaySSLProps();
      copySSLPropsToHTTPSSLProps();
    }

    // Make attributes writeable only
    modifiable = true;
    validateConfigurationProperties(props);
    validateSSLEnabledComponentsConfiguration();
    // Make attributes read only
    modifiable = false;
  }

  private void validateSSLEnabledComponentsConfiguration() {
    Object value = null;
    try {
      Method method = getters.get(SSL_ENABLED_COMPONENTS);
      if (method != null) {
        value = method.invoke(this);
      }
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw new InternalGemFireException(
            "error invoking getter for property" + SSL_ENABLED_COMPONENTS);
      }
    }
    SecurableCommunicationChannel[] sslEnabledComponents = (SecurableCommunicationChannel[]) value;
    for (SecurableCommunicationChannel securableCommunicationChannel : sslEnabledComponents) {
      if (!isAliasCorrectlyConfiguredForComponents(securableCommunicationChannel)) {
        throw new IllegalArgumentException(
            "The alias options for the SSL options provided seem to be invalid. Please check that all required aliases are set");
      }
    }
  }

  /**
   * Used by tests to create a default instance without reading in properties.
   */
  static DistributionConfig createDefaultInstance() {
    return new DistributionConfigImpl();
  }

  private boolean isAliasCorrectlyConfiguredForComponents(
      final SecurableCommunicationChannel component) {
    switch (component) {
      case ALL: {
        // If the default alias is not set, then check that all the other component aliases are set
        if (StringUtils.isEmpty(getSSLDefaultAlias())) {
          boolean correctAlias = true;
          correctAlias &=
              isAliasCorrectlyConfiguredForComponents(SecurableCommunicationChannel.CLUSTER);
          correctAlias &=
              isAliasCorrectlyConfiguredForComponents(SecurableCommunicationChannel.GATEWAY);
          correctAlias &=
              isAliasCorrectlyConfiguredForComponents(SecurableCommunicationChannel.WEB);
          correctAlias &=
              isAliasCorrectlyConfiguredForComponents(SecurableCommunicationChannel.JMX);
          correctAlias &=
              isAliasCorrectlyConfiguredForComponents(SecurableCommunicationChannel.LOCATOR);
          correctAlias &=
              isAliasCorrectlyConfiguredForComponents(SecurableCommunicationChannel.SERVER);
          return correctAlias;
        }
      }
      case CLUSTER: {
        return StringUtils.isEmpty(getClusterSSLAlias())
            || getSecurableCommunicationChannels().length <= 1 || !StringUtils
                .isEmpty(getSSLDefaultAlias());
      }
      case GATEWAY: {
        return StringUtils.isEmpty(getGatewaySSLAlias())
            || getSecurableCommunicationChannels().length <= 1 || !StringUtils
                .isEmpty(getSSLDefaultAlias());
      }
      case WEB: {
        return StringUtils.isEmpty(getHTTPServiceSSLAlias())
            || getSecurableCommunicationChannels().length <= 1 || !StringUtils
                .isEmpty(getSSLDefaultAlias());
      }
      case JMX: {
        return StringUtils.isEmpty(getJMXSSLAlias())
            || getSecurableCommunicationChannels().length <= 1 || !StringUtils
                .isEmpty(getSSLDefaultAlias());
      }
      case LOCATOR: {
        return StringUtils.isEmpty(getLocatorSSLAlias())
            || getSecurableCommunicationChannels().length <= 1 || !StringUtils
                .isEmpty(getSSLDefaultAlias());
      }
      case SERVER: {
        return StringUtils.isEmpty(getServerSSLAlias())
            || getSecurableCommunicationChannels().length <= 1 || !StringUtils
                .isEmpty(getSSLDefaultAlias());
      }
      default:
        return false;
    }
  }

  /**
   * Here we will validate the correctness of the set properties as per the CheckAttributeChecker
   * annotations defined in #AbstractDistributionConfig
   */
  private void validateConfigurationProperties(final Map<Object, Object> props) {
    for (Object o : props.keySet()) {
      String propertyName = (String) o;
      Object value = null;
      try {
        Method method = getters.get(propertyName);
        if (method != null) {
          value = method.invoke(this);
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

  /**
   * if jmx-manager-ssl is true and jmx-manager-ssl-enabled is false then override
   * jmx-manager-ssl-enabled with jmx-manager-ssl if jmx-manager-ssl-enabled is false, then use the
   * properties from cluster-ssl-* properties if jmx-manager-ssl-*properties are given then use
   * them, and copy the unspecified jmx-manager properties from cluster-properties
   */
  private void copySSLPropsToJMXSSLProps() {
    boolean jmxSSLEnabledOverriden = sourceMap.get(JMX_MANAGER_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !jmxSSLEnabledOverriden && !hasSSLComponents) {
      jmxManagerSSLEnabled = clusterSSLEnabled;
      sourceMap.put(JMX_MANAGER_SSL_ENABLED, sourceMap.get(CLUSTER_SSL_ENABLED));
      if (sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        jmxManagerSslCiphers = clusterSSLCiphers;
        sourceMap.put(JMX_MANAGER_SSL_CIPHERS, sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        jmxManagerSslProtocols = clusterSSLProtocols;
        sourceMap.put(JMX_MANAGER_SSL_PROTOCOLS, sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        jmxManagerSslRequireAuthentication = clusterSSLRequireAuthentication;
        sourceMap.put(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION,
            sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        jmxManagerSSLKeyStore = clusterSSLKeyStore;
        sourceMap.put(JMX_MANAGER_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        jmxManagerSSLKeyStoreType = clusterSSLKeyStoreType;
        sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_TYPE,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        jmxManagerSSLKeyStorePassword = clusterSSLKeyStorePassword;
        sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        jmxManagerSSLTrustStore = clusterSSLTrustStore;
        sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        jmxManagerSSLTrustStorePassword = clusterSSLTrustStorePassword;
        sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      jmxManagerSslProperties.putAll(clusterSSLProperties);
    }

    if (jmxSSLEnabledOverriden) {
      if (sourceMap.get(JMX_MANAGER_SSL_KEYSTORE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        jmxManagerSSLKeyStore = clusterSSLKeyStore;
        sourceMap.put(JMX_MANAGER_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(JMX_MANAGER_SSL_KEYSTORE_TYPE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        jmxManagerSSLKeyStoreType = clusterSSLKeyStoreType;
        sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_TYPE,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(JMX_MANAGER_SSL_KEYSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        jmxManagerSSLKeyStorePassword = clusterSSLKeyStorePassword;
        sourceMap.put(JMX_MANAGER_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(JMX_MANAGER_SSL_TRUSTSTORE) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        jmxManagerSSLTrustStore = clusterSSLTrustStore;
        sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        jmxManagerSSLTrustStorePassword = clusterSSLTrustStorePassword;
        sourceMap.put(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
    }
  }

  /**
   * if http-service-ssl-enabled is false, then use the properties from cluster-ssl-* properties if
   * http-service-ssl-*properties are given then use them, and copy the unspecified http-service
   * properties from cluster-properties
   */
  private void copySSLPropsToHTTPSSLProps() {
    boolean httpServiceSSLEnabledOverriden = sourceMap.get(HTTP_SERVICE_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !httpServiceSSLEnabledOverriden && !hasSSLComponents) {
      httpServiceSSLEnabled = clusterSSLEnabled;
      sourceMap.put(HTTP_SERVICE_SSL_ENABLED, sourceMap.get(CLUSTER_SSL_ENABLED));

      if (sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        httpServiceSSLCiphers = clusterSSLCiphers;
        sourceMap.put(HTTP_SERVICE_SSL_CIPHERS, sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        httpServiceSSLProtocols = clusterSSLProtocols;
        sourceMap.put(HTTP_SERVICE_SSL_PROTOCOLS, sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        httpServiceSSLRequireAuthentication = clusterSSLRequireAuthentication;
        sourceMap.put(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION,
            sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        setHttpServiceSSLKeyStore(clusterSSLKeyStore);
        sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        setHttpServiceSSLKeyStoreType(clusterSSLKeyStoreType);
        sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_TYPE,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        setHttpServiceSSLKeyStorePassword(clusterSSLKeyStorePassword);
        sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        setHttpServiceSSLTrustStore(clusterSSLTrustStore);
        sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        setHttpServiceSSLTrustStorePassword(clusterSSLTrustStorePassword);
        sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      httpServiceSSLProperties.putAll(clusterSSLProperties);
    }

    if (httpServiceSSLEnabledOverriden) {
      if (sourceMap.get(HTTP_SERVICE_SSL_KEYSTORE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        httpServiceSSLKeyStore = clusterSSLKeyStore;
        sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(HTTP_SERVICE_SSL_KEYSTORE_TYPE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        httpServiceSSLKeyStoreType = clusterSSLKeyStoreType;
        sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_TYPE,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        httpServiceSSLKeyStorePassword = clusterSSLKeyStorePassword;
        sourceMap.put(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(HTTP_SERVICE_SSL_TRUSTSTORE) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        httpServiceSSLTrustStore = clusterSSLTrustStore;
        sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        httpServiceSSLTrustStorePassword = clusterSSLTrustStorePassword;
        sourceMap.put(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
    }

  }

  /*
   * if server-ssl-enabled is false, then use the properties from cluster-ssl-* properties if
   * server-ssl-*properties are given then use them, and copy the unspecified server properties from
   * cluster-properties
   */
  private void copySSLPropsToServerSSLProps() {
    boolean cacheServerSSLOverriden = sourceMap.get(SERVER_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !cacheServerSSLOverriden && !hasSSLComponents) {
      serverSSLEnabled = clusterSSLEnabled;
      sourceMap.put(SERVER_SSL_ENABLED, sourceMap.get(CLUSTER_SSL_ENABLED));
      if (sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        serverSslCiphers = clusterSSLCiphers;
        sourceMap.put(SERVER_SSL_CIPHERS, sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        serverSslProtocols = clusterSSLProtocols;
        sourceMap.put(SERVER_SSL_PROTOCOLS, sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        serverSslRequireAuthentication = clusterSSLRequireAuthentication;
        sourceMap.put(SERVER_SSL_REQUIRE_AUTHENTICATION,
            sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        serverSSLKeyStore = clusterSSLKeyStore;
        sourceMap.put(SERVER_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        serverSSLKeyStoreType = clusterSSLKeyStoreType;
        sourceMap.put(SERVER_SSL_KEYSTORE_TYPE, sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        serverSSLKeyStorePassword = clusterSSLKeyStorePassword;
        sourceMap.put(SERVER_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        serverSSLTrustStore = clusterSSLTrustStore;
        sourceMap.put(SERVER_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        serverSSLTrustStorePassword = clusterSSLTrustStorePassword;
        sourceMap.put(SERVER_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      serverSslProperties.putAll(clusterSSLProperties);
    }

    if (cacheServerSSLOverriden) {
      if (sourceMap.get(SERVER_SSL_KEYSTORE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        serverSSLKeyStore = clusterSSLKeyStore;
        sourceMap.put(SERVER_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(SERVER_SSL_KEYSTORE_TYPE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        serverSSLKeyStoreType = clusterSSLKeyStoreType;
        sourceMap.put(SERVER_SSL_KEYSTORE_TYPE, sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(SERVER_SSL_KEYSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        serverSSLKeyStorePassword = clusterSSLKeyStorePassword;
        sourceMap.put(SERVER_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(SERVER_SSL_TRUSTSTORE) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        serverSSLTrustStore = clusterSSLTrustStore;
        sourceMap.put(SERVER_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(SERVER_SSL_TRUSTSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        serverSSLTrustStorePassword = clusterSSLTrustStorePassword;
        sourceMap.put(SERVER_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
    }
  }

  /**
   * if gateway-ssl-enabled is false, then use the properties from cluster-ssl-* properties if
   * gateway-ssl-*properties are given then use them, and copy the unspecified gateway properties
   * from cluster-properties
   */
  private void copyClusterSSLPropsToGatewaySSLProps() {
    boolean gatewaySSLOverriden = sourceMap.get(GATEWAY_SSL_ENABLED) != null;
    boolean clusterSSLOverRidden = sourceMap.get(CLUSTER_SSL_ENABLED) != null;
    boolean hasSSLComponents = sourceMap.get(SSL_ENABLED_COMPONENTS) != null;

    if (clusterSSLOverRidden && !gatewaySSLOverriden && !hasSSLComponents) {
      gatewaySSLEnabled = clusterSSLEnabled;
      sourceMap.put(GATEWAY_SSL_ENABLED, sourceMap.get(CLUSTER_SSL_ENABLED));
      if (sourceMap.get(CLUSTER_SSL_CIPHERS) != null) {
        gatewaySslCiphers = clusterSSLCiphers;
        sourceMap.put(GATEWAY_SSL_CIPHERS, sourceMap.get(CLUSTER_SSL_CIPHERS));
      }

      if (sourceMap.get(CLUSTER_SSL_PROTOCOLS) != null) {
        gatewaySslProtocols = clusterSSLProtocols;
        sourceMap.put(GATEWAY_SSL_PROTOCOLS, sourceMap.get(CLUSTER_SSL_PROTOCOLS));
      }

      if (sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION) != null) {
        gatewaySslRequireAuthentication = clusterSSLRequireAuthentication;
        sourceMap.put(GATEWAY_SSL_REQUIRE_AUTHENTICATION,
            sourceMap.get(CLUSTER_SSL_REQUIRE_AUTHENTICATION));
      }

      if (sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        gatewaySSLKeyStore = clusterSSLKeyStore;
        sourceMap.put(GATEWAY_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        gatewaySSLKeyStoreType = clusterSSLKeyStoreType;
        sourceMap.put(GATEWAY_SSL_KEYSTORE_TYPE,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        gatewaySSLKeyStorePassword = clusterSSLKeyStorePassword;
        sourceMap.put(GATEWAY_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        gatewaySSLTrustStore = clusterSSLTrustStore;
        sourceMap.put(GATEWAY_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        gatewaySSLTrustStorePassword = clusterSSLTrustStorePassword;
        sourceMap.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
      }
      gatewaySslProperties.putAll(clusterSSLProperties);
    }

    if (gatewaySSLOverriden) {
      if (sourceMap.get(GATEWAY_SSL_KEYSTORE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE) != null) {
        gatewaySSLKeyStore = clusterSSLKeyStore;
        sourceMap.put(GATEWAY_SSL_KEYSTORE, sourceMap.get(CLUSTER_SSL_KEYSTORE));
      }
      if (sourceMap.get(GATEWAY_SSL_KEYSTORE_TYPE) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE) != null) {
        gatewaySSLKeyStoreType = clusterSSLKeyStoreType;
        sourceMap.put(GATEWAY_SSL_KEYSTORE_TYPE,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_TYPE));
      }
      if (sourceMap.get(GATEWAY_SSL_KEYSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD) != null) {
        gatewaySSLKeyStorePassword = clusterSSLKeyStorePassword;
        sourceMap.put(GATEWAY_SSL_KEYSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_KEYSTORE_PASSWORD));
      }
      if (sourceMap.get(GATEWAY_SSL_TRUSTSTORE) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE) != null) {
        gatewaySSLTrustStore = clusterSSLTrustStore;
        sourceMap.put(GATEWAY_SSL_TRUSTSTORE, sourceMap.get(CLUSTER_SSL_TRUSTSTORE));
      }
      if (sourceMap.get(GATEWAY_SSL_TRUSTSTORE_PASSWORD) == null
          && sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD) != null) {
        gatewaySSLTrustStorePassword = clusterSSLTrustStorePassword;
        sourceMap.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD,
            sourceMap.get(CLUSTER_SSL_TRUSTSTORE_PASSWORD));
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

  void setApiProps(Properties apiProps) {
    if (apiProps != null) {
      setSource(apiProps, ConfigSource.api());
      modifiable = true;
      Iterator it = apiProps.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry) it.next();
        String propName = (String) me.getKey();
        props.put(propName, me.getValue());
        if (specialPropName(propName)) {
          continue;
        }
        String propVal = (String) me.getValue();
        if (propVal != null) {
          setAttribute(propName, propVal.trim(), sourceMap.get(propName));
        }
      }
      // Make attributes read only
      modifiable = false;
    }
  }

  private static boolean specialPropName(String propName) {
    return propName.equalsIgnoreCase(CLUSTER_SSL_ENABLED)
        || propName.equals(SECURITY_PEER_AUTH_INIT) || propName.equals(SECURITY_PEER_AUTHENTICATOR)
        || propName.equals(LOG_WRITER_NAME) || propName.equals(DS_CONFIG_NAME)
        || propName.equals(SECURITY_LOG_WRITER_NAME) || propName.equals(LOG_OUTPUTSTREAM_NAME)
        || propName.equals(SECURITY_LOG_OUTPUTSTREAM_NAME);
  }

  @Override
  protected Map<String, ConfigSource> getAttSourceMap() {
    return sourceMap;
  }

  @Override
  public Properties getUserDefinedProps() {
    return userDefinedProps;
  }

  /**
   * Loads the properties from gemfire.properties & gfsecurity.properties files into given
   * Properties object.
   *
   * @param properties the Properties to fill in
   *
   * @throws GemFireIOException when error occurs while reading properties file
   */
  public static void loadGemFireProperties(Properties properties) throws GemFireIOException {
    loadGemFireProperties(properties, false);
  }

  /**
   * Loads the properties from gemfire.properties & gfsecurity.properties files into given
   * Properties object. if <code>ignoreGemFirePropsFile</code> is <code>true</code>, properties are
   * not read from gemfire.properties.
   *
   * @param properties the Properties to fill in
   * @param ignoreGemFirePropsFile whether to ignore properties from gemfire.properties
   *
   * @throws GemFireIOException when error occurs while reading properties file
   */
  // Fix for #44924
  public static void loadGemFireProperties(Properties properties, boolean ignoreGemFirePropsFile)
      throws GemFireIOException {
    if (!ignoreGemFirePropsFile) {
      loadPropertiesFromURL(properties, DistributedSystem.getPropertyFileURL());
    }
    // load the security properties file
    loadPropertiesFromURL(properties, DistributedSystem.getSecurityPropertiesFileURL());
  }

  /**
   * For every key in p mark it as being from the given source.
   */
  private void setSource(Properties p, ConfigSource source) {
    if (source == null) {
      throw new IllegalArgumentException("Valid ConfigSource must be specified instead of null.");
    }
    for (Object k : p.keySet()) {
      sourceMap.put((String) k, source);
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

  private static void loadPropertiesFromURL(Properties properties, URL url) {
    if (url != null) {
      try {
        properties.load(url.openStream());
      } catch (IOException io) {
        throw new GemFireIOException(
            String.format("Failed reading %s", url), io);
      }
    }
  }

  private void initialize(Map props) {
    // Allow attributes to be modified
    modifiable = true;
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
      // weed out extraneous non-string properties
      if (propVal instanceof String) {
        setAttribute(propName, ((String) propVal).trim(), sourceMap.get(propName));
      }
    }
    if (props.containsKey(CLUSTER_SSL_ENABLED)) {
      setAttribute(CLUSTER_SSL_ENABLED, (String) props.get(CLUSTER_SSL_ENABLED),
          sourceMap.get(CLUSTER_SSL_ENABLED));
    }
    // now set the security authInit if needed
    if (props.containsKey(SECURITY_PEER_AUTH_INIT)) {
      setAttribute(SECURITY_PEER_AUTH_INIT, (String) props.get(SECURITY_PEER_AUTH_INIT),
          sourceMap.get(SECURITY_PEER_AUTH_INIT));
    }
    // and security authenticator if needed
    if (props.containsKey(SECURITY_PEER_AUTHENTICATOR)) {
      setAttribute(SECURITY_PEER_AUTHENTICATOR,
          (String) props.get(SECURITY_PEER_AUTHENTICATOR),
          sourceMap.get(SECURITY_PEER_AUTHENTICATOR));
    }

    // Make attributes read only
    modifiable = false;
  }

  private String convertCommaDelimitedToSpaceDelimitedString(final String propVal) {
    return propVal.replace(",", " ");
  }

  @Override
  public void close() {
    // Clear the extra stuff from System properties
    Properties properties = System.getProperties();
    properties.remove(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTH_INIT);
    properties.remove(SECURITY_SYSTEM_PREFIX + SECURITY_PEER_AUTHENTICATOR);

    Iterator iter = security.keySet().iterator();
    while (iter.hasNext()) {
      properties.remove(SECURITY_SYSTEM_PREFIX + iter.next());
    }
    System.setProperties(properties);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getTcpPort() {
    return tcpPort;
  }

  @Override
  public int getMcastPort() {
    return mcastPort;
  }

  @Override
  public int getMcastTtl() {
    return mcastTtl;
  }

  @Override
  public int getSocketLeaseTime() {
    return socketLeaseTime;
  }

  @Override
  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  @Override
  public boolean getConserveSockets() {
    return conserveSockets;
  }

  @Override
  public String getRoles() {
    return roles;
  }

  @Override
  public int getMaxWaitTimeForReconnect() {
    return maxWaitTimeForReconnect;
  }

  @Override
  public int getMaxNumReconnectTries() {
    return maxNumReconnectTries;
  }

  @Override
  public InetAddress getMcastAddress() {
    try {
      return mcastAddress;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  @Override
  public String getBindAddress() {
    return bindAddress;
  }

  @Override
  public String getServerBindAddress() {
    return serverBindAddress;
  }

  @Override
  public String getLocators() {
    if (startLocator != null && startLocator.length() > 0) {
      String locs = locators;
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
    return locators;
  }

  @Override
  public String getStartLocator() {
    if (startLocatorPort > 0) {
      if (bindAddress != null) {
        return bindAddress + "[" + startLocatorPort + "]";
      }
      try {
        return LocalHostUtil.getLocalHostName() + "[" + startLocatorPort
            + "]";
      } catch (UnknownHostException ignore) {
        // punt and use this.startLocator instead
      }
    }
    return startLocator;
  }

  @Override
  public File getDeployWorkingDir() {
    return deployWorkingDir;
  }

  @Override
  public File getLogFile() {
    return logFile;
  }

  @Override
  public int getLogLevel() {
    return logLevel;
  }

  @Override
  public boolean getStatisticSamplingEnabled() {
    return statisticSamplingEnabled;
  }

  @Override
  public int getStatisticSampleRate() {
    return statisticSampleRate;
  }

  @Override
  public File getStatisticArchiveFile() {
    return statisticArchiveFile;
  }

  @Override
  public int getAckWaitThreshold() {
    return ackWaitThreshold;
  }

  @Override
  public int getAckSevereAlertThreshold() {
    return ackForceDisconnectThreshold;
  }

  @Override
  public File getCacheXmlFile() {
    return cacheXmlFile;
  }

  @Override
  public boolean getClusterSSLEnabled() {
    return clusterSSLEnabled;
  }

  @Override
  public String getClusterSSLProtocols() {
    return clusterSSLProtocols;
  }

  @Override
  public String getClusterSSLCiphers() {
    return clusterSSLCiphers;
  }

  @Override
  public boolean getClusterSSLRequireAuthentication() {
    return clusterSSLRequireAuthentication;
  }

  @Override
  public String getClusterSSLKeyStore() {
    return clusterSSLKeyStore;
  }

  @Override
  public String getClusterSSLKeyStoreType() {
    return clusterSSLKeyStoreType;
  }

  @Override
  public String getClusterSSLKeyStorePassword() {
    return clusterSSLKeyStorePassword;
  }

  @Override
  public String getClusterSSLTrustStore() {
    return clusterSSLTrustStore;
  }

  @Override
  public String getClusterSSLTrustStorePassword() {
    return clusterSSLTrustStorePassword;
  }

  @Override
  public int getAsyncDistributionTimeout() {
    return asyncDistributionTimeout;
  }

  @Override
  public int getAsyncQueueTimeout() {
    return asyncQueueTimeout;
  }

  @Override
  public int getAsyncMaxQueueSize() {
    return asyncMaxQueueSize;
  }

  @Override
  public String getUserCommandPackages() {
    return userCommandPackages;
  }

  @Override
  public int getHttpServicePort() {
    return httpServicePort;
  }

  @Override
  public void setHttpServicePort(int value) {
    httpServicePort = value;
  }

  @Override
  public String getHttpServiceBindAddress() {
    return httpServiceBindAddress;
  }

  @Override
  public void setHttpServiceBindAddress(String value) {
    httpServiceBindAddress = value;
  }

  @Override
  public boolean getStartDevRestApi() {
    return startDevRestApi;
  }

  @Override
  public void setStartDevRestApi(boolean value) {
    startDevRestApi = value;
  }

  @Override
  public void setUserCommandPackages(String value) {
    userCommandPackages = value;
  }

  @Override
  public boolean getDeltaPropagation() {
    return deltaPropagation;
  }

  @Override
  public void setDeltaPropagation(boolean value) {
    deltaPropagation = value;
  }

  @Override
  public void setName(String value) {
    if (value == null) {
      value = DEFAULT_NAME;
    }
    name = value;
  }

  @Override
  public void setTcpPort(int value) {
    tcpPort = value;
  }

  @Override
  public void setMcastPort(int value) {
    mcastPort = value;
  }

  @Override
  public void setMcastTtl(int value) {
    mcastTtl = value;
  }

  @Override
  public void setSocketLeaseTime(int value) {
    socketLeaseTime = value;
  }

  @Override
  public void setSocketBufferSize(int value) {
    socketBufferSize = value;
  }

  @Override
  public void setConserveSockets(boolean newValue) {
    conserveSockets = newValue;
  }

  @Override
  public void setRoles(String roles) {
    this.roles = roles;
  }

  @Override
  public void setMaxWaitTimeForReconnect(int timeOut) {
    maxWaitTimeForReconnect = timeOut;
  }

  @Override
  public void setMaxNumReconnectTries(int tries) {
    maxNumReconnectTries = tries;
  }

  @Override
  public void setMcastAddress(InetAddress value) {
    mcastAddress = value;
  }

  @Override
  public void setBindAddress(String value) {
    bindAddress = value;
  }

  @Override
  public void setServerBindAddress(String value) {
    serverBindAddress = value;
  }

  @Override
  public void setLocators(String value) {
    if (value == null) {
      value = DEFAULT_LOCATORS;
    }
    locators = value;
  }

  @Override
  public void setLocatorWaitTime(int seconds) {
    locatorWaitTime = seconds;
  }

  @Override
  public int getLocatorWaitTime() {
    return locatorWaitTime;
  }

  @Override
  public void setDeployWorkingDir(File value) {
    deployWorkingDir = value;
  }

  @Override
  public void setLogFile(File value) {
    logFile = value;
  }

  @Override
  public void setLogLevel(int value) {
    logLevel = value;
  }

  /**
   * the locator startup code must be able to modify the locator log file in order to establish a
   * default log file if one hasn't been specified by the user. This method will change the log
   * file, but only in the configuration settings - it won't affect a running distributed system's
   * log file
   */
  void unsafeSetLogFile(File value) {
    logFile = value;
  }

  @Override
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
      }
    }
    startLocator = value;
  }

  @Override
  public void setStatisticSamplingEnabled(boolean newValue) {
    statisticSamplingEnabled = newValue;
  }

  @Override
  public void setStatisticSampleRate(int value) {
    if (value < DEFAULT_STATISTIC_SAMPLE_RATE) {
      // fix 48228
      InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
      if (ids != null) {
        ids.getLogWriter()
            .info("Setting statistic-sample-rate to " + DEFAULT_STATISTIC_SAMPLE_RATE
                + " instead of the requested " + value
                + " because VSD does not work with sub-second sampling.");
      }
      value = DEFAULT_STATISTIC_SAMPLE_RATE;
    }
    statisticSampleRate = value;
  }

  @Override
  public void setStatisticArchiveFile(File value) {
    if (value == null) {
      value = new File("");
    }
    statisticArchiveFile = value;
  }

  @Override
  public void setCacheXmlFile(File value) {
    cacheXmlFile = value;
  }

  @Override
  public void setAckWaitThreshold(int newThreshold) {
    ackWaitThreshold = newThreshold;
  }

  @Override
  public void setAckSevereAlertThreshold(int newThreshold) {
    ackForceDisconnectThreshold = newThreshold;
  }

  @Override
  public int getArchiveDiskSpaceLimit() {
    return archiveDiskSpaceLimit;
  }

  @Override
  public void setArchiveDiskSpaceLimit(int value) {
    archiveDiskSpaceLimit = value;
  }

  @Override
  public int getArchiveFileSizeLimit() {
    return archiveFileSizeLimit;
  }

  @Override
  public void setArchiveFileSizeLimit(int value) {
    archiveFileSizeLimit = value;
  }

  @Override
  public int getLogDiskSpaceLimit() {
    return logDiskSpaceLimit;
  }

  @Override
  public void setLogDiskSpaceLimit(int value) {
    logDiskSpaceLimit = value;
  }

  @Override
  public int getLogFileSizeLimit() {
    return logFileSizeLimit;
  }

  @Override
  public void setLogFileSizeLimit(int value) {
    logFileSizeLimit = value;
  }

  @Override
  public void setClusterSSLEnabled(boolean enabled) {
    clusterSSLEnabled = enabled;
  }

  @Override
  public void setClusterSSLProtocols(String protocols) {
    clusterSSLProtocols = protocols;
  }

  @Override
  public void setClusterSSLCiphers(String ciphers) {
    clusterSSLCiphers = ciphers;
  }

  @Override
  public void setClusterSSLRequireAuthentication(boolean enabled) {
    clusterSSLRequireAuthentication = enabled;
  }

  @Override
  public void setClusterSSLKeyStore(String keyStore) {
    getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, keyStore);
    clusterSSLKeyStore = keyStore;
  }

  @Override
  public void setClusterSSLKeyStoreType(String keyStoreType) {
    getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME,
        keyStoreType);
    clusterSSLKeyStoreType = keyStoreType;
  }

  @Override
  public void setClusterSSLKeyStorePassword(String keyStorePassword) {
    getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME,
        keyStorePassword);
    clusterSSLKeyStorePassword = keyStorePassword;
  }

  @Override
  public void setClusterSSLTrustStore(String trustStore) {
    getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, trustStore);
    clusterSSLTrustStore = trustStore;
  }

  @Override
  public void setClusterSSLTrustStorePassword(String trusStorePassword) {
    getClusterSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME,
        trusStorePassword);
    clusterSSLTrustStorePassword = trusStorePassword;
  }

  @Override
  public int getMcastSendBufferSize() {
    return mcastSendBufferSize;
  }

  @Override
  public void setMcastSendBufferSize(int value) {
    mcastSendBufferSize = value;
  }

  @Override
  public int getMcastRecvBufferSize() {
    return mcastRecvBufferSize;
  }

  @Override
  public void setMcastRecvBufferSize(int value) {
    mcastRecvBufferSize = value;
  }

  @Override
  public void setAsyncDistributionTimeout(int newValue) {
    asyncDistributionTimeout = newValue;
  }

  @Override
  public void setAsyncQueueTimeout(int newValue) {
    asyncQueueTimeout = newValue;
  }

  @Override
  public void setAsyncMaxQueueSize(int newValue) {
    asyncMaxQueueSize = newValue;
  }

  @Override
  public FlowControlParams getMcastFlowControl() {
    return mcastFlowControl;
  }

  @Override
  public void setMcastFlowControl(FlowControlParams values) {
    mcastFlowControl = values;
  }

  @Override
  public int getUdpFragmentSize() {
    return udpFragmentSize;
  }

  @Override
  public void setUdpFragmentSize(int value) {
    udpFragmentSize = value;
  }

  @Override
  public int getUdpSendBufferSize() {
    return udpSendBufferSize;
  }

  @Override
  public void setUdpSendBufferSize(int value) {
    udpSendBufferSize = value;
  }

  @Override
  public int getUdpRecvBufferSize() {
    return udpRecvBufferSize;
  }

  @Override
  public void setUdpRecvBufferSize(int value) {
    udpRecvBufferSize = value;
  }

  @Override
  public boolean getDisableTcp() {
    return disableTcp;
  }

  @Override
  public void setDisableTcp(boolean newValue) {
    disableTcp = newValue;
  }

  @Override
  public boolean getDisableJmx() {
    return disableJmx;
  }

  @Override
  public void setDisableJmx(boolean newValue) {
    disableJmx = newValue;
  }

  @Override
  public boolean getEnableTimeStatistics() {
    return enableTimeStatistics;
  }

  @Override
  public void setEnableTimeStatistics(boolean newValue) {
    enableTimeStatistics = newValue;
  }

  @Override
  public int getMemberTimeout() {
    return memberTimeout;
  }

  @Override
  public void setMemberTimeout(int value) {
    memberTimeout = value;
  }

  /**
   * @since GemFire 5.7
   */
  @Override
  public String getClientConflation() {
    return clientConflation;
  }

  /**
   * @since GemFire 5.7
   */
  @Override
  public void setClientConflation(String clientConflation) {
    this.clientConflation = clientConflation;
  }

  @Override
  public String getDurableClientId() {
    return durableClientId;
  }

  @Override
  public void setDurableClientId(String durableClientId) {
    this.durableClientId = durableClientId;
  }

  @Override
  public int getDurableClientTimeout() {
    return durableClientTimeout;
  }

  @Override
  public void setDurableClientTimeout(int durableClientTimeout) {
    this.durableClientTimeout = durableClientTimeout;
  }

  @Override
  public String getSecurityClientAuthInit() {
    return securityClientAuthInit;
  }

  @Override
  public void setSecurityClientAuthInit(String attValue) {
    securityClientAuthInit = attValue;
  }

  @Override
  public String getSecurityClientAuthenticator() {
    return securityClientAuthenticator;
  }

  @Override
  public String getSecurityManager() {
    return securityManager;
  }

  @Override
  public String getPostProcessor() {
    return postProcessor;
  }

  @Override
  public boolean getEnableNetworkPartitionDetection() {
    return enableNetworkPartitionDetection;
  }

  @Override
  public void setEnableNetworkPartitionDetection(boolean newValue) {
    enableNetworkPartitionDetection = newValue;
  }

  @Override
  public boolean getDisableAutoReconnect() {
    return disableAutoReconnect;
  }

  @Override
  public void setDisableAutoReconnect(boolean value) {
    disableAutoReconnect = value;
  }

  @Override
  public void setSecurityClientAuthenticator(String attValue) {
    securityClientAuthenticator = attValue;
  }

  @Override
  public void setSecurityManager(String attValue) {
    securityManager = attValue;
  }

  @Override
  public void setPostProcessor(String attValue) {
    postProcessor = attValue;
  }

  @Override
  public String getSecurityClientDHAlgo() {
    return securityClientDHAlgo;
  }

  @Override
  public void setSecurityClientDHAlgo(String attValue) {
    securityClientDHAlgo = attValue;
  }

  @Override
  public String getSecurityUDPDHAlgo() {
    return securityUDPDHAlgo;
  }

  @Override
  public void setSecurityUDPDHAlgo(String attValue) {
    securityUDPDHAlgo = (String) checkAttribute(SECURITY_UDP_DHALGO, attValue);
  }

  @Override
  public String getSecurityPeerAuthInit() {
    return securityPeerAuthInit;
  }

  @Override
  public void setSecurityPeerAuthInit(String attValue) {
    securityPeerAuthInit = attValue;
  }

  @Override
  public String getSecurityPeerAuthenticator() {
    return securityPeerAuthenticator;
  }

  @Override
  public void setSecurityPeerAuthenticator(String attValue) {
    securityPeerAuthenticator = attValue;
  }

  @Override
  public String getSecurityClientAccessor() {
    return securityClientAccessor;
  }

  @Override
  public void setSecurityClientAccessor(String attValue) {
    securityClientAccessor = attValue;
  }

  @Override
  public String getSecurityClientAccessorPP() {
    return securityClientAccessorPP;
  }

  @Override
  public void setSecurityClientAccessorPP(String attValue) {
    securityClientAccessorPP = attValue;
  }

  @Override
  public int getSecurityLogLevel() {
    return securityLogLevel;
  }

  @Override
  public void setSecurityLogLevel(int level) {
    securityLogLevel = level;
  }

  @Override
  public File getSecurityLogFile() {
    return securityLogFile;
  }

  @Override
  public void setSecurityLogFile(File value) {
    securityLogFile = value;
  }

  @Override
  public int getSecurityPeerMembershipTimeout() {
    return securityPeerMembershipTimeout;
  }

  @Override
  public void setSecurityPeerMembershipTimeout(int attValue) {
    securityPeerMembershipTimeout = attValue;
  }

  @Override
  public Properties getSecurityProps() {
    Properties result = new Properties();
    result.putAll(security);
    return result;
  }

  @Override
  public Properties toSecurityProperties() {
    Properties result = new Properties();
    for (Object attName : security.keySet()) {
      if (attName instanceof String) {
        result.put(attName, getAttribute((String) attName));
      } else {
        result.put(attName, security.get(attName));
      }
    }
    return result;
  }

  @Override
  public String getSecurity(String attName) {

    String attValue = security.getProperty(attName);
    return attValue == null ? "" : attValue;
  }

  @Override
  public void setSecurity(String attName, String attValue) {
    security.setProperty(attName, attValue);
  }

  @Override
  public void setSecurityAuthTokenEnabledComponents(String[] newValue) {
    // validate the value first
    for (int i = 0; i < newValue.length; i++) {
      String value = newValue[i];
      try {
        AuthTokenEnabledComponents.valueOf(value.toUpperCase());
        // normalize the values to all uppercase
        newValue[i] = value.toUpperCase();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Invalid security-auth-token-enabled-components value: " + value);
      }
    }
    securityAuthTokenEnabledComponents = newValue;
  }

  @Override
  public String[] getSecurityAuthTokenEnabledComponents() {
    return securityAuthTokenEnabledComponents;
  }

  @Override
  public boolean getRemoveUnresponsiveClient() {
    return removeUnresponsiveClient;
  }

  @Override
  public void setRemoveUnresponsiveClient(boolean value) {
    removeUnresponsiveClient = value;
  }

  @Override
  public int getDistributedSystemId() {
    return distributedSystemId;
  }

  @Override
  public void setDistributedSystemId(int distributedSystemId) {
    this.distributedSystemId = distributedSystemId;

  }

  @Override
  public boolean getEnforceUniqueHost() {
    return enforceUniqueHost;
  }

  @Override
  public String getRedundancyZone() {
    // TODO Auto-generated method stub
    return redundancyZone;
  }

  @Override
  public void setEnforceUniqueHost(boolean enforceUniqueHost) {
    this.enforceUniqueHost = enforceUniqueHost;

  }

  @Override
  public void setRedundancyZone(String redundancyZone) {
    this.redundancyZone = redundancyZone;

  }

  @Override
  public void setSSLProperty(String attName, String attValue) {
    if (attName.startsWith(SYS_PROP_NAME)) {
      attName = attName.substring(SYS_PROP_NAME.length());
    }
    if (attName.endsWith(JMX_SSL_PROPS_SUFFIX)) {
      jmxManagerSslProperties.setProperty(
          attName.substring(0, attName.length() - JMX_SSL_PROPS_SUFFIX.length()), attValue);
    } else {
      sslProperties.setProperty(attName, attValue);

      if (!jmxManagerSslProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix JMX_SSL_PROPS_SUFFIX override that
        // base
        jmxManagerSslProperties.setProperty(attName, attValue);
      }

      if (!serverSslProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix CACHESERVER_SSL_PROPS_SUFFIX override
        // that base
        serverSslProperties.setProperty(attName, attValue);
      }
      if (!gatewaySslProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix GATEWAY_SSL_PROPS_SUFFIX override
        // that base
        gatewaySslProperties.setProperty(attName, attValue);
      }
      if (!httpServiceSSLProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix GATEWAY_SSL_PROPS_SUFFIX override
        // that base
        httpServiceSSLProperties.setProperty(attName, attValue);
      }
      if (!clusterSSLProperties.containsKey(attName)) {
        // use sslProperties as base and let props with suffix GATEWAY_SSL_PROPS_SUFFIX override
        // that base
        clusterSSLProperties.setProperty(attName, attValue);
      }
    }
  }

  @Override
  public Properties getSSLProperties() {
    return sslProperties;
  }

  @Override
  public Properties getClusterSSLProperties() {
    return clusterSSLProperties;
  }

  @Override
  public Properties getJmxSSLProperties() {
    return jmxManagerSslProperties;
  }

  @Override
  public String getGroups() {
    return groups;
  }

  @Override
  public void setGroups(String value) {
    if (value == null) {
      value = DEFAULT_GROUPS;
    }
    groups = value;
  }

  @Override
  public boolean getJmxManager() {
    return jmxManager;
  }

  @Override
  public void setJmxManager(boolean value) {
    jmxManager = value;
  }

  @Override
  public boolean getJmxManagerStart() {
    return jmxManagerStart;
  }

  @Override
  public void setJmxManagerStart(boolean value) {
    jmxManagerStart = value;
  }

  @Override
  public boolean getJmxManagerSSLEnabled() {
    return jmxManagerSSLEnabled;
  }

  @Override
  public void setJmxManagerSSLEnabled(boolean enabled) {
    jmxManagerSSLEnabled = enabled;
  }

  @Override
  public boolean getJmxManagerSSLRequireAuthentication() {
    return jmxManagerSslRequireAuthentication;
  }

  @Override
  public void setJmxManagerSSLRequireAuthentication(boolean enabled) {
    jmxManagerSslRequireAuthentication = enabled;
  }

  @Override
  public String getJmxManagerSSLProtocols() {
    return jmxManagerSslProtocols;
  }

  @Override
  public void setJmxManagerSSLProtocols(String protocols) {
    jmxManagerSslProtocols = protocols;
  }

  @Override
  public String getJmxManagerSSLCiphers() {
    return jmxManagerSslCiphers;
  }

  @Override
  public void setJmxManagerSSLCiphers(String ciphers) {
    jmxManagerSslCiphers = ciphers;
  }

  @Override
  public void setJmxManagerSSLKeyStore(String keyStore) {
    getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, keyStore);
    jmxManagerSSLKeyStore = keyStore;
  }

  @Override
  public void setJmxManagerSSLKeyStoreType(String keyStoreType) {
    getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME, keyStoreType);
    jmxManagerSSLKeyStoreType = keyStoreType;
  }

  @Override
  public void setJmxManagerSSLKeyStorePassword(String keyStorePassword) {
    getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME,
        keyStorePassword);
    jmxManagerSSLKeyStorePassword = keyStorePassword;
  }

  @Override
  public void setJmxManagerSSLTrustStore(String trustStore) {
    getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, trustStore);
    jmxManagerSSLTrustStore = trustStore;
  }

  @Override
  public void setJmxManagerSSLTrustStorePassword(String trusStorePassword) {
    getJmxSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME,
        trusStorePassword);
    jmxManagerSSLTrustStorePassword = trusStorePassword;
  }

  @Override
  public String getJmxManagerSSLKeyStore() {
    return jmxManagerSSLKeyStore;
  }

  @Override
  public String getJmxManagerSSLKeyStoreType() {
    return jmxManagerSSLKeyStoreType;
  }

  @Override
  public String getJmxManagerSSLKeyStorePassword() {
    return jmxManagerSSLKeyStorePassword;
  }

  @Override
  public String getJmxManagerSSLTrustStore() {
    return jmxManagerSSLTrustStore;
  }

  @Override
  public String getJmxManagerSSLTrustStorePassword() {
    return jmxManagerSSLTrustStorePassword;
  }

  @Override
  public int getJmxManagerPort() {
    return jmxManagerPort;
  }

  @Override
  public void setJmxManagerPort(int value) {
    jmxManagerPort = value;
  }

  @Override
  public String getJmxManagerBindAddress() {
    return jmxManagerBindAddress;
  }

  @Override
  public void setJmxManagerBindAddress(String value) {
    if (value == null) {
      value = "";
    }
    jmxManagerBindAddress = value;
  }

  @Override
  public String getJmxManagerHostnameForClients() {
    return jmxManagerHostnameForClients;
  }

  @Override
  public void setJmxManagerHostnameForClients(String value) {
    if (value == null) {
      value = "";
    }
    jmxManagerHostnameForClients = value;
  }

  @Override
  public String getJmxManagerPasswordFile() {
    return jmxManagerPasswordFile;
  }

  @Override
  public void setJmxManagerPasswordFile(String value) {
    if (value == null) {
      value = "";
    }
    jmxManagerPasswordFile = value;
  }

  @Override
  public String getJmxManagerAccessFile() {
    return jmxManagerAccessFile;
  }

  @Override
  public void setJmxManagerAccessFile(String value) {
    if (value == null) {
      value = "";
    }
    jmxManagerAccessFile = value;
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
    return jmxManagerUpdateRate;
  }

  @Override
  public void setJmxManagerUpdateRate(int value) {
    jmxManagerUpdateRate = value;
  }

  @Override
  public boolean getLockMemory() {
    return lockMemory;
  }

  @Override
  public void setLockMemory(final boolean value) {
    lockMemory = value;
  }

  @Override
  public void setShiroInit(String value) {
    shiroInit = value;
  }

  @Override
  public String getShiroInit() {
    return shiroInit;
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
  public String getLocatorSSLAlias() {
    return locatorSSLAlias;
  }

  @Override
  public void setLocatorSSLAlias(final String alias) {
    locatorSSLAlias = alias;
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
  public String getJMXSSLAlias() {
    return jmxManagerSSLAlias;
  }

  @Override
  public void setJMXSSLAlias(final String alias) {
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
  public boolean getSSLEndPointIdentificationEnabled() {
    // sslEndPointIdentificationEnabled is a boxed boolean and no default value is set, so that
    // we can differentiate between an assigned default vs user provided override. This is set
    // to true when ssl-use-default-context is true or else its false. So return false if its null.
    if (this.sslEndPointIdentificationEnabled == null) {
      return false;
    }
    return sslEndPointIdentificationEnabled;
  }

  @Override
  public void setSSLEndPointIdentificationEnabled(final boolean sslEndPointIdentificationEnabled) {
    this.sslEndPointIdentificationEnabled = sslEndPointIdentificationEnabled;
  }

  @Override
  public SecurableCommunicationChannel[] getSecurableCommunicationChannels() {
    return securableCommunicationChannels;
  }

  @Override
  public void setSecurableCommunicationChannels(
      final SecurableCommunicationChannel[] sslEnabledComponents) {
    securableCommunicationChannels = sslEnabledComponents;
  }

  @Override
  public boolean getSSLUseDefaultContext() {
    return sslUseDefaultSSLContext;
  }

  @Override
  public void setSSLUseDefaultContext(final boolean sslUseDefaultSSLContext) {
    if (this.sslEndPointIdentificationEnabled == null) {
      this.sslEndPointIdentificationEnabled = Boolean.TRUE;
    }
    this.sslUseDefaultSSLContext = sslUseDefaultSSLContext;
  }

  @Override
  public String getSSLProtocols() {
    return sslProtocols;
  }

  @Override
  public void setSSLProtocols(final String protocols) {
    // This conversion is required due to backwards compatibility of the existing protocols code
    sslProtocols = convertCommaDelimitedToSpaceDelimitedString(protocols);
  }

  @Override
  public String getSSLCiphers() {
    return sslCiphers;
  }

  @Override
  public void setSSLCiphers(final String ciphers) {
    // This conversion is required due to backwards compatibility of the existing cipher code
    sslCiphers = convertCommaDelimitedToSpaceDelimitedString(ciphers);
  }

  @Override
  public boolean getSSLRequireAuthentication() {
    return sslRequireAuthentication;
  }

  @Override
  public void setSSLRequireAuthentication(final boolean enabled) {
    sslRequireAuthentication = enabled;
  }

  @Override
  public String getSSLKeyStore() {
    return sslKeyStore;
  }

  @Override
  public void setSSLKeyStore(final String keyStore) {
    sslKeyStore = keyStore;
  }

  @Override
  public String getSSLKeyStoreType() {
    return sslKeyStoreType;
  }

  @Override
  public void setSSLKeyStoreType(final String keyStoreType) {
    sslKeyStoreType = keyStoreType;
  }

  @Override
  public String getSSLKeyStorePassword() {
    return sslKeyStorePassword;
  }

  @Override
  public void setSSLKeyStorePassword(final String keyStorePassword) {
    sslKeyStorePassword = keyStorePassword;
  }

  @Override
  public String getSSLTrustStore() {
    return sslTrustStore;
  }

  @Override
  public void setSSLTrustStore(final String trustStore) {
    sslTrustStore = trustStore;
  }

  @Override
  public String getSSLDefaultAlias() {
    return sslDefaultAlias;
  }

  @Override
  public void setSSLDefaultAlias(final String sslDefaultAlias) {
    this.sslDefaultAlias = sslDefaultAlias;
  }

  @Override
  public String getSSLTrustStorePassword() {
    return sslTrustStorePassword;
  }

  @Override
  public void setSSLTrustStorePassword(final String trustStorePassword) {
    sslTrustStorePassword = trustStorePassword;
  }

  @Override
  public String getSSLTrustStoreType() {
    return sslTrustStoreType;
  }

  @Override
  public void setSSLTrustStoreType(final String trustStoreType) {
    sslTrustStoreType = trustStoreType;
  }

  @Override
  public boolean getSSLWebRequireAuthentication() {
    return sslWebServiceRequireAuthentication;
  }

  @Override
  public void setSSLWebRequireAuthentication(final boolean requiresAuthentication) {
    sslWebServiceRequireAuthentication = requiresAuthentication;
  }

  @Override
  public String getSSLParameterExtension() {
    return sslParameterExtension;
  }

  @Override
  public void setSSLParameterExtension(final String extension) {
    sslParameterExtension = extension;
  }

  @Override
  public boolean getValidateSerializableObjects() {
    return validateSerializableObjects;
  }

  @Override
  public void setValidateSerializableObjects(boolean value) {
    validateSerializableObjects = value;
  }

  @Override
  public String getSerializableObjectFilter() {
    return serializableObjectFilter;
  }

  @Override
  public void setSerializableObjectFilter(String value) {
    serializableObjectFilter = value;
  }

  /**
   * Two instances of <code>DistributedConfigImpl</code> are equal if all of their configuration
   * properties are the same. Be careful if you need to remove final and override this. See bug
   * #50939.
   */
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final DistributionConfigImpl that = (DistributionConfigImpl) obj;

    return new EqualsBuilder().append(tcpPort, that.tcpPort).append(mcastPort, that.mcastPort)
        .append(mcastTtl, that.mcastTtl).append(socketLeaseTime, that.socketLeaseTime)
        .append(socketBufferSize, that.socketBufferSize)
        .append(conserveSockets, that.conserveSockets).append(locatorWaitTime, that.locatorWaitTime)
        .append(logLevel, that.logLevel).append(startLocatorPort, that.startLocatorPort)
        .append(statisticSamplingEnabled, that.statisticSamplingEnabled)
        .append(statisticSampleRate, that.statisticSampleRate)
        .append(ackWaitThreshold, that.ackWaitThreshold)
        .append(ackForceDisconnectThreshold, that.ackForceDisconnectThreshold)
        .append(archiveDiskSpaceLimit, that.archiveDiskSpaceLimit)
        .append(archiveFileSizeLimit, that.archiveFileSizeLimit)
        .append(logDiskSpaceLimit, that.logDiskSpaceLimit)
        .append(logFileSizeLimit, that.logFileSizeLimit)
        .append(clusterSSLEnabled, that.clusterSSLEnabled)
        .append(clusterSSLRequireAuthentication, that.clusterSSLRequireAuthentication)
        .append(mcastSendBufferSize, that.mcastSendBufferSize)
        .append(mcastRecvBufferSize, that.mcastRecvBufferSize)
        .append(udpSendBufferSize, that.udpSendBufferSize)
        .append(udpRecvBufferSize, that.udpRecvBufferSize)
        .append(udpFragmentSize, that.udpFragmentSize).append(disableTcp, that.disableTcp)
        .append(disableJmx, that.disableJmx)
        .append(enableTimeStatistics, that.enableTimeStatistics)
        .append(memberTimeout, that.memberTimeout)
        .append(maxWaitTimeForReconnect, that.maxWaitTimeForReconnect)
        .append(maxNumReconnectTries, that.maxNumReconnectTries)
        .append(asyncDistributionTimeout, that.asyncDistributionTimeout)
        .append(asyncQueueTimeout, that.asyncQueueTimeout)
        .append(asyncMaxQueueSize, that.asyncMaxQueueSize)
        .append(durableClientTimeout, that.durableClientTimeout)
        .append(securityLogLevel, that.securityLogLevel)
        .append(enableNetworkPartitionDetection, that.enableNetworkPartitionDetection)
        .append(disableAutoReconnect, that.disableAutoReconnect)
        .append(securityPeerMembershipTimeout, that.securityPeerMembershipTimeout)
        .append(removeUnresponsiveClient, that.removeUnresponsiveClient)
        .append(deltaPropagation, that.deltaPropagation)
        .append(distributedSystemId, that.distributedSystemId)
        .append(enforceUniqueHost, that.enforceUniqueHost)
        .append(enableSharedConfiguration, that.enableSharedConfiguration)
        .append(useSharedConfiguration, that.useSharedConfiguration)
        .append(loadSharedConfigurationFromDir, that.loadSharedConfigurationFromDir)
        .append(httpServicePort, that.httpServicePort).append(startDevRestApi, that.startDevRestApi)
        .append(memcachedPort, that.memcachedPort)
        .append(distributedTransactions, that.distributedTransactions)
        .append(redisPort, that.redisPort).append(jmxManager, that.jmxManager)
        .append(jmxManagerStart, that.jmxManagerStart).append(jmxManagerPort, that.jmxManagerPort)
        .append(jmxManagerHttpPort, that.jmxManagerHttpPort)
        .append(jmxManagerUpdateRate, that.jmxManagerUpdateRate)
        .append(jmxManagerSSLEnabled, that.jmxManagerSSLEnabled)
        .append(jmxManagerSslRequireAuthentication, that.jmxManagerSslRequireAuthentication)
        .append(serverSSLEnabled, that.serverSSLEnabled)
        .append(serverSslRequireAuthentication, that.serverSslRequireAuthentication)
        .append(gatewaySSLEnabled, that.gatewaySSLEnabled)
        .append(gatewaySslRequireAuthentication, that.gatewaySslRequireAuthentication)
        .append(httpServiceSSLEnabled, that.httpServiceSSLEnabled)
        .append(httpServiceSSLRequireAuthentication, that.httpServiceSSLRequireAuthentication)
        .append(sslRequireAuthentication, that.sslRequireAuthentication)
        .append(sslWebServiceRequireAuthentication, that.sslWebServiceRequireAuthentication)
        .append(lockMemory, that.lockMemory).append(modifiable, that.modifiable)
        .append(name, that.name).append(roles, that.roles).append(mcastAddress, that.mcastAddress)
        .append(bindAddress, that.bindAddress).append(serverBindAddress, that.serverBindAddress)
        .append(locators, that.locators).append(logFile, that.logFile)
        .append(deployWorkingDir, that.deployWorkingDir).append(startLocator, that.startLocator)
        .append(statisticArchiveFile, that.statisticArchiveFile)
        .append(cacheXmlFile, that.cacheXmlFile)
        .append(clusterSSLProtocols, that.clusterSSLProtocols)
        .append(clusterSSLCiphers, that.clusterSSLCiphers)
        .append(clusterSSLKeyStore, that.clusterSSLKeyStore)
        .append(clusterSSLKeyStoreType, that.clusterSSLKeyStoreType)
        .append(clusterSSLKeyStorePassword, that.clusterSSLKeyStorePassword)
        .append(clusterSSLTrustStore, that.clusterSSLTrustStore)
        .append(clusterSSLTrustStorePassword, that.clusterSSLTrustStorePassword)
        .append(clusterSSLAlias, that.clusterSSLAlias)
        .append(mcastFlowControl, that.mcastFlowControl)
        .append(membershipPortRange, that.membershipPortRange)
        .append(clientConflation, that.clientConflation)
        .append(durableClientId, that.durableClientId)
        .append(securityClientAuthInit, that.securityClientAuthInit)
        .append(securityClientAuthenticator, that.securityClientAuthenticator)
        .append(securityManager, that.securityManager).append(postProcessor, that.postProcessor)
        .append(securityClientDHAlgo, that.securityClientDHAlgo)
        .append(securityPeerAuthInit, that.securityPeerAuthInit)
        .append(securityPeerAuthenticator, that.securityPeerAuthenticator)
        .append(securityClientAccessor, that.securityClientAccessor)
        .append(securityClientAccessorPP, that.securityClientAccessorPP)
        .append(securityLogFile, that.securityLogFile).append(security, that.security)
        .append(userDefinedProps, that.userDefinedProps).append(props, that.props)
        .append(remoteLocators, that.remoteLocators).append(redundancyZone, that.redundancyZone)
        .append(sslProperties, that.sslProperties)
        .append(clusterSSLProperties, that.clusterSSLProperties).append(groups, that.groups)
        .append(clusterConfigDir, that.clusterConfigDir)
        .append(httpServiceBindAddress, that.httpServiceBindAddress)
        .append(memcachedProtocol, that.memcachedProtocol)
        .append(memcachedBindAddress, that.memcachedBindAddress)
        .append(redisBindAddress, that.redisBindAddress).append(redisPassword, that.redisPassword)
        .append(jmxManagerBindAddress, that.jmxManagerBindAddress)
        .append(jmxManagerHostnameForClients, that.jmxManagerHostnameForClients)
        .append(jmxManagerPasswordFile, that.jmxManagerPasswordFile)
        .append(jmxManagerAccessFile, that.jmxManagerAccessFile)
        .append(jmxManagerSslProtocols, that.jmxManagerSslProtocols)
        .append(jmxManagerSslCiphers, that.jmxManagerSslCiphers)
        .append(jmxManagerSslProperties, that.jmxManagerSslProperties)
        .append(jmxManagerSSLKeyStore, that.jmxManagerSSLKeyStore)
        .append(jmxManagerSSLKeyStoreType, that.jmxManagerSSLKeyStoreType)
        .append(jmxManagerSSLKeyStorePassword, that.jmxManagerSSLKeyStorePassword)
        .append(jmxManagerSSLTrustStore, that.jmxManagerSSLTrustStore)
        .append(jmxManagerSSLTrustStorePassword, that.jmxManagerSSLTrustStorePassword)
        .append(jmxManagerSSLAlias, that.jmxManagerSSLAlias)
        .append(serverSslProtocols, that.serverSslProtocols)
        .append(serverSslCiphers, that.serverSslCiphers)
        .append(serverSslProperties, that.serverSslProperties)
        .append(serverSSLKeyStore, that.serverSSLKeyStore)
        .append(serverSSLKeyStoreType, that.serverSSLKeyStoreType)
        .append(serverSSLKeyStorePassword, that.serverSSLKeyStorePassword)
        .append(serverSSLTrustStore, that.serverSSLTrustStore)
        .append(serverSSLTrustStorePassword, that.serverSSLTrustStorePassword)
        .append(serverSSLAlias, that.serverSSLAlias)
        .append(gatewaySslProtocols, that.gatewaySslProtocols)
        .append(gatewaySslCiphers, that.gatewaySslCiphers)
        .append(gatewaySslProperties, that.gatewaySslProperties)
        .append(gatewaySSLKeyStore, that.gatewaySSLKeyStore)
        .append(gatewaySSLKeyStoreType, that.gatewaySSLKeyStoreType)
        .append(gatewaySSLKeyStorePassword, that.gatewaySSLKeyStorePassword)
        .append(gatewaySSLTrustStore, that.gatewaySSLTrustStore)
        .append(gatewaySSLTrustStorePassword, that.gatewaySSLTrustStorePassword)
        .append(gatewaySSLAlias, that.gatewaySSLAlias)
        .append(httpServiceSSLProtocols, that.httpServiceSSLProtocols)
        .append(httpServiceSSLCiphers, that.httpServiceSSLCiphers)
        .append(httpServiceSSLProperties, that.httpServiceSSLProperties)
        .append(httpServiceSSLKeyStore, that.httpServiceSSLKeyStore)
        .append(httpServiceSSLKeyStoreType, that.httpServiceSSLKeyStoreType)
        .append(httpServiceSSLKeyStorePassword, that.httpServiceSSLKeyStorePassword)
        .append(httpServiceSSLTrustStore, that.httpServiceSSLTrustStore)
        .append(httpServiceSSLTrustStorePassword, that.httpServiceSSLTrustStorePassword)
        .append(httpServiceSSLAlias, that.httpServiceSSLAlias)
        .append(securableCommunicationChannels, that.securableCommunicationChannels)
        .append(sslProtocols, that.sslProtocols).append(sslCiphers, that.sslCiphers)
        .append(sslKeyStore, that.sslKeyStore).append(sslKeyStoreType, that.sslKeyStoreType)
        .append(sslKeyStorePassword, that.sslKeyStorePassword)
        .append(sslTrustStore, that.sslTrustStore)
        .append(sslTrustStorePassword, that.sslTrustStorePassword)
        .append(sslParameterExtension, that.sslParameterExtension)
        .append(locatorSSLAlias, that.locatorSSLAlias).append(sslDefaultAlias, that.sslDefaultAlias)
        .append(sourceMap, that.sourceMap).append(userCommandPackages, that.userCommandPackages)
        .append(offHeapMemorySize, that.offHeapMemorySize).append(shiroInit, that.shiroInit)
        .append(threadMonitorEnabled, that.threadMonitorEnabled)
        .append(threadMonitorInterval, that.threadMonitorInterval)
        .append(threadMonitorTimeLimit, that.threadMonitorTimeLimit).isEquals();
  }

  /**
   * The hash code of a <code>DistributionConfigImpl</code> is based on the value of all of its
   * configuration properties. Be careful if you need to remove final and override this. See bug
   * #50939.
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(name).append(tcpPort).append(mcastPort)
        .append(mcastTtl).append(socketLeaseTime).append(socketBufferSize).append(conserveSockets)
        .append(roles).append(mcastAddress).append(bindAddress).append(serverBindAddress)
        .append(locators).append(locatorWaitTime).append(logFile).append(deployWorkingDir)
        .append(logLevel).append(startLocator).append(startLocatorPort)
        .append(statisticSamplingEnabled).append(statisticSampleRate).append(statisticArchiveFile)
        .append(ackWaitThreshold).append(ackForceDisconnectThreshold).append(cacheXmlFile)
        .append(archiveDiskSpaceLimit).append(archiveFileSizeLimit).append(logDiskSpaceLimit)
        .append(logFileSizeLimit).append(clusterSSLEnabled).append(clusterSSLProtocols)
        .append(clusterSSLCiphers).append(clusterSSLRequireAuthentication)
        .append(clusterSSLKeyStore).append(clusterSSLKeyStoreType)
        .append(clusterSSLKeyStorePassword).append(clusterSSLTrustStore)
        .append(clusterSSLTrustStorePassword).append(clusterSSLAlias).append(mcastSendBufferSize)
        .append(mcastRecvBufferSize).append(mcastFlowControl).append(udpSendBufferSize)
        .append(udpRecvBufferSize).append(udpFragmentSize).append(disableTcp).append(disableJmx)
        .append(enableTimeStatistics).append(memberTimeout).append(membershipPortRange)
        .append(maxWaitTimeForReconnect).append(maxNumReconnectTries)
        .append(asyncDistributionTimeout).append(asyncQueueTimeout).append(asyncMaxQueueSize)
        .append(clientConflation).append(durableClientId).append(durableClientTimeout)
        .append(securityClientAuthInit).append(securityClientAuthenticator).append(securityManager)
        .append(postProcessor).append(securityClientDHAlgo).append(securityPeerAuthInit)
        .append(securityPeerAuthenticator).append(securityClientAccessor)
        .append(securityClientAccessorPP).append(securityLogLevel)
        .append(enableNetworkPartitionDetection).append(disableAutoReconnect)
        .append(securityLogFile).append(securityPeerMembershipTimeout).append(security)
        .append(userDefinedProps).append(removeUnresponsiveClient).append(deltaPropagation)
        .append(props).append(distributedSystemId).append(remoteLocators).append(enforceUniqueHost)
        .append(redundancyZone).append(sslProperties).append(clusterSSLProperties).append(groups)
        .append(enableSharedConfiguration).append(useSharedConfiguration)
        .append(loadSharedConfigurationFromDir).append(clusterConfigDir).append(httpServicePort)
        .append(httpServiceBindAddress).append(startDevRestApi).append(memcachedPort)
        .append(memcachedProtocol).append(memcachedBindAddress).append(distributedTransactions)
        .append(redisPort).append(redisBindAddress).append(redisPassword).append(jmxManager)
        .append(jmxManagerStart).append(jmxManagerPort).append(jmxManagerBindAddress)
        .append(jmxManagerHostnameForClients).append(jmxManagerPasswordFile)
        .append(jmxManagerAccessFile).append(jmxManagerHttpPort).append(jmxManagerUpdateRate)
        .append(jmxManagerSSLEnabled).append(jmxManagerSslRequireAuthentication)
        .append(jmxManagerSslProtocols).append(jmxManagerSslCiphers).append(jmxManagerSslProperties)
        .append(jmxManagerSSLKeyStore).append(jmxManagerSSLKeyStoreType)
        .append(jmxManagerSSLKeyStorePassword).append(jmxManagerSSLTrustStore)
        .append(jmxManagerSSLTrustStorePassword).append(jmxManagerSSLAlias).append(serverSSLEnabled)
        .append(serverSslRequireAuthentication).append(serverSslProtocols).append(serverSslCiphers)
        .append(serverSslProperties).append(serverSSLKeyStore).append(serverSSLKeyStoreType)
        .append(serverSSLKeyStorePassword).append(serverSSLTrustStore)
        .append(serverSSLTrustStorePassword).append(serverSSLAlias).append(gatewaySSLEnabled)
        .append(gatewaySslRequireAuthentication).append(gatewaySslProtocols)
        .append(gatewaySslCiphers).append(gatewaySslProperties).append(gatewaySSLKeyStore)
        .append(gatewaySSLKeyStoreType).append(gatewaySSLKeyStorePassword)
        .append(gatewaySSLTrustStore).append(gatewaySSLTrustStorePassword).append(gatewaySSLAlias)
        .append(httpServiceSSLEnabled).append(httpServiceSSLRequireAuthentication)
        .append(httpServiceSSLProtocols).append(httpServiceSSLCiphers)
        .append(httpServiceSSLProperties).append(httpServiceSSLKeyStore)
        .append(httpServiceSSLKeyStoreType).append(httpServiceSSLKeyStorePassword)
        .append(httpServiceSSLTrustStore).append(httpServiceSSLTrustStorePassword)
        .append(httpServiceSSLAlias).append(securableCommunicationChannels).append(sslProtocols)
        .append(sslCiphers).append(sslRequireAuthentication).append(sslKeyStore)
        .append(sslKeyStoreType).append(sslKeyStorePassword).append(sslTrustStore)
        .append(sslTrustStorePassword).append(sslParameterExtension)
        .append(sslWebServiceRequireAuthentication)
        .append(locatorSSLAlias).append(sslDefaultAlias).append(sourceMap)
        .append(userCommandPackages).append(offHeapMemorySize).append(lockMemory).append(shiroInit)
        .append(modifiable).append(threadMonitorEnabled).append(threadMonitorInterval)
        .append(threadMonitorTimeLimit).toHashCode();
  }

  /**
   * For dunit tests we do not allow use of the default multicast address/port. Please use
   * AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS) to obtain a free port for your
   * test.
   */
  void checkForDisallowedDefaults() {
    if (Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "disallowMcastDefaults")) {
      if (getMcastPort() != 0) { // it is not disabled
        if (getMcastAddress().equals(DistributionConfig.DEFAULT_MCAST_ADDRESS)
            && getMcastPort() == DistributionConfig.DEFAULT_MCAST_PORT) {
          throw new IllegalStateException(GeodeGlossary.GEMFIRE_PREFIX
              + "disallowMcastDefaults set and default address and port are being used");
        }
      }
    }
  }

  @Override
  public int[] getMembershipPortRange() {
    return membershipPortRange;
  }

  @Override
  public void setMembershipPortRange(int[] range) {
    membershipPortRange = range;
  }

  /**
   * Set the host-port information of remote site locator
   */
  @Override
  public void setRemoteLocators(String locators) {
    remoteLocators = locators;
  }

  /**
   * get the host-port information of remote site locator
   */
  @Override
  public String getRemoteLocators() {
    return remoteLocators;
  }

  public Map getProps() {
    return props;
  }

  @Override
  public int getMemcachedPort() {
    return memcachedPort;
  }

  @Override
  public void setMemcachedPort(int value) {
    memcachedPort = value;
  }

  @Override
  public String getMemcachedProtocol() {
    return memcachedProtocol;
  }

  @Override
  public void setMemcachedProtocol(String protocol) {
    memcachedProtocol = protocol;
  }

  @Override
  public int getRedisPort() {
    return redisPort;
  }

  @Override
  public void setRedisPort(int value) {
    redisPort = value;
  }

  @Override
  public String getRedisBindAddress() {
    return redisBindAddress;
  }

  @Override
  public void setRedisBindAddress(String bindAddress) {
    redisBindAddress = bindAddress;
  }

  @Override
  public String getRedisPassword() {
    return redisPassword;
  }

  @Override
  public void setRedisPassword(String password) {
    redisPassword = password;
  }

  @Override
  public String getOffHeapMemorySize() {
    return offHeapMemorySize;
  }

  @Override
  public void setOffHeapMemorySize(String value) {
    offHeapMemorySize = value;
  }

  @Override
  public String getMemcachedBindAddress() {
    return memcachedBindAddress;
  }

  @Override
  public void setMemcachedBindAddress(String bindAddress) {
    memcachedBindAddress = bindAddress;
  }

  @Override
  public void setEnableClusterConfiguration(boolean newValue) {
    enableSharedConfiguration = newValue;
  }

  @Override
  public boolean getEnableClusterConfiguration() {
    return enableSharedConfiguration;
  }


  @Override
  public void setUseSharedConfiguration(boolean newValue) {
    useSharedConfiguration = newValue;
  }

  @Override
  public boolean getEnableManagementRestService() {
    return enableManagementRestService;
  }

  @Override
  public void setEnableManagementRestService(boolean enableManagementRestService) {
    this.enableManagementRestService = enableManagementRestService;
  }

  @Override
  public boolean getUseSharedConfiguration() {
    return useSharedConfiguration;
  }

  @Override
  public void setLoadClusterConfigFromDir(boolean newValue) {
    loadSharedConfigurationFromDir = newValue;
  }

  @Override
  public boolean getLoadClusterConfigFromDir() {
    return loadSharedConfigurationFromDir;
  }

  @Override
  public void setClusterConfigDir(String clusterConfigDir) {
    this.clusterConfigDir = clusterConfigDir;
  }

  @Override
  public String getClusterConfigDir() {
    return clusterConfigDir;
  }

  @Override
  public boolean getServerSSLEnabled() {
    return serverSSLEnabled;
  }

  @Override
  public void setServerSSLEnabled(boolean enabled) {
    serverSSLEnabled = enabled;

  }

  @Override
  public boolean getServerSSLRequireAuthentication() {
    return serverSslRequireAuthentication;
  }

  @Override
  public void setServerSSLRequireAuthentication(boolean enabled) {
    serverSslRequireAuthentication = enabled;
  }

  @Override
  public String getServerSSLProtocols() {
    return serverSslProtocols;
  }

  @Override
  public void setServerSSLProtocols(String protocols) {
    serverSslProtocols = protocols;
  }

  @Override
  public String getServerSSLCiphers() {
    return serverSslCiphers;
  }

  @Override
  public void setServerSSLCiphers(String ciphers) {
    serverSslCiphers = ciphers;
  }

  @Override
  public void setServerSSLKeyStore(String keyStore) {
    getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, keyStore);
    serverSSLKeyStore = keyStore;
  }

  @Override
  public void setServerSSLKeyStoreType(String keyStoreType) {
    getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME, keyStoreType);
    serverSSLKeyStoreType = keyStoreType;
  }

  @Override
  public void setServerSSLKeyStorePassword(String keyStorePassword) {
    getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME,
        keyStorePassword);
    serverSSLKeyStorePassword = keyStorePassword;
  }

  @Override
  public void setServerSSLTrustStore(String trustStore) {
    getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, trustStore);
    serverSSLTrustStore = trustStore;
  }

  @Override
  public void setServerSSLTrustStorePassword(String trusStorePassword) {
    getServerSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME,
        trusStorePassword);
    serverSSLTrustStorePassword = trusStorePassword;
  }

  @Override
  public String getServerSSLKeyStore() {
    return serverSSLKeyStore;
  }

  @Override
  public String getServerSSLKeyStoreType() {
    return serverSSLKeyStoreType;
  }

  @Override
  public String getServerSSLKeyStorePassword() {
    return serverSSLKeyStorePassword;
  }

  @Override
  public String getServerSSLTrustStore() {
    return serverSSLTrustStore;
  }

  @Override
  public String getServerSSLTrustStorePassword() {
    return serverSSLTrustStorePassword;
  }

  @Override
  public Properties getServerSSLProperties() {
    return serverSslProperties;
  }

  @Override
  public boolean getGatewaySSLEnabled() {
    return gatewaySSLEnabled;
  }

  @Override
  public void setGatewaySSLEnabled(boolean enabled) {
    gatewaySSLEnabled = enabled;

  }

  @Override
  public boolean getGatewaySSLRequireAuthentication() {
    return gatewaySslRequireAuthentication;
  }

  @Override
  public void setGatewaySSLRequireAuthentication(boolean enabled) {
    gatewaySslRequireAuthentication = enabled;
  }

  @Override
  public String getGatewaySSLProtocols() {
    return gatewaySslProtocols;
  }

  @Override
  public void setGatewaySSLProtocols(String protocols) {
    gatewaySslProtocols = protocols;
  }

  @Override
  public String getGatewaySSLCiphers() {
    return gatewaySslCiphers;
  }

  @Override
  public void setGatewaySSLCiphers(String ciphers) {
    gatewaySslCiphers = ciphers;
  }

  @Override
  public void setGatewaySSLKeyStore(String keyStore) {
    getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME, keyStore);
    gatewaySSLKeyStore = keyStore;
  }

  @Override
  public void setGatewaySSLKeyStoreType(String keyStoreType) {
    getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME,
        keyStoreType);
    gatewaySSLKeyStoreType = keyStoreType;
  }

  @Override
  public void setGatewaySSLKeyStorePassword(String keyStorePassword) {
    getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME,
        keyStorePassword);
    gatewaySSLKeyStorePassword = keyStorePassword;
  }

  @Override
  public void setGatewaySSLTrustStore(String trustStore) {
    getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME, trustStore);
    gatewaySSLTrustStore = trustStore;
  }

  @Override
  public void setGatewaySSLTrustStorePassword(String trusStorePassword) {
    getGatewaySSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME,
        trusStorePassword);
    gatewaySSLTrustStorePassword = trusStorePassword;
  }

  @Override
  public String getGatewaySSLKeyStore() {
    return gatewaySSLKeyStore;
  }

  @Override
  public String getGatewaySSLKeyStoreType() {
    return gatewaySSLKeyStoreType;
  }

  @Override
  public String getGatewaySSLKeyStorePassword() {
    return gatewaySSLKeyStorePassword;
  }

  @Override
  public String getGatewaySSLTrustStore() {
    return gatewaySSLTrustStore;
  }

  @Override
  public String getGatewaySSLTrustStorePassword() {
    return gatewaySSLTrustStorePassword;
  }

  @Override
  public Properties getGatewaySSLProperties() {
    return gatewaySslProperties;
  }

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
  public void setHttpServiceSSLProtocols(String protocols) {
    httpServiceSSLProtocols = protocols;
  }

  @Override
  public String getHttpServiceSSLCiphers() {
    return httpServiceSSLCiphers;
  }

  @Override
  public void setHttpServiceSSLCiphers(String ciphers) {
    httpServiceSSLCiphers = ciphers;
  }

  @Override
  public String getHttpServiceSSLKeyStore() {
    return httpServiceSSLKeyStore;
  }

  @Override
  public void setHttpServiceSSLKeyStore(String keyStore) {
    getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_NAME,
        keyStore);
    httpServiceSSLKeyStore = keyStore;
  }

  @Override
  public String getHttpServiceSSLKeyStoreType() {
    return httpServiceSSLKeyStoreType;
  }

  @Override
  public void setHttpServiceSSLKeyStoreType(String keyStoreType) {
    getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_TYPE_NAME,
        keyStoreType);
    httpServiceSSLKeyStoreType = keyStoreType;
  }

  @Override
  public String getHttpServiceSSLKeyStorePassword() {
    return httpServiceSSLKeyStorePassword;
  }

  @Override
  public void setHttpServiceSSLKeyStorePassword(String keyStorePassword) {
    getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + KEY_STORE_PASSWORD_NAME,
        keyStorePassword);
    httpServiceSSLKeyStorePassword = keyStorePassword;
  }

  @Override
  public String getHttpServiceSSLTrustStore() {
    return httpServiceSSLTrustStore;
  }

  @Override
  public void setHttpServiceSSLTrustStore(String trustStore) {
    getHttpServiceSSLProperties().setProperty(SSL_SYSTEM_PROPS_NAME + TRUST_STORE_NAME,
        trustStore);
    httpServiceSSLTrustStore = trustStore;
  }

  @Override
  public String getHttpServiceSSLTrustStorePassword() {
    return httpServiceSSLTrustStorePassword;
  }

  @Override
  public void setHttpServiceSSLTrustStorePassword(String trustStorePassword) {
    getHttpServiceSSLProperties().setProperty(
        SSL_SYSTEM_PROPS_NAME + TRUST_STORE_PASSWORD_NAME, trustStorePassword);
    httpServiceSSLTrustStorePassword = trustStorePassword;
  }

  @Override
  public Properties getHttpServiceSSLProperties() {
    return httpServiceSSLProperties;
  }

  @Override
  public ConfigSource getConfigSource(String attName) {
    return sourceMap.get(attName);
  }

  @Override
  public boolean getDistributedTransactions() {
    return distributedTransactions;
  }

  @Override
  public void setDistributedTransactions(boolean value) {
    distributedTransactions = value;
  }

  @Override
  public boolean getThreadMonitorEnabled() {
    return threadMonitorEnabled;
  }

  @Override
  public void setThreadMonitorEnabled(boolean newValue) {
    threadMonitorEnabled = newValue;
  }

  @Override
  public int getThreadMonitorInterval() {
    return threadMonitorInterval;
  }

  @Override
  public void setThreadMonitorInterval(int newValue) {
    threadMonitorInterval = newValue;
  }

  @Override
  public int getThreadMonitorTimeLimit() {
    return threadMonitorTimeLimit;
  }

  @Override
  public void setThreadMonitorTimeLimit(int newValue) {
    threadMonitorTimeLimit = newValue;
  }
}
