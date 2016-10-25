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
package org.apache.geode.management;


import org.apache.geode.internal.security.SecurableCommunicationChannel;

/**
 * Composite Data type to be used by member to depict gemfire properties in key value manner
 * @since GemFire 7.0
 *
 */
public class GemFireProperties {

  /**
   * a symbolic name that can be used by administrators to help identify a
   * connection to the distributed system.
   **/
  private String memberName;
  private String memberGroups;
  /**
   * The port used for multicast networking. If zero, then multicast will be
   * disabled and locators must be used to find the other members of the
   * distributed system. If "mcast-port" is zero and "locators" is "" then this
   * distributed system will be isolated from all other GemFire processes.
   * Default: "10334"
   **/
  private int mcastPort;
  /**
   * The IP address used for multicast networking. If mcast-port is zero, then
   * mcast-address is ignored. Default: "239.192.81.1"
   **/
  private String mcastAddress;
  /**
   * The IP address that this distributed system's server sockets will listen
   * on. If set to an empty string then the local machine's default address will
   * be listened on.
   **/
  private String bindAddress;
  /**
   * A 16-bit integer that determines the tcp/ip port number to listen on for
   * cache communications. If zero, the operating system will select an
   * available port to listen on. Each process on a machine must have its own
   * tcp-port. Note that some operating systems restrict the range of ports
   * usable by non-privileged users, and using restricted port numbers can cause
   * runtime errors in GemFire startup.
   **/
  private int tcpPort;
  /**
   * Specifies the name of the XML file or resource to initialize the cache with
   * when it is created. Create will first look for a file that matches the
   * value of this property. If a file is not found then it will be searched for
   * using ClassLoader.getResource(java.lang.String). If the value of this
   * property is the empty string (""), then the cache will not be declaratively
   * initialized
   **/
  private String cacheXMLFile;
  private String configFile;
  /**
   * Determines how far through your network the multicast packets used by
   * GemFire will propagate. Default: "32" Allowed values: 0..255
   **/
  private int mcastTTL;
  private String serverBindAddress;
  /**
   * A list of locators (host and port) that are used to find other member of
   * the distributed system. This attribute's value is a possibly empty comma
   * separated list. Each element must be of the form "hostName[portNum]" and
   * may be of the form "host:bindAddress[port]" if a specific bind address is
   * to be used on the locator machine. The square brackets around the portNum
   * are literal character and must be specified.
   * 
   * Since IPv6 bind addresses may contain colons, you may use an at symbol
   * instead of a colon to separate the host name and bind address. For example,
   * "server1@fdf0:76cf:a0ed:9449::5[12233]" specifies a locator running on
   * "server1" and bound to fdf0:76cf:a0ed:9449::5 on port 12233.
   * 
   * If "mcast-port" is zero and "locators" is "" then this distributed system
   * will be isolated from all other GemFire processes.
   **/
  private String locators;
  /**
   * A host name or bind-address and port ("host[port],peer=,server=") that are
   * used to start a locator in the same process as the DistributedSystem. The
   * locator is started when the DistributedSystem connects, and is stopped when
   * the DistributedSystem disconnects. To start a locator that is not tied to
   * the DistributedSystem's lifecycle, see the Locator class in this same
   * package.
   * 
   * The peer and server parameters are optional. They specify whether the
   * locator can be used for peers to discover eachother, or for clients to
   * discover peers. By default both are true. Default: "" (doesn't start a
   * locator)
   **/
  private String startLocator;
  /**
   * Name of the file to write logging messages to. If the file name if ""
   * (default) then messages are written to standard out.
   **/
  private String logFile;
  /** The type of log messages that will actually write to the log file. **/
  private int logLevel;
  /**
   * "true" causes the statistics to be sampled periodically and operating
   * system statistics to be fetched each time a sample is taken. "false"
   * disables sampling which also disables operating system statistic
   * collection. Non OS statistics will still be recorded in memory and can be
   * viewed by administration tools. However, charts will show no activity and
   * no statistics will be archived while sampling is disabled.
   **/
  private boolean statisticSamplingEnabled;
  /**
   * The rate, in milliseconds, at which samples of the statistics will be
   * taken.
   **/
  private int statisticSampleRate;
  /**
   * The file that statistic samples are written to. An empty string (default)
   * disables statistic archival.
   **/
  private String statisticArchiveFile;
  private String includeFile;
  /**
   * The number of seconds the distributed system will wait for a message to be
   * acknowledged before it sends a warning level alert to signal that something
   * might be wrong with the system node that is unresponsive. After sending
   * this alert the waiter continues to wait. The alerts are logged in the log
   * as warnings and will cause an alert notification in the Admin API and
   * GemFire JMX Agent. Default: "15"
   **/
  private int ackWaitThreshold;
  /**
   * The number of seconds the distributed system will wait after the
   * ack-wait-threshold for a message to be acknowledged before it issues an
   * alert at severe level. The default value is zero, which turns off this
   * feature.
   * 
   * when ack-severe-alert-threshold is used, GemFire will also initiate
   * additional checks to see if the process is alive. These checks will begin
   * when the ack-wait-threshold is reached and will continue until GemFire has
   * been able to communicate with the process and ascertain its status.
   **/
  private int ackSevereAlertThreshold;
  /**
   * Limits, in megabytes, how large the current statistic archive file can grow
   * before it is closed and archival rolls on to a new file. Set to zero to
   * disable archive rolling. Default: "0"
   **/
  private int archiveFileSizeLimit;
  /**
   * Limits, in megabytes, how much disk space can be consumed by old inactive
   * statistic archive files. When the limit is exceeded the oldest inactive
   * archive is deleted. Set to zero to disable automatic archive deletion.
   * Default: "0"
   **/
  private int archiveDiskSpaceLimit;
  /**
   * Limits, in megabytes, how large the current log file can grow before it is
   * closed and logging rolls on to a new file. Set to zero to disable log
   * rolling.
   **/
  private int logFileSizeLimit;
  /**
   * Limits, in megabytes, how much disk space can be consumed by old inactive
   * log files. When the limit is exceeded the oldest inactive log file is
   * deleted. Set to zero to disable automatic log file deletion.
   **/
  private int logDiskSpaceLimit;
//  /**
//   * If true, all gemfire socket communication is configured to use SSL through
//   * JSSE.
//   **/
//  private boolean sslEnabled;
//  /**
//   * A space seperated list of the SSL cipher suites to enable. Those listed
//   * must be supported by the available providers.
//   **/
//  private String sslCiphers;
//  /**
//   * A space seperated list of the SSL protocols to enable. Those listed must be
//   * supported by the available providers.
//   **/
//  private String sslProtocols;
//  /**
//   * If false, allow ciphers that do not require the client side of the
//   * connection to be authenticated.
//   **/
//  private boolean sslRequireAuthentication;
  /**
   * The number of milliseconds a thread can keep exclusive access to a socket
   * that it is not actively using. Once a thread loses its lease to a socket it
   * will need to re-acquire a socket the next time it sends a message. A value
   * of zero causes socket leases to never expire. This property is ignored if
   * "conserve-sockets" is true. Default: "15000"
   **/
  private int socketLeaseTime;
  /**
   * The size of each socket buffer, in bytes. Smaller buffers conserve memory.
   * Larger buffers can improve performance; in particular if large messages are
   * being sent. Default: "32768"
   **/
  private int socketBufferSize;
  /**
   * Description: Sets the size of the socket buffer used for outgoing multicast
   * transmissions. Default: "65535" Allowed values: 2048..Operating System
   * maximum
   **/
  private int mcastSendBufferSize;
  /**
   * Description: Sets the size of the socket buffer used for incoming multicast
   * transmissions. You should set this high if there will be high volumes of
   * messages. Default: "1048576" Allowed values: 2048..Operating System maximum
   **/
  private int mcastRecvBufferSize;
  /**
   * The byteAllowance determines how many bytes can be sent without a recharge
   * from other processes
   **/
  private int mcastByteAllowance;
  /**
   * The rechargeThreshold tells receivers how low the sender's initial to
   * remaining allowance ratio should be before sending a recharge
   **/
  private float mcastRechargeThreshold;
  /**
   * The rechargeBlockMs tells the sender how long to wait for a recharge before
   * explicitly requesting one.
   **/
  private int mcastRechargeBlockMs;
  /**
   * When messages are sent over datagram sockets, GemFire breaks large messages
   * down into fragments for transmission. This property sets the maximum
   * fragment size for transmission.
   **/
  private int udpFragmentSize;
  /**
   * Sets the size of the socket buffer used for outgoing udp point-to-point
   * transmissions.
   **/
  private int udpSendBufferSize;
  /**
   * Sets the size of the socket buffer used for incoming udp point-to-point
   * transmissions. Note: if multicast is not enabled and disable-tcp is not
   * enabled, a reduced default size of 65535 is used.
   **/
  private int udpRecvBufferSize;
  /**
   * Turns off use of tcp/ip sockets, forcing the cache to use datagram sockets
   * for all communication. This is useful if you have a large number of
   * processes in the distributed cache since it eliminates the per-connection
   * reader-thread that is otherwise required. However, udp communications are
   * somewhat slower than tcp/ip communications due to the extra work required
   * in Java to break messages down to transmittable sizes, and the extra work
   * required to guarantee message delivery.
   **/
  private boolean disableTcp;
  /**
   * "true" causes additional time-based statistics to be gathered for gemfire
   * operations. This can aid in discovering where time is going in cache
   * operations, albeit at the expense of extra clock probes on every operation.
   * "false" disables the additional time-based statistics.
   **/
  private boolean enableTimeStatistics;
  /**
   * Turns on network partitioning detection algorithms, which detect loss of
   * processes eligible to function as the membership coordinator and correlates
   * it with loss of a selected lead member.
   **/
  private boolean enableNetworkPartitionDetection;
  /**
   * Establishes the number of seconds of process failure history kept by the
   * system for correlating loss of processes eligible to be the membership
   * coordinator and the lead member.
   **/
  private int departureCorrelationWindow;
  /**
   * Sets the timeout interval, in milliseconds, used to determine whether
   * another process is alive or not. When another process appears to be gone,
   * GemFire tries five times to contact it before giving up. This property sets
   * the timeout interval for each of these attempts. Default: "5000"
   **/
  private int memberTimeout;
  /**
   * The allowed range of ports for use in forming an unique membership
   * identifier (UDP) and for failure detection purposes (TCP). This range is
   * given as two numbers separated by a minus sign.
   **/
  private int[] membershipPortRange;
  /**
   * If "true" then a minimal number of sockets will be used when connecting to
   * the distributed system. This conserves resource usage but can cause
   * performance to suffer. If "false" then every application thread that sends
   * distribution messages to other members of the distributed system will own
   * its own sockets and have exclusive access to them. The length of time a
   * thread can have exclusive access to a socket can be configured with
   * "socket-lease-time". Default: "true"
   **/
  private boolean conserveSockets;
  /**
   * Specifies the application roles that this member performs in the
   * distributed system. This is a comma delimited list of user-defined strings.
   * Any number of members can be configured to perform the same role, and a
   * member can be configured to perform any number of roles.
   * @deprecated this feature is scheduled to be removed
   **/
  private String roles;
  /**
   * Specifies the maximum number of milliseconds to wait for the distributed
   * system to reconnect in case of required role loss. The system will attempt
   * to reconnect more than once, and this timeout period applies to each
   * reconnection attempt. Default: "10000"
   * @deprecated this feature is scheduled to be removed
   **/
  private int maxWaitTimeForReconnect;
  /**
   * Specifies the maximum number or times to attempt to reconnect to the
   * distributed system when required roles are missing. Default: "3"
   **/
  private int maxNumReconnectTries;
  /**
   * The number of milliseconds before a publishing process should attempt to
   * distribute a cache operation before switching over to asynchronous
   * messaging for this process. To enable asynchronous messaging, the value
   * must be set above zero. If a thread that is publishing to the cache exceeds
   * this value when attempting to distribute to this process, it will switch to
   * asynchronous messaging until this process catches up, departs, or some
   * specified limit is reached, such as async-queue-timeout or
   * async-max-queue-size. Default: "0"
   * @deprecated this feature is scheduled to be removed
   **/
  private int asyncDistributionTimeout;
  /**
   * The number of milliseconds a queuing publisher may enqueue asynchronous
   * messages without any distribution to this process before that publisher
   * requests this process to depart. If a queuing publisher has not been able
   * to send this process any cache operations prior to the timeout, this
   * process will attempt to close its cache and disconnect from the distributed
   * system. Default: "60000"
   **/
  private int asyncQueueTimeout;
  /**
   * The maximum size in megabytes that a publishing process should be allowed
   * to asynchronously enqueue for this process before asking this process to
   * depart from the distributed system.
   **/
  private int asyncMaxQueueSize;
  /**
   * This is a client-side property that is passed to the server. Allowable
   * values are "server", "true", and "false". With the "server" setting, this
   * client's servers use their own client queue conflation settings. With a
   * "true" setting, the servers disregard their own configuration and enable
   * conflation of events for all regions for the client. A "false" setting
   * causes the client's servers to disable conflation for all regions for the
   * client. Default: "server"
   **/
  private String clientConflation;
  /**
   * The id to be used by this durable client. When a durable client connects to
   * a server, this id is used by the server to identify it. The server will
   * accumulate updates for a durable client while it is disconnected and
   * deliver these events to the client when it reconnects. Default: ""
   **/
  private String durableClientId;
  /**
   * The number of seconds a disconnected durable client is kept alive and
   * updates are accumulated for it by the server before it is terminated.
   * Default: "300"
   **/
  private int durableClientTimeout;
  /**
   * Authentication module name for Clients that requires to act upon
   * credentials read from the gemfire.properties file. Module must implement
   * AuthInitialize interface. Default: ""
   **/
  private String securityClientAuthInit;
  private String securityClientAuthenticator;
  private String securityClientDHAlgo;
  private String securityPeerAuthInit;
  private String securityPeerAuthenticator;
  private String securityClientAccessor;
  private String securityClientAccessorPP;
  private int securityLogLevel;
  private String securityLogFile;
  private int securityPeerMembershipTimeout;
  private boolean removeUnresponsiveClient;
  /**
   * "true" indicates that server propagates delta generated from Delta type of
   * objects. If "false" then server propagates full object but not delta.
   * Default: "true"
   **/
  private boolean deltaPropagation;
  /**
   * Defines the redundancy zone from this member. If this property is set,
   * partitioned regions will not put two redundant copies of data in two
   * members with the same redundancy zone setting.
   **/
  private String redundancyZone;
  /**
   * Whether or not partitioned regions will put redundant copies of the same
   * data in different JVMs running on the same physical host. By default,
   * partitioned regions will try to put redundancy copies on different physical
   * hosts, but it may put them on the same physical host if no other hosts are
   * available. Setting this property to true will prevent partitions regions
   * from ever putting redundant copies of data on the same physical host.
   **/
  private boolean enforceUniqueHost;

  private boolean jmxManager;
  private boolean jmxManagerStart;
  private boolean jmxManagerSSL;
  private int jmxManagerPort;
  private String jmxManagerBindAddress;
  private String jmxManagerHostnameForClients;
  private String jmxManagerPasswordFile;
  private String jmxManagerAccessFile;
  private int jmxManagerHttpPort;
  private int httpServicePort;
  private String httpServiceBindAddress;
  private boolean startDevRestApi;
  private int jmxManagerUpdateRate;


  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLEnabled}
   */
  @Deprecated
  private boolean jmxManagerSSLEnabled;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLProtocols}
   */
  @Deprecated
  private String jmxManagerSSLProtocols;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLCiphers}
   */
  @Deprecated
  private String jmxManagerSSLCiphers;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLRequireAuthentication}
   */
  @Deprecated
  private boolean jmxManagerSSLRequireAuthentication;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStore}
   */
  @Deprecated
  private String jmxManagerSSLKeyStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStoreType}
   */
  @Deprecated
  private String jmxManagerSSLKeyStoreType;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStorePassword}
   */
  @Deprecated
  private String jmxManagerSSLKeyStorePassword;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStore}
   */
  @Deprecated
  private String jmxManagerSSLTrustStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStorePassword}
   */
  @Deprecated
  private String jmxManagerSSLTrustStorePassword;
  private String jmxSSLAlias;
  
  private boolean clusterSSLEnabled;
  private String clusterSSLProtocols;
  private String clusterSSLCiphers;
  private boolean clusterSSLRequireAuthentication;
  private String clusterSSLKeyStore;
  private String clusterSSLKeyStoreType;
  private String clusterSSLKeyStorePassword;
  private String clusterSSLTrustStore;
  private String clusterSSLTrustStorePassword;
  private String clusterSSLAlias;

  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLEnabled}
   */
  @Deprecated
  private boolean serverSSLEnabled;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLProtocols}
   */
  @Deprecated
  private String serverSSLProtocols;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLCiphers}
   */
  @Deprecated
  private String serverSSLCiphers;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLRequireAuthentication}
   */
  @Deprecated
  private boolean serverSSLRequireAuthentication;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStore}
   */
  @Deprecated
  private String serverSSLKeyStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStoreType}
   */
  @Deprecated
  private String serverSSLKeyStoreType;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStorePassword}
   */
  @Deprecated
  private String serverSSLKeyStorePassword;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStore}
   */
  @Deprecated
  private String serverSSLTrustStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStorePassword}
   */
  @Deprecated
  private String serverSSLTrustStorePassword;
  private String serverSSLAlias;

  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLEnabled}
   */
  @Deprecated
  private boolean gatewaySSLEnabled;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLProtocols}
   */
  @Deprecated
  private String gatewaySSLProtocols;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLCiphers}
   */
  @Deprecated
  private String gatewaySSLCiphers;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLRequireAuthentication}
   */
  @Deprecated
  private boolean gatewaySSLRequireAuthentication;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStore}
   */
  @Deprecated
  private String gatewaySSLKeyStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStoreType}
   */
  @Deprecated
  private String gatewaySSLKeyStoreType;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStorePassword}
   */
  @Deprecated
  private String gatewaySSLKeyStorePassword;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStore}
   */
  @Deprecated
  private String gatewaySSLTrustStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStorePassword}
   */
  @Deprecated
  private String gatewaySSLTrustStorePassword;
  private String gatewaySSLAlias;

  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLEnabled}
   */
  @Deprecated
  private boolean httpServiceSSLEnabled;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLRequireAuthentication}
   */
  @Deprecated
  private boolean httpServiceSSLRequireAuthentication;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLProtocols}
   */
  @Deprecated
  private String httpServiceSSLProtocols;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLCiphers}
   */
  @Deprecated
  private String httpServiceSSLCiphers;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStore}
   */
  @Deprecated
  private String httpServiceSSLKeyStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStoreType}
   */
  @Deprecated
  private String httpServiceSSLKeyStoreType;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLKeyStorePassword}
   */
  @Deprecated
  private String httpServiceSSLKeyStorePassword;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStore}
   */
  @Deprecated
  private String httpServiceSSLTrustStore;
  /**
   * @deprecated Geode 1.0 use {@link #clusterSSLTrustStorePassword}
   */
  @Deprecated
  private String httpServiceSSLTrustStorePassword;
  private String httpServiceSSLAlias;
  
  /**
   * Specifies whether the default transaction mode should be distributed.
   */
  private boolean distributedTransactions;

  private String locatorSSLAlias;

  private SecurableCommunicationChannel[] securableCommunicationChannels;
  private String sslProtocols;
  private String sslCiphers;
  private boolean sslRequireAuthentication;
  private String sslKeyStore;
  private String sslKeyStoreType;
  private String sslKeyStorePassword;
  private String sslTrustStore;
  private String sslTrustStorePassword;
  private boolean sslWebServiceRequireAuthentication;
  private String sslDefaultAlias;
  
  
  /**
   * This constructor is to be used by internal JMX framework only. User should
   * not try to create an instance of this class.
   */
  public GemFireProperties() {

  }

  // Getters Starts ************************************//
  public String getMemberName() {
    return memberName;
  }

  public String getMemberGroups() {
    return memberGroups;
  }

  public int getMcastPort() {
    return mcastPort;
  }

  public String getMcastAddress() {
    return mcastAddress;
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public int getTcpPort() {
    return tcpPort;
  }

  public String getCacheXMLFile() {
    return cacheXMLFile;
  }

  public String getConfigFile() {
    return configFile;
  }

  public int getMcastTTL() {
    return mcastTTL;
  }

  public String getServerBindAddress() {
    return serverBindAddress;
  }

  public String getLocators() {
    return locators;
  }

  public String getStartLocator() {
    return startLocator;
  }

  public String getLogFile() {
    return logFile;
  }

  public int getLogLevel() {
    return logLevel;
  }

  public boolean isStatisticSamplingEnabled() {
    return statisticSamplingEnabled;
  }

  public String getStatisticArchiveFile() {
    return statisticArchiveFile;
  }

  public String getIncludeFile() {
    return includeFile;
  }

  public int getAckWaitThreshold() {
    return ackWaitThreshold;
  }

  public int getAckSevereAlertThreshold() {
    return ackSevereAlertThreshold;
  }

  public int getArchiveFileSizeLimit() {
    return archiveFileSizeLimit;
  }

  public int getArchiveDiskSpaceLimit() {
    return archiveDiskSpaceLimit;
  }

  public int getLogFileSizeLimit() {
    return logFileSizeLimit;
  }

  public int getLogDiskSpaceLimit() {
    return logDiskSpaceLimit;
  }

  public int getSocketLeaseTime() {
    return socketLeaseTime;
  }

  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  public int getMcastSendBufferSize() {
    return mcastSendBufferSize;
  }

  public int getMcastRecvBufferSize() {
    return mcastRecvBufferSize;
  }

  public int getMcastByteAllowance() {
    return mcastByteAllowance;
  }

  public float getMcastRechargeThreshold() {
    return mcastRechargeThreshold;
  }

  public int getMcastRechargeBlockMs() {
    return mcastRechargeBlockMs;
  }

  public int getUdpFragmentSize() {
    return udpFragmentSize;
  }

  public int getUdpSendBufferSize() {
    return udpSendBufferSize;
  }

  public int getUdpRecvBufferSize() {
    return udpRecvBufferSize;
  }

  public boolean isDisableTcp() {
    return disableTcp;
  }

  public boolean isEnableTimeStatistics() {
    return enableTimeStatistics;
  }

  public boolean isEnableNetworkPartitionDetection() {
    return enableNetworkPartitionDetection;
  }

  public int getDepartureCorrelationWindow() {
    return departureCorrelationWindow;
  }

  public int getMemberTimeout() {
    return memberTimeout;
  }

  public int[] getMembershipPortRange() {
    return membershipPortRange;
  }

  public boolean isConserveSockets() {
    return conserveSockets;
  }

  public String getRoles() {
    return roles;
  }

  public int getMaxWaitTimeForReconnect() {
    return maxWaitTimeForReconnect;
  }

  public int getMaxNumReconnectTries() {
    return maxNumReconnectTries;
  }

  public int getAsyncDistributionTimeout() {
    return asyncDistributionTimeout;
  }

  public int getAsyncQueueTimeout() {
    return asyncQueueTimeout;
  }

  public int getAsyncMaxQueueSize() {
    return asyncMaxQueueSize;
  }

  public String getClientConflation() {
    return clientConflation;
  }

  public String getDurableClientId() {
    return durableClientId;
  }

  public int getDurableClientTimeout() {
    return durableClientTimeout;
  }

  public String getSecurityClientAuthInit() {
    return securityClientAuthInit;
  }

  public String getSecurityClientAuthenticator() {
    return securityClientAuthenticator;
  }

  public String getSecurityClientDHAlgo() {
    return securityClientDHAlgo;
  }

  public String getSecurityPeerAuthInit() {
    return securityPeerAuthInit;
  }

  public String getSecurityPeerAuthenticator() {
    return securityPeerAuthenticator;
  }

  public String getSecurityClientAccessor() {
    return securityClientAccessor;
  }

  public String getSecurityClientAccessorPP() {
    return securityClientAccessorPP;
  }

  public int getSecurityLogLevel() {
    return securityLogLevel;
  }

  public String getSecurityLogFile() {
    return securityLogFile;
  }

  public int getSecurityPeerMembershipTimeout() {
    return securityPeerMembershipTimeout;
  }

  public boolean isRemoveUnresponsiveClient() {
    return removeUnresponsiveClient;
  }

  public boolean isDeltaPropagation() {
    return deltaPropagation;
  }

  public String getRedundancyZone() {
    return redundancyZone;
  }

  public boolean isEnforceUniqueHost() {
    return enforceUniqueHost;
  }
  
  public int getStatisticSampleRate(){
    return statisticSampleRate;
  }

  public void setMemberName(String memberName) {
    this.memberName = memberName;
    
  }

  public void setMemberGroups(String memberGroups) {
    this.memberGroups = memberGroups;
    
  }

  public void setMcastPort(int mcastPort) {
    this.mcastPort = mcastPort;
    
  }

  public void setMcastAddress(String mcastAddress) {
    this.mcastAddress = mcastAddress;
    
  }

  public void setBindAddress(String bindAddress) {
    this.bindAddress = bindAddress;
    
  }

  public void setTcpPort(int tcpPort) {
    this.tcpPort = tcpPort;
    
  }

  public void setCacheXMLFile(String cacheXMLFile) {
    this.cacheXMLFile = cacheXMLFile;
    
  }

  public void setConfigFile(String configFile) {
    this.configFile = configFile;
    
  }

  public void setMcastTTL(int mcastTTL) {
    this.mcastTTL = mcastTTL;
    
  }

  public void setServerBindAddress(String serverBindAddress) {
    this.serverBindAddress = serverBindAddress;
    
  }

  public void setLocators(String locators) {
    this.locators = locators;
    
  }

  public void setStartLocator(String startLocator) {
    this.startLocator = startLocator;
    
  }

  public void setLogFile(String logFile) {
    this.logFile = logFile;
    
  }

  public void setLogLevel(int logLevel) {
    this.logLevel = logLevel;
    
  }

  public void setStatisticSamplingEnabled(boolean statisticSamplingEnabled) {
    this.statisticSamplingEnabled = statisticSamplingEnabled;
    
  }

  public void setStatisticArchiveFile(String statisticArchiveFile) {
    this.statisticArchiveFile = statisticArchiveFile;
    
  }

  public void setIncludeFile(String includeFile) {
    this.includeFile = includeFile;
    
  }

  public void setAckWaitThreshold(int ackWaitThreshold) {
    this.ackWaitThreshold = ackWaitThreshold;
    
  }

  public void setAckSevereAlertThreshold(int ackSevereAlertThreshold) {
    this.ackSevereAlertThreshold = ackSevereAlertThreshold;
    
  }

  public void setArchiveFileSizeLimit(int archiveFileSizeLimit) {
    this.archiveFileSizeLimit = archiveFileSizeLimit;
    
  }

  public void setArchiveDiskSpaceLimit(int archiveDiskSpaceLimit) {
    this.archiveDiskSpaceLimit = archiveDiskSpaceLimit;
    
  }

  public void setLogFileSizeLimit(int logFileSizeLimit) {
    this.logFileSizeLimit = logFileSizeLimit;
    
  }

  public void setLogDiskSpaceLimit(int logDiskSpaceLimit) {
    this.logDiskSpaceLimit = logDiskSpaceLimit;
    
  }

//  public void setSSLEnabled(boolean sslEnabled) {
//    this.sslEnabled = sslEnabled;
//
//  }
//
//  public void setSSLCiphers(String sslCiphers) {
//    this.sslCiphers = sslCiphers;
//
//  }
//
//  public void setSSLProtocols(String sslProtocols) {
//    this.sslProtocols = sslProtocols;
//
//  }
//
//  public void setSSLRequireAuthentication(boolean sslRequireAuthentication) {
//    this.sslRequireAuthentication = sslRequireAuthentication;
//
//  }

  public void setSocketLeaseTime(int socketLeaseTime) {
    this.socketLeaseTime = socketLeaseTime;
    
  }

  public void setSocketBufferSize(int socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
    
  }

  public void setMcastSendBufferSize(int mcastSendBufferSize) {
    this.mcastSendBufferSize = mcastSendBufferSize;
    
  }

  public void setMcastRecvBufferSize(int mcastRecvBufferSize) {
    this.mcastRecvBufferSize = mcastRecvBufferSize;
    
  }

  public void setMcastByteAllowance(int mcastByteAllowance) {
    this.mcastByteAllowance = mcastByteAllowance;
    
  }

  public void setMcastRechargeThreshold(float mcastRechargeThreshold) {
    this.mcastRechargeThreshold = mcastRechargeThreshold;
    
  }

  public void setMcastRechargeBlockMs(int mcastRechargeBlockMs) {
    this.mcastRechargeBlockMs = mcastRechargeBlockMs;
    
  }

  public void setUdpFragmentSize(int udpFragmentSize) {
    this.udpFragmentSize = udpFragmentSize;
    
  }

  public void setUdpSendBufferSize(int udpSendBufferSize) {
    this.udpSendBufferSize = udpSendBufferSize;
    
  }

  public void setUdpRecvBufferSize(int udpRecvBufferSize) {
    this.udpRecvBufferSize = udpRecvBufferSize;
    
  }

  public void setDisableTcp(boolean disableTcp) {
    this.disableTcp = disableTcp;
    
  }

  public void setEnableTimeStatistics(boolean enableTimeStatistics) {
    this.enableTimeStatistics = enableTimeStatistics;
    
  }

  public void setEnableNetworkPartitionDetection(
      boolean enableNetworkPartitionDetection) {
    this.enableNetworkPartitionDetection = enableNetworkPartitionDetection;
    
  }

  public void setDepartureCorrelationWindow(int departureCorrelationWindow) {
    this.departureCorrelationWindow = departureCorrelationWindow;
    
  }

  public void setMemberTimeout(int memberTimeout) {
    this.memberTimeout = memberTimeout;
    
  }

  public void setMembershipPortRange(int[] membershipPortRange) {
    this.membershipPortRange = membershipPortRange;
    
  }

  public void setConserveSockets(boolean conserveSockets) {
    this.conserveSockets = conserveSockets;
    
  }

  public void setRoles(String roles) {
    this.roles = roles;
    
  }

  public void setMaxWaitTimeForReconnect(int maxWaitTimeForReconnect) {
    this.maxWaitTimeForReconnect = maxWaitTimeForReconnect;
    
  }

  public void setMaxNumReconnectTries(int maxNumReconnectTries) {
    this.maxNumReconnectTries = maxNumReconnectTries;
    
  }

  public void setAsyncDistributionTimeout(int asyncDistributionTimeout) {
    this.asyncDistributionTimeout = asyncDistributionTimeout;
    
  }

  public void setAsyncQueueTimeout(int asyncQueueTimeout) {
    this.asyncQueueTimeout = asyncQueueTimeout;
    
  }

  public void setAsyncMaxQueueSize(int asyncMaxQueueSize) {
    this.asyncMaxQueueSize = asyncMaxQueueSize;
    
  }

  public void setClientConflation(String clientConflation) {
    this.clientConflation = clientConflation;
    
  }

  public void setDurableClientId(String durableClientId) {
    this.durableClientId = durableClientId;
    
  }

  public void setDurableClientTimeout(int durableClientTimeout) {
    this.durableClientTimeout = durableClientTimeout;
    
  }

  public void setSecurityClientAuthInit(String securityClientAuthInit) {
    this.securityClientAuthInit = securityClientAuthInit;
    
  }

  public void setSecurityClientAuthenticator(String securityClientAuthenticator) {
    this.securityClientAuthenticator = securityClientAuthenticator;
    
  }

  public void setSecurityClientDHAlgo(String securityClientDHAlgo) {
    this.securityClientDHAlgo = securityClientDHAlgo;
    
  }

  public void setSecurityPeerAuthInit(String securityPeerAuthInit) {
    this.securityPeerAuthInit = securityPeerAuthInit;
    
  }

  public void setSecurityPeerAuthenticator(String securityPeerAuthenticator) {
    this.securityPeerAuthenticator = securityPeerAuthenticator;
    
  }

  public void setSecurityClientAccessor(String securityClientAccessor) {
    this.securityClientAccessor = securityClientAccessor;
    
  }

  public void setSecurityClientAccessorPP(String securityClientAccessorPP) {
    this.securityClientAccessorPP = securityClientAccessorPP;
    
  }

  public void setSecurityLogLevel(int securityLogLevel) {
    this.securityLogLevel = securityLogLevel;
     
  }

  public void setSecurityLogFile(String securityLogFile) {
    this.securityLogFile = securityLogFile;
    
  }

  public void setSecurityPeerMembershipTimeout(int securityPeerMembershipTimeout) {
    this.securityPeerMembershipTimeout = securityPeerMembershipTimeout;
    
  }

  public void setRemoveUnresponsiveClient(boolean removeUnresponsiveClient) {
    this.removeUnresponsiveClient = removeUnresponsiveClient;
    
  }

  public void setDeltaPropagation(boolean deltaPropagation) {
    this.deltaPropagation = deltaPropagation;
    
  }

  public void setRedundancyZone(String redundancyZone) {
    this.redundancyZone = redundancyZone;
    
  }

  public void setEnforceUniqueHost(boolean enforceUniqueHost) {
    this.enforceUniqueHost = enforceUniqueHost;
     
  }


  public void setStatisticSampleRate(int statisticSampleRate) {
    this.statisticSampleRate = statisticSampleRate;
    
  }

  public boolean isJmxManager() {
    return jmxManager;
  }

  public void setJmxManager(boolean jmxManager) {
    this.jmxManager = jmxManager;
  }

  public boolean isJmxManagerStart() {
    return jmxManagerStart;
  }

  public void setJmxManagerStart(boolean jmxManagerStart) {
    this.jmxManagerStart = jmxManagerStart;
  }

  public boolean isJmxManagerSSL() {
    return jmxManagerSSL;
  }

  public void setJmxManagerSSL(boolean jmxManagerSSL) {
    this.jmxManagerSSL = jmxManagerSSL;
  }
  
  public int getJmxManagerPort() {
    return jmxManagerPort;
  }

  public void setJmxManagerPort(int jmxManagerPort) {
    this.jmxManagerPort = jmxManagerPort;
  }

  public String getJmxManagerBindAddress() {
    return jmxManagerBindAddress;
  }

  public void setJmxManagerBindAddress(String jmxManagerBindAddress) {
    this.jmxManagerBindAddress = jmxManagerBindAddress;
  }

  public String getJmxManagerHostnameForClients() {
    return jmxManagerHostnameForClients;
  }

  public void setJmxManagerHostnameForClients(String jmxManagerHostnameForClients) {
    this.jmxManagerHostnameForClients = jmxManagerHostnameForClients;
  }

  public String getJmxManagerPasswordFile() {
    return jmxManagerPasswordFile;
  }

  public void setJmxManagerPasswordFile(String jmxManagerPasswordFile) {
    this.jmxManagerPasswordFile = jmxManagerPasswordFile;
  }

  public String getJmxManagerAccessFile() {
    return jmxManagerAccessFile;
  }

  public void setJmxManagerAccessFile(String jmxManagerAccessFile) {
    this.jmxManagerAccessFile = jmxManagerAccessFile;
  }

  public int getJmxManagerHttpPort() {
    return getHttpServicePort();
  }
 
  public void setJmxManagerHttpPort(int jmxManagerHttpPort) {
    //this.jmxManagerHttpPort = jmxManagerHttpPort;
    setHttpServicePort(jmxManagerHttpPort);
  }

  public int getHttpServicePort() {
    return httpServicePort;
  }
 
  public void setHttpServicePort(int httpServicePort) {
    this.httpServicePort = httpServicePort;
  }

  public String getHttpServiceBindAddress() {
    return httpServiceBindAddress;
  }

  public void setHttpServiceBindAddress(String httpServiceBindAddress) {
    this.httpServiceBindAddress = httpServiceBindAddress;
  }
  
  public boolean getStartDevRestApi() {
    return startDevRestApi;
  }

  public void setStartDevRestApi(boolean startDevRestApi) {
    this.startDevRestApi = startDevRestApi;
  }
  
  public int getJmxManagerUpdateRate() {
    return jmxManagerUpdateRate;
  }

  public void setJmxManagerUpdateRate(int jmxManagerUpdateRate) {
    this.jmxManagerUpdateRate = jmxManagerUpdateRate;
  }
  
  public boolean isJmxManagerSSLEnabled() {
    return jmxManagerSSLEnabled;
  }

  public void setJmxManagerSSLEnabled(boolean jmxManagerSSLEnabled) {
    this.jmxManagerSSLEnabled = jmxManagerSSLEnabled;
  }

  public String getJmxManagerSSLProtocols() {
    return jmxManagerSSLProtocols;
  }

  public void setJmxManagerSSLProtocols(String jmxManagerSSLProtocols) {
    this.jmxManagerSSLProtocols = jmxManagerSSLProtocols;
  }

  public String getJmxManagerSSLCiphers() {
    return jmxManagerSSLCiphers;
  }

  public void setJmxManagerSSLCiphers(String jmxManagerSSLCiphers) {
    this.jmxManagerSSLCiphers = jmxManagerSSLCiphers;
  }

  public boolean isJmxManagerSSLRequireAuthentication() {
    return jmxManagerSSLRequireAuthentication;
  }

  public void setJmxManagerSSLRequireAuthentication(
      boolean jmxManagerSSLRequireAuthentication) {
    this.jmxManagerSSLRequireAuthentication = jmxManagerSSLRequireAuthentication;
  }

  public String getJmxManagerSSLKeyStore() {
    return jmxManagerSSLKeyStore;
  }

  public void setJmxManagerSSLKeyStore(String jmxManagerSSLKeyStore) {
    this.jmxManagerSSLKeyStore = jmxManagerSSLKeyStore;
  }

  public String getJmxManagerSSLKeyStoreType() {
    return jmxManagerSSLKeyStoreType;
  }

  public void setJmxManagerSSLKeyStoreType(String jmxManagerSSLKeyStoreType) {
    this.jmxManagerSSLKeyStoreType = jmxManagerSSLKeyStoreType;
  }

  public String getJmxManagerSSLKeyStorePassword() {
    return jmxManagerSSLKeyStorePassword;
  }

  public void setJmxManagerSSLKeyStorePassword(String jmxManagerSSLKeyStorePassword) {
    this.jmxManagerSSLKeyStorePassword = jmxManagerSSLKeyStorePassword;
  }

  public String getJmxManagerSSLTrustStore() {
    return jmxManagerSSLTrustStore;
  }

  public void setJmxManagerSSLTrustStore(String jmxManagerSSLTrustStore) {
    this.jmxManagerSSLTrustStore = jmxManagerSSLTrustStore;
  }

  public String getJmxManagerSSLTrustStorePassword() {
    return jmxManagerSSLTrustStorePassword;
  }

  public void setJmxManagerSSLTrustStorePassword(String jmxManagerSSLTrustStorePassword) {
    this.jmxManagerSSLTrustStorePassword = jmxManagerSSLTrustStorePassword;
  }
  
  public boolean isClusterSSLEnabled() {
    return clusterSSLEnabled;
  }

  public void setClusterSSLEnabled(boolean clusterSSLEnabled) {
    this.clusterSSLEnabled = clusterSSLEnabled;
  }

  public String getClusterSSLProtocols() {
    return clusterSSLProtocols;
  }

  public void setClusterSSLProtocols(String clusterSSLProtocols) {
    this.clusterSSLProtocols = clusterSSLProtocols;
  }

  public String getClusterSSLCiphers() {
    return clusterSSLCiphers;
  }

  public void setClusterSSLCiphers(String clusterSSLCiphers) {
    this.clusterSSLCiphers = clusterSSLCiphers;
  }

  public boolean isClusterSSLRequireAuthentication() {
    return clusterSSLRequireAuthentication;
  }

  public void setClusterSSLRequireAuthentication(
      boolean clusterSSLRequireAuthentication) {
    this.clusterSSLRequireAuthentication = clusterSSLRequireAuthentication;
  }

  public String getClusterSSLKeyStore() {
    return clusterSSLKeyStore;
  }

  public void setClusterSSLKeyStore(String clusterSSLKeyStore) {
    this.clusterSSLKeyStore = clusterSSLKeyStore;
  }

  public String getClusterSSLKeyStoreType() {
    return clusterSSLKeyStoreType;
  }

  public void setClusterSSLKeyStoreType(String clusterSSLKeyStoreType) {
    this.clusterSSLKeyStoreType = clusterSSLKeyStoreType;
  }

  public String getClusterSSLKeyStorePassword() {
    return clusterSSLKeyStorePassword;
  }

  public void setClusterSSLKeyStorePassword(String clusterSSLKeyStorePassword) {
    this.clusterSSLKeyStorePassword = clusterSSLKeyStorePassword;
  }

  public String getClusterSSLTrustStore() {
    return clusterSSLTrustStore;
  }

  public void setClusterSSLTrustStore(String clusterSSLTrustStore) {
    this.clusterSSLTrustStore = clusterSSLTrustStore;
  }

  public String getClusterSSLTrustStorePassword() {
    return clusterSSLTrustStorePassword;
  }

  public void setClusterSSLTrustStorePassword(String clusterSSLTrustStorePassword) {
    this.clusterSSLTrustStorePassword = clusterSSLTrustStorePassword;
  }

  public boolean isServerSSLEnabled() {
    return serverSSLEnabled;
  }

  public void setServerSSLEnabled(boolean serverSSLEnabled) {
    this.serverSSLEnabled = serverSSLEnabled;
  }

  public String getServerSSLProtocols() {
    return serverSSLProtocols;
  }

  public void setServerSSLProtocols(String serverSSLProtocols) {
    this.serverSSLProtocols = serverSSLProtocols;
  }

  public String getServerSSLCiphers() {
    return serverSSLCiphers;
  }

  public void setServerSSLCiphers(String serverSSLCiphers) {
    this.serverSSLCiphers = serverSSLCiphers;
  }

  public boolean isServerSSLRequireAuthentication() {
    return serverSSLRequireAuthentication;
  }

  public void setServerSSLRequireAuthentication(
      boolean serverSSLRequireAuthentication) {
    this.serverSSLRequireAuthentication = serverSSLRequireAuthentication;
  }

  public String getServerSSLKeyStore() {
    return serverSSLKeyStore;
  }

  public void setServerSSLKeyStore(String serverSSLKeyStore) {
    this.serverSSLKeyStore = serverSSLKeyStore;
  }

  public String getServerSSLKeyStoreType() {
    return serverSSLKeyStoreType;
  }

  public void setServerSSLKeyStoreType(String serverSSLKeyStoreType) {
    this.serverSSLKeyStoreType = serverSSLKeyStoreType;
  }

  public String getServerSSLKeyStorePassword() {
    return serverSSLKeyStorePassword;
  }

  public void setServerSSLKeyStorePassword(String serverSSLKeyStorePassword) {
    this.serverSSLKeyStorePassword = serverSSLKeyStorePassword;
  }

  public String getServerSSLTrustStore() {
    return serverSSLTrustStore;
  }

  public void setServerSSLTrustStore(String serverSSLTrustStore) {
    this.serverSSLTrustStore = serverSSLTrustStore;
  }

  public String getServerSSLTrustStorePassword() {
    return serverSSLTrustStorePassword;
  }

  public void setServerSSLTrustStorePassword(String serverSSLTrustStorePassword) {
    this.serverSSLTrustStorePassword = serverSSLTrustStorePassword;
  }

  public boolean isGatewaySSLEnabled() {
    return gatewaySSLEnabled;
  }

  public void setGatewaySSLEnabled(boolean gatewaySSLEnabled) {
    this.gatewaySSLEnabled = gatewaySSLEnabled;
  }

  public String getGatewaySSLProtocols() {
    return gatewaySSLProtocols;
  }

  public void setGatewaySSLProtocols(String gatewaySSLProtocols) {
    this.gatewaySSLProtocols = gatewaySSLProtocols;
  }

  public String getGatewaySSLCiphers() {
    return gatewaySSLCiphers;
  }

  public void setGatewaySSLCiphers(String gatewaySSLCiphers) {
    this.gatewaySSLCiphers = gatewaySSLCiphers;
  }

  public boolean isGatewaySSLRequireAuthentication() {
    return gatewaySSLRequireAuthentication;
  }

  public void setGatewaySSLRequireAuthentication(
      boolean gatewaySSLRequireAuthentication) {
    this.gatewaySSLRequireAuthentication = gatewaySSLRequireAuthentication;
  }

  public String getGatewaySSLKeyStore() {
    return gatewaySSLKeyStore;
  }

  public void setGatewaySSLKeyStore(String gatewaySSLKeyStore) {
    this.gatewaySSLKeyStore = gatewaySSLKeyStore;
  }

  public String getGatewaySSLKeyStoreType() {
    return gatewaySSLKeyStoreType;
  }

  public void setGatewaySSLKeyStoreType(String gatewaySSLKeyStoreType) {
    this.gatewaySSLKeyStoreType = gatewaySSLKeyStoreType;
  }

  public String getGatewaySSLKeyStorePassword() {
    return gatewaySSLKeyStorePassword;
  }

  public void setGatewaySSLKeyStorePassword(String gatewaySSLKeyStorePassword) {
    this.gatewaySSLKeyStorePassword = gatewaySSLKeyStorePassword;
  }

  public String getGatewaySSLTrustStore() {
    return gatewaySSLTrustStore;
  }

  public void setGatewaySSLTrustStore(String gatewaySSLTrustStore) {
    this.gatewaySSLTrustStore = gatewaySSLTrustStore;
  }

  public String getGatewaySSLTrustStorePassword() {
    return gatewaySSLTrustStorePassword;
  }

  public void setGatewaySSLTrustStorePassword(String gatewaySSLTrustStorePassword) {
    this.gatewaySSLTrustStorePassword = gatewaySSLTrustStorePassword;
  }

  public boolean isHttpServiceSSLEnabled() {
    return httpServiceSSLEnabled;
  }

  public void setHttpServiceSSLEnabled(boolean httpServiceSSLEnabled) {
    this.httpServiceSSLEnabled = httpServiceSSLEnabled;
  }

  public boolean isHttpServiceSSLRequireAuthentication() {
    return httpServiceSSLRequireAuthentication;
  }

  public void setHttpServiceSSLRequireAuthentication(boolean httpServiceSSLRequireAuthentication) {
    this.httpServiceSSLRequireAuthentication = httpServiceSSLRequireAuthentication;
  }

  public String getHttpServiceSSLProtocols() {
    return httpServiceSSLProtocols;
  }

  public void setHttpServiceSSLProtocols(String httpServiceSSLProtocols) {
    this.httpServiceSSLProtocols = httpServiceSSLProtocols;
  }

  public String getHttpServiceSSLCiphers() {
    return httpServiceSSLCiphers;
  }

  public void setHttpServiceSSLCiphers(String httpServiceSSLCiphers) {
    this.httpServiceSSLCiphers = httpServiceSSLCiphers;
  }

  public String getHttpServiceSSLKeyStore() {
    return httpServiceSSLKeyStore;
  }

  public void setHttpServiceSSLKeyStore(String httpServiceSSLKeyStore) {
    this.httpServiceSSLKeyStore = httpServiceSSLKeyStore;
  }

  public String getHttpServiceSSLKeyStoreType() {
    return httpServiceSSLKeyStoreType;
  }

  public void setHttpServiceSSLKeyStoreType(String httpServiceSSLKeyStoreType) {
    this.httpServiceSSLKeyStoreType = httpServiceSSLKeyStoreType;
  }

  public String getHttpServiceSSLKeyStorePassword() {
    return httpServiceSSLKeyStorePassword;
  }

  public void setHttpServiceSSLKeyStorePassword(String httpServiceSSLKeyStorePassword) {
    this.httpServiceSSLKeyStorePassword = httpServiceSSLKeyStorePassword;
  }

  public String getHttpServiceSSLTrustStore() {
    return httpServiceSSLTrustStore;
  }

  public void setHttpServiceSSLTrustStore(String httpServiceSSLTrustStore) {
    this.httpServiceSSLTrustStore = httpServiceSSLTrustStore;
  }

  public String getHttpServiceSSLTrustStorePassword() {
    return httpServiceSSLTrustStorePassword;
  }

  public void setHttpServiceSSLTrustStorePassword(String httpServiceSSLTrustStorePassword) {
    this.httpServiceSSLTrustStorePassword = httpServiceSSLTrustStorePassword;
  }
  
  public void setDistributedTransactions(boolean value) {
    this.distributedTransactions = value;
  }
  
  public boolean getDistributedTransactions() {
    return this.distributedTransactions;
  }

  public String getJmxSSLAlias() {
    return jmxSSLAlias;
  }

  public void setJmxSSLAlias(final String jmxSSLAlias) {
    this.jmxSSLAlias = jmxSSLAlias;
  }

  public String getClusterSSLAlias() {
    return clusterSSLAlias;
  }

  public void setClusterSSLAlias(final String clusterSSLAlias) {
    this.clusterSSLAlias = clusterSSLAlias;
  }

  public String getServerSSLAlias() {
    return serverSSLAlias;
  }

  public void setServerSSLAlias(final String serverSSLAlias) {
    this.serverSSLAlias = serverSSLAlias;
  }

  public String getGatewaySSLAlias() {
    return gatewaySSLAlias;
  }

  public void setGatewaySSLAlias(final String gatewaySSLAlias) {
    this.gatewaySSLAlias = gatewaySSLAlias;
  }

  public String getHttpServiceSSLAlias() {
    return httpServiceSSLAlias;
  }

  public void setHttpServiceSSLAlias(final String httpServiceSSLAlias) {
    this.httpServiceSSLAlias = httpServiceSSLAlias;
  }

  public String getLocatorSSLAlias() {
    return locatorSSLAlias;
  }

  public void setLocatorSSLAlias(final String locatorSSLAlias) {
    this.locatorSSLAlias = locatorSSLAlias;
  }

  public SecurableCommunicationChannel[] getSecurableCommunicationChannel() {
    return securableCommunicationChannels;
  }

  public void setSecurableCommunicationChannel(final SecurableCommunicationChannel[] sslEnabledComponents) {
    this.securableCommunicationChannels = sslEnabledComponents;
  }

  public String getSSLProtocols() {
    return sslProtocols;
  }

  public void setSSLProtocols(final String sslProtocols) {
    this.sslProtocols = sslProtocols;
  }

  public String getSSLCiphers() {
    return sslCiphers;
  }

  public void setSSLCiphers(final String sslCiphers) {
    this.sslCiphers = sslCiphers;
  }

  public boolean isSSLRequireAuthentication() {
    return sslRequireAuthentication;
  }

  public void setSSLRequireAuthentication(final boolean sslRequireAuthentication) {
    this.sslRequireAuthentication = sslRequireAuthentication;
  }

  public String getSSLKeyStore() {
    return sslKeyStore;
  }

  public void setSSLKeyStore(final String sslKeyStore) {
    this.sslKeyStore = sslKeyStore;
  }

  public String getSSLKeyStoreType() {
    return sslKeyStoreType;
  }

  public void setSSLKeyStoreType(final String sslKeyStoreType) {
    this.sslKeyStoreType = sslKeyStoreType;
  }

  public String getSSLKeyStorePassword() {
    return sslKeyStorePassword;
  }

  public void setSSLKeyStorePassword(final String sslKeyStorePassword) {
    this.sslKeyStorePassword = sslKeyStorePassword;
  }

  public String getSSLTrustStore() {
    return sslTrustStore;
  }

  public void setSSLTrustStore(final String sslTrustStore) {
    this.sslTrustStore = sslTrustStore;
  }

  public String getSSLTrustStorePassword() {
    return sslTrustStorePassword;
  }

  public void setSSLTrustStorePassword(final String sslTrustStorePassword) {
    this.sslTrustStorePassword = sslTrustStorePassword;
  }

  public boolean getSSLWebServiceRequireAuthentication() {
    return sslWebServiceRequireAuthentication;
  }

  public void setSSLWebServiceRequireAuthentication(final boolean sslWebServiceRequireAuthentication) {
    this.sslWebServiceRequireAuthentication = sslWebServiceRequireAuthentication;
  }

  public String getSSLDefaultAlias() {
    return sslDefaultAlias;
  }

  public void setSSLDefaultAlias(final String sslDefaultAlias) {
    this.sslDefaultAlias = sslDefaultAlias;
  }
}
