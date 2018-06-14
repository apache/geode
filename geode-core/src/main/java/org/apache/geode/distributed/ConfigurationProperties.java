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

package org.apache.geode.distributed;

import org.apache.geode.redis.GeodeRedisServer;

/**
 * This interface defines all the configuration properties that can be used. <U>Since</U>: Geode 1.0
 */
public interface ConfigurationProperties {

  /**
   * The static string definition of the <i>"ack-severe-alert-threshold"</i> property
   * <a name="ack-severe-alert-threshold"/>
   * <p>
   * <U>Description</U>: The number of seconds the distributed system will wait after the
   * ack-wait-threshold for a message to be acknowledged before it issues an alert at <i>severe</i>
   * level. The default value is zero, which turns off this feature. When ack-severe-alert-threshold
   * is used, GemFire will also initiate additional checks to see if the process is alive. These
   * checks will begin when the ack-wait-threshold is reached and will continue until GemFire has
   * been able to communicate with the process and ascertain its status.
   * <p>
   * <U>Default</U>: "0"
   * <p>
   * <U>Allowed values</U>: 0..2147483647
   */
  String ACK_SEVERE_ALERT_THRESHOLD = "ack-severe-alert-threshold";
  /**
   * The static String definition of the <i>"ack-wait-threshold"</i> property <a
   * name="ack-wait-threshold"/a>
   * </p>
   * <U>Description</U>: The number of seconds the distributed system will wait for a message to be
   * acknowledged before it sends a <i>warning</i> level alert to signal that something might be
   * wrong with the system node that is unresponsive. After sending this alert the waiter continues
   * to wait. The alerts are logged in the log as warnings and will cause an alert notification in
   * the Admin API and GemFire JMX Agent.
   * </p>
   * <U>Default</U>: "15"
   * </p>
   * <U>Allowed values</U>: 1..2147483647
   */
  String ACK_WAIT_THRESHOLD = "ack-wait-threshold";
  /**
   * The static String definition of the <i>"archive-disk-space-limit"</i> property <a
   * name="archive-disk-space-limit"/a>
   * </p>
   * <U>Description</U>: Limits, in megabytes, how much disk space can be consumed by old inactive
   * statistic archive files. When the limit is exceeded the oldest inactive archive is deleted. Set
   * to zero to disable automatic archive deletion.
   * </p>
   * <U>Default</U>: "0"
   * </p>
   * <U>Allowed values</U>: 0..1000000
   */
  String ARCHIVE_DISK_SPACE_LIMIT = "archive-disk-space-limit";
  /**
   * The static String definition of the <i>"archive-file-size-limit"</i> property <a
   * name="archive-file-size-limit"/a>
   * </p>
   * <U>Description</U>: Limits, in megabytes, how large the current statistic archive file can grow
   * before it is closed and archival rolls on to a new file. Set to zero to disable archive
   * rolling.
   * </p>
   * <U>Default</U>: "0"
   * </p>
   * <U>Allowed values</U>: 0..1000000
   */
  String ARCHIVE_FILE_SIZE_LIMIT = "archive-file-size-limit";
  /**
   * The static String definition of the <i>"async-distribution-timeout"</i> property <a
   * name="async-distribution-timeout"/a>
   * </p>
   * <U>Description</U>: The number of milliseconds before a publishing process should attempt to
   * distribute a cache operation before switching over to asynchronous messaging for this process.
   * To enable asynchronous messaging, the value must be set above zero. If a thread that is
   * publishing to the cache exceeds this value when attempting to distribute to this process, it
   * will switch to asynchronous messaging until this process catches up, departs, or some specified
   * limit is reached, such as <a href="#async-queue-timeout"> async-queue-timeout</a> or
   * <a href="#async-max-queue-size"> async-max-queue-size</a>.
   * </p>
   * <U>Default</U>: "0"
   * </p>
   * <U>Allowed values</U>: 0..60000
   */
  String ASYNC_DISTRIBUTION_TIMEOUT = "async-distribution-timeout";
  /**
   * The static String definition of the <i>"async-max-queue-size"</i> property <a
   * name="async-max-queue-size"/a>
   * </p>
   * <U>Description</U>: The maximum size in megabytes that a publishing process should be allowed
   * to asynchronously enqueue for this process before asking this process to depart from the
   * distributed system.
   * </p>
   * <U>Default</U>: "8"
   * </p>
   * <U>Allowed values</U>: 0..1024
   */
  String ASYNC_MAX_QUEUE_SIZE = "async-max-queue-size";
  /**
   * The static String definition of the <i>"async-queue-timeout"</i> property <a
   * name="async-queue-timeout"/a>
   * </p>
   * <U>Description</U>: The number of milliseconds a queuing publisher may enqueue asynchronous
   * messages without any distribution to this process before that publisher requests this process
   * to depart. If a queuing publisher has not been able to send this process any cache operations
   * prior to the timeout, this process will attempt to close its cache and disconnect from the
   * distributed system.
   * </p>
   * <U>Default</U>: "60000"
   * </p>
   * <U>Allowed values</U>: 0..86400000
   */
  String ASYNC_QUEUE_TIMEOUT = "async-queue-timeout";
  /**
   * The static String definition of the <i>"bind-address"</i> property <a name="bind-address"/a>
   * <p>
   * <U>Description</U>: The IP address that this distributed system's server sockets will listen
   * on. If set to an empty string then the local machine's default address will be listened on.
   * <p>
   * <U>Default</U>: ""
   */
  String BIND_ADDRESS = "bind-address";
  /**
   * The static String definition of the <i>"cache-xml-file"</i> property <a
   * name="cache-xml-file"/a>
   * </p>
   * <U>Description</U>: Specifies the name of the XML file or resource to initialize the cache with
   * when it is {@linkplain org.apache.geode.cache.CacheFactory#create created}. Create will first
   * look for a file that matches the value of this property. If a file is not found then it will be
   * searched for using {@link java.lang.ClassLoader#getResource}. If the value of this property is
   * the empty string (<code>""</code>), then the cache will not be declaratively initialized.
   * </p>
   * <U>Default</U>: "cache.xml"
   */
  String CACHE_XML_FILE = "cache-xml-file";
  /**
   * The static String definition of the <i>"cluster-configuration-dir"</i> property <a
   * name="cluster-configuration-dir"/a>
   * </p>
   * <U>Description</U>: This property specifies the directory in which the cluster configuration
   * related disk-store and artifacts are stored This property is only applicable to dedicated
   * locators which have "enable-cluster-configuration" set to true.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.1
   */
  String CLUSTER_CONFIGURATION_DIR = "cluster-configuration-dir";

  /**
   * The static String definition of the cluster ssl prefix <i>"cluster-ssl"</i> used in conjunction
   * with other <i>cluster-ssl-*</i> properties property <a name="cluster-ssl"/a>
   * </p>
   * <U>Description</U>: The cluster-ssl property prefix
   *
   * @deprecated Since Geode1.0, use ssl-* properties and ssl-enabled-components
   */
  @Deprecated
  String CLUSTER_SSL_PREFIX = "cluster-ssl";

  /**
   * The static String definition of the <i>"ssl-cluster-alias"</i> property <a
   * name="ssl-cluster-alias"/a>
   * </p>
   * <U>Description</U>: This property is to be used if a specific key is to be used out of a
   * keystore for the cluster ssl certificate.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_CLUSTER_ALIAS = "ssl-cluster-alias";
  /**
   * The static String definition of the <i>"cluster-ssl-ciphers"</i> property <a
   * name="cluster-ssl-ciphers"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL cipher suites to enable. Those listed
   * must be supported by the available providers.Preferably use cluster-ssl-* properties rather
   * than ssl-* properties.
   * </p>
   * <U>Default</U>: "any"
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_CIPHERS}
   */
  String CLUSTER_SSL_CIPHERS = "cluster-ssl-ciphers";
  /**
   * The static String definition of the <i>"cluster-ssl-enabled"</i> property <a
   * name="cluster-ssl-enabled"/a>
   * </p>
   * <U>Description</U>: If true, all gemfire socket communication is configured to use SSL through
   * JSSE.
   * </p>
   * <U>Default</U>: "false"
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 see {@link #SSL_ENABLED_COMPONENTS}
   */
  String CLUSTER_SSL_ENABLED = "cluster-ssl-enabled";
  /**
   * The static String definition of the <i>"cluster-ssl-keystore"</i> property <a
   * name="cluster-ssl-keystore"/a>
   * </p>
   * <U>Description</U>Location of the Java keystore file containing certificate and private key.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE}
   */
  String CLUSTER_SSL_KEYSTORE = "cluster-ssl-keystore";
  /**
   * The static String definition of the <i>"cluster-ssl-keystore-password"</i> property <a
   * name="cluster-ssl-keystore-password"/a>
   * </p>
   * <U>Description</U>Password to access the private key from the keystore file specified by
   * javax.net.ssl.keyStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_PASSWORD}
   */
  String CLUSTER_SSL_KEYSTORE_PASSWORD = "cluster-ssl-keystore-password";
  /**
   * The static String definition of the <i>"cluster-ssl-keystore-type"</i> property <a
   * name="cluster-ssl-keystore-type"/a>
   * </p>
   * <U>Description</U>For Java keystore file format, this property has the value jks (or JKS).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_TYPE}
   */
  String CLUSTER_SSL_KEYSTORE_TYPE = "cluster-ssl-keystore-type";
  /**
   * The static String definition of the <i>"cluster-ssl-protocols"</i> property <a
   * name="cluster-ssl-protocols"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL protocols to enable. Those listed must be
   * supported by the available providers.
   * </p>
   * <U>Default</U>: "any"
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_PROTOCOLS}
   */
  String CLUSTER_SSL_PROTOCOLS = "cluster-ssl-protocols";
  /**
   * The static String definition of the <i>"cluster-ssl-require-authentication"</i> property <a
   * name="cluster-ssl-require-authentication"/a>
   * </p>
   * <U>Description</U>: If false, allow ciphers that do not require the client side of the
   * connection to be authenticated.
   * </p>
   * <U>Default</U>: "true"
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_REQUIRE_AUTHENTICATION}
   */
  String CLUSTER_SSL_REQUIRE_AUTHENTICATION = "cluster-ssl-require-authentication";
  /**
   * The static String definition of the <i>"cluster-ssl-truststore"</i> property <a
   * name="cluster-ssl-truststore"/a>
   * </p>
   * <U>Description</U>Location of the Java keystore file containing the collection of CA
   * certificates trusted by distributed member (trust store).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE}
   */
  String CLUSTER_SSL_TRUSTSTORE = "cluster-ssl-truststore";
  /**
   * The static String definition of the <i>"cluster-ssl-truststore-password"</i> property <a
   * name="cluster-ssl-truststore-password"/a>
   * </p>
   * <U>Description</U>Password to unlock the keystore file (store password) specified by
   * javax.net.ssl.trustStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE_PASSWORD}
   */
  String CLUSTER_SSL_TRUSTSTORE_PASSWORD = "cluster-ssl-truststore-password";
  /**
   * The static String definition of the <i>"conflate-events"</i> property <a
   * name="conflate-events"/a>
   * </p>
   * <U>Description</U>: This is a client-side property that is passed to the server. Allowable
   * values are "server", "true", and "false". With the "server" setting, this client&apos;s servers
   * use their own client queue conflation settings. With a "true" setting, the servers disregard
   * their own configuration and enable conflation of events for all regions for the client. A
   * "false" setting causes the client&apos;s servers to disable conflation for all regions for the
   * client. <U>Default</U>: "server"
   * </p>
   * <U>Since</U>: GemFire 5.7
   */
  String CONFLATE_EVENTS = "conflate-events";
  /**
   * The static String definition of the <i>"conserve-sockets"</i> property <a
   * name="conserve-sockets"/a>
   * </p>
   * <U>Description</U>: If "true" then a minimal number of sockets will be used when connecting to
   * the distributed system. This conserves resource usage but can cause performance to suffer. If
   * "false" then every application thread that sends distribution messages to other members of the
   * distributed system will own its own sockets and have exclusive access to them. The length of
   * time a thread can have exclusive access to a socket can be configured with "socket-lease-time".
   * <U>Default</U>: "true"
   * </p>
   * <U>Allowed values</U>: true|false
   * </p>
   */
  String CONSERVE_SOCKETS = "conserve-sockets";
  /**
   * The static String definition of the <i>"delta-propagation"</i> property
   * <a name="delta-propagation">
   * <dt>delta-propagation</dt> </a> <U>Description</U>: "true" indicates that server propagates
   * delta generated from {@link org.apache.geode.Delta} type of objects. If "false" then server
   * propagates full object but not delta.
   * </p>
   * <U>Default</U>: "true"
   * </p>
   * <U>Allowed values</U>: true|false
   */
  String DELTA_PROPAGATION = "delta-propagation";
  /**
   * The static String definition of the <i>"deploy-working-dir"</i> property <a
   * name="deploy-working-dir"/a>
   * </p>
   * <U>Description</U>: Specifies the working directory which this distributed member will use to
   * persist deployed JAR files. This directory should be unchanged when restarting the member so
   * that it can read what was previously persisted. The default is the current working directory as
   * determined by <code>System.getProperty("user.dir")</code>.
   * </p>
   * <U>Default</U>: <code>System.getProperty("user.dir")</code>
   * </p>
   * <U>Since</U>: GemFire 7.0
   */
  String DEPLOY_WORKING_DIR = "deploy-working-dir";
  /**
   * The static String definition of the <i>"disable-auto-reconnect"</i> property <a
   * name="disable-auto-reconnect"/a>
   * </p>
   * <U>Description</U>: By default GemFire will attempt to reconnect and reinitialize the cache
   * when it has been forced out of the distributed system by a network-partition event or has
   * otherwise been shunned by other members. This setting will turn off this behavior.
   * </p>
   * <U>Default</U>: "false"
   */
  String DISABLE_AUTO_RECONNECT = "disable-auto-reconnect";
  /**
   * The static String definition of the <i>"disable-tcp"</i> property <a name="disable-tcp"/a>
   * <p>
   * <U>Description</U>: Turns off use of tcp/ip sockets, forcing the cache to use datagram sockets
   * for all communication. This is useful if you have a large number of processes in the
   * distributed cache since it eliminates the per-connection reader-thread that is otherwise
   * required. However, udp communications are somewhat slower than tcp/ip communications due to the
   * extra work required in Java to break messages down to transmittable sizes, and the extra work
   * required to guarantee message delivery.
   * <p>
   * <U>Default</U>: "false"
   * <p>
   * <U>Allowed values</U>: true or false
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String DISABLE_TCP = "disable-tcp";
  /**
   * The static String definition of the <i>"distributed-system-id"</i> property
   * <p>
   * <a name="distributed-system-id"/a> <U>Decription:</U>A number that uniquely identifies this
   * distributed system, when using the WAN gateway to share data between multiple distributed
   * systems. This setting is only required when using the WAN gateway in conjunction with the
   * <b>P</b>ortable <b>D</b>ata e<b>X</b>change (PDX) serialization format.
   * <p>
   * If set, this setting must be the same for every member in this distributed system. It must be
   * different than the number in other distributed systems that this one will connect to using the
   * WAN. -1 means no setting.
   * <p>
   * <U>Range:</U>-1..255
   * <p>
   * <U>Default:</U>: "-1"
   */
  String DISTRIBUTED_SYSTEM_ID = "distributed-system-id";
  /**
   * The static String definition of the <i>"durable-client-id"</i> property <a
   * name="durable-client-id"/a>
   * </p>
   * <U>Description</U>: The id to be used by this durable client. When a durable client connects to
   * a server, this id is used by the server to identify it. The server will accumulate updates for
   * a durable client while it is disconnected and deliver these events to the client when it
   * reconnects. <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 5.5
   */
  String DURABLE_CLIENT_ID = "durable-client-id";
  /**
   * The static String definition of the <i>"durable-client-timeout"</i> property <a
   * name="durable-client-timeout"/a>
   * </p>
   * <U>Description</U>: The number of seconds a disconnected durable client is kept alive and
   * updates are accumulated for it by the server before it is terminated. <U>Default</U>: "300"
   * </p>
   * <U>Since</U>: GemFire 5.5
   */
  String DURABLE_CLIENT_TIMEOUT = "durable-client-timeout";
  /**
   * The static String definition of the <i>"enable-cluster-configuration"</i> property <a
   * name="enable-cluster-configuration"/a>
   * </p>
   * <U>Description</U>: "true" causes creation of cluster configuration service on dedicated
   * locators. The cluster configuration service on dedicated locator(s) would serve the
   * configuration to new members joining the distributed system and also save the configuration
   * changes caused by the Gfsh commands. This property is only applicable to dedicated locators.
   * </p>
   * <U>Default</U>: "true"
   * </p>
   * <U>Allowed values</U>: true or false
   * </p>
   * <U>Since</U>: GemFire 8.0
   */
  String ENABLE_CLUSTER_CONFIGURATION = "enable-cluster-configuration";
  /**
   * The static String definition of the <i>"enable-network-partition-detection"</i> property <a
   * name="enable-network-partition-detection"/a>
   * </p>
   * <U>Description</U>: Turns on network partitioning detection algorithms, which detect loss of
   * quorum and shuts down losing partitions.
   * </p>
   * <U>Default</U>: "true"
   */
  String ENABLE_NETWORK_PARTITION_DETECTION = "enable-network-partition-detection";
  /**
   * The static String definition of the <i>"enable-time-statistics"</i> property <a
   * name="enable-time-statistics"/a>
   * </p>
   * <U>Description</U>: "true" causes additional time-based statistics to be gathered for gemfire
   * operations. This can aid in discovering where time is going in cache operations, albeit at the
   * expense of extra clock probes on every operation. "false" disables the additional time-based
   * statistics.
   * </p>
   * <U>Default</U>: "false"
   * </p>
   * <U>Allowed values</U>: true or false
   * </p>
   * <U>Since</U>: 5.0
   */
  String ENABLE_TIME_STATISTICS = "enable-time-statistics";
  /**
   * The static String definition of the <i>"enforce-unique-host"</i> property <a
   * name="enforce-unique-host"/a>
   * </p>
   * <U>Description</U>: Whether or not partitioned regions will put redundant copies of the same
   * data in different JVMs running on the same physical host.
   * <p>
   * By default, partitioned regions will try to put redundancy copies on different physical hosts,
   * but it may put them on the same physical host if no other hosts are available. Setting this
   * property to true will prevent partitions regions from ever putting redundant copies of data on
   * the same physical host.
   * </p>
   * <U>Default</U>: "false"
   */
  String ENFORCE_UNIQUE_HOST = "enforce-unique-host";
  /**
   * The static String definition of the <i>"ssl-gateway-alias"</i> property <a
   * name="ssl-gateway-alias"/a>
   * </p>
   * <U>Description</U>: This property is to be used if a specific key is to be used for the SSL
   * communications for the Gateways.
   * </p>
   * <U><i>Optional</i></U> <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_GATEWAY_ALIAS = "ssl-gateway-alias";
  /**
   * The static String definition of the <i>"gateway-ssl-ciphers"</i> property <a
   * name="gateway-ssl-ciphers"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL cipher suites to enable. Those listed
   * must be supported by the available providers.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_CIPHERS}
   */
  @Deprecated
  String GATEWAY_SSL_CIPHERS = "gateway-ssl-ciphers";
  /**
   * The static String definition of the <i>"gateway-ssl-enabled"</i> property <a
   * name="gateway-ssl-enabled"/a>
   * </p>
   * <U>Description</U>: Specifies if gateway is started with separate ssl configuration. If not
   * specified global property ssl-enabled (and its other related properties) are used to create
   * gateway socket
   * </p>
   * <U>Default</U>: <code>false</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_ENABLED_COMPONENTS} with the optional
   *             {@link #SSL_GATEWAY_ALIAS}
   */
  @Deprecated
  String GATEWAY_SSL_ENABLED = "gateway-ssl-enabled";
  /**
   * The static String definition of the <i>"gateway-ssl-keystore"</i> property <a
   * name="gateway-ssl-keystore"/a>
   * </p>
   * <U>Description</U>Location of the Java keystore file containing certificate and private key.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE}
   */
  @Deprecated
  String GATEWAY_SSL_KEYSTORE = "gateway-ssl-keystore";
  /**
   * The static String definition of the <i>"gateway-ssl-keystore-password"</i> property <a
   * name="gateway-ssl-keystore-password"/a>
   * </p>
   * <U>Description</U>Password to access the private key from the keystore file specified by
   * javax.net.ssl.keyStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String GATEWAY_SSL_KEYSTORE_PASSWORD = "gateway-ssl-keystore-password";
  /**
   * The static String definition of the <i>"gateway-ssl-keystore-type"</i> property <a
   * name="gateway-ssl-keystore-type"/a>
   * </p>
   * <U>Description</U>For Java keystore file format, this property has the value jks (or JKS).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String GATEWAY_SSL_KEYSTORE_TYPE = "gateway-ssl-keystore-type";
  /**
   * The static String definition of the <i>"gateway-ssl-protocols"</i> property <a
   * name="gateway-ssl-protocols"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL protocols to enable. Those listed must be
   * supported by the available providers.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_PROTOCOLS}
   */
  @Deprecated
  String GATEWAY_SSL_PROTOCOLS = "gateway-ssl-protocols";
  /**
   * The static String definition of the <i>"gateway-ssl-require-authentication"</i> property <a
   * name="gateway-ssl-require-authentication"/a>
   * </p>
   * <U>Description</U>: If false, allow ciphers that do not require the Gateway Sender side of the
   * connection to be authenticated.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  String GATEWAY_SSL_REQUIRE_AUTHENTICATION = "gateway-ssl-require-authentication";
  /**
   * The static String definition of the <i>"gateway-ssl-truststore"</i> property <a
   * name="gateway-ssl-truststore"/a>
   * </p>
   * <U>Description</U>Location of the Java keystore file containing the collection of CA
   * certificates trusted by server (trust store).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE}
   */
  @Deprecated
  String GATEWAY_SSL_TRUSTSTORE = "gateway-ssl-truststore";
  /**
   * The static String definition of the <i>"gateway-ssl-truststore-password"</i> property <a
   * name="gateway-ssl-truststore-password"/a>
   * </p>
   * <U>Description</U>: Password to unlock the keystore file (store password) specified by
   * javax.net.ssl.trustStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String GATEWAY_SSL_TRUSTSTORE_PASSWORD = "gateway-ssl-truststore-password";
  /**
   * The static String definition of the <i>"groups"</i> property. <a name="groups"/a>
   * <p>
   * <U>Description</U>: Defines the list of groups this member belongs to. Use commas to separate
   * group names. Note that anything defined by the deprecated roles gemfire property will also be
   * considered a group.
   * <p>
   * <U>Default</U>: ""
   * <p>
   * <U>Since</U>: GemFire 7.0
   */
  String GROUPS = "groups";
  /**
   * The static String definition of the <i>"http-service-bind-address"</i> property <a
   * name="http-service-bind-address"/a>
   * </p>
   * <U>Description</U>: The address where the GemFire HTTP service will listen for remote
   * connections. One can use this property to configure what ip address or host name the HTTP
   * service will listen on. When not set, by default the HTTP service will listen on the local
   * host's address.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   */
  String HTTP_SERVICE_BIND_ADDRESS = "http-service-bind-address";
  /**
   * The static String definition of the <i>"http-service-port"</i> property <a
   * name="http-service-port"/a>
   * </p>
   * <U>Description</U>: Specifies the port used by the GemFire HTTP service. If configured with
   * non-zero value, then an HTTP service will listen on this port. A value of "0" disables Gemfire
   * HTTP service.
   * </p>
   * <U>Default</U>: "7070"
   * </p>
   * <U>Allowed values</U>: 0..65535
   * </p>
   * <U>Since</U>: GemFire 8.0
   */
  String HTTP_SERVICE_PORT = "http-service-port";
  /**
   * The static String definition of the <i>"ssl-web-alias"</i> property <a name="ssl-web-alias"/a>
   * </p>
   * <U>Description</U>: This property is to be used if a specific key is to be used for the SSL
   * communications for the HTTP service.
   * </p>
   * <U><i>Optional</i></U> <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_WEB_ALIAS = "ssl-web-alias";


  @Deprecated
  String HTTP_SERVICE_SSL_PREFIX = "http-service-ssl-";

  /**
   * The static String definition of the <i>"http-service-ssl-ciphers"</i> property <a
   * name="http-service-ssl-ciphers"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL cipher suites to enable. Those listed
   * must be supported by the available providers.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_CIPHERS}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_CIPHERS = "http-service-ssl-ciphers";
  /**
   * The static String definition of the <i>"http-service-ssl-enabled"</i> property a
   * name="http-service-ssl-enabled"/a>
   * </p>
   * <U>Description</U>: Specifies if http service is started with separate ssl configuration. If
   * not specified, global property cluster-ssl-enabled (and its other related properties) are used
   * to secure http service. All http-service-ssl-* properties are inherited from cluster-ssl-*
   * properties. User can ovverride them using specific http-service-ssl-* property.
   * </p>
   * <U>Default</U>: <code>false</code>
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_ENABLED_COMPONENTS} with optional
   *             {@link #SSL_WEB_ALIAS}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_ENABLED = "http-service-ssl-enabled";
  /**
   * The static String definition of the <i>"http-service-ssl-keystore"</i> property <a
   * name="http-service-ssl-keystore"/a>
   * </p>
   * <U>Description</U>: Location of the Java keystore file containing certificate and private key.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_KEYSTORE = "http-service-ssl-keystore";
  /**
   * The static String definition of the <i>"http-service-ssl-keystore-password"</i> property <a
   * name="http-service-ssl-keystore-password"/a>
   * </p>
   * <U>Description</U>: Password to access the private key from the keystore file specified by
   * javax.net.ssl.keyStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_KEYSTORE_PASSWORD = "http-service-ssl-keystore-password";
  /**
   * The static String definition of the <i>"http-service-ssl-keystore-type"</i> property <a
   * name="http-service-ssl-keystore-type"/a>
   * </p>
   * <U>Description</U>: For Java keystore file format, this property has the value jks (or JKS).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_KEYSTORE_TYPE = "http-service-ssl-keystore-type";
  /**
   * The static String definition of the <i>"http-service-ssl-protocols"</i> property <a
   * name="http-service-ssl-protocols"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL protocols to enable. Those listed must be
   * supported by the available providers.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_PROTOCOLS}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_PROTOCOLS = "http-service-ssl-protocols";
  /**
   * The static String definition of the <i>"http-service-ssl-require-authentication"</i> property
   * <a name="http-service-ssl-require-authentication"/a>
   * </p>
   * <U>Description</U>: If false, allow ciphers that do not require the client side of the
   * connection to be authenticated.
   * </p>
   * <U>Default</U>: <code>false</code>
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION = "http-service-ssl-require-authentication";
  /**
   * The static String definition of the <i>"http-service-ssl-truststore"</i> property <a
   * name="http-service-ssl-truststore"/a>
   * </p>
   * <U>Description</U>: Location of the Java keystore file containing the collection of CA
   * certificates trusted by server (trust store).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_TRUSTSTORE = "http-service-ssl-truststore";
  /**
   * The static String definition of the <i>"http-service-ssl-truststore-password"</i> property <a
   * name="http-service-ssl-truststore-password"/a>
   * </p>
   * <U>Description</U>: Password to unlock the keystore file (store password) specified by
   * javax.net.ssl.trustStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.1
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD = "http-service-ssl-truststore-password";
  /**
   * The static String definition of the jmx-manager-ssl prefix "jmx-manager-ssl-" used in
   * conjunction with other jmx-manager-ssl-* properties</i> property <a name="jmx-manager-ssl-"/a>
   * </p>
   * <U>Description</U>: The jmx-manager-ssl prefix for.
   * </p>
   * <U>Default</U>: Optional
   * </p>
   *
   * @deprecated Since Geode1.0, use ssl-* properties and ssl-enabled-components
   */
  @Deprecated
  String JMX_MANAGER_SSL_PREFIX = "jmx-manager-ssl-";

  /**
   * The static String definition of the <i>"jmx-manager"</i> property <a name="jmx-manager"/a>
   * </p>
   * <U>Description</U>: If true then this member is willing to be a jmx-manager. All the other
   * jmx-manager properties will be used when it does become a manager. If this property is false
   * then all other jmx-manager properties are ignored.
   * </p>
   * <U>Default</U>: "false except on locators"
   */
  String JMX_MANAGER = "jmx-manager";
  /**
   * The static String definition of the <i>"jmx-manager-access-file"</i> property <a
   * name="jmx-manager-access-file"/a>
   * </p>
   * <U>Description</U>: By default the jmx-manager will allow full access to all mbeans by any
   * client. If this property is set to the name of a file then it can restrict clients to only
   * being able to read mbeans; they will not be able to modify mbeans. The access level can be
   * configured differently in this file for each user name defined in the password file. For more
   * information about the format of this file see Oracle's documentation of the
   * com.sun.management.jmxremote.access.file system property. Ignored if jmx-manager is false or if
   * jmx-manager-port is zero.
   * </p>
   * <U>Default</U>: ""
   */
  String JMX_MANAGER_ACCESS_FILE = "jmx-manager-access-file";
  /**
   * The static String definition of the <i>"jmx-manager-bind-address"</i> property <a
   * name="jmx-manager-bind-address"/a>
   * </p>
   * <U>Description</U>: By default the jmx-manager when configured with a port will listen on all
   * the local host's addresses. You can use this property to configure what ip address or host name
   * the jmx-manager will listen on. In addition, if the embedded http server is started, it will
   * also bind to this address if it is set. Ignored if jmx-manager is false or jmx-manager-port is
   * zero.
   * </p>
   * <U>Default</U>: ""
   */
  String JMX_MANAGER_BIND_ADDRESS = "jmx-manager-bind-address";
  /**
   * The static String definition of the <i>"jmx-manager-hostname-for-clients"</i> property <a
   * name="jmx-manager-hostname-for-clients"/a>
   * </p>
   * <U>Description</U>: Lets you control what hostname will be given to clients that ask the
   * locator for the location of a jmx manager. By default the ip address that the jmx-manager
   * reports is used. But for clients on a different network this property allows you to configure a
   * different hostname that will be given to clients. Ignored if jmx-manager is false or
   * jmx-manager-port is zero.
   * </p>
   * <U>Default</U>: ""
   */
  String JMX_MANAGER_HOSTNAME_FOR_CLIENTS = "jmx-manager-hostname-for-clients";
  /**
   * The static String definition of the <i>"jmx-manager-http-port"</i> property <a
   * name="jmx-manager-http-port"/a>
   * </p>
   * <U>Description</U>: If non-zero then when the jmx manager is started, an embedded web server
   * will also be started and will listen on this port. The web server is used to host the GemFire
   * Pulse application. If you are hosting the Pulse web app in your own web server, then disable
   * this embedded server by setting this property to zero. Ignored if jmx-manager is false.
   * </p>
   * <U>Default</U>: "7070"
   * </p>
   * <U>Deprecated</U>: as of GemFire8.0 use
   * <a href="#http-service-port"><code>http-service-port</code></a> instead.
   *
   * @deprecated as of GemFire 8.0 use {@link #HTTP_SERVICE_PORT} instead.
   */
  String JMX_MANAGER_HTTP_PORT = "jmx-manager-http-port";
  /**
   * The static String definition of the <i>"jmx-manager-password-file"</i> property <a
   * name="jmx-manager-password-file"/a>
   * </p>
   * <U>Description</U>: By default the jmx-manager will allow clients without credentials to
   * connect. If this property is set to the name of a file then only clients that connect with
   * credentials that match an entry in this file will be allowed. Most JVMs require that the file
   * is only readable by the owner. For more information about the format of this file see Oracle's
   * documentation of the com.sun.management.jmxremote.password.file system property. Ignored if
   * jmx-manager is false or if jmx-manager-port is zero.
   * </p>
   * <U>Default</U>: ""
   */
  String JMX_MANAGER_PASSWORD_FILE = "jmx-manager-password-file";
  /**
   * The static String definition of the <i>"jmx-manager-port"</i> property <a
   * name="jmx-manager-port"/a>
   * </p>
   * <U>Description</U>: The port this jmx manager will listen to for client connections. If this
   * property is set to zero then GemFire will not allow remote client connections but you can
   * alternatively use the standard system properties supported by the JVM for configuring access
   * from remote JMX clients. Ignored if jmx-manager is false.
   * </p>
   * <U>Default</U>: "1099"
   */
  String JMX_MANAGER_PORT = "jmx-manager-port";
  /**
   * The static String definition of the <i>"jmx-manager-start"</i> property <a
   * name="jmx-manager-start"/a>
   * </p>
   * <U>Description</U>: If true then this member will start a jmx manager when it creates a cache.
   * Management tools like gfsh can be configured to connect to the jmx-manager. In most cases you
   * should not set this because a jmx manager will automatically be started when needed on a member
   * that sets "jmx-manager" to true. Ignored if jmx-manager is false.
   * </p>
   * <U>Default</U>: "false"
   */
  String JMX_MANAGER_START = "jmx-manager-start";
  /**
   * The static String definition of the <i>"jmx-manager-update-rate"</i> property <a
   * name="jmx-manager-update-rate"/a>
   * </p>
   * <U>Description</U>: The rate, in milliseconds, at which this member will push updates to any
   * jmx managers. Currently this value should be greater than or equal to the
   * statistic-sample-rate. Setting this value too high will cause stale values to be seen by gfsh
   * and pulse.
   * </p>
   * <U>Default</U>: "2000"
   */
  String JMX_MANAGER_UPDATE_RATE = "jmx-manager-update-rate";
  /**
   * The static String definition of the <i>"ssl-jmx-alias"</i> property <a name="ssl-jmx-alias"/a>
   * </p>
   * <U>Description</U>: This property is to be used if a specific key is to be used for the SSL
   * communications for the jmx manager.
   * </p>
   * <U><i>Optional</i></U> <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_JMX_ALIAS = "ssl-jmx-alias";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-ciphers"</i> property <a
   * name="jmx-manager-ssl-ciphers"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL cipher suites to enable. Those listed
   * must be supported by the available providers.
   * </p>
   * <U>Default</U>: "any"
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_CIPHERS}
   */
  @Deprecated
  String JMX_MANAGER_SSL_CIPHERS = "jmx-manager-ssl-ciphers";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-enabled"</i> property <a
   * name="jmx-manager-ssl-enabled"/a>
   * </p>
   * <U>Description</U>: If true and jmx-manager-port is not zero then the jmx-manager will only
   * accept ssl connections. Note that the ssl-enabled property does not apply to the jmx-manager
   * but the other ssl properties do. This allows ssl to be configured for just the jmx-manager
   * without needing to configure it for the other GemFire connections. Ignored if jmx-manager is
   * false.
   * </p>
   * <U>Default</U>: "false"
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_ENABLED_COMPONENTS} with optional
   *             {@link #SSL_JMX_ALIAS}
   */
  @Deprecated
  String JMX_MANAGER_SSL_ENABLED = "jmx-manager-ssl-enabled";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-keystore"</i> property <a
   * name="jmx-manager-ssl-keystore"/a>
   * </p>
   * <U>Description</U>: Location of the Java keystore file containing certificate and private key.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE}
   */
  @Deprecated
  String JMX_MANAGER_SSL_KEYSTORE = "jmx-manager-ssl-keystore";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-keystore-password"</i> property <a
   * name="jmx-manager-ssl-keystore-password"/a>
   * </p>
   * <U>Description</U>: Password to access the private key from the keystore file specified by
   * javax.net.ssl.keyStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String JMX_MANAGER_SSL_KEYSTORE_PASSWORD = "jmx-manager-ssl-keystore-password";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-keystore-type"</i> property <a
   * name="jmx-manager-ssl-keystore-type"/a>
   * </p>
   * <U>Description</U>: For Java keystore file format, this property has the value jks (or JKS).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String JMX_MANAGER_SSL_KEYSTORE_TYPE = "jmx-manager-ssl-keystore-type";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-protocols"</i> property <a
   * name="jmx-manager-ssl-protocols"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL protocols to enable. Those listed must be
   * supported by the available providers.
   * </p>
   * <U>Default</U>: "any"
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_PROTOCOLS}
   */
  @Deprecated
  String JMX_MANAGER_SSL_PROTOCOLS = "jmx-manager-ssl-protocols";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-require-authentication"</i> property <a
   * name="jmx-manager-ssl-require-authentication"/a>
   * </p>
   * <U>Description</U>: If false, allow ciphers that do not require the client side of the
   * connection to be authenticated.
   * </p>
   * <U>Default</U>: "true"
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  String JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION = "jmx-manager-ssl-require-authentication";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-truststore"</i> property <a
   * name="jmx-manager-ssl-truststore"/a>
   * </p>
   * <U>Description</U>: Location of the Java keystore file containing the collection of CA
   * certificates trusted by manager (trust store).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE}
   */
  @Deprecated
  String JMX_MANAGER_SSL_TRUSTSTORE = "jmx-manager-ssl-truststore";
  /**
   * The static String definition of the <i>"jmx-manager-ssl-truststore-password"</i> property <a
   * name="jmx-manager-ssl-truststore-password"/a>
   * </p>
   * <U>Description</U>: Password to unlock the keystore file (store password) specified by
   * javax.net.ssl.trustStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD = "jmx-manager-ssl-truststore-password";
  /**
   * The static String definition of the <i>"load-cluster-configuration-from-dir"</i> property <a
   * name="load-cluster-configuration-from-dir"/a>
   * </p>
   * <U>Description</U>: "true" causes loading of cluster configuration from "cluster_config"
   * directory in the locator. This property is only applicable to dedicated locators which have
   * "enable-cluster-configuration" set to true.
   * </p>
   * <U>Default</U>: "false"
   * </p>
   * <U>Allowed values</U>: true or false
   * </p>
   * <U>Since</U>: GemFire 8.0
   */
  String LOAD_CLUSTER_CONFIGURATION_FROM_DIR = "load-cluster-configuration-from-dir";
  /**
   * The static String definition of the <i>"ssl-locator-alias"</i> property <a
   * name="ssl-locator-alias"/a>
   * </p>
   * <U>Description</U>: This property is to be used if a specific key is to be used for the SSL
   * communications for locators and for communicating with locators.
   * </p>
   * <U><i>Optional</i></U> <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_LOCATOR_ALIAS = "ssl-locator-alias";
  /**
   * The static String definition of the <i>"locator-wait-time"</i> property <a
   * name="locator-wait-time"/a>
   * <p>
   * <U>Description</U>: The number of seconds to wait for a locator to start if one is not
   * available when attempting to join the distributed system. This setting can be used when
   * locators and peers are being started all at once in order to have the peers be patient and wait
   * for the locators to finish starting up before attempting to join the distributed system..
   * <p>
   * <p>
   * <U>Default</U>: "0"
   */
  String LOCATOR_WAIT_TIME = "locator-wait-time";
  /**
   * The static String definition of the <i>"locators"</i> property <a name="locators"/a>
   * <p>
   * <U>Description</U>: A list of locators (host and port) that are used to find other member of
   * the distributed system. This attribute's value is a possibly empty comma separated list. Each
   * element must be of the form "hostName[portNum]" and may be of the form "host:bindAddress[port]"
   * if a specific bind address is to be used on the locator machine. The square brackets around the
   * portNum are literal character and must be specified.
   * <p>
   * Since IPv6 bind addresses may contain colons, you may use an at symbol instead of a colon to
   * separate the host name and bind address. For example, "server1@fdf0:76cf:a0ed:9449::5[12233]"
   * specifies a locator running on "server1" and bound to fdf0:76cf:a0ed:9449::5 on port 12233.
   * <p>
   * If "locators" is empty then this distributed system will be isolated from all other GemFire
   * processes.
   * <p>
   * <p>
   * <U>Default</U>: ""
   */
  String LOCATORS = "locators";
  /**
   * The static String definition of the <i>"log-disk-space-limit"</i> property <a
   * name="log-disk-space-limit"/a>
   * </p>
   * <U>Description</U>: Limits, in megabytes, how much disk space can be consumed by old inactive
   * log files. When the limit is exceeded the oldest inactive log file is deleted. Set to zero to
   * disable automatic log file deletion.
   * </p>
   * <U>Default</U>: "0"
   * </p>
   * <U>Allowed values</U>: 0..1000000
   */
  String LOG_DISK_SPACE_LIMIT = "log-disk-space-limit";
  /**
   * The static String definition of the <i>"log-file"</i> property <a name="log-file"/a>
   * </p>
   * <U>Description</U>: Name of the file to write logging messages to. If the file name if ""
   * (default) then messages are written to standard out.
   * </p>
   * <U>Default</U>: ""
   */
  String LOG_FILE = "log-file";
  /**
   * The static String definition of the <i>"log-file-size-limit"</i> property <a
   * name="log-file-size-limit"/a>
   * </p>
   * <U>Description</U>: Limits, in megabytes, how large the current log file can grow before it is
   * closed and logging rolls on to a new file. Set to zero to disable log rolling.
   * </p>
   * <U>Default</U>: "0"
   * </p>
   * <U>Allowed values</U>: 0..1000000
   */
  String LOG_FILE_SIZE_LIMIT = "log-file-size-limit";
  /**
   * The static String definition of the <i>"log-level"</i> property <a name="log-level"/a>
   * </p>
   * <U>Description</U>:The type of log messages that will actually write to the log file.
   * </p>
   * <U>Default</U>: "config"
   * </p>
   * <U>Allowed values</U>: all|finest|finer|fine|config|info|warning|severe|none
   */
  String LOG_LEVEL = "log-level";
  /**
   * The static String definition of the <i>"max-num-reconnect-tries"</i> property <a
   * name="max-num-reconnect-tries"/a>
   * </p>
   * <U>Description</U>: Specifies the maximum number or times to attempt to reconnect to the
   * distributed system when required roles are missing. This does not apply to reconnect attempts
   * due to a forced disconnect.
   * </p>
   * <U>Deprecated</U>: this setting is scheduled to be removed.
   * </p>
   * <U>Default</U>: "3"
   * </p>
   * <U>Since</U>: GemFire 5.0
   */
  String MAX_NUM_RECONNECT_TRIES = "max-num-reconnect-tries";
  /**
   * The static String definition of the <i>"max-wait-time-reconnect"</i> property <a
   * name="max-wait-time-reconnect"/a>
   * </p>
   * <U>Description</U>: Specifies the time in milliseconds to wait before each reconnect attempt
   * when a member of the distributed system is forced out of the system and auto-reconnect is
   * enabled (see <a href="#disable-auto-reconnect"><code>disable-auto-reconnect</code></a>) or if
   * the deprecated required-roles feature is being used and a role-loss has triggered a shutdown
   * and reconnect.
   * </p>
   * <U>Default</U>: "60000"
   * </p>
   * <U>Since</U>: GemFire 5.0
   */
  String MAX_WAIT_TIME_RECONNECT = "max-wait-time-reconnect";
  /**
   * The static String definition of the <i>"mcast-address"</i> property <a name="mcast-address"/a>
   * <p>
   * <U>Description</U>: The IP address used for multicast networking. If mcast-port is zero, then
   * mcast-address is ignored.
   * <p>
   * <U>Default</U>: "239.192.81.1"
   */
  String MCAST_ADDRESS = "mcast-address";
  /**
   * The static String definition of the <i>"mcast-flow-control"</i> property <a
   * name="mcast-flow-control"/a>
   * <p>
   * <U>Description</U>: Configures the flow-of-control protocol for multicast messaging. There are
   * three settings that are separated by commas: byteAllowance (integer), rechargeThreshold (float)
   * and rechargeBlockMs (integer). The byteAllowance determines how many bytes can be sent without
   * a recharge from other processes. The rechargeThreshold tells receivers how low the sender's
   * initial to remaining allowance ratio should be before sending a recharge. The rechargeBlockMs
   * tells the sender how long to wait for a recharge before explicitly requesting one.
   * <p>
   * <U>Deprecated</U>: as of 9.0 GemFire does not include a flow-of-control protocol for multicast
   * messaging.
   * <p>
   * <U>Default</U>: "1048576,0.25,5000"
   * <p>
   * <U>Allowed values</U>: 100000-maxInt, 0.1-0.5, 500-60000
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String MCAST_FLOW_CONTROL = "mcast-flow-control";
  /**
   * The static String definition of the <i>"mcast-port"</i> property <a name="mcast-port"/a>
   * <p>
   * <U>Description</U>: The port used for multicast networking. If zero, then multicast will be
   * disabled and unicast messaging will be used.
   * <p>
   * <U>Default</U>: "0"
   */
  String MCAST_PORT = "mcast-port";
  /**
   * The static String definition of the <i>"mcast-recv-buffer-size"</i> property <a
   * name="mcast-recv-buffer-size"/a>
   * <p>
   * <U>Description</U>: Sets the size of the socket buffer used for incoming multicast
   * transmissions. You should set this high if there will be high volumes of messages.
   * <U>Default</U>: "1048576"
   * <p>
   * <U>Allowed values</U>: 2048..Operating System maximum
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String MCAST_RECV_BUFFER_SIZE = "mcast-recv-buffer-size";
  /**
   * The static String definition of the <i>"mcast-send-buffer-size"</i> property <a
   * name="mcast-send-buffer-size"/a>
   * <p>
   * <U>Description</U>: Sets the size of the socket buffer used for outgoing multicast
   * transmissions. <U>Default</U>: "65535"
   * <p>
   * <U>Allowed values</U>: 2048..Operating System maximum
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String MCAST_SEND_BUFFER_SIZE = "mcast-send-buffer-size";
  /**
   * The static String definition of the <i>"mcast-ttl"</i> property <a name="mcast-ttl"/a>
   * <p>
   * <U>Description</U>: Determines how far through your network the multicast packets used by
   * GemFire will propagate. <U>Default</U>: "32"
   * <p>
   * <U>Allowed values</U>: 0..255
   * <p>
   * <U>Since</U>: GemFire 4.1
   */
  String MCAST_TTL = "mcast-ttl";
  /**
   * The static String definition of the <i>"member-timeout"</i> property <a
   * name="member-timeout"/a>
   * <p>
   * <U>Description</U>: Sets the timeout interval, in milliseconds, used to determine whether
   * another process is alive or not. When another process appears to be gone, GemFire sends it an
   * ARE-YOU-DEAD message and waits for the member-timeout period for it to respond and declare it
   * is not dead.
   * <p>
   * <U>Default</U>: "5000"
   * <p>
   * <U>Allowed values</U>: 1000-600000
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String MEMBER_TIMEOUT = "member-timeout";
  /**
   * The static String definition of the <i>"membership-port-range"</i> property <a
   * name="membership-port-range"/a>
   * <p>
   * <U>Description</U>: The allowed range of ports for use in forming an unique membership
   * identifier (UDP), for failure detection purposes (TCP) and to listen on for peer connections
   * (TCP). This range is given as two numbers separated by a minus sign. Minimum 3 values in range
   * are required to successfully startup.
   * <p>
   * <U>Default</U>: 1024-65535
   */
  String MEMBERSHIP_PORT_RANGE = "membership-port-range";
  /**
   * The static String definition of the <i>"memcached-bind-address"</i> property <a
   * name="memcached-bind-address"/a>
   * </p>
   * <U>Description</U>: Specifies the bind address used by
   * {@link org.apache.geode.memcached.GemFireMemcachedServer}
   * </p>
   * <U>Default</U>: ""
   */
  String MEMCACHED_BIND_ADDRESS = "memcached-bind-address";
  /**
   * The static String definition of the <i>"memcached-port"</i> property <a
   * name="memcached-port"/a>
   * </p>
   * <U>Description</U>: Specifies the port used by
   * {@link org.apache.geode.memcached.GemFireMemcachedServer} which enables memcached clients to
   * connect and store data in GemFire distributed system. see
   * {@link org.apache.geode.memcached.GemFireMemcachedServer} for other configuration options.
   * </p>
   * <U>Default</U>: "0" disables GemFireMemcachedServer
   * </p>
   * <U>Allowed values</U>: 0..65535
   */
  String MEMCACHED_PORT = "memcached-port";
  /**
   * The static String definition of the <i>"memcached-protocol"</i> property <a
   * name="memcached-protocol"/a>
   * </p>
   * <U>Description</U>: Specifies the protocol used by
   * {@link org.apache.geode.memcached.GemFireMemcachedServer}
   * </p>
   * <U>Default</U>: "ASCII"
   * </p>
   * <U>Allowed values</U>: "ASCII" "BINARY"
   */
  String MEMCACHED_PROTOCOL = "memcached-protocol";
  /**
   * The static string definition of the <i>"name"</i> property <a name="name">
   * <p>
   * <U>Description</U>: Uniquely identifies a member in its distributed system. If two members with
   * the same name try to join the same distributed system then the second join will fail.
   * <p>
   * <U>Default</U>: ""
   */
  String NAME = "name";
  /**
   * The static String definition of the <i>"redundancy-zone"</i> property <a
   * name="redundancy-zone"/a>
   * </p>
   * <u>Description</u>: Defines the redundancy zone from this member. If this property is set,
   * partitioned regions will not put two redundant copies of data in two members with the same
   * redundancy zone setting.
   * </p>
   * <u>Default</u>: ""
   */
  String REDUNDANCY_ZONE = "redundancy-zone";
  /**
   * The static String definition of the <i>"remote-locators"</i> property <a
   * name="remote-locators"/a>
   * <p>
   * <U>Description</U>: A list of locators (host and port) that a cluster will use in order to
   * connect to a remote site in a multi-site (WAN) configuration. This attribute's value is a
   * possibly comma separated list.
   * <p>
   * For each remote locator, provide a hostname and/or address (separated by '@', if you use both),
   * followed by a port number in brackets.
   * <p>
   * Examples: remote-locators=address1[port1],address2[port2]
   * <p>
   * remote-locators=hostName1@address1[port1],hostName2@address2[port2]
   * <p>
   * remote-locators=hostName1[port1],hostName2[port2]
   * <p>
   * <p>
   * <U>Default</U>: ""
   */
  String REMOTE_LOCATORS = "remote-locators";
  /**
   * The static String definition of the <i>"remove-unresponsive-client"</i> property
   */
  String REMOVE_UNRESPONSIVE_CLIENT = "remove-unresponsive-client";
  /**
   * The static String definition of the <i>"roles"</i> property <a name="roles"/a>
   * </p>
   * <U>Description</U>: Specifies the application roles that this member performs in the
   * distributed system. This is a comma delimited list of user-defined strings. Any number of
   * members can be configured to perform the same role, and a member can be configured to perform
   * any number of roles. Note that anything defined by the groups gemfire property will also be
   * considered a role.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 5.0
   * </p>
   * <U>Deprecated</U>: This feature is scheduled to be removed.
   *
   * @deprecated : This feature is scheduled to be removed.
   */
  String ROLES = "roles";
  /**
   * The static String definition of the security prefix "security-" used in conjunction with other
   * security-* properties</i> property <a name="security-"/a>
   * </p>
   * <U>Description</U>: Mechanism to define client credentials. All tags with "security-" prefix is
   * packaged together as security properties and passed as an argument to getCredentials of
   * Authentication module. These tags cannot have null values.
   * </p>
   * <U>Default</U>: Optional
   * </p>
   * <U>Allowed values</U>: any string
   */
  String SECURITY_PREFIX = "security-";
  /**
   * The static String definition of the <i>"security-client-accessor"</i> property
   *
   * @deprecated since Geode 1.0, use security-manager
   */
  String SECURITY_CLIENT_ACCESSOR = SECURITY_PREFIX + "client-accessor";
  /**
   * The static String definition of the <i>"security-client-accessor-pp"</i> property
   *
   * @deprecated since Geode 1.0, use security-post-processor
   */
  String SECURITY_CLIENT_ACCESSOR_PP = SECURITY_PREFIX + "client-accessor-pp";
  /**
   * The static String definition of the <i>"security-client-auth-init"</i> property <a
   * name="security-client-auth-init"/a>
   * </p>
   * <U>Description</U>: Authentication module name for Clients that requires to act upon
   * credentials read from the gemfire.properties file. Module must implement AuthInitialize
   * interface.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Allowed values</U>: jar file:class name
   */
  String SECURITY_CLIENT_AUTH_INIT = SECURITY_PREFIX + "client-auth-init";
  /**
   * The static String definition of the <i>"security-manager"</i> property
   *
   * @since Geode 1.0
   */
  String SECURITY_MANAGER = SECURITY_PREFIX + "manager";

  /**
   * The static String definition of the <i>"security-post-processor"</i> property
   *
   * @since Geode 1.0
   */
  String SECURITY_POST_PROCESSOR = SECURITY_PREFIX + "post-processor";

  /**
   * The static String definition of the <i>"security-client-authenticator"</i> property
   *
   * @deprecated since Geode 1.0, use security-manager
   */
  String SECURITY_CLIENT_AUTHENTICATOR = SECURITY_PREFIX + "client-authenticator";
  /**
   * The static String definition of the <i>"security-client-dhalgo"</i> property
   *
   * @deprecated since Geode 1.5. Use SSL instead. See {{@link #SSL_ENABLED_COMPONENTS}}
   */
  String SECURITY_CLIENT_DHALGO = SECURITY_PREFIX + "client-dhalgo";
  /**
   * The static String definition of the <i>"security-udp-dhalgo"</i> property. Application can set
   * this property to valid symmetric key algorithm, to encrypt udp messages in Geode. Geode will
   * generate symmetric key using Diffie-Hellman key exchange algorithm between peers. That key
   * further used by specified algorithm to encrypt the udp messages.
   */
  String SECURITY_UDP_DHALGO = SECURITY_PREFIX + "udp-dhalgo";
  /**
   * The static String definition of the <i>"security-log-file"</i> property
   */
  String SECURITY_LOG_FILE = SECURITY_PREFIX + "log-file";
  /**
   * The static String definition of the <i>"security-log-level"</i> property
   */
  String SECURITY_LOG_LEVEL = SECURITY_PREFIX + "log-level";
  /**
   * The static String definition of the <i>"security-peer-auth-init"</i> property
   */
  String SECURITY_PEER_AUTH_INIT = SECURITY_PREFIX + "peer-auth-init";
  /**
   * The static String definition of the <i>"security-peer-authenticator"</i> property
   *
   * @deprecated since Geode 1.0, use security-manager
   */
  String SECURITY_PEER_AUTHENTICATOR = SECURITY_PREFIX + "peer-authenticator";
  /**
   * The static String definition of the <i>"security-peer-verifymember-timeout"</i> property
   */
  String SECURITY_PEER_VERIFY_MEMBER_TIMEOUT = SECURITY_PREFIX + "peer-verifymember-timeout";
  /**
   * The static String definition of the <i>"server-bind-address"</i> property <a
   * name="server-bind-address"/a>
   * <p>
   * <U>Description</U>: The IP address that this distributed system's server sockets in a
   * client-server topology will listen on. If set to an empty string then all of the local
   * machine's addresses will be listened on.
   * <p>
   * <U>Default</U>: ""
   */
  String SERVER_BIND_ADDRESS = "server-bind-address";
  /**
   * The static String definition of the <i>"ssl-server-alias"</i> property <a
   * name="ssl-server-alias"/a>
   * </p>
   * <U>Description</U>: This property is to be used if a specific key is to be used for the SSL
   * communications for client-server.
   * </p>
   * <U><i>Optional</i></U> <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_SERVER_ALIAS = "ssl-server-alias";
  /**
   * The static String definition of the <i>"server-ssl-ciphers"</i> property <a
   * name="server-ssl-ciphers"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL cipher suites to enable. Those listed
   * must be supported by the available providers.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_CIPHERS}
   */
  @Deprecated
  String SERVER_SSL_CIPHERS = "server-ssl-ciphers";
  /**
   * The static String definition of the <i>"server-ssl-enabled"</i> property <a
   * name="server-ssl-enabled"/a>
   * </p>
   * <U>Description</U>: Specifies if server is started with separate ssl configuration. If not
   * specified global property ssl-enabled (and its other related properties) are used to create
   * server socket
   * </p>
   * <U>Default</U>: <code>false</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_ENABLED_COMPONENTS} with optional
   *             {@link #SSL_SERVER_ALIAS}
   */
  @Deprecated
  String SERVER_SSL_ENABLED = "server-ssl-enabled";
  /**
   * The static String definition of the <i>"server-ssl-keystore"</i> property <a
   * name="server-ssl-keystore"/a>
   * </p>
   * <U>Description</U>: Location of the Java keystore file containing certificate and private key.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE}
   */
  @Deprecated
  String SERVER_SSL_KEYSTORE = "server-ssl-keystore";
  /**
   * The static String definition of the <i>"server-ssl-keystore-password"</i> property <a
   * name="server-ssl-keystore-password"/a>
   * </p>
   * <U>Description</U>: Password to access the private key from the keystore file specified by
   * javax.net.ssl.keyStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_PASSWORD}
   */
  @Deprecated
  String SERVER_SSL_KEYSTORE_PASSWORD = "server-ssl-keystore-password";
  /**
   * The static String definition of the <i>"server-ssl-keystore-type"</i> property <a
   * name="server-ssl-keystore-type"/a>
   * </p>
   * <U>Description</U>: For Java keystore file format, this property has the value jks (or JKS).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_KEYSTORE_TYPE}
   */
  @Deprecated
  String SERVER_SSL_KEYSTORE_TYPE = "server-ssl-keystore-type";
  /**
   * The static String definition of the <i>"server-ssl-protocols"</i> property <a
   * name="server-ssl-protocols"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL protocols to enable. Those listed must be
   * supported by the available providers.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_PROTOCOLS}
   */
  @Deprecated
  String SERVER_SSL_PROTOCOLS = "server-ssl-protocols";
  /**
   * The static String definition of the <i>"server-ssl-require-authentication"</i> property <a
   * name="server-ssl-require-authentication"/a>
   * </p>
   * <U>Description</U>: If false, allow ciphers that do not require the client side of the
   * connection to be authenticated.
   * </p>
   * <U>Default</U>: <code>any</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_REQUIRE_AUTHENTICATION}
   */
  @Deprecated
  String SERVER_SSL_REQUIRE_AUTHENTICATION = "server-ssl-require-authentication";
  /**
   * The static String definition of the <i>"server-ssl-truststore"</i> property <a
   * name="server-ssl-truststore"/a>
   * </p>
   * <U>Description</U>: Location of the Java keystore file containing the collection of CA
   * certificates trusted by server (trust store).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE}
   */
  @Deprecated
  String SERVER_SSL_TRUSTSTORE = "server-ssl-truststore";
  /**
   * The static String definition of the <i>"server-ssl-truststore-password"</i> property <a
   * name="server-ssl-truststore-password"/a>
   * </p>
   * <U>Description</U>: Password to unlock the keystore file (store password) specified by
   * javax.net.ssl.trustStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: GemFire 8.0
   *
   * @deprecated Since Geode 1.0 use {@link #SSL_TRUSTSTORE_PASSWORD}
   */
  @Deprecated
  String SERVER_SSL_TRUSTSTORE_PASSWORD = "server-ssl-truststore-password";
  /**
   * The static String definition of the <i>"socket-buffer-size"</i> property <a
   * name="socket-buffer-size"/a>
   * </p>
   * <U>Description</U>: The size of each socket buffer, in bytes. Smaller buffers conserve memory.
   * Larger buffers can improve performance; in particular if large messages are being sent.
   * <U>Default</U>: "32768"
   * </p>
   * <U>Allowed values</U>: 128..16777215
   * </p>
   * <U>Since</U>: GemFire 4.1
   */
  String SOCKET_BUFFER_SIZE = "socket-buffer-size";
  /**
   * The static String definition of the <i>"socket-lease-time"</i> property <a
   * name="socket-lease-time"/a>
   * </p>
   * <U>Description</U>: The number of milliseconds a thread can keep exclusive access to a socket
   * that it is not actively using. Once a thread loses its lease to a socket it will need to
   * re-acquire a socket the next time it sends a message. A value of zero causes socket leases to
   * never expire. This property is ignored if "conserve-sockets" is true. <U>Default</U>: "60000"
   * </p>
   * <U>Allowed values</U>: 0..600000
   * </p>
   * <U>Since</U>: GemFire 4.1
   */
  String SOCKET_LEASE_TIME = "socket-lease-time";
  /**
   * The static String definition of the <i>"start-dev-rest-api"</i> property <a
   * name="start-dev-rest-api"/a>
   * </p>
   * <U>Description</U>: If true then developer(API) REST service will be started when cache is
   * created. REST service can be configured using <code>http-service-port</code> and
   * <code>http-service-bind-address</code> properties.
   * </p>
   * <U>Default</U>: "false"
   * </p>
   * <U>Since</U>: GemFire 8.0
   */
  String START_DEV_REST_API = "start-dev-rest-api";
  /**
   * The static String definition of the <i>"start-locator"</i> property <a name="start-locator"/a>
   * </p>
   * <U>Description</U>: A host name or bind-address and port ("host[port]") that are used to start
   * a locator in the same process as the DistributedSystem. The locator is started when the
   * DistributedSystem connects, and is stopped when the DistributedSystem disconnects. To start a
   * locator that is not tied to the DistributedSystem's lifecycle, see the {@link Locator} class in
   * this same package.
   * <p>
   * <U>Default</U>: "" (doesn't start a locator)
   */
  String START_LOCATOR = "start-locator";
  /**
   * The static String definition of the <i>"statistic-archive-file"</i> property <a
   * name="statistic-archive-file"/a>
   * </p>
   * <U>Description</U>: The file that statistic samples are written to. An empty string (default)
   * disables statistic archival.
   * </p>
   * <U>Default</U>: ""
   */
  String STATISTIC_ARCHIVE_FILE = "statistic-archive-file";
  /**
   * The static String definition of the <i>"statistic-sample-rate"</i> property <a
   * name="statistic-sample-rate"/a>
   * </p>
   * <U>Description</U>: The rate, in milliseconds, at which samples of the statistics will be
   * taken. If set to a value less than 1000 the rate will be set to 1000 because the VSD tool does
   * not support sub-second sampling.
   * </p>
   * <U>Default</U>: "1000"
   * </p>
   * <U>Allowed values</U>: 100..60000
   */
  String STATISTIC_SAMPLE_RATE = "statistic-sample-rate";
  /**
   * The static String definition of the <i>"statistic-sampling-enabled"</i> property <a
   * name="statistic-sampling-enabled"/a>
   * </p>
   * <U>Description</U>: "true" causes the statistics to be sampled periodically and operating
   * system statistics to be fetched each time a sample is taken. "false" disables sampling which
   * also disables operating system statistic collection. Non OS statistics will still be recorded
   * in memory and can be viewed by administration tools. However, charts will show no activity and
   * no statistics will be archived while sampling is disabled. Starting in 7.0 the default value
   * has been changed to true. If statistic sampling is disabled it will also cause various metrics
   * seen in gfsh and pulse to always be zero.
   * </p>
   * <U>Default</U>: "true"
   * </p>
   * <U>Allowed values</U>: true|false
   */
  String STATISTIC_SAMPLING_ENABLED = "statistic-sampling-enabled";
  /**
   * The static String definition of the <i>"tcp-port"</i> property <a name="tcp-port"/a>
   * <p>
   * <U>Description</U>: A 16-bit integer that determines the tcp/ip port number to listen on for
   * cache communications. If zero, the operating system will select an available port to listen on.
   * Each process on a machine must have its own tcp-port. Note that some operating systems restrict
   * the range of ports usable by non-privileged users, and using restricted port numbers can cause
   * runtime errors in GemFire startup.
   * <p>
   * <U>Default</U>: "0"
   * <p>
   * <U>Allowed values</U>: 0..65535
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String TCP_PORT = "tcp-port";
  /**
   * The static String definition of the <i>"udp-fragment-size"</i> property <a
   * name="udp-fragment-size"/a>
   * <p>
   * <U>Description</U>: When messages are sent over datagram sockets, GemFire breaks large messages
   * down into fragments for transmission. This property sets the maximum fragment size for
   * transmission. <U>Default</U>: "60000"
   * <p>
   * <U>Allowed values</U>: 1000..60000
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String UDP_FRAGMENT_SIZE = "udp-fragment-size";
  /**
   * The static String definition of the <i>"udp-recv-buffer-size"</i> property <a
   * name="udp-recv-buffer-size"/a>
   * <p>
   * <U>Description</U>: Sets the size of the socket buffer used for incoming udp point-to-point
   * transmissions. Note: if multicast is not enabled and disable-tcp is not enabled, a reduced
   * default size of 65535 is used. <U>Default</U>: "1048576 if multicast is enabled or disable-tcp
   * is true, 131071 if not"
   * <p>
   * <U>Allowed values</U>: 2048..Operating System maximum
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String UDP_RECV_BUFFER_SIZE = "udp-recv-buffer-size";
  /**
   * The static String definition of the <i>"udp-send-buffer-size"</i> property <a
   * name="udp-send-buffer-size"/a>
   * <p>
   * <U>Description</U>: Sets the size of the socket buffer used for outgoing udp point-to-point
   * transmissions. <U>Default</U>: "65535"
   * <p>
   * <U>Allowed values</U>: 2048..Operating System maximum
   * <p>
   * <U>Since</U>: GemFire 5.0
   */
  String UDP_SEND_BUFFER_SIZE = "udp-send-buffer-size";
  /**
   * The static String definition of the <i>"use-cluster-configuration"</i> property <a
   * name="use-cluster-configuration"/a>
   * </p>
   * <U>Description</U>: This property is only applicable for data members (non client and non
   * locator) "true" causes a member to request and uses the configuration from cluster
   * configuration services running on dedicated locators. "false" causes a member to not request
   * the configuration from the configuration services running on the locator(s).
   * </p>
   * <U>Default</U>: "true"
   * </p>
   * <U>Allowed values</U>: true or false
   * </p>
   * <U>Since</U>: GemFire 8.0
   */
  String USE_CLUSTER_CONFIGURATION = "use-cluster-configuration";
  /**
   * The static String definition of the <i>"user-command-packages"</i> property <a
   * name="user-command-packages"/a>
   * </p>
   * <U>Description</U>: A comma separated list of Java packages that contain classes implementing
   * the <code>CommandMarker</code> interface. Matching classes will be loaded when the VM starts
   * and will be available in the GFSH command-line utility.
   * </p>
   * <U>Default</U>: <code>""</code>
   * </p>
   * <U>Since</U>: GemFire 8.0
   */
  String USER_COMMAND_PACKAGES = "user-command-packages";
  /**
   * The static String definition of the <i>"off-heap-memory-size"</i> property <a
   * name="off-heap-memory-size"/a>
   * </p>
   * <U>Description</U>: The total size of off-heap memory specified as
   * off-heap-memory-size=<n>[g|m]. <n> is the size. [g|m] indicates whether the size should be
   * interpreted as gigabytes or megabytes. By default no off-heap memory is allocated. A non-zero
   * value will cause that much memory to be allocated from the operating system and reserved for
   * off-heap use.
   * </p>
   * <U>Default</U>: <code>""</code>
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String OFF_HEAP_MEMORY_SIZE = "off-heap-memory-size";
  /**
   * The static String definition of the <i>"redis-port"</i> property <a name="redis-port"/a>
   * </p>
   * <U>Description</U>: Specifies the port used by {@link GeodeRedisServer} which enables redis
   * clients to connect and store data in GemFire distributed system. see {@link GeodeRedisServer}
   * for other configuration options.
   * </p>
   * <U>Default</U>: "0" disables GemFireMemcachedServer
   * </p>
   * <U>Allowed values</U>: 0..65535
   */
  String REDIS_PORT = "redis-port";
  /**
   * The static String definition of the <i>"redis-bind-address"</i> property <a
   * name="redis-bind-address"/a>
   * </p>
   * <U>Description</U>: Specifies the bind address used by {@link GeodeRedisServer}
   * </p>
   * <U>Default</U>: ""
   */
  String REDIS_BIND_ADDRESS = "redis-bind-address";
  /**
   * The static String definition of the <i>"redis-password"</i> property <a
   * name="redis-password"/a>
   * </p>
   * <U>Description</U>: Specifies the password to authenticate a client of {@link GeodeRedisServer}
   * </p>
   * <U>Default</U>: ""
   */
  String REDIS_PASSWORD = "redis-password";
  /**
   * The static String definition of the <i>"lock-memory"</i> property <a name="lock-memory"/a>
   * </p>
   * <U>Description</U>: Include this option to lock GemFire heap and off-heap memory pages into
   * RAM. This prevents the operating system from swapping the pages out to disk, which can cause
   * sever performance degradation. When you use this command, also configure the operating system
   * limits for locked memory.
   * </p>
   * <U>Default</U>: <code>"false"</code>
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String LOCK_MEMORY = "lock-memory";
  /**
   * The static String definition of the <i>"shiro-init"</i> property <U>Since</U>: Geode 1.0
   */
  String SECURITY_SHIRO_INIT = SECURITY_PREFIX + "shiro-init";
  /**
   * The static String definition of the <i>"distributed-transactions"</i> property <U>Since</U>:
   * Geode 1.0
   */
  String DISTRIBUTED_TRANSACTIONS = "distributed-transactions";
  /**
   * The static String definition of the <i>"ssl-enabled-components"</i> property <a
   * name="ssl-enabled-components"/a>
   * </p>
   * <U>Description</U>: This setting is a comma delimited fields which works in conjunction with
   * the {@link #CLUSTER_SSL_PREFIX} properties. This property will determine which components will
   * use SSL for their communications.
   * </p>
   * <U>Options</U>: "all","server","cluster","gateway","web","jmx","none" -- As described
   * {@link org.apache.geode.security.SecurableCommunicationChannels} <U>Since</U>: Geode 1.0
   */
  String SSL_ENABLED_COMPONENTS = "ssl-enabled-components";
  /**
   * The static String definition of the <i>"ssl-ciphers"</i> property <a name="ssl-ciphers"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL cipher suites to enable. Those listed
   * must be supported by the available providers.
   * </p>
   * <U>Default</U>: "any"
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_CIPHERS = "ssl-ciphers";
  /**
   * The static String definition of the <i>"ssl-keystore"</i> property <a name="ssl-keystore"/a>
   * </p>
   * <U>Description</U>Location of the Java keystore file containing certificate and private key.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_KEYSTORE = "ssl-keystore";
  /**
   * The static String definition of the <i>"ssl-keystore-password"</i> property <a
   * name="ssl-keystore-password"/a>
   * </p>
   * <U>Description</U>Password to access the private key from the keystore file specified by
   * javax.net.ssl.keyStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_KEYSTORE_PASSWORD = "ssl-keystore-password";
  /**
   * The static String definition of the <i>"ssl-keystore-type"</i> property <a
   * name="ssl-keystore-type"/a>
   * </p>
   * <U>Description</U>For Java keystore file format, this property has the value jks (or JKS).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_KEYSTORE_TYPE = "ssl-keystore-type";

  /**
   * The static String definition of the <i>"ssl-truststore-type"</i> property <a
   * name="ssl-truststore-type"/a>
   * </p>
   * <U>Description</U>For Java truststore file format, this property has the value jks (or JKS).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.3
   */
  String SSL_TRUSTSTORE_TYPE = "ssl-truststore-type";
  /**
   * The static String definition of the <i>"cluster-ssl-protocols"</i> property <a
   * name="ssl-protocols"/a>
   * </p>
   * <U>Description</U>: A space separated list of the SSL protocols to enable. Those listed must be
   * supported by the available providers.
   * </p>
   * <U>Default</U>: "any"
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_PROTOCOLS = "ssl-protocols";
  /**
   * The static String definition of the <i>"ssl-require-authentication"</i> property <a
   * name="ssl-require-authentication"/a>
   * </p>
   * <U>Description</U>: If false, allow ciphers that do not require the client side of the
   * connection to be authenticated.
   * </p>
   * <U>Default</U>: "true"
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_REQUIRE_AUTHENTICATION = "ssl-require-authentication";
  /**
   * The static String definition of the <i>"ssl-truststore"</i> property <a
   * name="ssl-truststore"/a>
   * </p>
   * <U>Description</U>Location of the Java keystore file containing the collection of CA
   * certificates trusted by distributed member (trust store).
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_TRUSTSTORE = "ssl-truststore";
  /**
   * The static String definition of the <i>"ssl-truststore-password"</i> property <a
   * name="ssl-truststore-password"/a>
   * </p>
   * <U>Description</U>Password to unlock the keystore file (store password) specified by
   * javax.net.ssl.trustStore.
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_TRUSTSTORE_PASSWORD = "ssl-truststore-password";
  /**
   * The static String definition of the <i>"ssl-default-alias"</i> property
   *
   * <U>Description</U>This property will be set when using multi-key keystores. This will define
   * the alias that the ssl connection factory would use when no alias has been set for the
   * different component aliases. {@link #SSL_CLUSTER_ALIAS},
   * {@link #SSL_SERVER_ALIAS},{@link #SSL_LOCATOR_ALIAS},{@link #SSL_GATEWAY_ALIAS},{@link #SSL_JMX_ALIAS}
   * , {@link #SSL_WEB_ALIAS}
   * </p>
   * <U>Default</U>: ""
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_DEFAULT_ALIAS = "ssl-default-alias";
  /**
   * The static String definition of the <i>"ssl-web-require-authentication"</i> property
   *
   * <U>Description</U>If false allows client side's http connection to be authenticated without a
   * 2-way SSL authentication.
   * </p>
   * <U>Default</U>: "false"
   * </p>
   * <U>Since</U>: Geode 1.0
   */
  String SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION = "ssl-web-require-authentication";
  /**
   * The static String definition of the <i>"validate-serializable-objects"</i> property
   *
   * <U>Description</U>If true checks incoming java serializable objects against a filter (allows
   * internal Geode classes and any others provided in the serializable-object-filter property).
   * </p>
   * <U>Default</U>: "false"
   * </p>
   * <U>Since</U>: Geode 1.4
   */
  String VALIDATE_SERIALIZABLE_OBJECTS = "validate-serializable-objects";
  /**
   * The static String definition of the <i>"serializable-object-filter"</i> property
   *
   * <U>Description</U>A user provided whitelist of objects that the system will allow to serialize.
   *
   * <p>
   * See java.io.ObjectInputFilter.Config for details on the syntax for creating filters.
   * https://docs.oracle.com/javase/9/docs/api/java/io/ObjectInputFilter.Config.html
   * </p>
   * </p>
   * <U>Default</U>: "!*"
   * </p>
   * <U>Since</U>: Geode 1.4
   *
   */
  String SERIALIZABLE_OBJECT_FILTER = "serializable-object-filter";
}
