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
package com.gemstone.gemfire.distributed;


public interface DistributedSystemConfigProperties {
  /**
   * The static definition of the <a href="DistributedSystem.html#ack-severe-alert-threshold">"ack-severe-alert-threshold"</a>
   * property
   */
  String ACK_SEVERE_ALERT_THRESHOLD = "ack-severe-alert-threshold";
  /**
   * The static definition of the <a href="DistributedSystem.html#ack-wait-threshold">"ack-wait-threshold"</a>
   * property
   */
  String ACK_WAIT_THRESHOLD = "ack-wait-threshold";
  /**
   * The static definition of the <a href="DistributedSystem.html#archive-disk-space-limit">"archive-disk-space-limit"</a>
   * property
   */
  String ARCHIVE_DISK_SPACE_LIMIT = "archive-disk-space-limit";
  /**
   * The static definition of the <a href="DistributedSystem.html#archive-file-size-limit">"archive-file-size-limit"</a>
   * property
   */
  String ARCHIVE_FILE_SIZE_LIMIT = "archive-file-size-limit";
  /**
   * The static definition of the <a href="DistributedSystem.html#async-distribution-timeout">"async-distribution-timeout"</a>
   * property
   */
  String ASYNC_DISTRIBUTION_TIMEOUT = "async-distribution-timeout";
  /**
   * The static definition of the <a href="DistributedSystem.html#async-max-queue-size">"async-max-queue-size"</a>
   * property
   */
  String ASYNC_MAX_QUEUE_SIZE = "async-max-queue-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#async-queue-timeout">"async-queue-timeout"</a>
   * property
   */
  String ASYNC_QUEUE_TIMEOUT = "async-queue-timeout";
  /**
   * The static definition of the <a href="DistributedSystem.html#bind-address">"bind-address"</a>
   * property
   */
  String BIND_ADDRESS = "bind-address";
  /**
   * The static definition of the <a href="DistributedSystem.html#cache-xml-file">"cache-xml-file"</a>
   * property
   */
  String CACHE_XML_FILE = "cache-xml-file";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-configuration-dir">"cluster-configuration-dir"</a>
   * property
   */
  String CLUSTER_CONFIGURATION_DIR = "cluster-configuration-dir";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-ciphers">"cluster-ssl-ciphers"</a>
   * property
   */
  String CLUSTER_SSL_CIPHERS = "cluster-ssl-ciphers";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-enabled">"cluster-ssl-enabled"</a>
   * property
   */
  String CLUSTER_SSL_ENABLED = "cluster-ssl-enabled";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-keystore">"cluster-ssl-keystore"</a>
   * property
   */
  String CLUSTER_SSL_KEYSTORE = "cluster-ssl-keystore";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-keystore-password">"cluster-ssl-keystore-password"</a>
   * property
   */
  String CLUSTER_SSL_KEYSTORE_PASSWORD = "cluster-ssl-keystore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-keystore-type">"cluster-ssl-keystore-type"</a>
   * property
   */
  String CLUSTER_SSL_KEYSTORE_TYPE = "cluster-ssl-keystore-type";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-protocols">"cluster-ssl-protocols"</a>
   * property
   */
  String CLUSTER_SSL_PROTOCOLS = "cluster-ssl-protocols";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-require-authentication">"cluster-ssl-require-authentication"</a>
   * property
   */
  String CLUSTER_SSL_REQUIRE_AUTHENTICATION = "cluster-ssl-require-authentication";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-truststore">"cluster-ssl-truststore"</a>
   * property
   */
  String CLUSTER_SSL_TRUSTSTORE = "cluster-ssl-truststore";
  /**
   * The static definition of the <a href="DistributedSystem.html#cluster-ssl-truststore-password">"cluster-ssl-truststore-password"</a>
   * property
   */
  String CLUSTER_SSL_TRUSTSTORE_PASSWORD = "cluster-ssl-truststore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#conflate-events">"conflate-events"</a>
   * property
   */
  String CONFLATE_EVENTS = "conflate-events";
  /**
   * The static definition of the <a href="DistributedSystem.html#conserve-sockets">"conserve-sockets"</a>
   * property
   */
  String CONSERVE_SOCKETS = "conserve-sockets";
  /**
   * The static definition of the <a href="DistributedSystem.html#delta-propagation">"delta-propagation"</a>
   * property
   */
  String DELTA_PROPAGATION = "delta-propagation";
  /**
   * The static definition of the <a href="DistributedSystem.html#deploy-working-dir">"deploy-working-dir"</a>
   * property
   */
  String DEPLOY_WORKING_DIR = "deploy-working-dir";
  /**
   * The static definition of the <a href="DistributedSystem.html#disable-auto-reconnect">"disable-auto-reconnect"</a>
   * property
   */
  String DISABLE_AUTO_RECONNECT = "disable-auto-reconnect";
  /**
   * The static definition of the <a href="DistributedSystem.html#disable-tcp">"disable-tcp"</a>
   * property
   */
  String DISABLE_TCP = "disable-tcp";
  /**
   * The static definition of the <a href="DistributedSystem.html#distributed-system-id">"distributed-system-id"</a>
   * property
   */
  String DISTRIBUTED_SYSTEM_ID = "distributed-system-id";
  /**
   * The static definition of the <a href="DistributedSystem.html#durable-client-id">"durable-client-id"</a>
   * property
   */
  String DURABLE_CLIENT_ID = "durable-client-id";
  /**
   * The static definition of the <a href="DistributedSystem.html#durable-client-timeout">"durable-client-timeout"</a>
   * property
   */
  String DURABLE_CLIENT_TIMEOUT = "durable-client-timeout";
  /**
   * The static definition of the <a href="DistributedSystem.html#enable-cluster-configuration">"enable-cluster-configuration"</a>
   * property
   */
  String ENABLE_CLUSTER_CONFIGURATION = "enable-cluster-configuration";
  /**
   * The static definition of the <a href="DistributedSystem.html#enable-network-partition-detection">"enable-network-partition-detection"</a>
   * property
   */
  String ENABLE_NETWORK_PARTITION_DETECTION = "enable-network-partition-detection";
  /**
   * The static definition of the <a href="DistributedSystem.html#enable-time-statistics">"enable-time-statistics"</a>
   * property
   */
  String ENABLE_TIME_STATISTICS = "enable-time-statistics";
  /**
   * The static definition of the <a href="DistributedSystem.html#enforce-unique-host">"enforce-unique-host"</a>
   * property
   */
  String ENFORCE_UNIQUE_HOST = "enforce-unique-host";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-ciphers">"gateway-ssl-ciphers"</a>
   * property
   */
  String GATEWAY_SSL_CIPHERS = "gateway-ssl-ciphers";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-enabled">"gateway-ssl-enabled"</a>
   * property
   */
  String GATEWAY_SSL_ENABLED = "gateway-ssl-enabled";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-keystore">"gateway-ssl-keystore"</a>
   * property
   */
  String GATEWAY_SSL_KEYSTORE = "gateway-ssl-keystore";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-keystore-password">"gateway-ssl-keystore-password"</a>
   * property
   */
  String GATEWAY_SSL_KEYSTORE_PASSWORD = "gateway-ssl-keystore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-keystore-type">"gateway-ssl-keystore-type"</a>
   * property
   */
  String GATEWAY_SSL_KEYSTORE_TYPE = "gateway-ssl-keystore-type";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-protocols">"gateway-ssl-protocols"</a>
   * property
   */
  String GATEWAY_SSL_PROTOCOLS = "gateway-ssl-protocols";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-require-authentication">"gateway-ssl-require-authentication"</a>
   * property
   */
  String GATEWAY_SSL_REQUIRE_AUTHENTICATION = "gateway-ssl-require-authentication";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-truststore">"gateway-ssl-truststore"</a>
   * property
   */
  String GATEWAY_SSL_TRUSTSTORE = "gateway-ssl-truststore";
  /**
   * The static definition of the <a href="DistributedSystem.html#gateway-ssl-truststore-password">"gateway-ssl-truststore-password"</a>
   * property
   */
  String GATEWAY_SSL_TRUSTSTORE_PASSWORD = "gateway-ssl-truststore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#groups">"groups"</a>
   * property
   */
  String GROUPS = "groups";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-bind-address">"http-service-bind-address"</a>
   * property
   */
  String HTTP_SERVICE_BIND_ADDRESS = "http-service-bind-address";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-port">"http-service-port"</a>
   * property
   */
  String HTTP_SERVICE_PORT = "http-service-port";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-ciphers">"http-service-ssl-ciphers"</a>
   * property
   */
  String HTTP_SERVICE_SSL_CIPHERS = "http-service-ssl-ciphers";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-enabled">"http-service-ssl-enabled"</a>
   * property
   */
  String HTTP_SERVICE_SSL_ENABLED = "http-service-ssl-enabled";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-keystore">"http-service-ssl-keystore"</a>
   * property
   */
  String HTTP_SERVICE_SSL_KEYSTORE = "http-service-ssl-keystore";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-keystore-password">"http-service-ssl-keystore-password"</a>
   * property
   */
  String HTTP_SERVICE_SSL_KEYSTORE_PASSWORD = "http-service-ssl-keystore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-keystore-type">"http-service-ssl-keystore-type"</a>
   * property
   */
  String HTTP_SERVICE_SSL_KEYSTORE_TYPE = "http-service-ssl-keystore-type";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-protocols">"http-service-ssl-protocols"</a>
   * property
   */
  String HTTP_SERVICE_SSL_PROTOCOLS = "http-service-ssl-protocols";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-require-authentication">"http-service-ssl-require-authentication"</a>
   * property
   */
  String HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION = "http-service-ssl-require-authentication";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-truststore">"http-service-ssl-truststore"</a>
   * property
   */
  String HTTP_SERVICE_SSL_TRUSTSTORE = "http-service-ssl-truststore";
  /**
   * The static definition of the <a href="DistributedSystem.html#http-service-ssl-truststore-password">"http-service-ssl-truststore-password"</a>
   * property
   */
  String HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD = "http-service-ssl-truststore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager">"jmx-manager"</a>
   * property
   */
  String JMX_MANAGER = "jmx-manager";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-access-file">"jmx-manager-access-file"</a>
   * property
   */
  String JMX_MANAGER_ACCESS_FILE = "jmx-manager-access-file";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-bind-address">"jmx-manager-bind-address"</a>
   * property
   */
  String JMX_MANAGER_BIND_ADDRESS = "jmx-manager-bind-address";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-hostname-for-clients">"jmx-manager-hostname-for-clients"</a>
   * property
   */
  String JMX_MANAGER_HOSTNAME_FOR_CLIENTS = "jmx-manager-hostname-for-clients";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-http-port">"jmx-manager-http-port"</a>
   * property
   *
   * @deprecated as of GemFire 8.0 use {@link #HTTP_SERVICE_PORT} instead.
   */
  String JMX_MANAGER_HTTP_PORT = "jmx-manager-http-port";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-password-file">"jmx-manager-password-file"</a>
   * property
   */
  String JMX_MANAGER_PASSWORD_FILE = "jmx-manager-password-file";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-port">"jmx-manager-port"</a>
   * property
   */
  String JMX_MANAGER_PORT = "jmx-manager-port";
  /**
   * * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl">"jmx-manager-ssl"</a>
   * property
   * @deprecated as of GemFire 8.0 use {@link #JMX_MANAGER_SSL_ENABLED} instead.
   */
  String JMX_MANAGER_SSL = "jmx-manager-ssl";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-start">"jmx-manager-start"</a>
   * property
   */
  String JMX_MANAGER_START = "jmx-manager-start";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-update-rate">"jmx-manager-update-rate"</a>
   * property
   */
  String JMX_MANAGER_UPDATE_RATE = "jmx-manager-update-rate";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-ciphers">"jmx-manager-ssl-ciphers"</a>
   * property
   */
  String JMX_MANAGER_SSL_CIPHERS = "jmx-manager-ssl-ciphers";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-enabled">"jmx-manager-ssl-enabled"</a>
   * property
   */
  String JMX_MANAGER_SSL_ENABLED = "jmx-manager-ssl-enabled";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-keystore">"jmx-manager-ssl-keystore"</a>
   * property
   */
  String JMX_MANAGER_SSL_KEYSTORE = "jmx-manager-ssl-keystore";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-keystore-password">"jmx-manager-ssl-keystore-password"</a>
   * property
   */
  String JMX_MANAGER_SSL_KEYSTORE_PASSWORD = "jmx-manager-ssl-keystore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-keystore-type">"jmx-manager-ssl-keystore-type"</a>
   * property
   */
  String JMX_MANAGER_SSL_KEYSTORE_TYPE = "jmx-manager-ssl-keystore-type";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-protocols">"jmx-manager-ssl-protocols"</a>
   * property
   */
  String JMX_MANAGER_SSL_PROTOCOLS = "jmx-manager-ssl-protocols";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-require-authentication">"jmx-manager-ssl-require-authentication"</a>
   * property
   */
  String JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION = "jmx-manager-ssl-require-authentication";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-truststore">"jmx-manager-ssl-truststore"</a>
   * property
   */
  String JMX_MANAGER_SSL_TRUSTSTORE = "jmx-manager-ssl-truststore";
  /**
   * The static definition of the <a href="DistributedSystem.html#jmx-manager-ssl-truststore-password">"jmx-manager-ssl-truststore-password"</a>
   * property
   */
  String JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD = "jmx-manager-ssl-truststore-password";
  String LICENCE_APPLICATION_CACHE = "license-application-cache";
  String LICENCE_DATA_MANAGEMENT = "license-data-management";
  String LICENCE_SERVER_TIMEOUT = "license-server-timeout";
  String LICENCE_WORKING_DIR = "license-working-dir";
  /**
   * The static definition of the <a href="DistributedSystem.html#load-cluster-configuration-from-dir">"load-cluster-configuration-from-dir"</a>
   * property
   */
  String LOAD_CLUSTER_CONFIGURATION_FROM_DIR = "load-cluster-configuration-from-dir";
  /**
   * The static definition of the <a href="DistributedSystem.html#locator-wait-time">"locator-wait-time"</a>
   * property
   */
  String LOCATOR_WAIT_TIME = "locator-wait-time";
  /**
   * The static definition of the <a href="DistributedSystem.html#locators">"locators"</a>
   * property
   */
  String LOCATORS = "locators";
  /**
   * The static definition of the <a href="DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  String LOG_DISK_SPACE_LIMIT = "log-disk-space-limit";
  /**
   * The static definition of the <a href="DistributedSystem.html#log-file">"log-file"</a>
   * property
   */
  String LOG_FILE = "log-file";
  /**
   * The static definition of the <a href="DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  String LOG_FILE_SIZE_LIMIT = "log-file-size-limit";
  /**
   * The static definition of the <a href="DistributedSystem.html#log-level">"log-level"</a>
   * property
   */
  String LOG_LEVEL = "log-level";
  /**
   * The static definition of the <a href="DistributedSystem.html#max-num-reconnect-tries">"max-num-reconnect-tries"</a>
   * property
   */
  String MAX_NUM_RECONNECT_TRIES = "max-num-reconnect-tries";
  /**
   * The static definition of the <a href="DistributedSystem.html#max-wait-time-reconnect">"max-wait-time-reconnect"</a>
   * property
   */
  String MAX_WAIT_TIME_RECONNECT = "max-wait-time-reconnect";
  /**
   * The static definition of the <a href="DistributedSystem.html#mcast-address">"mcast-address"</a>
   * property
   */
  String MCAST_ADDRESS = "mcast-address";
  /**
   * The static definition of the <a href="DistributedSystem.html#mcast-flow-control">"mcast-flow-control"</a>
   * property
   */
  String MCAST_FLOW_CONTROL = "mcast-flow-control";
  /**
   * The static definition of the <a href="DistributedSystem.html#mcast-port">"mcast-port"</a>
   * property
   */
  String MCAST_PORT = "mcast-port";
  /**
   * The static definition of the <a href="DistributedSystem.html#mcast-recv-buffer-size">"mcast-recv-buffer-size"</a>
   * property
   */
  String MCAST_RECV_BUFFER_SIZE = "mcast-recv-buffer-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#mcast-send-buffer-size">"mcast-send-buffer-size"</a>
   * property
   */
  String MCAST_SEND_BUFFER_SIZE = "mcast-send-buffer-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#mcast-ttl">"mcast-ttl"</a>
   * property
   */
  String MCAST_TTL = "mcast-ttl";
  /**
   * The static definition of the <a href="DistributedSystem.html#member-timeout">"member-timeout"</a>
   * property
   */
  String MEMBER_TIMEOUT = "member-timeout";
  /**
   * The static definition of the <a href="DistributedSystem.html#membership-port-range">"membership-port-range"</a>
   * property
   */
  String MEMBERSHIP_PORT_RANGE = "membership-port-range";
  /**
   * The static definition of the <a href="DistributedSystem.html#memcached-bind-address">"memcached-bind-address"</a>
   * property
   */
  String MEMCACHED_BIND_ADDRESS = "memcached-bind-address";
  /**
   * The static definition of the <a href="DistributedSystem.html#memcached-port">"memcached-port"</a>
   * property
   */
  String MEMCACHED_PORT = "memcached-port";
  /**
   * The static definition of the <a href="DistributedSystem.html#memcached-protocol">"memcached-protocol"</a>
   * property
   */
  String MEMCACHED_PROTOCOL = "memcached-protocol";
  /**
   * The static definition of the <a href="DistributedSystem.html#name">"name"</a>
   * property
   */
  String NAME = "name";
  /**
   * The static definition of the <a href="DistributedSystem.html#redundancy-zone">"redundancy-zone"</a>
   * property
   */
  String REDUNDANCY_ZONE = "redundancy-zone";
  /**
   * The static definition of the <a href="DistributedSystem.html#remote-locators">"remote-locators"</a>
   * property
   */
  String REMOTE_LOCATORS = "remote-locators";
  /**
   * The static definition of the <a href="DistributedSystem.html#remove-unresponsive-client">"remove-unresponsive-client"</a>
   * property
   */
  String REMOVE_UNRESPONSIVE_CLIENT = "remove-unresponsive-client";
  /**
   * The static definition of the <a href="DistributedSystem.html#roles">"roles"</a>
   * property
   */
  String ROLES = "roles";
  /**
   * The static definition of the security prefix "security-" used in conjuntion with other security-* properties</a>
   * property
   */
  String SECURITY_PREFIX = "security-";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-client-accessor">"security-client-accessor"</a>
   * property
   */
  String SECURITY_CLIENT_ACCESSOR = SECURITY_PREFIX + "client-accessor";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-client-accessor-pp">"security-client-accessor-pp"</a>
   * property
   */
  String SECURITY_CLIENT_ACCESSOR_PP = SECURITY_PREFIX + "client-accessor-pp";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-client-auth-init">"security-client-auth-init"</a>
   * property
   */
  String SECURITY_CLIENT_AUTH_INIT = SECURITY_PREFIX + "client-auth-init";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-client-authenticator">"security-client-authenticator"</a>
   * property
   */
  String SECURITY_CLIENT_AUTHENTICATOR = SECURITY_PREFIX + "client-authenticator";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-client-dhalgo">"security-client-dhalgo"</a>
   * property
   */
  String SECURITY_CLIENT_DHALGO = SECURITY_PREFIX + "client-dhalgo";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-udp-dhalgo">"security-udp-dhalgo"</a>
   * property
   */
  String SECURITY_UDP_DHALGO = SECURITY_PREFIX + "udp-dhalgo";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-log-file">"security-log-file"</a>
   * property
   */
  String SECURITY_LOG_FILE = SECURITY_PREFIX + "log-file";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-log-level">"security-log-level"</a>
   * property
   */
  String SECURITY_LOG_LEVEL = SECURITY_PREFIX + "log-level";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-peer-auth-init">"security-peer-auth-init"</a>
   * property
   */
  String SECURITY_PEER_AUTH_INIT = SECURITY_PREFIX + "peer-auth-init";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-peer-authenticator">"security-peer-authenticator"</a>
   * property
   */
  String SECURITY_PEER_AUTHENTICATOR = SECURITY_PREFIX + "peer-authenticator";
  /**
   * The static definition of the <a href="DistributedSystem.html#security-peer-verifymember-timeout">"security-peer-verifymember-timeout"</a>
   * property
   */
  String SECURITY_PEER_VERIFY_MEMBER_TIMEOUT = SECURITY_PREFIX + "peer-verifymember-timeout";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-bind-address">"server-bind-address"</a>
   * property
   */
  String SERVER_BIND_ADDRESS = "server-bind-address";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-ciphers">"server-ssl-ciphers"</a>
   * property
   */
  String SERVER_SSL_CIPHERS = "server-ssl-ciphers";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-enabled">"server-ssl-enabled"</a>
   * property
   */
  String SERVER_SSL_ENABLED = "server-ssl-enabled";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-keystore">"server-ssl-keystore"</a>
   * property
   */
  String SERVER_SSL_KEYSTORE = "server-ssl-keystore";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-keystore-password">"server-ssl-keystore-password"</a>
   * property
   */
  String SERVER_SSL_KEYSTORE_PASSWORD = "server-ssl-keystore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-keystore-type">"server-ssl-keystore-type"</a>
   * property
   */
  String SERVER_SSL_KEYSTORE_TYPE = "server-ssl-keystore-type";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-protocols">"server-ssl-protocols"</a>
   * property
   */
  String SERVER_SSL_PROTOCOLS = "server-ssl-protocols";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-require-authentication">"server-ssl-require-authentication"</a>
   * property
   */
  String SERVER_SSL_REQUIRE_AUTHENTICATION = "server-ssl-require-authentication";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-truststore">"server-ssl-truststore"</a>
   * property
   */
  String SERVER_SSL_TRUSTSTORE = "server-ssl-truststore";
  /**
   * The static definition of the <a href="DistributedSystem.html#server-ssl-truststore-password">"server-ssl-truststore-password"</a>
   * property
   */
  String SERVER_SSL_TRUSTSTORE_PASSWORD = "server-ssl-truststore-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#socket-buffer-size">"socket-buffer-size"</a>
   * property
   */
  String SOCKET_BUFFER_SIZE = "socket-buffer-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#socket-lease-time">"socket-lease-time"</a>
   * property
   */
  String SOCKET_LEASE_TIME = "socket-lease-time";
  /**
   * The static definition of the <a href="DistributedSystem.html#start-dev-rest-api">"start-dev-rest-api"</a>
   * property
   */
  String START_DEV_REST_API = "start-dev-rest-api";
  /**
   * The static definition of the <a href="DistributedSystem.html#start-locator">"start-locator"</a>
   * property
   */
  String START_LOCATOR = "start-locator";
  /**
   * The static definition of the <a href="DistributedSystem.html#statistic-archive-file">"statistic-archive-file"</a>
   * property
   */
  String STATISTIC_ARCHIVE_FILE = "statistic-archive-file";
  /**
   * The static definition of the <a href="DistributedSystem.html#statistic-sample-rate">"statistic-sample-rate"</a>
   * property
   */
  String STATISTIC_SAMPLE_RATE = "statistic-sample-rate";
  /**
   * The static definition of the <a href="DistributedSystem.html#statistic-sampling-enabled">"statistic-sampling-enabled"</a>
   * property
   */
  String STATISTIC_SAMPLING_ENABLED = "statistic-sampling-enabled";
  /**
   * The static definition of the <a href="DistributedSystem.html#tcp-port">"tcp-port"</a>
   * property
   */
  String TCP_PORT = "tcp-port";
  /**
   * The static definition of the <a href="DistributedSystem.html#udp-fragment-size">"udp-fragment-size"</a>
   * property
   */
  String UDP_FRAGMENT_SIZE = "udp-fragment-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#udp-recv-buffer-size">"udp-recv-buffer-size"</a>
   * property
   */
  String UDP_RECV_BUFFER_SIZE = "udp-recv-buffer-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#udp-send-buffer-size">"udp-send-buffer-size"</a>
   * property
   */
  String UDP_SEND_BUFFER_SIZE = "udp-send-buffer-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#use-cluster-configuration">"use-cluster-configuration"</a>
   * property
   */
  String USE_CLUSTER_CONFIGURATION = "use-cluster-configuration";
  /**
   * The static definition of the <a href="DistributedSystem.html#user-command-packages">"user-command-packages"</a>
   * property
   */
  String USER_COMMAND_PACKAGES = "user-command-packages";
  /**
   * The static definition of the <a href="DistributedSystem.html#off-heap-memory-size">"off-heap-memory-size"</a>
   * property
   */
  String OFF_HEAP_MEMORY_SIZE = "off-heap-memory-size";
  /**
   * The static definition of the <a href="DistributedSystem.html#redis-port">"redis-port"</a>
   * property
   */
  String REDIS_PORT = "redis-port";
  /**
   * The static definition of the <a href="DistributedSystem.html#redis-bind-address">"redis-bind-address"</a>
   * property
   */
  String REDIS_BIND_ADDRESS = "redis-bind-address";
  /**
   * The static definition of the <a href="DistributedSystem.html#redis-password">"redis-password"</a>
   * property
   */
  String REDIS_PASSWORD = "redis-password";
  /**
   * The static definition of the <a href="DistributedSystem.html#lock-memory">"lock-memory"</a>
   * property
   */
  String LOCK_MEMORY = "lock-memory";
  /**
   * The static definition of the <a href="DistributedSystem.html#shiro-init">"shiro-init"</a>
   * property
   */
  String SECURITY_SHIRO_INIT = SECURITY_PREFIX + "shiro-init";
  /**
   * The static definition of the <a href="DistributedSystem.html#distributed-transactions">"distributed-transactions"</a>
   * property
   */
  String DISTRIBUTED_TRANSACTIONS = "distributed-transactions";
  /**
   * The static definition of the <a href="DistributedSystem.html#ssl-enabled">"ssl-enabled"</a>
   * property
   *
   * @deprecated as of Gemfire 8.0 use {@link #CLUSTER_SSL_ENABLED} instead.
   */
  String SSL_ENABLED = "ssl-enabled";
  /**
   * The static definition of the <a href="DistributedSystem.html#ssl-protocols">"ssl-protocols"</a>
   * property
   *
   * @deprecated as of GemFire 8.0 use {@link #CLUSTER_SSL_PROTOCOLS} instead.
   */
  String SSL_PROTOCOLS = "ssl-protocols";
  /**
   * The static definition of the <a href="DistributedSystem.html#ssl-ciphers">"ssl-ciphers"</a>
   * property
   *
   * @deprecated as of GemFire 8.0 use {@link #CLUSTER_SSL_CIPHERS} instead.
   */
  String SSL_CIPHERS = "ssl-ciphers";
  /**
   * The static definition of the <a href="DistributedSystem.html#ssl-require-authentication">"ssl-require-authentication"</a>
   * property
   *
   * @deprecated as of GemFire 8.0 use {@link #CLUSTER_SSL_REQUIRE_AUTHENTICATION} instead.
   */
  String SSL_REQUIRE_AUTHENTICATION = "ssl-require-authentication";
}
