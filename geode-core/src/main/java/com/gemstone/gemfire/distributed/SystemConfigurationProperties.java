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

import com.gemstone.gemfire.distributed.internal.ConfigAttribute;

/**
 * Created by ukohlmeyer on 26/05/2016.
 */
public interface SystemConfigurationProperties {
  String ACK_SEVERE_ALERT_THRESHOLD = "ack-severe-alert-threshold";
  String ACK_WAIT_THRESHOLD = "ack-wait-threshold";
  String ARCHIVE_DISK_SPACE_LIMIT = "archive-disk-space-limit";
  String ARCHIVE_FILE_SIZE_LIMIT = "archive-file-size-limit";
  String ASYNC_DISTRIBUTION_TIMEOUT = "async-distribution-timeout";
  String ASYNC_MAX_QUEUE_SIZE = "async-max-queue-size";
  String ASYNC_QUEUE_TIMEOUT = "async-queue-timeout";
  String BIND_ADDRESS = "bind-address";
  String CACHE_XML_FILE = "cache-xml-file";
  String CLUSTER_CONFIGURATION_DIR = "cluster-configuration-dir";
  String CLUSTER_SSL_CIPHERS = "cluster-ssl-ciphers";
  String CLUSTER_SSL_ENABLED = "cluster-ssl-enabled";
  String CLUSTER_SSL_KEYSTORE = "cluster-ssl-keystore";
  String CLUSTER_SSL_KEYSTORE_PASSWORD = "cluster-ssl-keystore-password";
  String CLUSTER_SSL_KEYSTORE_TYPE = "cluster-ssl-keystore-type";
  String CLUSTER_SSL_PROTOCOLS = "cluster-ssl-protocols";
  String CLUSTER_SSL_REQUIRE_AUTHENTICATION = "cluster-ssl-require-authentication";
  String CLUSTER_SSL_TRUSTSTORE = "cluster-ssl-truststore";
  String CLUSTER_SSL_TRUSTSTORE_PASSWORD = "cluster-ssl-truststore-password";
  String CONFLATE_EVENTS = "conflate-events";
  String CONSERVE_SOCKETS = "conserve-sockets";
  String DELTA_PROPAGATION = "delta-propagation";
  String DEPLOY_WORKING_DIR = "deploy-working-dir";
  String DISABLE_AUTO_RECONNECT = "disable-auto-reconnect";
  String DISABLE_TCP = "disable-tcp";
  String DISTRIBUTED_SYSTEM_ID = "distributed-system-id";
  String DURABLE_CLIENT_ID = "durable-client-id";
  String DURABLE_CLIENT_TIMEOUT = "durable-client-timeout";
  String ENABLE_CLUSTER_CONFIGURATION = "enable-cluster-configuration";
  String ENABLE_NETWORK_PARTITION_DETECTION = "enable-network-partition-detection";
  String ENABLE_TIME_STATISTICS = "enable-time-statistics";
  String ENFORE_UNIQUE_HOST = "enforce-unique-host";
  String GATEWAY_SSL_CIPHERS = "gateway-ssl-ciphers";
  String GATEWAY_SSL_ENABLED = "gateway-ssl-enabled";
  String GATEWAY_SSL_KEYSTORE = "gateway-ssl-keystore";
  String GATEWAY_SSL_KEYSTORE_PASSWORD = "gateway-ssl-keystore-password";
  String GATEWAY_SSL_KEYSTORE_TYPE = "gateway-ssl-keystore-type";
  String GATEWAY_SSL_PROTOCOLS = "gateway-ssl-protocols";
  String GATEWAY_SSL_REQUIRE_AUTHENTICATION = "gateway-ssl-require-authentication";
  String GATEWAY_SSL_TRUSTSTORE = "gateway-ssl-truststore";
  String GATEWAY_SSL_TRUSTSTORE_PASSWORD = "gateway-ssl-truststore-password";
  String GROUPS = "groups";
  String HTTP_SERVICE_BIND_ADDRESS = "http-service-bind-address";
  String HTTP_SERVICE_PORT = "http-service-port";
  String HTTP_SERVICE_SSL_CIPHERS = "http-service-ssl-ciphers";
  String HTTP_SERVICE_SSL_ENABLED = "http-service-ssl-enabled";
  String HTTP_SERVICE_SSL_KEYSTORE = "http-service-ssl-keystore";
  String HTTP_SERVICE_SSL_KEYSTORE_PASSWORD = "http-service-ssl-keystore-password";
  String HTTP_SERVICE_SSL_KEYSTORE_TYPE = "http-service-ssl-keystore-type";
  String HTTP_SERVICE_SSL_PROTOCOLS = "http-service-ssl-protocols";
  String HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION = "http-service-ssl-require-authentication";
  String HTTP_SERVICE_SSL_TRUSTSTORE = "http-service-ssl-truststore";
  String HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD = "http-service-ssl-truststore-password";
  String JMX_MANAGER = "jmx-manager";
  String JMX_MANAGER_ACCESS_FILE = "jmx-manager-access-file";
  String JMX_MANAGER_BIND_ADDRESS = "jmx-manager-bind-address";
  String JMX_MANAGER_HOSTNAME_FOR_CLIENTS = "jmx-manager-hostname-for-clients";
  String JMX_MANAGER_HTTP_PORT = "jmx-manager-http-port";
  String JMX_MANAGER_PASSWORD_FILE = "jmx-manager-password-file";
  String JMX_MANAGER_PORT = "jmx-manager-port";
  String JMX_MANAGER_SSL = "jmx-manager-ssl";
  String JMX_MANAGER_START = "jmx-manager-start";
  String JMX_MANAGER_UPDATE_RATE = "jmx-manager-update-rate";
  String JMX_MANAGER_SSL_CIPHERS = "jmx-manager-ssl-ciphers";
  String JMX_MANAGER_SSL_ENABLED = "jmx-manager-ssl-enabled";
  String JMX_MANAGER_SSL_KEYSTORE = "jmx-manager-ssl-keystore";
  String JMX_MANAGER_SSL_KEYSTORE_PASSWORD = "jmx-manager-ssl-keystore-password";
  String JMX_MANAGER_SSL_KEYSTORE_TYPE = "jmx-manager-ssl-keystore-type";
  String JMX_MANAGER_SSL_PROTOCOLS = "jmx-manager-ssl-protocols";
  String JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION = "jmx-manager-ssl-require-authentication";
  String JMX_MANAGER_SSL_TRUSTSTORE = "jmx-manager-ssl-truststore";
  String JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD = "jmx-manager-ssl-truststore-password";
  String LICENCE_APPLICATION_CACHE = "license-application-cache";
  String LICENCE_DATA_MANAGEMENT = "license-data-management";
  String LICENCE_SERVER_TIMEOUT = "license-server-timeout";
  String LICENCE_WORKING_DIR = "license-working-dir";
  String LOAD_CLUSTER_CONFIGURATION_FROM_DIR = "load-cluster-configuration-from-dir";
  String LOCATOR_WAIT_TIME = "locator-wait-time";
  String LOCATORS = "locators";
  String LOG_DISK_SPACE_LIMIT = "log-disk-space-limit";
  String LOG_FILE = "log-file";
  String LOG_FILE_SIZE_LIMIT = "log-file-size-limit";
  String LOG_LEVEL = "log-level";
  String MAX_NUM_RECONNECT_TRIES = "max-num-reconnect-tries";
  String MAX_WAIT_TIME_RECONNECT = "max-wait-time-reconnect";
  String MCAST_ADDRESS = "mcast-address";
  String MCAST_FLOW_CONTROL = "mcast-flow-control";
  String MCAST_PORT = "mcast-port";
  String MCAST_RECV_BUFFER_SIZE = "mcast-recv-buffer-size";
  String MCAST_SEND_BUFFER_SIZE = "mcast-send-buffer-size";
  String MCAST_TTL = "mcast-ttl";
  String MEMBER_TIMEOUT = "member-timeout";
  String MEMBERSHIP_PORT_RANGE = "membership-port-range";
  String MEMCACHED_BIND_ADDRESS = "memcached-bind-address";
  String MEMCACHED_PORT = "memcached-port";
  String MEMCACHED_PROTOCOL = "memcached-protocol";

  /**
   * The name of the "name" property
   */
  @ConfigAttribute(type = String.class)
  String NAME = "name";
  String REDUNDANCY_ZONE = "redundancy-zone";
  String REMOTE_LOCATORS = "remote-locators";
  String REMOVE_UNRESPONSIVE_CLIENT = "remove-unresponsive-client";
  String ROLES = "roles";
  String SECURITY_PREFIX = "security-";
  String SECURITY_CLIENT_ACCESSOR = SECURITY_PREFIX + "client-accessor";
  String SECURITY_CLIENT_ACCESSOR_PP = SECURITY_PREFIX + "client-accessor-pp";
  String SECURITY_CLIENT_AUTH_INIT = SECURITY_PREFIX + "client-auth-init";
  String SECURITY_CLIENT_AUTHENTICATOR = SECURITY_PREFIX + "client-authenticator";
  String SECURITY_CLIENT_DHALGO = SECURITY_PREFIX + "client-dhalgo";
  String SECURITY_LOG_FILE = SECURITY_PREFIX + "log-file";
  String SECURITY_LOG_LEVEL = SECURITY_PREFIX + "log-level";
  String SECURITY_PEER_AUTH_INIT = SECURITY_PREFIX + "peer-auth-init";
  String SECURITY_PEER_AUTHENTICATOR = SECURITY_PREFIX + "peer-authenticator";
  String SECURITY_PEER_VERIFY_MEMBER_TIMEOUT = SECURITY_PREFIX + "peer-verifymember-timeout";
  String SERVER_BIND_ADDRESS = "server-bind-address";
  String SERVER_SSL_CIPHERS = "server-ssl-ciphers";
  String SERVER_SSL_ENABLED = "server-ssl-enabled";
  String SERVER_SSL_KEYSTORE = "server-ssl-keystore";
  String SERVER_SSL_KEYSTORE_PASSWORD = "server-ssl-keystore-password";
  String SERVER_SSL_KEYSTORE_TYPE = "server-ssl-keystore-type";
  String SERVER_SSL_PROTOCOLS = "server-ssl-protocols";
  String SERVER_SSL_REQUIRE_AUTHENTICATION = "server-ssl-require-authentication";
  String SERVER_SSL_TRUSTSTORE = "server-ssl-truststore";
  String SERVER_SSL_TRUSTSTORE_PASSWORD = "server-ssl-truststore-password";
  String SOCKET_BUFFER_SIZE = "socket-buffer-size";
  String SOCKET_LEASE_TIME = "socket-lease-time";
  String START_DEV_REST_API = "start-dev-rest-api";
  String START_LOCATOR = "start-locator";
  String STATISTIC_ARCHIVE_FILE = "statistic-archive-file";
  String STATISTIC_SAMPLE_RATE = "statistic-sample-rate";
  String STATISTIC_SAMPLING_ENABLED = "statistic-sampling-enabled";
  String TCP_PORT = "tcp-port";
  String UDP_FRAGMENT_SIZE = "udp-fragment-size";
  String UDP_RECV_BUFFER_SIZE = "udp-recv-buffer-size";
  String UDP_SEND_BUFFER_SIZE = "udp-send-buffer-size";
  String USE_CLUSTER_CONFIGURATION = "use-cluster-configuration";
  String USER_COMMAND_PACKAGES = "user-command-packages";
  String OFF_HEAP_MEMORY_SIZE = "off-heap-memory-size";

  String REDIS_PORT = "redis-port";
  String REDIS_BIND_ADDRESS = "redis-bind-address";
  String REDIS_PASSWORD = "redis-password";
  String LOCK_MEMORY = "lock-memory";
  String SECURITY_SHIRO_INIT = SECURITY_PREFIX + "shiro-init";
  String DISTRIBUTED_TRANSACTIONS = "distributed-transactions";

  @Deprecated
  String SSL_ENABLED = "ssl-enabled";
  @Deprecated
  String SSL_PROTOCOLS = "ssl-protocols";
  @Deprecated
  String SSL_CIPHERS = "ssl-ciphers";
  @Deprecated
  String SSL_REQUIRE_AUTHENTICATION = "ssl-require-authentication";

}
