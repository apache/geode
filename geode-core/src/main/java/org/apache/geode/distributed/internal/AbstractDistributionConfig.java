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

import static org.apache.geode.distributed.ConfigurationProperties.ACK_SEVERE_ALERT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.ACK_WAIT_THRESHOLD;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ASYNC_DISTRIBUTION_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.ASYNC_MAX_QUEUE_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.ASYNC_QUEUE_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CONFLATE_EVENTS;
import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.DELTA_PROPAGATION;
import static org.apache.geode.distributed.ConfigurationProperties.DEPLOY_WORKING_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_JMX;
import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_TCP;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_TRANSACTIONS;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_MANAGEMENT_REST_SERVICE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.ENFORCE_UNIQUE_HOST;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_ACCESS_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HTTP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PASSWORD_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_UPDATE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.LOAD_CLUSTER_CONFIGURATION_FROM_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATOR_WAIT_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.LOCK_MEMORY;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_NUM_RECONNECT_TRIES;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_FLOW_CONTROL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_RECV_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_SEND_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_TTL;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBERSHIP_PORT_RANGE;
import static org.apache.geode.distributed.ConfigurationProperties.MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MEMCACHED_PROTOCOL;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOVE_UNRESPONSIVE_CLIENT;
import static org.apache.geode.distributed.ConfigurationProperties.ROLES;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR_PP;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_DHALGO;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_VERIFY_MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PREFIX;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_UDP_DHALGO;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.SOCKET_LEASE_TIME;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CLUSTER_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_DEFAULT_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_GATEWAY_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_JMX_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_LOCATOR_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PARAMETER_EXTENSION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_SERVER_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_USE_DEFAULT_CONTEXT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.TCP_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.THREAD_MONITOR_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.THREAD_MONITOR_INTERVAL;
import static org.apache.geode.distributed.ConfigurationProperties.THREAD_MONITOR_TIME_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.UDP_FRAGMENT_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.UDP_RECV_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.UDP_SEND_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.USER_COMMAND_PACKAGES;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.InvalidValueException;
import org.apache.geode.UnmodifiableException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.AbstractConfig;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.LogLevel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Provides an implementation of <code>DistributionConfig</code> that knows how to read the
 * configuration file.
 * <p>
 * Note that if you add a property to this interface, you should update the
 * {@link AbstractConfig#sameAs} method and the
 * {@link DistributionConfigImpl#DistributionConfigImpl(DistributionConfig) copy constructor}.
 */
@SuppressWarnings("deprecation")
public abstract class AbstractDistributionConfig extends AbstractConfig
    implements DistributionConfig {

  private static final Logger logger = LogService.getLogger();

  Object checkAttribute(String attName, Object value) {
    // first check to see if this attribute is modifiable, this also checks if the attribute is a
    // valid one.
    if (!isAttributeModifiable(attName)) {
      throw new UnmodifiableException(_getUnmodifiableMsg(attName));
    }

    ConfigAttribute attribute = attributes.get(attName);
    if (attribute == null) {
      // isAttributeModifiable already checks the validity of the attName, if reached here, then
      // they
      // must be those special attributes that starts with ssl_system_props or sys_props, no further
      // checking needed
      return value;
    }
    // for integer attribute, do the range check.
    if (attribute.type().equals(Integer.class)) {
      Integer intValue = (Integer) value;
      minMaxCheck(attName, intValue, attribute.min(), attribute.max());
    }

    Method checker = checkers.get(attName);
    if (checker == null) {
      return value;
    }

    // if specific checker exists for this attribute, call that with the value
    try {
      return checker.invoke(this, value);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw new InternalGemFireException(
            "error invoking " + checker.getName() + " with value " + value);
      }
    }
  }

  private void minMaxCheck(String propName, int value, int minValue, int maxValue) {
    if (value < minValue) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set \"%s\" to \"%s\" because its value can not be less than \"%s\".",

              propName, value, minValue));
    } else if (value > maxValue) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set \"%s\" to \"%s\" because its value can not be greater than \"%s\".",
              propName, value, maxValue));
    }
  }

  @ConfigAttributeChecker(name = START_LOCATOR)
  protected String checkStartLocator(String value) {
    if (value != null && value.trim().length() > 0) {
      // throws IllegalArgumentException if string is malformed
      new DistributionLocatorId(value);
    }
    return value;
  }

  @ConfigAttributeChecker(name = TCP_PORT)
  protected int checkTcpPort(int value) {
    if (getClusterSSLEnabled() && value != 0) {
      throw new IllegalArgumentException(
          String.format("Could not set %s to %s because its value must be 0 when %s is true.",
              TCP_PORT, value, CLUSTER_SSL_ENABLED));
    }
    return value;
  }

  @ConfigAttributeChecker(name = MCAST_PORT)
  protected int checkMcastPort(int value) {
    if (getClusterSSLEnabled() && value != 0) {
      throw new IllegalArgumentException(
          String.format("Could not set %s to %s because its value must be 0 when %s is true.",
              MCAST_PORT, value, CLUSTER_SSL_ENABLED));
    }
    return value;
  }

  @ConfigAttributeChecker(name = MCAST_ADDRESS)
  protected InetAddress checkMcastAddress(InetAddress value) {
    if (!value.isMulticastAddress()) {
      throw new IllegalArgumentException(
          String.format("Could not set %s to %s because it was not a multicast address.",
              MCAST_ADDRESS, value));
    }
    return value;
  }

  @ConfigAttributeChecker(name = BIND_ADDRESS)
  protected String checkBindAddress(String value) {
    if (value != null && value.length() > 0 && !LocalHostUtil.isLocalHost(value)) {
      throw new IllegalArgumentException(
          String.format(
              "The bind-address %s is not a valid address for this machine.  These are the valid addresses for this machine: %s",
              value, LocalHostUtil.getMyAddresses()));
    }
    return value;
  }

  @ConfigAttributeChecker(name = SERVER_BIND_ADDRESS)
  protected String checkServerBindAddress(String value) {
    if (value != null && value.length() > 0 && !LocalHostUtil.isLocalHost(value)) {
      throw new IllegalArgumentException(
          String.format(
              "The bind-address %s is not a valid address for this machine.  These are the valid addresses for this machine: %s",
              value, LocalHostUtil.getMyAddresses()));
    }
    return value;
  }

  @ConfigAttributeChecker(name = CLUSTER_SSL_ENABLED)
  protected Boolean checkClusterSSLEnabled(Boolean value) {
    if (value && getMcastPort() != 0) {
      throw new IllegalArgumentException(
          String.format("Could not set %s to %s because its value must be false when %s is not 0.",
              CLUSTER_SSL_ENABLED, value, MCAST_PORT));
    }
    return value;
  }

  @ConfigAttributeChecker(name = HTTP_SERVICE_BIND_ADDRESS)
  protected String checkHttpServiceBindAddress(String value) {
    if (value != null && value.length() > 0 && !LocalHostUtil.isLocalHost(value)) {
      throw new IllegalArgumentException(
          String.format(
              "The bind-address %s is not a valid address for this machine.  These are the valid addresses for this machine: %s",
              value, LocalHostUtil.getMyAddresses()));
    }
    return value;
  }

  @ConfigAttributeChecker(name = DISTRIBUTED_SYSTEM_ID)
  protected int checkDistributedSystemId(int value) {
    String distributedSystemListener =
        System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "DistributedSystemListener");
    // this check is specific for Jayesh's use case of WAN BootStrapping
    if (distributedSystemListener == null) {
      if (value < MIN_DISTRIBUTED_SYSTEM_ID) {
        throw new IllegalArgumentException(
            String.format(
                "Could not set \"%s\" to \"%s\" because its value can not be less than \"%s\".",
                DISTRIBUTED_SYSTEM_ID, value,
                MIN_DISTRIBUTED_SYSTEM_ID));
      }
    }
    if (value > MAX_DISTRIBUTED_SYSTEM_ID) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set \"%s\" to \"%s\" because its value can not be greater than \"%s\".",
              DISTRIBUTED_SYSTEM_ID, value,
              MAX_DISTRIBUTED_SYSTEM_ID));
    }
    return value;
  }

  /**
   * Makes sure that the locator string used to configure discovery is valid.
   * <p>
   * Starting in 4.0, we accept locators in the format "host:port" in addition to the traditional
   * "host:bind-address[port]" format. See bug 32306.
   * <p>
   * Starting in 5.1.0.4, we accept locators in the format "host@bind-address[port]" to allow use of
   * numeric IPv6 addresses
   *
   * @return The locator string in the traditional "host:bind-address[port]" format.
   *
   * @throws IllegalArgumentException If <code>value</code> is not a valid locator configuration
   */
  @ConfigAttributeChecker(name = LOCATORS)
  protected String checkLocators(String value) {
    // validate locators value
    StringBuilder sb = new StringBuilder();

    Set<InetSocketAddress> locs = new HashSet<>();

    StringTokenizer st = new StringTokenizer(value, ",");
    boolean firstUniqueLocator = true;
    while (st.hasMoreTokens()) {
      String locator = st.nextToken();
      // string for this locator is accumulated in this buffer
      StringBuilder locatorsb = new StringBuilder();

      int portIndex = locator.indexOf('[');
      if (portIndex < 1) {
        portIndex = locator.lastIndexOf(':');
      }
      if (portIndex < 1) {
        throw new IllegalArgumentException(
            String.format("Invalid locator %s. Host name was empty.",
                value));
      }

      // starting in 5.1.0.4 we allow '@' as the bind-addr separator
      // to let people use IPv6 numeric addresses (which contain colons)
      int bindAddrIdx = locator.lastIndexOf('@', portIndex - 1);

      if (bindAddrIdx < 0) {
        bindAddrIdx = locator.lastIndexOf(':', portIndex - 1);
      }

      String host = locator.substring(0, bindAddrIdx > -1 ? bindAddrIdx : portIndex);

      if (host.indexOf(':') >= 0) {
        bindAddrIdx = locator.lastIndexOf('@');
        host = locator.substring(0, bindAddrIdx > -1 ? bindAddrIdx : portIndex);
      }

      InetAddress hostAddress = null;

      try {
        hostAddress = InetAddress.getByName(host);

      } catch (UnknownHostException ex) {
        logger.warn("Unknown locator host: " + host);
      }

      locatorsb.append(host);

      if (bindAddrIdx > -1) {
        // validate the bindAddress... (console needs this)
        String bindAddr = locator.substring(bindAddrIdx + 1, portIndex);
        try {
          hostAddress = InetAddress.getByName(bindAddr);

        } catch (UnknownHostException ex) {
          throw new IllegalArgumentException(
              String.format("Unknown locator bind address: %s",
                  bindAddr));
        }

        if (bindAddr.indexOf(':') >= 0) {
          locatorsb.append('@');
        } else {
          locatorsb.append(':');
        }
        locatorsb.append(bindAddr);
      }

      int lastIndex = locator.lastIndexOf(']');
      if (lastIndex == -1) {
        if (locator.indexOf('[') >= 0) {
          throw new IllegalArgumentException(
              String.format("Invalid locator: %s",
                  value));

        } else {
          // Using host:port syntax
          lastIndex = locator.length();
        }
      }

      String port = locator.substring(portIndex + 1, lastIndex);
      int portVal;
      try {
        portVal = Integer.parseInt(port);
        if (0 == portVal) {
          return "";
        } else if (portVal < 1 || portVal > 65535) {
          throw new IllegalArgumentException(
              String.format(
                  "Invalid locator %s. The port %s was not greater than zero and less than 65,536.",
                  value, portVal));
        }
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException(
            String.format("Invalid locator: %s", value));
      }

      locatorsb.append('[');
      locatorsb.append(port);
      locatorsb.append(']');

      // if this wasn't a duplicate, add it to the locators string
      InetSocketAddress sockAddr = new InetSocketAddress(hostAddress, portVal);
      if (!locs.contains(sockAddr)) {
        if (!firstUniqueLocator) {
          sb.append(',');
        } else {
          firstUniqueLocator = false;
        }
        locs.add(new InetSocketAddress(hostAddress, portVal));
        sb.append(locatorsb);
      }
    }

    return sb.toString();
  }

  /**
   * check a new mcast flow-control setting
   */
  @ConfigAttributeChecker(name = MCAST_FLOW_CONTROL)
  protected FlowControlParams checkMcastFlowControl(FlowControlParams params) {
    int value = params.getByteAllowance();
    if (value < MIN_FC_BYTE_ALLOWANCE) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set %s.byteAllowance to %s because its value can not be less than %s",
              MCAST_FLOW_CONTROL, value,
              MIN_FC_BYTE_ALLOWANCE));
    }
    float fvalue = params.getRechargeThreshold();
    if (fvalue < MIN_FC_RECHARGE_THRESHOLD) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set %s.rechargeThreshold to %s because its value can not be less than %s",
              MCAST_FLOW_CONTROL, fvalue,
              MIN_FC_RECHARGE_THRESHOLD));
    } else if (fvalue > MAX_FC_RECHARGE_THRESHOLD) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set %s.rechargeThreshold to %s because its value can not be greater than %s",
              MCAST_FLOW_CONTROL, fvalue,
              MAX_FC_RECHARGE_THRESHOLD));
    }
    value = params.getRechargeBlockMs();
    if (value < MIN_FC_RECHARGE_BLOCK_MS) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set %s.rechargeBlockMs to %s because its value can not be less than %s",
              MCAST_FLOW_CONTROL, value,
              MIN_FC_RECHARGE_BLOCK_MS));
    } else if (value > MAX_FC_RECHARGE_BLOCK_MS) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set %s.rechargeBlockMs to %s because its value can not be greater than %s",
              MCAST_FLOW_CONTROL, value,
              MAX_FC_RECHARGE_BLOCK_MS));
    }
    return params;
  }

  @ConfigAttributeChecker(name = MEMBERSHIP_PORT_RANGE)
  protected int[] checkMembershipPortRange(int[] value) {
    minMaxCheck(MEMBERSHIP_PORT_RANGE, value[0], 1024, value[1]);
    minMaxCheck(MEMBERSHIP_PORT_RANGE, value[1], value[0], 65535);

    // Minimum 3 ports are required to start a Gemfire data node,
    // One for each, UDP, FD_SOCk protocols and Cache Server.
    if (value[1] - value[0] < 2) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set \"%s\" to \"%s\" because its value can not be less than \"%s\".",
              MEMBERSHIP_PORT_RANGE, value[0] + "-" + value[1],
              3));
    }
    return value;
  }

  /**
   * @since GemFire 5.7
   */
  @ConfigAttributeChecker(name = CLIENT_CONFLATION_PROP_NAME)
  protected String checkClientConflation(String value) {
    if (!(value.equals(CLIENT_CONFLATION_PROP_VALUE_DEFAULT)
        || value.equals(CLIENT_CONFLATION_PROP_VALUE_ON)
        || value.equals(CLIENT_CONFLATION_PROP_VALUE_OFF))) {
      throw new IllegalArgumentException("Could not set \"" + CONFLATE_EVENTS + "\" to \"" + value
          + "\" because its value is not recognized");
    }
    return value;
  }

  @ConfigAttributeChecker(name = SECURITY_PEER_AUTH_INIT)
  protected String checkSecurityPeerAuthInit(String value) {
    if (value != null && value.length() > 0 && getMcastPort() != 0) {
      String mcastInfo = MCAST_PORT + "[" + getMcastPort() + "]";
      throw new IllegalArgumentException(
          String.format("Could not set %s to %s because %s must be 0 when security is enabled.",
              SECURITY_PEER_AUTH_INIT, value, mcastInfo));
    }
    return value;
  }

  @ConfigAttributeChecker(name = SECURITY_PEER_AUTHENTICATOR)
  protected String checkSecurityPeerAuthenticator(String value) {
    if (value != null && value.length() > 0 && getMcastPort() != 0) {
      String mcastInfo = MCAST_PORT + "[" + getMcastPort() + "]";
      throw new IllegalArgumentException(
          String.format("Could not set %s to %s because %s must be 0 when security is enabled.",
              SECURITY_PEER_AUTHENTICATOR, value, mcastInfo));
    }
    return value;
  }

  @ConfigAttributeChecker(name = SECURITY_LOG_LEVEL)
  protected int checkSecurityLogLevel(int value) {
    if (value < MIN_LOG_LEVEL) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set \"%s\" to \"%s\" because its value can not be less than \"%s\".",
              SECURITY_LOG_LEVEL,
              LogWriterImpl.levelToString(value), LogWriterImpl.levelToString(MIN_LOG_LEVEL)));
    }
    if (value > MAX_LOG_LEVEL) {
      throw new IllegalArgumentException(
          String.format(
              "Could not set \"%s\" to \"%s\" because its value can not be greater than \"%s\".",
              SECURITY_LOG_LEVEL,
              LogWriterImpl.levelToString(value), LogWriterImpl.levelToString(MAX_LOG_LEVEL)));
    }
    return value;
  }

  @ConfigAttributeChecker(name = MEMCACHED_PROTOCOL)
  protected String checkMemcachedProtocol(String protocol) {
    if (protocol == null
        || !protocol.equalsIgnoreCase("ASCII")
            && !protocol.equalsIgnoreCase("BINARY")) {
      throw new IllegalArgumentException(
          "memcached-protocol must be ASCII or BINARY ");
    }
    return protocol;
  }

  public boolean isMemcachedProtocolModifiable() {
    return false;
  }

  @ConfigAttributeChecker(name = MEMCACHED_BIND_ADDRESS)
  protected String checkMemcachedBindAddress(String value) {
    if (value != null && value.length() > 0 && !LocalHostUtil.isLocalHost(value)) {
      throw new IllegalArgumentException(
          String.format(
              "The memcached-bind-address %s is not a valid address for this machine.  These are the valid addresses for this machine: %s",
              value, LocalHostUtil.getMyAddresses()));
    }
    return value;
  }

  @ConfigAttributeChecker(name = REDIS_BIND_ADDRESS)
  protected String checkRedisBindAddress(String value) {
    if (value != null && value.length() > 0 && !LocalHostUtil.isLocalHost(value)) {
      throw new IllegalArgumentException(
          String.format(
              "The redis-bind-address %s is not a valid address for this machine.  These are the valid addresses for this machine: %s",
              value, LocalHostUtil.getMyAddresses()));
    }
    return value;
  }

  /**
   * First check if sslComponents are in the list of valid components. If so, check that no other
   * *-ssl-* properties other than cluster-ssl-* are set. This would mean one is mixing the "old"
   * with the "new"
   */
  @ConfigAttributeChecker(name = SSL_ENABLED_COMPONENTS)
  protected SecurableCommunicationChannel[] checkLegacySSLWhenSSLEnabledComponentsSet(
      SecurableCommunicationChannel[] value) {
    for (SecurableCommunicationChannel component : value) {
      switch (component) {
        case ALL:
        case CLUSTER:
        case SERVER:
        case GATEWAY:
        case JMX:
        case WEB:
        case LOCATOR:
          continue;
        default:
          throw new IllegalArgumentException(
              String.format("%s is not in the valid set of options %s",
                  Arrays.toString(value),
                  StringUtils
                      .join(new String[] {SecurableCommunicationChannel.ALL.getConstant(),
                          SecurableCommunicationChannel.CLUSTER.getConstant(),
                          SecurableCommunicationChannel.SERVER.getConstant(),
                          SecurableCommunicationChannel.GATEWAY.getConstant(),
                          SecurableCommunicationChannel.JMX.getConstant(),
                          SecurableCommunicationChannel.WEB.getConstant(),
                          SecurableCommunicationChannel.LOCATOR.getConstant()}, ",")));
      }
    }
    if (value.length > 0) {
      if (getClusterSSLEnabled() || getJmxManagerSSLEnabled() || getHttpServiceSSLEnabled()
          || getServerSSLEnabled() || getGatewaySSLEnabled()) {
        throw new IllegalArgumentException(
            "When using ssl-enabled-components one cannot use any other SSL properties other than cluster-ssl-* or the corresponding aliases");
      }
    }
    return value;
  }

  @Override
  protected void checkAttributeName(String attName) {
    if (!attName.startsWith(SECURITY_PREFIX) && !attName.startsWith(USERDEFINED_PREFIX_NAME)
        && !attName.startsWith(SSL_SYSTEM_PROPS_NAME) && !attName.startsWith(SYS_PROP_NAME)) {
      super.checkAttributeName(attName);
    }
  }

  public static boolean isWellKnownAttribute(String attName) {
    return Arrays.binarySearch(dcValidAttributeNames, attName) >= 0;
  }

  @Override
  public void setAttributeObject(String attName, Object attValue, ConfigSource source) {
    // TODO: the setters is already checking the parameter type, do we still need to do this?
    Class validValueClass = getAttributeType(attName);
    if (attValue != null) {
      // null is a "valid" value for any class
      if (!validValueClass.isInstance(attValue)) {
        throw new InvalidValueException(
            String.format("%s value %s must be of type %s",
                attName, attValue, validValueClass.getName()));
      }
    }

    if (attName.startsWith(USERDEFINED_PREFIX_NAME)) {
      // Do nothing its user defined property.
      return;
    }

    // special case: log-level and security-log-level attributes are String type, but the setter
    // accepts int
    if (attName.equalsIgnoreCase(LOG_LEVEL) || attName.equalsIgnoreCase(SECURITY_LOG_LEVEL)) {
      if (attValue instanceof String) {
        attValue = LogLevel.getLogWriterLevel((String) attValue);
      }
    }

    if (attName.startsWith(SECURITY_PREFIX)) {
      // some security properties will be an array, such as security-auth-token-enabled-components
      if (attValue instanceof Object[]) {
        setSecurity(attName, StringUtils.join((Object[]) attValue, ','));
      } else {
        setSecurity(attName, attValue.toString());
      }
    }

    if (attName.startsWith(SSL_SYSTEM_PROPS_NAME) || attName.startsWith(SYS_PROP_NAME)) {
      setSSLProperty(attName, attValue.toString());
    }

    Method setter = setters.get(attName);
    if (setter == null) {
      // if we cann't find the defined setter, but the attributeName starts with these special
      // characters
      // since we already set it in the respecitive properties above, we need to set the source then
      // return
      if (attName.startsWith(SECURITY_PREFIX) || attName.startsWith(SSL_SYSTEM_PROPS_NAME)
          || attName.startsWith(SYS_PROP_NAME)) {
        getAttSourceMap().put(attName, source);
        return;
      }
      throw new InternalGemFireException(
          String.format("unhandled attribute name %s.",
              attName));
    }

    Class[] pTypes = setter.getParameterTypes();
    if (pTypes.length != 1) {
      throw new InternalGemFireException(
          "the attribute setter must have one and only one parametter");
    }

    checkAttribute(attName, attValue);
    try {
      setter.invoke(this, attValue);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw new InternalGemFireException(
            "error invoking " + setter.getName() + " with " + attValue, e);
      }
    }

    getAttSourceMap().put(attName, source);
  }

  @Override
  public Object getAttributeObject(String attName) {
    checkAttributeName(attName);

    // special case:
    if (attName.equalsIgnoreCase(LOG_LEVEL)) {
      return LogWriterImpl.levelToString(getLogLevel());
    }

    if (attName.equalsIgnoreCase(SECURITY_LOG_LEVEL)) {
      return LogWriterImpl.levelToString(getSecurityLogLevel());
    }

    Method getter = getters.get(attName);
    if (getter == null) {
      if (attName.startsWith(SECURITY_PREFIX)) {
        return getSecurity(attName);
      }
      throw new InternalGemFireException(
          String.format("unhandled attribute name %s.",
              attName));
    }

    try {
      return getter.invoke(this);
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      } else {
        throw new InternalGemFireException("error invoking " + getter.getName(), e);
      }
    }
  }

  @Override
  public boolean isAttributeModifiable(String name) {
    checkAttributeName(name);
    if (getModifiableAttributes().contains(name)) {
      return true;
    }

    if (getUnModifiableAttributes().contains(name)) {
      return false;
    }
    // otherwise, return the default
    return _modifiableDefault();
  }

  /**
   * child class can override this method to return a list of modifiable attributes no matter what
   * the default is
   *
   * @return an empty list
   */
  public List<String> getModifiableAttributes() {
    String[] modifiables = {HTTP_SERVICE_PORT, JMX_MANAGER_HTTP_PORT};
    return Arrays.asList(modifiables);
  }


  /**
   * child class can override this method to return a list of unModifiable attributes no matter what
   * the default is
   *
   * @return an empty list
   */
  private List<String> getUnModifiableAttributes() {
    return new ArrayList<>();
  }

  @Override
  public Class getAttributeType(String attName) {
    checkAttributeName(attName);
    return _getAttributeType(attName);
  }

  static Class _getAttributeType(String attName) {
    ConfigAttribute ca = attributes.get(attName);
    if (ca == null) {
      if (attName.startsWith(SECURITY_PREFIX) || attName.startsWith(SSL_SYSTEM_PROPS_NAME)
          || attName.startsWith(SYS_PROP_NAME)) {
        return String.class;
      }
      throw new InternalGemFireException(
          String.format("unhandled attribute name %s.",
              attName));
    }
    return ca.type();
  }

  @Immutable
  static final Map dcAttDescriptions;

  static {
    Map<String, String> m = new HashMap<>();

    m.put(ACK_WAIT_THRESHOLD,
        String.format(
            "The number of seconds a distributed message can wait for acknowledgment before it sends an alert to signal that something might be wrong with the system node that is unresponsive. After sending this alert the waiter continues to wait. The alerts are logged in the system log as warnings and if a gfc is running will cause a console alert to be signalled.  Defaults to %s.  Legal values are in the range [%s..%s].",
            DEFAULT_ACK_WAIT_THRESHOLD,
            MIN_ACK_WAIT_THRESHOLD, MIN_ACK_WAIT_THRESHOLD));

    m.put(ARCHIVE_FILE_SIZE_LIMIT,
        "The maximum size in megabytes of a statistic archive file. Once this limit is exceeded, a new statistic archive file is created, and the current archive file becomes inactive. If set to zero, file size is unlimited.");

    m.put(ACK_SEVERE_ALERT_THRESHOLD,
        String.format(
            "The number of seconds a distributed message can wait for acknowledgment past %s before it ejects unresponsive members from the distributed system.  Defaults to %s.  Legal values are in the range [%s..%s].",
            ACK_WAIT_THRESHOLD,
            DEFAULT_ACK_SEVERE_ALERT_THRESHOLD,
            MIN_ACK_SEVERE_ALERT_THRESHOLD,
            MAX_ACK_SEVERE_ALERT_THRESHOLD));

    m.put(ARCHIVE_DISK_SPACE_LIMIT,
        "The maximum size in megabytes of all inactive statistic archive files combined. If this limit is exceeded, inactive archive files will be deleted, oldest first, until the total size is within the limit. If set to zero, disk space usage is unlimited.");

    m.put(CACHE_XML_FILE, String.format(
        "The file whose contents is used, by default, to initialize a cache if one is created.  Defaults to %s.",
        DEFAULT_CACHE_XML_FILE));

    m.put(DISABLE_TCP, String.format(
        "Determines whether TCP/IP communications will be disabled, forcing use of datagrams between members of the distributed system. Defaults to %s",
        Boolean.FALSE));

    m.put(DISABLE_JMX, String.format(
        "Determines whether JMX will be disabled which prevents Geode from creating MBeans. Defaults to %s",
        Boolean.FALSE));

    m.put(ENABLE_TIME_STATISTICS,
        "Turns on timings in distribution and cache statistics.  These are normally turned off to avoid expensive clock probes.");

    m.put(DEPLOY_WORKING_DIR, String.format(
        "The working directory that can be used to persist JARs deployed during runtime. Defaults to %s.",
        DEFAULT_DEPLOY_WORKING_DIR));

    m.put(LOG_FILE,
        String.format("The file a running system will write log messages to.  Defaults to %s.",
            DEFAULT_LOG_FILE));

    m.put(LOG_LEVEL,
        String.format(
            "Controls the type of messages that will actually be written to the system log.  Defaults to %s.  Allowed values %s.",
            LogWriterImpl.levelToString(DEFAULT_LOG_LEVEL),
            LogWriterImpl.allowedLogLevels()));

    m.put(LOG_FILE_SIZE_LIMIT,
        "The maximum size in megabytes of a child log file. Once this limit is exceeded, a new child log is created, and the current child log becomes inactive. If set to zero, child logging is disabled.");

    m.put(LOG_DISK_SPACE_LIMIT,
        "The maximum size in megabytes of all inactive log files combined. If this limit is exceeded, inactive log files will be deleted, oldest first, until the total size is within the limit. If set to zero, disk space usage is unlimited.");

    m.put(LOCATORS, String.format(
        "A possibly empty list of locators used to find other system nodes. Each element of the list must be a host name followed by bracketed, [], port number. Host names may be followed by a colon and a bind address used by the locator on that host.  Multiple elements must be comma separated. Defaults to %s.",
        DEFAULT_LOCATORS));

    m.put(LOCATOR_WAIT_TIME, String.format(
        "The amount of time, in seconds, to wait for a locator to be available before throwing an exception during startup.  The default is %s.",
        DEFAULT_LOCATOR_WAIT_TIME));

    m.put(TCP_PORT,
        String.format(
            "The port used for tcp/ip communcations in the distributed system. If zero then a random available port is selected by the operating system.   Defaults to %s.  Legal values are in the range [%s..%s].",
            DEFAULT_TCP_PORT,
            MIN_TCP_PORT, MAX_TCP_PORT));

    m.put(MCAST_PORT,
        String.format(
            "The port used for multicast communcations in the distributed system. If zero then locators are used, and multicast is disabled.   Defaults to %s.  Legal values are in the range [%s..%s].",
            DEFAULT_MCAST_PORT,
            MIN_MCAST_PORT, MAX_MCAST_PORT));

    m.put(MCAST_ADDRESS,
        String.format(
            "The address used for multicast communications. Only used if %s is non-zero.  Defaults to %s.",
            DEFAULT_MCAST_PORT, DEFAULT_MCAST_ADDRESS));

    m.put(MCAST_TTL,
        String.format(
            "Determines how far through your network mulicast packets will propogate. Defaults to %s.  Legal values are in the range [%s..%s].",
            DEFAULT_MCAST_TTL,
            MIN_MCAST_TTL, MAX_MCAST_TTL));

    m.put(MCAST_SEND_BUFFER_SIZE,
        String.format(
            "Sets the size of multicast socket transmission buffers, in bytes.  Defaults to %s but this may be limited by operating system settings",
            DEFAULT_MCAST_SEND_BUFFER_SIZE));

    m.put(MCAST_RECV_BUFFER_SIZE,
        String.format(
            "Sets the size of multicast socket receive buffers, in bytes.  Defaults to %s but this may be limited by operating system settings",
            DEFAULT_MCAST_RECV_BUFFER_SIZE));

    m.put(MCAST_FLOW_CONTROL, String.format(
        "Sets the flow-of-control parameters for multicast messaging.  Defaults to %s.",
        DEFAULT_MCAST_FLOW_CONTROL));

    m.put(MEMBER_TIMEOUT, String.format(
        "Sets the number of milliseconds to wait for ping responses when determining whether another member is still alive. Defaults to %s.",
        DEFAULT_MEMBER_TIMEOUT));

    String srange = "" + DEFAULT_MEMBERSHIP_PORT_RANGE[0] + "-" + DEFAULT_MEMBERSHIP_PORT_RANGE[1];
    String msg = String.format(
        "Sets the range of datagram socket ports that can be used for membership ID purposes and unicast datagram messaging. Defaults to %s.",
        srange);
    m.put(MEMBERSHIP_PORT_RANGE, msg);

    m.put(UDP_SEND_BUFFER_SIZE,
        String.format(
            "Sets the size of datagram socket transmission buffers, in bytes.  Defaults to %s but this may be limited by operating system settings",
            DEFAULT_UDP_SEND_BUFFER_SIZE));

    m.put(UDP_RECV_BUFFER_SIZE,
        String.format(
            "Sets the size of datagram socket receive buffers, in bytes. Defaults to %s but this may be limited by operating system settings",
            DEFAULT_UDP_RECV_BUFFER_SIZE));

    m.put(UDP_FRAGMENT_SIZE, String.format(
        "Sets the maximum size of a datagram for UDP and multicast transmission.  Defaults to %s.",
        DEFAULT_UDP_FRAGMENT_SIZE));

    m.put(SOCKET_LEASE_TIME,
        String.format(
            "The number of milliseconds a thread can keep exclusive access to a socket that it is not actively using. Once a thread loses its lease to a socket it will need to re-acquire a socket the next time it sends a message. A value of zero causes socket leases to never expire. Defaults to %s .  Legal values are in the range [%s..%s].",
            DEFAULT_SOCKET_LEASE_TIME,
            MIN_SOCKET_LEASE_TIME, MAX_SOCKET_LEASE_TIME));

    m.put(SOCKET_BUFFER_SIZE,
        String.format(
            "The size of each socket buffer, in bytes. Smaller buffers conserve memory. Larger buffers can improve performance; in particular if large messages are being sent. Defaults to %s.  Legal values are in the range [%s..%s].",
            DEFAULT_SOCKET_BUFFER_SIZE,
            MIN_SOCKET_BUFFER_SIZE, MAX_SOCKET_BUFFER_SIZE));

    m.put(CONSERVE_SOCKETS, String.format(
        "If true then a minimal number of sockets will be used when connecting to the distributed system. This conserves resource usage but can cause performance to suffer. If false, the default, then every application thread that sends distribution messages to other members of the distributed system will own its own sockets and have exclusive access to them. Defaults to %s.",
        DEFAULT_CONSERVE_SOCKETS));

    m.put(ROLES,
        String.format(
            "The application roles that this member performs in the distributed system. This is a comma delimited list of user-defined strings. Any number of members can be configured to perform the same role, and a member can be configured to perform any number of roles. Defaults to %s.",
            DEFAULT_ROLES));

    m.put(BIND_ADDRESS, String.format(
        "The address server sockets will listen on. An empty string causes the server socket to listen on all local addresses. Defaults to %s.",
        DEFAULT_BIND_ADDRESS));

    m.put(SERVER_BIND_ADDRESS,
        String.format(
            "The address server sockets in a client-server topology will listen on. An empty string causes the server socket to listen on all local addresses. Defaults to %s.",
            DEFAULT_BIND_ADDRESS));

    m.put(NAME,
        "A name that uniquely identifies a member in its distributed system."
            + " Multiple members in the same distributed system can not have the same name."
            + " Defaults to \"\".");

    m.put(STATISTIC_ARCHIVE_FILE,
        String.format("The file a running system will write statistic samples to.  Defaults to %s.",
            DEFAULT_STATISTIC_ARCHIVE_FILE));

    m.put(STATISTIC_SAMPLE_RATE,
        String.format(
            "The rate, in milliseconds, that a running system will sample statistics.  Defaults to %s.  Legal values are in the range [%s..%s].",
            DEFAULT_STATISTIC_SAMPLE_RATE,
            MIN_STATISTIC_SAMPLE_RATE,
            MAX_STATISTIC_SAMPLE_RATE));

    m.put(STATISTIC_SAMPLING_ENABLED,
        String.format(
            "If false then archiving is disabled and operating system statistics are no longer updated.  Defaults to %s.",
            Boolean.TRUE));

    m.put(SSL_CLUSTER_ALIAS, String.format(
        "SSL communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to %s.",
        Boolean.valueOf(DEFAULT_SSL_ALIAS)));

    m.put(CLUSTER_SSL_ENABLED, String.format(
        "Communication is performed through SSL when this property is set to true. Defaults to %s.",
        Boolean.FALSE));

    m.put(CLUSTER_SSL_PROTOCOLS, String.format(
        "List of available SSL protocols that are to be enabled. Defaults to %s meaning your provider's defaults.",
        DEFAULT_SSL_PROTOCOLS));

    m.put(CLUSTER_SSL_CIPHERS, String.format(
        "List of available SSL cipher suites that are to be enabled. Defaults to %s meaning your provider's defaults.",
        DEFAULT_SSL_CIPHERS));

    m.put(CLUSTER_SSL_REQUIRE_AUTHENTICATION,
        String.format(
            "if set to false, ciphers and protocols that permit anonymous peers are allowed. Defaults to %s.",
            Boolean.TRUE));

    m.put(CLUSTER_SSL_KEYSTORE,
        "Location of the Java keystore file containing an distributed member's own certificate and private key.");

    m.put(CLUSTER_SSL_KEYSTORE_TYPE,
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(CLUSTER_SSL_KEYSTORE_PASSWORD,
        "Password to access the private key from the keystore file specified by javax.net.ssl.keyStore.");

    m.put(CLUSTER_SSL_TRUSTSTORE,
        "Location of the Java keystore file containing the collection of CA certificates trusted by distributed member (trust store).");

    m.put(CLUSTER_SSL_TRUSTSTORE_PASSWORD,
        "Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");

    m.put(MAX_WAIT_TIME_RECONNECT,
        "Specifies the maximum time to wait before trying to reconnect to distributed system in the case of required role loss.");

    m.put(MAX_NUM_RECONNECT_TRIES,
        "Maximum number of tries before shutting the member down in the case of required role loss.");

    m.put(ASYNC_DISTRIBUTION_TIMEOUT,
        String.format(
            "The number of milliseconds before a publishing process should attempt to distribute a cache operation before switching over to asynchronous messaging for this process. Defaults to %s. Legal values are in the range [%s..%s].",
            DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT,
            MIN_ASYNC_DISTRIBUTION_TIMEOUT,
            MAX_ASYNC_DISTRIBUTION_TIMEOUT));

    m.put(ASYNC_QUEUE_TIMEOUT,
        String.format(
            "The number of milliseconds a queuing may enqueue asynchronous messages without any distribution to this process before that publisher requests this process to depart. Defaults to %s Legal values are in the range [%s..%s].",
            DEFAULT_ASYNC_QUEUE_TIMEOUT,
            MIN_ASYNC_QUEUE_TIMEOUT,
            MAX_ASYNC_QUEUE_TIMEOUT));

    m.put(ASYNC_MAX_QUEUE_SIZE,
        String.format(
            "The maximum size in megabytes that a publishing process should be allowed to asynchronously enqueue for this process before asking this process to depart from the distributed system. Defaults to %s. Legal values are in the range [%s..%s].",
            DEFAULT_ASYNC_MAX_QUEUE_SIZE,
            MIN_ASYNC_MAX_QUEUE_SIZE,
            MAX_ASYNC_MAX_QUEUE_SIZE));

    m.put(START_LOCATOR,
        "The host|bindAddress[port] of a Locator to start in this VM along with the DistributedSystem. The default is to not start a Locator.");

    m.put(DURABLE_CLIENT_ID, String.format(
        "An id used by durable clients to identify themselves as durable to servers. Defaults to %s.",
        DEFAULT_DURABLE_CLIENT_ID));

    m.put(CONFLATE_EVENTS, "Client override for server queue conflation setting");

    m.put(DURABLE_CLIENT_TIMEOUT,
        String.format(
            "The value (in seconds) used by the server to keep disconnected durable clients alive. Defaults to %s.",
            DEFAULT_DURABLE_CLIENT_TIMEOUT));

    m.put(SECURITY_CLIENT_AUTH_INIT,
        String.format(
            "User defined fully qualified method name implementing AuthInitialize interface for client. Defaults to %s. Legal values can be any method name of a static method that is present in the classpath.",
            DEFAULT_SECURITY_CLIENT_AUTH_INIT));

    m.put(ENABLE_NETWORK_PARTITION_DETECTION, "Whether network partitioning detection is enabled");

    m.put(DISABLE_AUTO_RECONNECT, "Whether auto reconnect is attempted after a network partition");

    m.put(SECURITY_CLIENT_AUTHENTICATOR,
        String.format(
            "User defined fully qualified method name implementing Authenticator interface for client verification. Defaults to %s. Legal values can be any method name of a static method that is present in the classpath.",
            DEFAULT_SECURITY_CLIENT_AUTHENTICATOR));

    m.put(SECURITY_CLIENT_DHALGO,
        String.format(
            "User defined name for the symmetric encryption algorithm to use in Diffie-Hellman key exchange for encryption of credentials.  Defaults to %s. Legal values can be any of the available symmetric algorithm names in JDK like DES, DESede, AES, Blowfish. It may be required to install Unlimited Strength Jurisdiction Policy Files from Sun for some symmetric algorithms to work (like AES)",
            DEFAULT_SECURITY_CLIENT_DHALGO));

    m.put(SECURITY_UDP_DHALGO,
        String.format(
            "User defined name for the symmetric encryption algorithm to use in Diffie-Hellman key exchange for encryption of udp messages.  Defaults to %s. Legal values can be any of the available symmetric algorithm names in JDK like DES, DESede, AES, Blowfish. It may be required to install Unlimited Strength Jurisdiction Policy Files from Sun for some symmetric algorithms to work (like AES)",
            DEFAULT_SECURITY_UDP_DHALGO));

    m.put(SECURITY_PEER_AUTH_INIT,
        String.format(
            "User defined fully qualified method name implementing AuthInitialize interface for peer. Defaults to %s. Legal values can be any method name of a static method that is present in the classpath.",
            DEFAULT_SECURITY_PEER_AUTH_INIT));

    m.put(SECURITY_PEER_AUTHENTICATOR,
        String.format(
            "User defined fully qualified method name implementing Authenticator interface for peer verificaiton. Defaults to %s. Legal values can be any method name of a static method that is present in the classpath.",
            DEFAULT_SECURITY_PEER_AUTHENTICATOR));

    m.put(SECURITY_CLIENT_ACCESSOR,
        String.format(
            "User defined fully qualified method name implementing AccessControl interface for client authorization. Defaults to %s. Legal values can be any method name of a static method that is present in the classpath.",
            DEFAULT_SECURITY_CLIENT_ACCESSOR));

    m.put(SECURITY_CLIENT_ACCESSOR_PP,
        String.format(
            "User defined fully qualified method name implementing AccessControl interface for client authorization in post-processing phase. Defaults to %s. Legal values can be any method name of a static method that is present in the classpath.",
            DEFAULT_SECURITY_CLIENT_ACCESSOR_PP));

    m.put(SECURITY_LOG_LEVEL,
        String.format(
            "Controls the type of messages that will actually be written to the system security log. Defaults to %s.  Allowed values %s.",
            LogWriterImpl.levelToString(DEFAULT_LOG_LEVEL),
            LogWriterImpl.allowedLogLevels()));

    m.put(SECURITY_LOG_FILE, String.format(
        "The file a running system will write security log messages to. Defaults to %s.",
        DEFAULT_SECURITY_LOG_FILE));

    m.put(SECURITY_PEER_VERIFY_MEMBER_TIMEOUT,
        String.format(
            "The timeout value (in milliseconds) used by a peer to verify membership of an unknown authenticated peer requesting a secure connection. Defaults to %s milliseconds. The timeout value should not exceed peer handshake timeout.",
            DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT));

    m.put(SECURITY_PREFIX,
        "Prefix for security related properties which are packed together and invoked as authentication parameter. Neither key nor value can be NULL. Legal tags can be [security-username, security-digitalid] and Legal values can be any string data.");

    m.put(SECURITY_AUTH_TOKEN_ENABLED_COMPONENTS,
        "list of rest service to authenticate request with a Bearer token passed in the 'Authentication' header of the REST request. Otherwise BASIC authentication scheme is used. Possible value is a comma separated list of: 'all', 'management'. This property is ignored if 'security-manager' is not set. Default value is empty.");

    m.put(USERDEFINED_PREFIX_NAME,
        "Prefix for user defined properties which are used for replacements in Cache.xml. Neither key nor value can be NULL. Legal tags can be [custom-any-string] and Legal values can be any string data.");

    m.put(REMOVE_UNRESPONSIVE_CLIENT,
        String.format("Whether to remove unresponsive client or not. Defaults to %s.",
            DEFAULT_REMOVE_UNRESPONSIVE_CLIENT));

    m.put(DELTA_PROPAGATION, "Whether delta propagation is enabled");

    m.put(REMOTE_LOCATORS,
        String.format(
            "A possibly empty list of locators used to find other distributed systems. Each element of the list must be a host name followed by bracketed, [], port number. Host names may be followed by a colon and a bind address used by the locator on that host.  Multiple elements must be comma separated. Defaults to %s.",
            DEFAULT_REMOTE_LOCATORS));

    m.put(DISTRIBUTED_SYSTEM_ID,
        "An id that uniquely idenitifies this distributed system. "
            + "Required when using portable data exchange objects and the WAN."
            + "Must be the same on each member in this distributed system if set.");
    m.put(ENFORCE_UNIQUE_HOST, "Whether to require partitioned regions to put "
        + "redundant copies of data on different physical machines");

    m.put(REDUNDANCY_ZONE, "The zone that this member is in. When this is set, "
        + "partitioned regions will not put two copies of the same data in the same zone.");

    m.put(GROUPS,
        "A comma separated list of all the groups this member belongs to." + " Defaults to \"\".");

    m.put(USER_COMMAND_PACKAGES,
        "A comma separated list of the names of the packages containing classes that implement user commands.");

    m.put(JMX_MANAGER,
        "If true then this member is willing to be a jmx manager. Defaults to false except on a locator.");
    m.put(JMX_MANAGER_START,
        "If true then the jmx manager will be started when the cache is created. Defaults to false.");
    m.put(JMX_MANAGER_SSL_ENABLED,
        "If true then the jmx manager will only allow SSL clients to connect. Defaults to false. This property is ignored if jmx-manager-port is \"0\".");
    m.put(SSL_JMX_ALIAS, String.format(
        "SSL jmx communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to %s.",
        Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(JMX_MANAGER_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for JMX Manager. Defaults to \""
            + DEFAULT_JMX_MANAGER_SSL_CIPHERS + "\" meaning your provider's defaults.");
    m.put(JMX_MANAGER_SSL_PROTOCOLS,
        "List of available SSL protocols that are to be enabled for JMX Manager. Defaults to \""
            + DEFAULT_JMX_MANAGER_SSL_PROTOCOLS + "\" meaning defaults of your provider.");
    m.put(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION,
        "If set to false, ciphers and protocols that permit anonymous JMX Clients are allowed. Defaults to \""
            + DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION + "\".");
    m.put(JMX_MANAGER_SSL_KEYSTORE,
        "Location of the Java keystore file containing jmx manager's own certificate and private key.");
    m.put(JMX_MANAGER_SSL_KEYSTORE_TYPE,
        "For Java keystore file format, this property has the value jks (or JKS).");
    m.put(JMX_MANAGER_SSL_KEYSTORE_PASSWORD,
        "Password to access the private key from the keystore file specified by javax.net.ssl.keyStore. ");
    m.put(JMX_MANAGER_SSL_TRUSTSTORE,
        "Location of the Java keystore file containing the collection of CA certificates trusted by jmx manager.");
    m.put(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD,
        "Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");
    m.put(JMX_MANAGER_PORT,
        "The port the jmx manager will listen on. Default is \"" + DEFAULT_JMX_MANAGER_PORT
            + "\". Set to zero to disable GemFire's creation of a jmx listening port.");
    m.put(JMX_MANAGER_BIND_ADDRESS,
        "The address the jmx manager will listen on for remote connections. Default is \"\" which causes the jmx manager to listen on the host's default address. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_HOSTNAME_FOR_CLIENTS,
        "The hostname that will be given to clients when they ask a locator for the location of this jmx manager. Default is \"\" which causes the locator to report the jmx manager's actual ip address as its location. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_PASSWORD_FILE,
        "The name of the file the jmx manager will use to only allow authenticated clients to connect. Default is \"\" which causes the jmx manager to allow all clients to connect. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_ACCESS_FILE,
        "The name of the file the jmx manager will use to define the access level of authenticated clients. Default is \"\" which causes the jmx manager to allow all clients all access. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_HTTP_PORT,
        "By default when a jmx-manager is started it will also start an http server on this port. This server is used by the GemFire Pulse application. Setting this property to zero disables the http server. It defaults to 8080. Ignored if jmx-manager is false.");
    m.put(JMX_MANAGER_UPDATE_RATE,
        "The rate in milliseconds at which this member will send updates to each jmx manager. Default is "
            + DEFAULT_JMX_MANAGER_UPDATE_RATE + ". Values must be in the range "
            + MIN_JMX_MANAGER_UPDATE_RATE + ".." + MAX_JMX_MANAGER_UPDATE_RATE + ".");
    m.put(SSL_LOCATOR_ALIAS, String.format(
        "SSL locator communications uses this alias when determining the key to use from the keystore for SSL. Defaults to %s.",
        Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(MEMCACHED_PORT,
        "The port GemFireMemcachedServer will listen on. Default is 0. Set to zero to disable GemFireMemcachedServer.");
    m.put(MEMCACHED_PROTOCOL,
        "The protocol that GemFireMemcachedServer understands. Default is ASCII. Values may be ASCII or BINARY");
    m.put(MEMCACHED_BIND_ADDRESS,
        "The address the GemFireMemcachedServer will listen on for remote connections. Default is \"\" which causes the GemFireMemcachedServer to listen on the host's default address. This property is ignored if memcached-port is \"0\".");
    m.put(REDIS_PORT,
        "The port GeodeRedisServer will listen on. Default is 0. Set to zero to disable GeodeRedisServer.");
    m.put(REDIS_BIND_ADDRESS,
        "The address the GeodeRedisServer will listen on for remote connections. Default is \"\" which causes the GeodeRedisServer to listen on the host's default address. This property is ignored if redis-port is \"0\".");
    m.put(REDIS_PASSWORD,
        "The password which client of GeodeRedisServer must use to authenticate themselves. The default is none and no authentication will be required.");
    m.put(ENABLE_CLUSTER_CONFIGURATION,
        "Enables cluster configuration support in dedicated locators.  This allows the locator to share configuration information amongst members and save configuration changes made using GFSH.");
    m.put(ENABLE_MANAGEMENT_REST_SERVICE,
        "Enables management rest service in dedicated locators.  This allows users to manage the cluster through rest api.");
    m.put(USE_CLUSTER_CONFIGURATION,
        "Boolean flag that allows the cache to use the cluster configuration provided by the cluster config service");
    m.put(LOAD_CLUSTER_CONFIGURATION_FROM_DIR,
        String.format(
            "Loads cluster configuration from the %s directory of a locator. This is property is only applicable to the locator(s)",

            InternalConfigurationPersistenceService.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME));
    m.put(CLUSTER_CONFIGURATION_DIR,
        "The directory to store the cluster configuration artifacts and disk-store. This property is only applicable to the locator(s)");
    m.put(SSL_SERVER_ALIAS, String.format(
        "SSL inter-server communication (peer-to-peer) uses the this alias when determining the key to use from the keystore for SSL. Defaults to %s.",
        Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(SERVER_SSL_ENABLED,
        "If true then the cache server will only allow SSL clients to connect. Defaults to false.");
    m.put(SERVER_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for CacheServer. Defaults to \""
            + DEFAULT_SERVER_SSL_CIPHERS + "\" meaning your provider's defaults.");
    m.put(SERVER_SSL_PROTOCOLS,
        "List of available SSL protocols that are to be enabled for CacheServer. Defaults to \""
            + DEFAULT_SERVER_SSL_PROTOCOLS + "\" meaning defaults of your provider.");
    m.put(SERVER_SSL_REQUIRE_AUTHENTICATION,
        "If set to false, ciphers and protocols that permit anonymous Clients are allowed. Defaults to \""
            + DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION + "\".");

    m.put(SERVER_SSL_KEYSTORE,
        "Location of the Java keystore file containing server's or client's own certificate and private key.");

    m.put(SERVER_SSL_KEYSTORE_TYPE,
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(SERVER_SSL_KEYSTORE_PASSWORD,
        "Password to access the private key from the keystore file specified by javax.net.ssl.keyStore. ");

    m.put(SERVER_SSL_TRUSTSTORE,
        "Location of the Java keystore file containing the collection of CA certificates trusted by server or client(trust store).");

    m.put(SERVER_SSL_TRUSTSTORE_PASSWORD,
        "Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");

    m.put(SSL_GATEWAY_ALIAS, String.format(
        "SSL gateway communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to %s.",
        Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(GATEWAY_SSL_ENABLED,
        "If true then the gateway receiver will only allow SSL gateway sender to connect. Defaults to false.");
    m.put(GATEWAY_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for Gateway Receiver. Defaults to \""
            + DEFAULT_GATEWAY_SSL_CIPHERS + "\" meaning your provider's defaults.");
    m.put(GATEWAY_SSL_PROTOCOLS,
        "List of available SSL protocols that are to be enabled for Gateway Receiver. Defaults to \""
            + DEFAULT_GATEWAY_SSL_PROTOCOLS + "\" meaning defaults of your provider.");
    m.put(GATEWAY_SSL_REQUIRE_AUTHENTICATION,
        "If set to false, ciphers and protocols that permit anonymous gateway senders are allowed. Defaults to \""
            + DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION + "\".");

    m.put(GATEWAY_SSL_KEYSTORE,
        "Location of the Java keystore file containing gateway's own certificate and private key.");

    m.put(GATEWAY_SSL_KEYSTORE_TYPE,
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(GATEWAY_SSL_KEYSTORE_PASSWORD,
        "Password to access the private key from the keystore file specified by javax.net.ssl.keyStore.");

    m.put(GATEWAY_SSL_TRUSTSTORE,
        "Location of the Java keystore file containing the collection of CA certificates trusted by gateway.");

    m.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD,
        "Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");

    m.put(SSL_WEB_ALIAS, String.format(
        "SSL http service communication uses the this alias when determining the key to use from the keystore for SSL. Defaults to %s.",
        Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(HTTP_SERVICE_PORT,
        "If non zero, then the gemfire developer REST service will be deployed and started when the cache is created. Default value is 0.");
    m.put(HTTP_SERVICE_BIND_ADDRESS,
        "The address where gemfire developer REST service will listen for remote REST connections. Default is \"\" which causes the Rest service to listen on the host's default address.");

    m.put(HTTP_SERVICE_SSL_ENABLED,
        "If true then the http service like REST dev api and Pulse will only allow SSL enabled clients to connect. Defaults to false.");
    m.put(HTTP_SERVICE_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for Http Service. Defaults to \""
            + DEFAULT_HTTP_SERVICE_SSL_CIPHERS + "\" meaning your provider's defaults.");
    m.put(HTTP_SERVICE_SSL_PROTOCOLS,
        "List of available SSL protocols that are to be enabled for Http Service. Defaults to \""
            + DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS + "\" meaning defaults of your provider.");
    m.put(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION,
        "If set to false, ciphers and protocols that permit anonymous http clients are allowed. Defaults to \""
            + DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION + "\".");

    m.put(HTTP_SERVICE_SSL_KEYSTORE,
        "Location of the Java keystore file containing Http Service's own certificate and private key.");

    m.put(HTTP_SERVICE_SSL_KEYSTORE_TYPE,
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD,
        "Password to access the private key from the keystore file specified by javax.net.ssl.keyStore.");

    m.put(HTTP_SERVICE_SSL_TRUSTSTORE,
        "Location of the Java keystore file containing the collection of CA certificates trusted by Http Service.");

    m.put(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD,
        "Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");

    m.put(START_DEV_REST_API,
        "If true then the developer(API) REST service will be started when the cache is created. Defaults to false.");
    m.put(OFF_HEAP_MEMORY_SIZE, String.format(
        "The amount of off-heap memory to be allocated for GemFire. Value is <n>[g|m], where <n> is the size and [g|m] specifies the units in gigabytes or megabytes. Defaults to %s.",
        DEFAULT_OFF_HEAP_MEMORY_SIZE));
    m.put(LOCK_MEMORY, String.format(
        "Locks heap and off-heap memory pages into RAM, thereby preventing the operating system from swapping them out to disk. Defaults to %s",
        DEFAULT_LOCK_MEMORY));
    m.put(DISTRIBUTED_TRANSACTIONS,
        "Flag to indicate whether all transactions including JTA should be distributed transactions.  Default is false, meaning colocated transactions.");

    m.put(SECURITY_SHIRO_INIT,
        "The name of the shiro configuration file in the classpath, e.g. shiro.ini");
    m.put(SECURITY_MANAGER,
        "User defined fully qualified class name implementing SecurityManager interface for integrated security. Defaults to \"{0}\". Legal values can be any \"class name\" implementing SecurityManager that is present in the classpath.");
    m.put(SECURITY_POST_PROCESSOR,
        "User defined fully qualified class name implementing PostProcessor interface for integrated security. Defaults to \"{0}\". Legal values can be any \"class name\" implementing PostProcessor that is present in the classpath.");

    m.put(SSL_ENDPOINT_IDENTIFICATION_ENABLED,
        "If true, clients validate server hostname using server certificate during SSL handshake. It defaults to true when ssl-use-default-context is true or else false.");

    m.put(SSL_USE_DEFAULT_CONTEXT,
        "When true, either uses the default context as returned by SSLContext.getInstance('Default') or uses the context as set by using SSLContext.setDefault(). "
            + "If false, then specify the keystore and the truststore by setting ssl-keystore-* and ssl-truststore-* properties. If true, then ssl-endpoint-identification-enabled is set to true. This property does not enable SSL.");

    m.put(SSL_ENABLED_COMPONENTS,
        "A comma delimited list of components that require SSL communications");

    m.put(SSL_CIPHERS, "List of available SSL cipher suites that are to be enabled. Defaults to \""
        + DEFAULT_SSL_CIPHERS + "\" meaning your provider's defaults.");
    m.put(SSL_PROTOCOLS, "List of available SSL protocols that are to be enabled. Defaults to \""
        + DEFAULT_SSL_PROTOCOLS + "\" meaning defaults of your provider.");
    m.put(SSL_REQUIRE_AUTHENTICATION,
        "If set to false, ciphers and protocols that permit anonymous clients are allowed. Defaults to \""
            + DEFAULT_SSL_REQUIRE_AUTHENTICATION + "\".");
    m.put(SSL_KEYSTORE,
        "Location of the Java keystore file containing the certificate and private key.");
    m.put(SSL_KEYSTORE_TYPE,
        "For Java keystore file format, this property has the value jks (or JKS).");
    m.put(SSL_KEYSTORE_PASSWORD, "Password to access the private key from the keystore.");
    m.put(SSL_TRUSTSTORE,
        "Location of the Java keystore file containing the collection of trusted certificates.");
    m.put(SSL_TRUSTSTORE_PASSWORD, "Password to unlock the truststore.");
    m.put(SSL_TRUSTSTORE_TYPE,
        "For Java truststore file format, this property has the value jks (or JKS).");
    m.put(SSL_DEFAULT_ALIAS, "The default certificate alias to be used in a multi-key keystore");
    m.put(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION,
        "This property determines is the HTTP service with use mutual ssl authentication.");
    m.put(SSL_PARAMETER_EXTENSION,
        "User defined fully qualified class name implementing SSLParameterExtension interface for SSL parameter extensions. Defaults to \"{0}\". Legal values can be any \"class name\" implementing SSLParameterExtension that is present in the classpath.");
    m.put(VALIDATE_SERIALIZABLE_OBJECTS,
        "If true checks incoming java serializable objects against a filter");
    m.put(SERIALIZABLE_OBJECT_FILTER, "The filter to check incoming java serializables against");

    m.put(THREAD_MONITOR_INTERVAL,
        "Defines the time interval (in milliseconds) with which thread monitoring is scheduled to run.");
    m.put(THREAD_MONITOR_ENABLED,
        "Defines whether thread monitoring is to be enabled.");
    m.put(THREAD_MONITOR_TIME_LIMIT,
        "Defines the time period (in milliseconds) after which the monitored thread is considered to be stuck.");
    dcAttDescriptions = Collections.unmodifiableMap(m);
  }

  /**
   * Used by unit tests.
   */
  public static String[] _getAttNames() {
    return dcValidAttributeNames;
  }

  @Override
  public String[] getAttributeNames() {
    return dcValidAttributeNames;
  }

  @Override
  protected Map getAttDescMap() {
    return dcAttDescriptions;
  }

  @Override
  public boolean isLoner() {
    return getLocators().equals("") && getMcastPort() == 0;
  }

  @Immutable
  static final Map<String, Method> checkers;

  static {
    Map<String, Method> checkersMap = new HashMap<>();
    for (Method method : AbstractDistributionConfig.class.getDeclaredMethods()) {
      if (method.isAnnotationPresent(ConfigAttributeChecker.class)) {
        ConfigAttributeChecker checker = method.getAnnotation(ConfigAttributeChecker.class);
        checkersMap.put(checker.name(), method);
      }
    }

    checkers = Collections.unmodifiableMap(checkersMap);
  }
}
