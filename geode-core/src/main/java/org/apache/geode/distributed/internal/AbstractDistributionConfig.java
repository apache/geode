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

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.InvalidValueException;
import org.apache.geode.UnmodifiableException;
import org.apache.geode.internal.AbstractConfig;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.memcached.GemFireMemcachedServer;

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

  protected Object checkAttribute(String attName, Object value) {
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


  protected void minMaxCheck(String propName, int value, int minValue, int maxValue) {
    if (value < minValue) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
              .toLocalizedString(
                  new Object[] {propName, Integer.valueOf(value), Integer.valueOf(minValue)}));
    } else if (value > maxValue) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2
              .toLocalizedString(
                  new Object[] {propName, Integer.valueOf(value), Integer.valueOf(maxValue)}));
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
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE
              .toLocalizedString(
                  new Object[] {TCP_PORT, Integer.valueOf(value), CLUSTER_SSL_ENABLED}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = MCAST_PORT)
  protected int checkMcastPort(int value) {
    if (getClusterSSLEnabled() && value != 0) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE
              .toLocalizedString(
                  new Object[] {MCAST_PORT, Integer.valueOf(value), CLUSTER_SSL_ENABLED}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = MCAST_ADDRESS)
  protected InetAddress checkMcastAddress(InetAddress value) {
    if (!value.isMulticastAddress()) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_IT_WAS_NOT_A_MULTICAST_ADDRESS
              .toLocalizedString(new Object[] {MCAST_ADDRESS, value}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = BIND_ADDRESS)
  protected String checkBindAddress(String value) {
    if (value != null && value.length() > 0 && !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
              .toLocalizedString(new Object[] {value, SocketCreator.getMyAddresses()}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = SERVER_BIND_ADDRESS)
  protected String checkServerBindAddress(String value) {
    if (value != null && value.length() > 0 && !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
              .toLocalizedString(new Object[] {value, SocketCreator.getMyAddresses()}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = CLUSTER_SSL_ENABLED)
  protected Boolean checkClusterSSLEnabled(Boolean value) {
    if (value.booleanValue() && (getMcastPort() != 0)) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_FALSE_WHEN_2_IS_NOT_0
              .toLocalizedString(new Object[] {CLUSTER_SSL_ENABLED, value, MCAST_PORT}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = HTTP_SERVICE_BIND_ADDRESS)
  protected String checkHttpServiceBindAddress(String value) {
    if (value != null && value.length() > 0 && !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
              .toLocalizedString(new Object[] {value, SocketCreator.getMyAddresses()}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = DISTRIBUTED_SYSTEM_ID)
  protected int checkDistributedSystemId(int value) {
    String distributedSystemListener =
        System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "DistributedSystemListener");
    // this check is specific for Jayesh's use case of WAN BootStraping
    if (distributedSystemListener == null) {
      if (value < MIN_DISTRIBUTED_SYSTEM_ID) {
        throw new IllegalArgumentException(
            LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
                .toLocalizedString(new Object[] {DISTRIBUTED_SYSTEM_ID, Integer.valueOf(value),
                    Integer.valueOf(MIN_DISTRIBUTED_SYSTEM_ID)}));
      }
    }
    if (value > MAX_DISTRIBUTED_SYSTEM_ID) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2
              .toLocalizedString(new Object[] {DISTRIBUTED_SYSTEM_ID, Integer.valueOf(value),
                  Integer.valueOf(MAX_DISTRIBUTED_SYSTEM_ID)}));
    }
    return value;
  }

  /**
   * Makes sure that the locator string used to configure discovery is valid.
   * <p>
   * <p>
   * Starting in 4.0, we accept locators in the format "host:port" in addition to the traditional
   * "host:bind-address[port]" format. See bug 32306.
   * <p>
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
    StringBuffer sb = new StringBuffer();

    Set locs = new java.util.HashSet();

    StringTokenizer st = new StringTokenizer(value, ",");
    boolean firstUniqueLocator = true;
    while (st.hasMoreTokens()) {
      String locator = st.nextToken();
      StringBuffer locatorsb = new StringBuffer(); // string for this locator is accumulated in this
                                                   // buffer

      int portIndex = locator.indexOf('[');
      if (portIndex < 1) {
        portIndex = locator.lastIndexOf(':');
      }
      if (portIndex < 1) {
        throw new IllegalArgumentException(
            LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0_HOST_NAME_WAS_EMPTY
                .toLocalizedString(value));
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
              LocalizedStrings.AbstractDistributionConfig_UNKNOWN_LOCATOR_BIND_ADDRESS_0
                  .toLocalizedString(bindAddr));
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
              LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0
                  .toLocalizedString(value));

        } else {
          // Using host:port syntax
          lastIndex = locator.length();
        }
      }

      String port = locator.substring(portIndex + 1, lastIndex);
      int portVal = 0;
      try {
        portVal = Integer.parseInt(port);
        if (0 == portVal) {
          return "";
        } else if (portVal < 1 || portVal > 65535) {
          throw new IllegalArgumentException(
              LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0_THE_PORT_1_WAS_NOT_GREATER_THAN_ZERO_AND_LESS_THAN_65536
                  .toLocalizedString(new Object[] {value, Integer.valueOf(portVal)}));
        }
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException(
            LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0.toLocalizedString(value));
      }

      locatorsb.append('[');
      locatorsb.append(port);
      locatorsb.append(']');

      // if this wasn't a duplicate, add it to the locators string
      java.net.InetSocketAddress sockAddr = new java.net.InetSocketAddress(hostAddress, portVal);
      if (!locs.contains(sockAddr)) {
        if (!firstUniqueLocator) {
          sb.append(',');
        } else {
          firstUniqueLocator = false;
        }
        locs.add(new java.net.InetSocketAddress(hostAddress, portVal));
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
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_BYTEALLOWANCE_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
              .toLocalizedString(new Object[] {MCAST_FLOW_CONTROL, Integer.valueOf(value),
                  Integer.valueOf(MIN_FC_BYTE_ALLOWANCE)}));
    }
    float fvalue = params.getRechargeThreshold();
    if (fvalue < MIN_FC_RECHARGE_THRESHOLD) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGETHRESHOLD_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
              .toLocalizedString(new Object[] {MCAST_FLOW_CONTROL, new Float(fvalue),
                  new Float(MIN_FC_RECHARGE_THRESHOLD)}));
    } else if (fvalue > MAX_FC_RECHARGE_THRESHOLD) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGETHRESHOLD_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2
              .toLocalizedString(new Object[] {MCAST_FLOW_CONTROL, new Float(fvalue),
                  new Float(MAX_FC_RECHARGE_THRESHOLD)}));
    }
    value = params.getRechargeBlockMs();
    if (value < MIN_FC_RECHARGE_BLOCK_MS) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGEBLOCKMS_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
              .toLocalizedString(new Object[] {MCAST_FLOW_CONTROL, Integer.valueOf(value),
                  Integer.valueOf(MIN_FC_RECHARGE_BLOCK_MS)}));
    } else if (value > MAX_FC_RECHARGE_BLOCK_MS) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGEBLOCKMS_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2
              .toLocalizedString(new Object[] {MCAST_FLOW_CONTROL, Integer.valueOf(value),
                  Integer.valueOf(MAX_FC_RECHARGE_BLOCK_MS)}));
    }
    return params;
  }

  @ConfigAttributeChecker(name = MEMBERSHIP_PORT_RANGE)
  protected int[] checkMembershipPortRange(int[] value) {
    minMaxCheck(MEMBERSHIP_PORT_RANGE, value[0], DEFAULT_MEMBERSHIP_PORT_RANGE[0], value[1]);
    minMaxCheck(MEMBERSHIP_PORT_RANGE, value[1], value[0], DEFAULT_MEMBERSHIP_PORT_RANGE[1]);

    // Minimum 3 ports are required to start a Gemfire data node,
    // One for each, UDP, FD_SOCk protocols and Cache Server.
    if (value[1] - value[0] < 2) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
              .toLocalizedString(new Object[] {MEMBERSHIP_PORT_RANGE, value[0] + "-" + value[1],
                  Integer.valueOf(3)}));
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
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_2_MUST_BE_0_WHEN_SECURITY_IS_ENABLED
              .toLocalizedString(new Object[] {SECURITY_PEER_AUTH_INIT, value, mcastInfo}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = SECURITY_PEER_AUTHENTICATOR)
  protected String checkSecurityPeerAuthenticator(String value) {
    if (value != null && value.length() > 0 && getMcastPort() != 0) {
      String mcastInfo = MCAST_PORT + "[" + getMcastPort() + "]";
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_2_MUST_BE_0_WHEN_SECURITY_IS_ENABLED
              .toLocalizedString(new Object[] {SECURITY_PEER_AUTHENTICATOR, value, mcastInfo}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = SECURITY_LOG_LEVEL)
  protected int checkSecurityLogLevel(int value) {
    if (value < MIN_LOG_LEVEL) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
              .toLocalizedString(new Object[] {SECURITY_LOG_LEVEL,
                  LogWriterImpl.levelToString(value), LogWriterImpl.levelToString(MIN_LOG_LEVEL)}));
    }
    if (value > MAX_LOG_LEVEL) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2
              .toLocalizedString(new Object[] {SECURITY_LOG_LEVEL,
                  LogWriterImpl.levelToString(value), LogWriterImpl.levelToString(MAX_LOG_LEVEL)}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = MEMCACHED_PROTOCOL)
  protected String checkMemcachedProtocol(String protocol) {
    if (protocol == null
        || (!protocol.equalsIgnoreCase(GemFireMemcachedServer.Protocol.ASCII.name())
            && !protocol.equalsIgnoreCase(GemFireMemcachedServer.Protocol.BINARY.name()))) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_MEMCACHED_PROTOCOL_MUST_BE_ASCII_OR_BINARY
              .toLocalizedString());
    }
    return protocol;
  }

  public boolean isMemcachedProtocolModifiable() {
    return false;
  }

  @ConfigAttributeChecker(name = MEMCACHED_BIND_ADDRESS)
  protected String checkMemcachedBindAddress(String value) {
    if (value != null && value.length() > 0 && !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_MEMCACHED_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
              .toLocalizedString(new Object[] {value, SocketCreator.getMyAddresses()}));
    }
    return value;
  }

  @ConfigAttributeChecker(name = REDIS_BIND_ADDRESS)
  protected String checkRedisBindAddress(String value) {
    if (value != null && value.length() > 0 && !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
          LocalizedStrings.AbstractDistributionConfig_REDIS_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
              .toLocalizedString(new Object[] {value, SocketCreator.getMyAddresses()}));
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
              LocalizedStrings.AbstractDistributionConfig_SSL_ENABLED_COMPONENTS_0_INVALID_TRY_1
                  .toLocalizedString(new Object[] {value,
                      StringUtils
                          .join(new String[] {SecurableCommunicationChannel.ALL.getConstant(),
                              SecurableCommunicationChannel.CLUSTER.getConstant(),
                              SecurableCommunicationChannel.SERVER.getConstant(),
                              SecurableCommunicationChannel.GATEWAY.getConstant(),
                              SecurableCommunicationChannel.JMX.getConstant(),
                              SecurableCommunicationChannel.WEB.getConstant(),
                              SecurableCommunicationChannel.LOCATOR.getConstant()}, ",")}));
      }
    }
    if (value.length > 0) {
      if (getClusterSSLEnabled() || getJmxManagerSSLEnabled() || getHttpServiceSSLEnabled()
          || getServerSSLEnabled() || getGatewaySSLEnabled()) {
        throw new IllegalArgumentException(
            LocalizedStrings.AbstractDistributionConfig_SSL_ENABLED_COMPONENTS_SET_INVALID_DEPRECATED_SSL_SET
                .toLocalizedString());
      }
    }
    return value;
  }

  // AbstractConfig overriding methods

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

  public void setAttributeObject(String attName, Object attValue, ConfigSource source) {
    // TODO: the setters is already checking the parameter type, do we still need to do this?
    Class validValueClass = getAttributeType(attName);
    if (attValue != null) {
      // null is a "valid" value for any class
      if (!validValueClass.isInstance(attValue)) {
        throw new InvalidValueException(
            LocalizedStrings.AbstractDistributionConfig_0_VALUE_1_MUST_BE_OF_TYPE_2
                .toLocalizedString(new Object[] {attName, attValue, validValueClass.getName()}));
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
      this.setSecurity(attName, attValue.toString());
    }

    if (attName.startsWith(SSL_SYSTEM_PROPS_NAME) || attName.startsWith(SYS_PROP_NAME)) {
      this.setSSLProperty(attName, attValue.toString());
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
          LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0
              .toLocalizedString(attName));
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

  public Object getAttributeObject(String attName) {
    checkAttributeName(attName);

    // special case:
    if (attName.equalsIgnoreCase(LOG_LEVEL)) {
      return LogWriterImpl.levelToString(this.getLogLevel());
    }

    if (attName.equalsIgnoreCase(SECURITY_LOG_LEVEL)) {
      return LogWriterImpl.levelToString(this.getSecurityLogLevel());
    }

    Method getter = getters.get(attName);
    if (getter == null) {
      if (attName.startsWith(SECURITY_PREFIX)) {
        return this.getSecurity(attName);
      }
      throw new InternalGemFireException(
          LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0
              .toLocalizedString(attName));
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


  public boolean isAttributeModifiable(String attName) {
    checkAttributeName(attName);
    if (getModifiableAttributes().contains(attName)) {
      return true;
    }

    if (getUnModifiableAttributes().contains(attName)) {
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

  ;

  /**
   * child class can override this method to return a list of unModifiable attributes no matter what
   * the default is
   *
   * @return an empty list
   */
  public List<String> getUnModifiableAttributes() {
    return new ArrayList<>();
  }

  public Class getAttributeType(String attName) {
    checkAttributeName(attName);
    return _getAttributeType(attName);
  }

  public static Class _getAttributeType(String attName) {
    ConfigAttribute ca = attributes.get(attName);
    if (ca == null) {
      if (attName.startsWith(SECURITY_PREFIX) || attName.startsWith(SSL_SYSTEM_PROPS_NAME)
          || attName.startsWith(SYS_PROP_NAME)) {
        return String.class;
      }
      throw new InternalGemFireException(
          LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0
              .toLocalizedString(attName));
    }
    return ca.type();
  }

  protected static final Map dcAttDescriptions;

  static {
    Map<String, String> m = new HashMap<String, String>();

    m.put(ACK_WAIT_THRESHOLD,
        LocalizedStrings.AbstractDistributionConfig_DEFAULT_ACK_WAIT_THRESHOLD_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_ACK_WAIT_THRESHOLD),
                Integer.valueOf(MIN_ACK_WAIT_THRESHOLD), Integer.valueOf(MIN_ACK_WAIT_THRESHOLD)}));

    m.put(ARCHIVE_FILE_SIZE_LIMIT,
        LocalizedStrings.AbstractDistributionConfig_ARCHIVE_FILE_SIZE_LIMIT_NAME
            .toLocalizedString());

    m.put(ACK_SEVERE_ALERT_THRESHOLD,
        LocalizedStrings.AbstractDistributionConfig_ACK_SEVERE_ALERT_THRESHOLD_NAME
            .toLocalizedString(new Object[] {ACK_WAIT_THRESHOLD,
                Integer.valueOf(DEFAULT_ACK_SEVERE_ALERT_THRESHOLD),
                Integer.valueOf(MIN_ACK_SEVERE_ALERT_THRESHOLD),
                Integer.valueOf(MAX_ACK_SEVERE_ALERT_THRESHOLD)}));

    m.put(ARCHIVE_DISK_SPACE_LIMIT,
        LocalizedStrings.AbstractDistributionConfig_ARCHIVE_DISK_SPACE_LIMIT_NAME
            .toLocalizedString());

    m.put(CACHE_XML_FILE, LocalizedStrings.AbstractDistributionConfig_CACHE_XML_FILE_NAME_0
        .toLocalizedString(DEFAULT_CACHE_XML_FILE));

    m.put(DISABLE_TCP, LocalizedStrings.AbstractDistributionConfig_DISABLE_TCP_NAME_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_DISABLE_TCP)));

    m.put(ENABLE_TIME_STATISTICS,
        LocalizedStrings.AbstractDistributionConfig_ENABLE_TIME_STATISTICS_NAME
            .toLocalizedString());

    m.put(DEPLOY_WORKING_DIR, LocalizedStrings.AbstractDistributionConfig_DEPLOY_WORKING_DIR_0
        .toLocalizedString(DEFAULT_DEPLOY_WORKING_DIR));

    m.put(LOG_FILE, LocalizedStrings.AbstractDistributionConfig_LOG_FILE_NAME_0
        .toLocalizedString(DEFAULT_LOG_FILE));

    m.put(LOG_LEVEL,
        LocalizedStrings.AbstractDistributionConfig_LOG_LEVEL_NAME_0_1
            .toLocalizedString(new Object[] {LogWriterImpl.levelToString(DEFAULT_LOG_LEVEL),
                LogWriterImpl.allowedLogLevels()}));

    m.put(LOG_FILE_SIZE_LIMIT,
        LocalizedStrings.AbstractDistributionConfig_LOG_FILE_SIZE_LIMIT_NAME.toLocalizedString());

    m.put(LOG_DISK_SPACE_LIMIT,
        LocalizedStrings.AbstractDistributionConfig_LOG_DISK_SPACE_LIMIT_NAME.toLocalizedString());

    m.put(LOCATORS, LocalizedStrings.AbstractDistributionConfig_LOCATORS_NAME_0
        .toLocalizedString(DEFAULT_LOCATORS));

    m.put(LOCATOR_WAIT_TIME, LocalizedStrings.AbstractDistributionConfig_LOCATOR_WAIT_TIME_NAME_0
        .toLocalizedString(Integer.valueOf(DEFAULT_LOCATOR_WAIT_TIME)));

    m.put(TCP_PORT,
        LocalizedStrings.AbstractDistributionConfig_TCP_PORT_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_TCP_PORT),
                Integer.valueOf(MIN_TCP_PORT), Integer.valueOf(MAX_TCP_PORT)}));

    m.put(MCAST_PORT,
        LocalizedStrings.AbstractDistributionConfig_MCAST_PORT_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_MCAST_PORT),
                Integer.valueOf(MIN_MCAST_PORT), Integer.valueOf(MAX_MCAST_PORT)}));

    m.put(MCAST_ADDRESS,
        LocalizedStrings.AbstractDistributionConfig_MCAST_ADDRESS_NAME_0_1.toLocalizedString(
            new Object[] {Integer.valueOf(DEFAULT_MCAST_PORT), DEFAULT_MCAST_ADDRESS}));

    m.put(MCAST_TTL,
        LocalizedStrings.AbstractDistributionConfig_MCAST_TTL_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_MCAST_TTL),
                Integer.valueOf(MIN_MCAST_TTL), Integer.valueOf(MAX_MCAST_TTL)}));

    m.put(MCAST_SEND_BUFFER_SIZE,
        LocalizedStrings.AbstractDistributionConfig_MCAST_SEND_BUFFER_SIZE_NAME_0
            .toLocalizedString(Integer.valueOf(DEFAULT_MCAST_SEND_BUFFER_SIZE)));

    m.put(MCAST_RECV_BUFFER_SIZE,
        LocalizedStrings.AbstractDistributionConfig_MCAST_RECV_BUFFER_SIZE_NAME_0
            .toLocalizedString(Integer.valueOf(DEFAULT_MCAST_RECV_BUFFER_SIZE)));

    m.put(MCAST_FLOW_CONTROL, LocalizedStrings.AbstractDistributionConfig_MCAST_FLOW_CONTROL_NAME_0
        .toLocalizedString(DEFAULT_MCAST_FLOW_CONTROL));

    m.put(MEMBER_TIMEOUT, LocalizedStrings.AbstractDistributionConfig_MEMBER_TIMEOUT_NAME_0
        .toLocalizedString(Integer.valueOf(DEFAULT_MEMBER_TIMEOUT)));

    // for some reason the default port range is null under some circumstances
    int[] range = DEFAULT_MEMBERSHIP_PORT_RANGE;
    String srange = range == null ? "not available" : "" + range[0] + "-" + range[1];
    String msg = LocalizedStrings.AbstractDistributionConfig_MEMBERSHIP_PORT_RANGE_NAME_0
        .toLocalizedString(srange);
    m.put(MEMBERSHIP_PORT_RANGE, msg);

    m.put(UDP_SEND_BUFFER_SIZE,
        LocalizedStrings.AbstractDistributionConfig_UDP_SEND_BUFFER_SIZE_NAME_0
            .toLocalizedString(Integer.valueOf(DEFAULT_UDP_SEND_BUFFER_SIZE)));

    m.put(UDP_RECV_BUFFER_SIZE,
        LocalizedStrings.AbstractDistributionConfig_UDP_RECV_BUFFER_SIZE_NAME_0
            .toLocalizedString(Integer.valueOf(DEFAULT_UDP_RECV_BUFFER_SIZE)));

    m.put(UDP_FRAGMENT_SIZE, LocalizedStrings.AbstractDistributionConfig_UDP_FRAGMENT_SIZE_NAME_0
        .toLocalizedString(Integer.valueOf(DEFAULT_UDP_FRAGMENT_SIZE)));

    m.put(SOCKET_LEASE_TIME,
        LocalizedStrings.AbstractDistributionConfig_SOCKET_LEASE_TIME_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_SOCKET_LEASE_TIME),
                Integer.valueOf(MIN_SOCKET_LEASE_TIME), Integer.valueOf(MAX_SOCKET_LEASE_TIME)}));

    m.put(SOCKET_BUFFER_SIZE,
        LocalizedStrings.AbstractDistributionConfig_SOCKET_BUFFER_SIZE_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_SOCKET_BUFFER_SIZE),
                Integer.valueOf(MIN_SOCKET_BUFFER_SIZE), Integer.valueOf(MAX_SOCKET_BUFFER_SIZE)}));

    m.put(CONSERVE_SOCKETS, LocalizedStrings.AbstractDistributionConfig_CONSERVE_SOCKETS_NAME_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_CONSERVE_SOCKETS)));

    m.put(ROLES,
        LocalizedStrings.AbstractDistributionConfig_ROLES_NAME_0.toLocalizedString(DEFAULT_ROLES));

    m.put(BIND_ADDRESS, LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_NAME_0
        .toLocalizedString(DEFAULT_BIND_ADDRESS));

    m.put(SERVER_BIND_ADDRESS,
        LocalizedStrings.AbstractDistributionConfig_SERVER_BIND_ADDRESS_NAME_0
            .toLocalizedString(DEFAULT_BIND_ADDRESS));

    m.put(NAME,
        "A name that uniquely identifies a member in its distributed system."
            + " Multiple members in the same distributed system can not have the same name."
            + " Defaults to \"\".");

    m.put(STATISTIC_ARCHIVE_FILE,
        LocalizedStrings.AbstractDistributionConfig_STATISTIC_ARCHIVE_FILE_NAME_0
            .toLocalizedString(DEFAULT_STATISTIC_ARCHIVE_FILE));

    m.put(STATISTIC_SAMPLE_RATE,
        LocalizedStrings.AbstractDistributionConfig_STATISTIC_SAMPLE_RATE_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_STATISTIC_SAMPLE_RATE),
                Integer.valueOf(MIN_STATISTIC_SAMPLE_RATE),
                Integer.valueOf(MAX_STATISTIC_SAMPLE_RATE)}));

    m.put(STATISTIC_SAMPLING_ENABLED,
        LocalizedStrings.AbstractDistributionConfig_STATISTIC_SAMPLING_ENABLED_NAME_0
            .toLocalizedString(Boolean.valueOf(DEFAULT_STATISTIC_SAMPLING_ENABLED)));

    m.put(SSL_CLUSTER_ALIAS, LocalizedStrings.AbstractDistributionConfig_CLUSTER_SSL_ALIAS_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_ALIAS)));

    m.put(CLUSTER_SSL_ENABLED, LocalizedStrings.AbstractDistributionConfig_SSL_ENABLED_NAME_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_ENABLED)));

    m.put(CLUSTER_SSL_PROTOCOLS, LocalizedStrings.AbstractDistributionConfig_SSL_PROTOCOLS_NAME_0
        .toLocalizedString(DEFAULT_SSL_PROTOCOLS));

    m.put(CLUSTER_SSL_CIPHERS, LocalizedStrings.AbstractDistributionConfig_SSL_CIPHERS_NAME_0
        .toLocalizedString(DEFAULT_SSL_CIPHERS));

    m.put(CLUSTER_SSL_REQUIRE_AUTHENTICATION,
        LocalizedStrings.AbstractDistributionConfig_SSL_REQUIRE_AUTHENTICATION_NAME
            .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_REQUIRE_AUTHENTICATION)));

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
        LocalizedStrings.AbstractDistributionConfig_MAX_WAIT_TIME_FOR_RECONNECT
            .toLocalizedString());

    m.put(MAX_NUM_RECONNECT_TRIES,
        LocalizedStrings.AbstractDistributionConfig_MAX_NUM_RECONNECT_TRIES.toLocalizedString());

    m.put(ASYNC_DISTRIBUTION_TIMEOUT,
        LocalizedStrings.AbstractDistributionConfig_ASYNC_DISTRIBUTION_TIMEOUT_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT),
                Integer.valueOf(MIN_ASYNC_DISTRIBUTION_TIMEOUT),
                Integer.valueOf(MAX_ASYNC_DISTRIBUTION_TIMEOUT)}));


    m.put(ASYNC_QUEUE_TIMEOUT,
        LocalizedStrings.AbstractDistributionConfig_ASYNC_QUEUE_TIMEOUT_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_ASYNC_QUEUE_TIMEOUT),
                Integer.valueOf(MIN_ASYNC_QUEUE_TIMEOUT),
                Integer.valueOf(MAX_ASYNC_QUEUE_TIMEOUT)}));

    m.put(ASYNC_MAX_QUEUE_SIZE,
        LocalizedStrings.AbstractDistributionConfig_ASYNC_MAX_QUEUE_SIZE_NAME_0_1_2
            .toLocalizedString(new Object[] {Integer.valueOf(DEFAULT_ASYNC_MAX_QUEUE_SIZE),
                Integer.valueOf(MIN_ASYNC_MAX_QUEUE_SIZE),
                Integer.valueOf(MAX_ASYNC_MAX_QUEUE_SIZE)}));

    m.put(START_LOCATOR,
        LocalizedStrings.AbstractDistributionConfig_START_LOCATOR_NAME.toLocalizedString());

    m.put(DURABLE_CLIENT_ID, LocalizedStrings.AbstractDistributionConfig_DURABLE_CLIENT_ID_NAME_0
        .toLocalizedString(DEFAULT_DURABLE_CLIENT_ID));

    m.put(CONFLATE_EVENTS, LocalizedStrings.AbstractDistributionConfig_CLIENT_CONFLATION_PROP_NAME
        .toLocalizedString());

    m.put(DURABLE_CLIENT_TIMEOUT,
        LocalizedStrings.AbstractDistributionConfig_DURABLE_CLIENT_TIMEOUT_NAME_0
            .toLocalizedString(Integer.valueOf(DEFAULT_DURABLE_CLIENT_TIMEOUT)));

    m.put(SECURITY_CLIENT_AUTH_INIT,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_AUTH_INIT_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_CLIENT_AUTH_INIT));

    m.put(ENABLE_NETWORK_PARTITION_DETECTION, "Whether network partitioning detection is enabled");

    m.put(DISABLE_AUTO_RECONNECT, "Whether auto reconnect is attempted after a network partition");

    m.put(SECURITY_CLIENT_AUTHENTICATOR,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_AUTHENTICATOR_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_CLIENT_AUTHENTICATOR));

    m.put(SECURITY_CLIENT_DHALGO,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_DHALGO_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_CLIENT_DHALGO));

    m.put(SECURITY_UDP_DHALGO,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_UDP_DHALGO_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_UDP_DHALGO));

    m.put(SECURITY_PEER_AUTH_INIT,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_PEER_AUTH_INIT_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_PEER_AUTH_INIT));

    m.put(SECURITY_PEER_AUTHENTICATOR,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_PEER_AUTHENTICATOR_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_PEER_AUTHENTICATOR));

    m.put(SECURITY_CLIENT_ACCESSOR,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_ACCESSOR_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_CLIENT_ACCESSOR));

    m.put(SECURITY_CLIENT_ACCESSOR_PP,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_ACCESSOR_PP_NAME_0
            .toLocalizedString(DEFAULT_SECURITY_CLIENT_ACCESSOR_PP));

    m.put(SECURITY_LOG_LEVEL,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_LOG_LEVEL_NAME_0_1
            .toLocalizedString(new Object[] {LogWriterImpl.levelToString(DEFAULT_LOG_LEVEL),
                LogWriterImpl.allowedLogLevels()}));

    m.put(SECURITY_LOG_FILE, LocalizedStrings.AbstractDistributionConfig_SECURITY_LOG_FILE_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_LOG_FILE));

    m.put(SECURITY_PEER_VERIFY_MEMBER_TIMEOUT,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME_0
            .toLocalizedString(Integer.valueOf(DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT)));

    m.put(SECURITY_PREFIX,
        LocalizedStrings.AbstractDistributionConfig_SECURITY_PREFIX_NAME.toLocalizedString());

    m.put(USERDEFINED_PREFIX_NAME,
        LocalizedStrings.AbstractDistributionConfig_USERDEFINED_PREFIX_NAME.toLocalizedString());

    m.put(REMOVE_UNRESPONSIVE_CLIENT,
        LocalizedStrings.AbstractDistributionConfig_REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME_0
            .toLocalizedString(DEFAULT_REMOVE_UNRESPONSIVE_CLIENT));

    m.put(DELTA_PROPAGATION, "Whether delta propagation is enabled");

    m.put(REMOTE_LOCATORS,
        LocalizedStrings.AbstractDistributionConfig_REMOTE_DISTRIBUTED_SYSTEMS_NAME_0
            .toLocalizedString(DEFAULT_REMOTE_LOCATORS));

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
    m.put(SSL_JMX_ALIAS, LocalizedStrings.AbstractDistributionConfig_JMX_MANAGER_SSL_ALIAS_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(JMX_MANAGER_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for JMX Manager. Defaults to \""
            + DEFAULT_JMX_MANAGER_SSL_CIPHERS + "\" meaning your provider''s defaults.");
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
    m.put(SSL_LOCATOR_ALIAS, LocalizedStrings.AbstractDistributionConfig_LOCATOR_SSL_ALIAS_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_ALIAS)));
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
        LocalizedStrings.AbstractDistributionConfig_ENABLE_SHARED_CONFIGURATION
            .toLocalizedString());
    m.put(USE_CLUSTER_CONFIGURATION,
        LocalizedStrings.AbstractDistributionConfig_USE_SHARED_CONFIGURATION.toLocalizedString());
    m.put(LOAD_CLUSTER_CONFIGURATION_FROM_DIR,
        LocalizedStrings.AbstractDistributionConfig_LOAD_SHARED_CONFIGURATION_FROM_DIR
            .toLocalizedString(
                InternalConfigurationPersistenceService.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME));
    m.put(CLUSTER_CONFIGURATION_DIR,
        LocalizedStrings.AbstractDistributionConfig_CLUSTER_CONFIGURATION_DIR.toLocalizedString());
    m.put(SSL_SERVER_ALIAS, LocalizedStrings.AbstractDistributionConfig_SERVER_SSL_ALIAS_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(SERVER_SSL_ENABLED,
        "If true then the cache server will only allow SSL clients to connect. Defaults to false.");
    m.put(SERVER_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for CacheServer. Defaults to \""
            + DEFAULT_SERVER_SSL_CIPHERS + "\" meaning your provider''s defaults.");
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

    m.put(SSL_GATEWAY_ALIAS, LocalizedStrings.AbstractDistributionConfig_GATEWAY_SSL_ALIAS_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(GATEWAY_SSL_ENABLED,
        "If true then the gateway receiver will only allow SSL gateway sender to connect. Defaults to false.");
    m.put(GATEWAY_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for Gateway Receiver. Defaults to \""
            + DEFAULT_GATEWAY_SSL_CIPHERS + "\" meaning your provider''s defaults.");
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

    m.put(SSL_WEB_ALIAS, LocalizedStrings.AbstractDistributionConfig_HTTP_SERVICE_SSL_ALIAS_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_ALIAS)));
    m.put(HTTP_SERVICE_PORT,
        "If non zero, then the gemfire developer REST service will be deployed and started when the cache is created. Default value is 0.");
    m.put(HTTP_SERVICE_BIND_ADDRESS,
        "The address where gemfire developer REST service will listen for remote REST connections. Default is \"\" which causes the Rest service to listen on the host's default address.");

    m.put(HTTP_SERVICE_SSL_ENABLED,
        "If true then the http service like REST dev api and Pulse will only allow SSL enabled clients to connect. Defaults to false.");
    m.put(HTTP_SERVICE_SSL_CIPHERS,
        "List of available SSL cipher suites that are to be enabled for Http Service. Defaults to \""
            + DEFAULT_HTTP_SERVICE_SSL_CIPHERS + "\" meaning your provider''s defaults.");
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
    m.put(OFF_HEAP_MEMORY_SIZE, LocalizedStrings.AbstractDistributionConfig_OFF_HEAP_MEMORY_SIZE_0
        .toLocalizedString(DEFAULT_OFF_HEAP_MEMORY_SIZE));
    m.put(LOCK_MEMORY, LocalizedStrings.AbstractDistributionConfig_LOCK_MEMORY
        .toLocalizedString(DEFAULT_LOCK_MEMORY));
    m.put(DISTRIBUTED_TRANSACTIONS,
        "Flag to indicate whether all transactions including JTA should be distributed transactions.  Default is false, meaning colocated transactions.");

    m.put(SECURITY_SHIRO_INIT,
        "The name of the shiro configuration file in the classpath, e.g. shiro.ini");
    m.put(SECURITY_MANAGER,
        "User defined fully qualified class name implementing SecurityManager interface for integrated security. Defaults to \"{0}\". Legal values can be any \"class name\" implementing SecurityManager that is present in the classpath.");
    m.put(SECURITY_POST_PROCESSOR,
        "User defined fully qualified class name implementing PostProcessor interface for integrated security. Defaults to \"{0}\". Legal values can be any \"class name\" implementing PostProcessor that is present in the classpath.");

    m.put(SSL_ENABLED_COMPONENTS,
        "A comma delimited list of components that require SSL communications");

    m.put(SSL_CIPHERS, "List of available SSL cipher suites that are to be enabled. Defaults to \""
        + DEFAULT_SSL_CIPHERS + "\" meaning your provider''s defaults.");
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
    m.put(VALIDATE_SERIALIZABLE_OBJECTS,
        "If true checks incoming java serializable objects against a filter");
    m.put(SERIALIZABLE_OBJECT_FILTER, "The filter to check incoming java serializables against");

    dcAttDescriptions = Collections.unmodifiableMap(m);

  }

  /**
   * Used by unit tests.
   */
  public static String[] _getAttNames() {
    return dcValidAttributeNames;
  }

  public String[] getAttributeNames() {
    return dcValidAttributeNames;
  }

  public String[] getSpecificAttributeNames() {
    return dcValidAttributeNames;
  }

  @Override
  protected Map getAttDescMap() {
    return dcAttDescriptions;
  }

  public static InetAddress _getDefaultMcastAddress() {
    // Default MCast address can be just IPv4 address.
    // On IPv6 machines, JGroups converts IPv4 address to equivalent IPv6 address.
    String ipLiteral = "239.192.81.1";
    try {
      return InetAddress.getByName(ipLiteral);
    } catch (UnknownHostException ex) {
      // this should never happen
      throw new Error(
          LocalizedStrings.AbstractDistributionConfig_UNEXPECTED_PROBLEM_GETTING_INETADDRESS_0
              .toLocalizedString(ex),
          ex);
    }
  }

  /****************************
   * static initializers to gather all the checkers in this class
   *************************/
  static final Map<String, Method> checkers = new HashMap<String, Method>();

  static {
    for (Method method : AbstractDistributionConfig.class.getDeclaredMethods()) {
      if (method.isAnnotationPresent(ConfigAttributeChecker.class)) {
        ConfigAttributeChecker checker = method.getAnnotation(ConfigAttributeChecker.class);
        checkers.put(checker.name(), method);
      }
    }
  }
}
