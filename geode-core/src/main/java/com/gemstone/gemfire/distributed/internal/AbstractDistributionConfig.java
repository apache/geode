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

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.InvalidValueException;
import com.gemstone.gemfire.UnmodifiableException;
import com.gemstone.gemfire.internal.AbstractConfig;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer;

/**
 * Provides an implementation of <code>DistributionConfig</code> that
 * knows how to read the configuration file.
 * <p>
 * Note that if you add a property to this interface, you should
 * update the {@link AbstractConfig#sameAs} method and the {@link
 * DistributionConfigImpl#DistributionConfigImpl(DistributionConfig)
 * copy constructor}.
 *
 *
 */
@SuppressWarnings("deprecation")
public abstract class AbstractDistributionConfig
  extends AbstractConfig
  implements DistributionConfig
{

  protected Object checkAttribute(String attName, Object value){
    // first check to see if this attribute is modifiable, this also checks if the attribute is a valid one.
    if (!isAttributeModifiable(attName)) {
      throw new UnmodifiableException(_getUnmodifiableMsg(attName));
    }

    ConfigAttribute attribute = attributes.get(attName);
    if(attribute==null){
      // isAttributeModifiable already checks the validity of the attName, if reached here, then they
      // must be those special attributes that starts with ssl_system_props or sys_props, no further checking needed
      return value;
    }
    // for integer attribute, do the range check.
    if(attribute.type().equals(Integer.class)){
      Integer intValue = (Integer)value;
      minMaxCheck(attName, intValue, attribute.min(), attribute.max());
    }

    Method checker = checkers.get(attName);
    if(checker==null)
      return value;

    // if specific checker exists for this attribute, call that with the value
    try {
      return checker.invoke(this, value);
    } catch (Exception e) {
      if(e instanceof RuntimeException){
        throw (RuntimeException)e;
      }
      if(e.getCause() instanceof RuntimeException){
        throw (RuntimeException)e.getCause();
      }
      else
        throw new InternalGemFireException("error invoking "+checker.getName()+" with value "+value);
    }
  }


  protected void minMaxCheck(String propName, int value, int minValue, int maxValue) {
    if (value < minValue) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[]{propName, Integer.valueOf(value), Integer.valueOf(minValue)}));
    } else if (value > maxValue) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[]{propName, Integer.valueOf(value), Integer.valueOf(maxValue)}));
    }
  }


  @ConfigAttributeChecker(name=START_LOCATOR_NAME)
  protected String checkStartLocator(String value) {
    if (value != null && value.trim().length() > 0) {
      // throws IllegalArgumentException if string is malformed
      new DistributionLocatorId(value);
    }
    return value;
  }


  @ConfigAttributeChecker(name=TCP_PORT_NAME)
  protected int checkTcpPort(int value) {
    if ( getSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {TCP_PORT_NAME, Integer.valueOf(value), SSL_ENABLED_NAME}));
    }
    if ( getClusterSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {TCP_PORT_NAME, Integer.valueOf(value), CLUSTER_SSL_ENABLED_NAME}));
    }
    return value;
  }

  @ConfigAttributeChecker(name=MCAST_PORT_NAME)
  protected int checkMcastPort(int value) {
    if ( getSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {MCAST_PORT_NAME, Integer.valueOf(value), SSL_ENABLED_NAME}));
    }
    if ( getClusterSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {MCAST_PORT_NAME, Integer.valueOf(value), CLUSTER_SSL_ENABLED_NAME}));
    }
    return value;
  }


  @ConfigAttributeChecker(name=MCAST_ADDRESS_NAME)
  protected InetAddress checkMcastAddress(InetAddress value) {
    if (!value.isMulticastAddress()) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_IT_WAS_NOT_A_MULTICAST_ADDRESS.toLocalizedString(new Object[] {MCAST_ADDRESS_NAME, value}));
    }
    return value;
  }

  @ConfigAttributeChecker(name=BIND_ADDRESS_NAME)
  protected String checkBindAddress(String value) {
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
    return value;
  }


  @ConfigAttributeChecker(name=SERVER_BIND_ADDRESS_NAME)
  protected String checkServerBindAddress(String value) {
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
    return value;
  }

  @ConfigAttributeChecker(name=SSL_ENABLED_NAME)
  protected Boolean checkSSLEnabled(Boolean value) {
    if ( value.booleanValue() && (getMcastPort() != 0) ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_FALSE_WHEN_2_IS_NOT_0.toLocalizedString(new Object[] {SSL_ENABLED_NAME, value, MCAST_PORT_NAME}));
    }
    return value;
  }

  @ConfigAttributeChecker(name=CLUSTER_SSL_ENABLED_NAME)
  protected Boolean checkClusterSSLEnabled(Boolean value) {
    if ( value.booleanValue() && (getMcastPort() != 0) ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_FALSE_WHEN_2_IS_NOT_0.toLocalizedString(new Object[] {CLUSTER_SSL_ENABLED_NAME, value, MCAST_PORT_NAME}));
    }
    return value;
  }

  @ConfigAttributeChecker(name=HTTP_SERVICE_BIND_ADDRESS_NAME)
  protected String checkHttpServiceBindAddress(String value) {
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
    return value;
  }


  @ConfigAttributeChecker(name=DISTRIBUTED_SYSTEM_ID_NAME)
  protected int checkDistributedSystemId(int value) {
    String distributedSystemListener = System
    .getProperty("gemfire.DistributedSystemListener");
    //this check is specific for Jayesh's use case of WAN BootStraping
    if(distributedSystemListener == null){
      if (value < MIN_DISTRIBUTED_SYSTEM_ID) {
        throw new IllegalArgumentException(
            LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2
                .toLocalizedString(new Object[] { DISTRIBUTED_SYSTEM_ID_NAME,
                    Integer.valueOf(value),
                    Integer.valueOf(MIN_DISTRIBUTED_SYSTEM_ID) }));
      }
    }
    if (value > MAX_DISTRIBUTED_SYSTEM_ID) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {DISTRIBUTED_SYSTEM_ID_NAME, Integer.valueOf(value), Integer.valueOf(MAX_DISTRIBUTED_SYSTEM_ID)}));
    }
    return value;
  }

  /**
   * Makes sure that the locator string used to configure discovery is
   * valid.
   *
   * <p>Starting in 4.0, we accept locators in the format
   * "host:port" in addition to the traditional "host:bind-address[port]" format.
   * See bug 32306.
   *
   * <p>Starting in 5.1.0.4, we accept locators in the format
   * "host@bind-address[port]" to allow use of numeric IPv6 addresses
   *
   * @return The locator string in the traditional "host:bind-address[port]"
   *         format.
   *
   * @throws IllegalArgumentException
   *         If <code>value</code> is not a valid locator
   *         configuration
   */
  @ConfigAttributeChecker(name=LOCATORS_NAME)
  protected String checkLocators(String value) {
    // validate locators value
    StringBuffer sb = new StringBuffer();

    Set locs = new java.util.HashSet();

    StringTokenizer st = new StringTokenizer(value, ",");
    boolean firstUniqueLocator = true;
    while (st.hasMoreTokens()) {
      String locator = st.nextToken();
      StringBuffer locatorsb = new StringBuffer();  // string for this locator is accumulated in this buffer

      int portIndex = locator.indexOf('[');
      if (portIndex < 1) {
        portIndex = locator.lastIndexOf(':');
      }
      if (portIndex < 1) {
        throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0_HOST_NAME_WAS_EMPTY.toLocalizedString(value));
      }

      // starting in 5.1.0.4 we allow '@' as the bind-addr separator
      // to let people use IPv6 numeric addresses (which contain colons)
      int bindAddrIdx = locator.lastIndexOf('@', portIndex - 1);

      if (bindAddrIdx < 0) {
        bindAddrIdx = locator.lastIndexOf(':', portIndex - 1);
      }

      String host = locator.substring(0,
          bindAddrIdx > -1 ? bindAddrIdx : portIndex);

      if (host.indexOf(':') >= 0) {
        bindAddrIdx = locator.lastIndexOf('@');
        host = locator.substring(0, bindAddrIdx > -1 ? bindAddrIdx : portIndex);
      }

      InetAddress hostAddress = null;

      try {
        hostAddress = InetAddress.getByName(host);

      } catch (UnknownHostException ex) {
        throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_UNKNOWN_LOCATOR_HOST_0.toLocalizedString(host));
      }

      locatorsb.append(host);

      if (bindAddrIdx > -1) {
        // validate the bindAddress... (console needs this)
        String bindAddr = locator.substring(bindAddrIdx + 1, portIndex);
        try {
          hostAddress = InetAddress.getByName(bindAddr);

        } catch (UnknownHostException ex) {
          throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_UNKNOWN_LOCATOR_BIND_ADDRESS_0.toLocalizedString(bindAddr));
        }

        if (bindAddr.indexOf(':') >= 0) {
          locatorsb.append('@');
        }
        else {
          locatorsb.append(':');
        }
        locatorsb.append(bindAddr);
      }

      int lastIndex = locator.lastIndexOf(']');
      if (lastIndex == -1) {
        if (locator.indexOf('[') >= 0) {
          throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0.toLocalizedString(value));

        } else {
          // Using host:port syntax
          lastIndex = locator.length();
        }
      }

      String port = locator.substring(portIndex + 1, lastIndex);
      int portVal = 0;
      try {
        portVal = Integer.parseInt(port);
        if (portVal < 1 || portVal > 65535) {
          throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0_THE_PORT_1_WAS_NOT_GREATER_THAN_ZERO_AND_LESS_THAN_65536.toLocalizedString(new Object[] {value, Integer.valueOf(portVal)}));
        }
      } catch (NumberFormatException ex) {
        throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_INVALID_LOCATOR_0.toLocalizedString(value));
      }

      locatorsb.append('[');
      locatorsb.append(port);
      locatorsb.append(']');

      // if this wasn't a duplicate, add it to the locators string
      java.net.InetSocketAddress sockAddr = new java.net.InetSocketAddress(hostAddress, portVal);
      if (!locs.contains(sockAddr)) {
        if (!firstUniqueLocator) {
          sb.append(',');
        }
        else {
          firstUniqueLocator=false;
        }
        locs.add(new java.net.InetSocketAddress(hostAddress, portVal));
        sb.append(locatorsb);
      }
    }

    return sb.toString();
  }

  /** check a new mcast flow-control setting */
  @ConfigAttributeChecker(name=MCAST_FLOW_CONTROL_NAME)
  protected FlowControlParams checkMcastFlowControl(FlowControlParams params) {
    int value = params.getByteAllowance();
    if (value < MIN_FC_BYTE_ALLOWANCE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_BYTEALLOWANCE_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {MCAST_FLOW_CONTROL_NAME, Integer.valueOf(value), Integer.valueOf(MIN_FC_BYTE_ALLOWANCE)}));
    }
    float fvalue = params.getRechargeThreshold();
    if (fvalue < MIN_FC_RECHARGE_THRESHOLD) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGETHRESHOLD_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {MCAST_FLOW_CONTROL_NAME, new Float(fvalue), new Float(MIN_FC_RECHARGE_THRESHOLD)}));
    }
    else if (fvalue > MAX_FC_RECHARGE_THRESHOLD) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGETHRESHOLD_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {MCAST_FLOW_CONTROL_NAME, new Float(fvalue), new Float(MAX_FC_RECHARGE_THRESHOLD)}));
    }
    value = params.getRechargeBlockMs();
    if (value < MIN_FC_RECHARGE_BLOCK_MS) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGEBLOCKMS_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {MCAST_FLOW_CONTROL_NAME, Integer.valueOf(value), Integer.valueOf(MIN_FC_RECHARGE_BLOCK_MS)}));
    }
    else if (value > MAX_FC_RECHARGE_BLOCK_MS) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_RECHARGEBLOCKMS_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {MCAST_FLOW_CONTROL_NAME, Integer.valueOf(value), Integer.valueOf(MAX_FC_RECHARGE_BLOCK_MS)}));
    }
    return params;
  }


  @ConfigAttributeChecker(name=MEMBERSHIP_PORT_RANGE_NAME)
  protected int[] checkMembershipPortRange(int[] value) {
    minMaxCheck(MEMBERSHIP_PORT_RANGE_NAME, value[0],
        DEFAULT_MEMBERSHIP_PORT_RANGE[0],
        value[1]);
    minMaxCheck(MEMBERSHIP_PORT_RANGE_NAME, value[1],
        value[0],
        DEFAULT_MEMBERSHIP_PORT_RANGE[1]);

    // Minimum 3 ports are required to start a Gemfire data node,
    // One for each, UDP, FD_SOCk protocols and Cache Server.
    if (value[1] - value[0] < 2) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.
          toLocalizedString(new Object[] {MEMBERSHIP_PORT_RANGE_NAME, value[0]+"-"+value[1], Integer.valueOf(3)}));
    }
    return value;
  }


  /** @since 5.7 */
  @ConfigAttributeChecker(name=CLIENT_CONFLATION_PROP_NAME)
  protected String checkClientConflation(String value) {
    if (! (value.equals(CLIENT_CONFLATION_PROP_VALUE_DEFAULT) ||
            value.equals(CLIENT_CONFLATION_PROP_VALUE_ON) ||
              value.equals(CLIENT_CONFLATION_PROP_VALUE_OFF)) ) {
      throw new IllegalArgumentException("Could not set \"" + CLIENT_CONFLATION_PROP_NAME + "\" to \"" + value + "\" because its value is not recognized");
    }
    return value;
  }

  @ConfigAttributeChecker(name=SECURITY_PEER_AUTH_INIT_NAME)
  protected String checkSecurityPeerAuthInit(String value) {
    if (value != null && value.length() > 0 && getMcastPort() != 0) {
      String mcastInfo = MCAST_PORT_NAME + "[" + getMcastPort() + "]";
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_2_MUST_BE_0_WHEN_SECURITY_IS_ENABLED
          .toLocalizedString(new Object[] {
             SECURITY_PEER_AUTH_INIT_NAME, value, mcastInfo }));
    }
    return value;
  }


  @ConfigAttributeChecker(name=SECURITY_PEER_AUTHENTICATOR_NAME)
  protected String checkSecurityPeerAuthenticator(String value) {
    if (value != null && value.length() > 0 && getMcastPort() != 0) {
       String mcastInfo = MCAST_PORT_NAME + "[" + getMcastPort() + "]";
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_2_MUST_BE_0_WHEN_SECURITY_IS_ENABLED
        .toLocalizedString(
          new Object[] {
            SECURITY_PEER_AUTHENTICATOR_NAME,
            value,
            mcastInfo}));
    }
    return value;
  }


  @ConfigAttributeChecker(name=SECURITY_LOG_LEVEL_NAME)
  protected int checkSecurityLogLevel(int value) {
    if (value < MIN_LOG_LEVEL) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(
          new Object[] {
              SECURITY_LOG_LEVEL_NAME,
              LogWriterImpl.levelToString(value),
              LogWriterImpl.levelToString(MIN_LOG_LEVEL)}));
    }
    if (value > MAX_LOG_LEVEL) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(
        new Object[] {
            SECURITY_LOG_LEVEL_NAME,
            LogWriterImpl.levelToString(value),
            LogWriterImpl.levelToString(MAX_LOG_LEVEL)}));
    }
    return value;
  }


  @ConfigAttributeChecker(name=MEMCACHED_PROTOCOL_NAME)
  protected String checkMemcachedProtocol(String protocol) {
    if (protocol == null
        || (!protocol.equalsIgnoreCase(GemFireMemcachedServer.Protocol.ASCII.name()) &&
            !protocol.equalsIgnoreCase(GemFireMemcachedServer.Protocol.BINARY.name()))) {
      throw new IllegalArgumentException(LocalizedStrings.
          AbstractDistributionConfig_MEMCACHED_PROTOCOL_MUST_BE_ASCII_OR_BINARY.toLocalizedString());
    }
    return protocol;
  }

  public boolean isMemcachedProtocolModifiable() {
    return false;
  }

  @ConfigAttributeChecker(name=MEMCACHED_BIND_ADDRESS_NAME)
  protected String checkMemcachedBindAddress(String value) {
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_MEMCACHED_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
    return value;
  }

  @ConfigAttributeChecker(name=REDIS_BIND_ADDRESS_NAME)
  protected String checkRedisBindAddress(String value) {
    if (value != null && value.length() > 0 &&
            !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
              LocalizedStrings.AbstractDistributionConfig_REDIS_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
                      .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
                      }));
    }
    return value;
  }

  // AbstractConfig overriding methods

  @Override
  protected void checkAttributeName(String attName) {
    if(!attName.startsWith(SECURITY_PREFIX_NAME) && !attName.startsWith(USERDEFINED_PREFIX_NAME)
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
        throw new InvalidValueException(LocalizedStrings.AbstractDistributionConfig_0_VALUE_1_MUST_BE_OF_TYPE_2.toLocalizedString(new Object[]{attName, attValue, validValueClass.getName()}));
      }
    }

    if (attName.startsWith(USERDEFINED_PREFIX_NAME)) {
      //Do nothing its user defined property.
      return;
    }

    // special case: log-level and security-log-level attributes are String type, but the setter accepts int
    if(attName.equalsIgnoreCase(LOG_LEVEL_NAME) || attName.equalsIgnoreCase(SECURITY_LOG_LEVEL_NAME)){
      if(attValue instanceof String) {
        attValue = LogWriterImpl.levelNameToCode((String) attValue);
      }
    }

    if (attName.startsWith(SECURITY_PREFIX_NAME)) {
      this.setSecurity(attName,attValue.toString());
    }

    if (attName.startsWith(SSL_SYSTEM_PROPS_NAME) || attName.startsWith(SYS_PROP_NAME)) {
      this.setSSLProperty(attName, attValue.toString());
    }

    Method setter = setters.get(attName);
    if (setter == null) {
      // if we cann't find the defined setter, but the attributeName starts with these special characters
      // since we already set it in the respecitive properties above, we need to set the source then return
      if (attName.startsWith(SECURITY_PREFIX_NAME) ||
        attName.startsWith(SSL_SYSTEM_PROPS_NAME) ||
        attName.startsWith(SYS_PROP_NAME)) {
        getAttSourceMap().put(attName, source);
        return;
      }
      throw new InternalGemFireException(LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }

    Class[] pTypes = setter.getParameterTypes();
    if (pTypes.length != 1)
      throw new InternalGemFireException("the attribute setter must have one and only one parametter");


    try {
      setter.invoke(this, attValue);
    } catch (Exception e) {
      if(e instanceof RuntimeException){
        throw (RuntimeException)e;
      }
      if(e.getCause() instanceof RuntimeException){
        throw (RuntimeException)e.getCause();
      }
      else
        throw new InternalGemFireException("error invoking "+setter.getName()+" with "+attValue, e);
    }

    getAttSourceMap().put(attName, source);
  }

  public Object getAttributeObject(String attName) {
    checkAttributeName(attName);

    // special case:
    if (attName.equalsIgnoreCase(LOG_LEVEL_NAME)) {
      return LogWriterImpl.levelToString(this.getLogLevel());
    }

    if (attName.equalsIgnoreCase(SECURITY_LOG_LEVEL_NAME)) {
      return LogWriterImpl.levelToString(this.getSecurityLogLevel());
    }

    Method getter = getters.get(attName);
    if(getter==null) {
      if (attName.startsWith(SECURITY_PREFIX_NAME)) {
        return this.getSecurity(attName);
      }
      throw new InternalGemFireException(LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }

    try {
      return getter.invoke(this);
    } catch (Exception e) {
      if(e instanceof RuntimeException){
        throw (RuntimeException)e;
      }
      if(e.getCause() instanceof RuntimeException){
        throw (RuntimeException)e.getCause();
      }
      else
        throw new InternalGemFireException("error invoking " + getter.getName(), e);
    }
  }


  public boolean isAttributeModifiable(String attName) {
    checkAttributeName(attName);
    if(getModifiableAttributes().contains(attName))
      return true;

    if(getUnModifiableAttributes().contains(attName))
      return false;
    // otherwise, return the default
    return _modifiableDefault();
  }


  /**
   * child class can override this method to return a list of modifiable attributes
   * no matter what the default is
   * @return an empty list
     */
  public List<String> getModifiableAttributes(){
    String[] modifiables = {HTTP_SERVICE_PORT_NAME,JMX_MANAGER_HTTP_PORT_NAME};
    return Arrays.asList(modifiables);
  };

  /**
   * child class can override this method to return a list of unModifiable attributes
   * no matter what the default is
   * @return an empty list
   */
  public List<String> getUnModifiableAttributes(){
    String[] list = {};
    return Arrays.asList(list);
  };

  public Class getAttributeType(String attName) {
    checkAttributeName(attName);
    return _getAttributeType(attName);
  }

  public static Class _getAttributeType(String attName) {
    ConfigAttribute ca = attributes.get(attName);
    if(ca==null){
      if(attName.startsWith(SECURITY_PREFIX_NAME) || attName.startsWith(SSL_SYSTEM_PROPS_NAME) || attName.startsWith(SYS_PROP_NAME) ){
        return String.class;
      }
      throw new InternalGemFireException(LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }
    return ca.type();
  }

  protected static final Map dcAttDescriptions;
  static {
    Map<String, String> m =  new HashMap<String, String>();

    m.put(ACK_WAIT_THRESHOLD_NAME, 
      LocalizedStrings.AbstractDistributionConfig_DEFAULT_ACK_WAIT_THRESHOLD_0_1_2
      .toLocalizedString( new Object[] { 
          Integer.valueOf(DEFAULT_ACK_WAIT_THRESHOLD),
          Integer.valueOf(MIN_ACK_WAIT_THRESHOLD),
          Integer.valueOf(MIN_ACK_WAIT_THRESHOLD)}));

    m.put(ARCHIVE_FILE_SIZE_LIMIT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_ARCHIVE_FILE_SIZE_LIMIT_NAME
        .toLocalizedString());

    m.put(ACK_SEVERE_ALERT_THRESHOLD_NAME, 
      LocalizedStrings.AbstractDistributionConfig_ACK_SEVERE_ALERT_THRESHOLD_NAME
        .toLocalizedString( 
           new Object[] { ACK_WAIT_THRESHOLD_NAME, 
                          Integer.valueOf(DEFAULT_ACK_SEVERE_ALERT_THRESHOLD),
                          Integer.valueOf(MIN_ACK_SEVERE_ALERT_THRESHOLD),
                          Integer.valueOf(MAX_ACK_SEVERE_ALERT_THRESHOLD)}));

    m.put(ARCHIVE_DISK_SPACE_LIMIT_NAME,
      LocalizedStrings.AbstractDistributionConfig_ARCHIVE_DISK_SPACE_LIMIT_NAME
        .toLocalizedString());

    m.put(CACHE_XML_FILE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_CACHE_XML_FILE_NAME_0
        .toLocalizedString( DEFAULT_CACHE_XML_FILE ));

    m.put(DISABLE_TCP_NAME, 
      LocalizedStrings.AbstractDistributionConfig_DISABLE_TCP_NAME_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_DISABLE_TCP)));

    m.put(ENABLE_TIME_STATISTICS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_ENABLE_TIME_STATISTICS_NAME
        .toLocalizedString());

    m.put(DEPLOY_WORKING_DIR, 
        LocalizedStrings.AbstractDistributionConfig_DEPLOY_WORKING_DIR_0 
          .toLocalizedString(DEFAULT_DEPLOY_WORKING_DIR));

    m.put(LOG_FILE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_LOG_FILE_NAME_0
        .toLocalizedString(DEFAULT_LOG_FILE));

    m.put(LOG_LEVEL_NAME,
      LocalizedStrings.AbstractDistributionConfig_LOG_LEVEL_NAME_0_1 
        .toLocalizedString(new Object[] { LogWriterImpl.levelToString(DEFAULT_LOG_LEVEL), LogWriterImpl.allowedLogLevels()}));

    m.put(LOG_FILE_SIZE_LIMIT_NAME,
      LocalizedStrings.AbstractDistributionConfig_LOG_FILE_SIZE_LIMIT_NAME
        .toLocalizedString());

    m.put(LOG_DISK_SPACE_LIMIT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_LOG_DISK_SPACE_LIMIT_NAME
        .toLocalizedString());

    m.put(LOCATORS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_LOCATORS_NAME_0
        .toLocalizedString(DEFAULT_LOCATORS));
    
    m.put(LOCATOR_WAIT_TIME_NAME,
      LocalizedStrings.AbstractDistributionConfig_LOCATOR_WAIT_TIME_NAME_0
        .toLocalizedString(Integer.valueOf(DEFAULT_LOCATOR_WAIT_TIME)));

    m.put(TCP_PORT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_TCP_PORT_NAME_0_1_2
        .toLocalizedString( new Object[] {
          Integer.valueOf(DEFAULT_TCP_PORT),
          Integer.valueOf(MIN_TCP_PORT),
          Integer.valueOf(MAX_TCP_PORT)}));

    m.put(MCAST_PORT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MCAST_PORT_NAME_0_1_2
       .toLocalizedString(new Object[] {
          Integer.valueOf(DEFAULT_MCAST_PORT),
          Integer.valueOf(MIN_MCAST_PORT), 
          Integer.valueOf(MAX_MCAST_PORT)}));

    m.put(MCAST_ADDRESS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MCAST_ADDRESS_NAME_0_1
       .toLocalizedString(new Object[] {
          Integer.valueOf(DEFAULT_MCAST_PORT),
          DEFAULT_MCAST_ADDRESS}));

    m.put(MCAST_TTL_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MCAST_TTL_NAME_0_1_2
       .toLocalizedString(new Object[] {
          Integer.valueOf(DEFAULT_MCAST_TTL),
          Integer.valueOf(MIN_MCAST_TTL),
          Integer.valueOf(MAX_MCAST_TTL)}));

    m.put(MCAST_SEND_BUFFER_SIZE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MCAST_SEND_BUFFER_SIZE_NAME_0
       .toLocalizedString(Integer.valueOf(DEFAULT_MCAST_SEND_BUFFER_SIZE)));

    m.put(MCAST_RECV_BUFFER_SIZE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MCAST_RECV_BUFFER_SIZE_NAME_0
       .toLocalizedString(Integer.valueOf(DEFAULT_MCAST_RECV_BUFFER_SIZE)));

    m.put(MCAST_FLOW_CONTROL_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MCAST_FLOW_CONTROL_NAME_0
       .toLocalizedString(DEFAULT_MCAST_FLOW_CONTROL));

    m.put(MEMBER_TIMEOUT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MEMBER_TIMEOUT_NAME_0
        .toLocalizedString(Integer.valueOf(DEFAULT_MEMBER_TIMEOUT)));
    
    // for some reason the default port range is null under some circumstances
    int[] range = DEFAULT_MEMBERSHIP_PORT_RANGE;
    String srange = range==null? "not available" : "" + range[0] + "-" + range[1];
    String msg = LocalizedStrings.AbstractDistributionConfig_MEMBERSHIP_PORT_RANGE_NAME_0
                          .toLocalizedString(srange); 
    m.put(MEMBERSHIP_PORT_RANGE_NAME,
        msg);

    m.put(UDP_SEND_BUFFER_SIZE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_UDP_SEND_BUFFER_SIZE_NAME_0
       .toLocalizedString(Integer.valueOf(DEFAULT_UDP_SEND_BUFFER_SIZE)));

    m.put(UDP_RECV_BUFFER_SIZE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_UDP_RECV_BUFFER_SIZE_NAME_0
       .toLocalizedString(Integer.valueOf(DEFAULT_UDP_RECV_BUFFER_SIZE)));

    m.put(UDP_FRAGMENT_SIZE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_UDP_FRAGMENT_SIZE_NAME_0
       .toLocalizedString(Integer.valueOf(DEFAULT_UDP_FRAGMENT_SIZE)));

    m.put(SOCKET_LEASE_TIME_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SOCKET_LEASE_TIME_NAME_0_1_2
       .toLocalizedString(new Object[] { 
           Integer.valueOf(DEFAULT_SOCKET_LEASE_TIME),
           Integer.valueOf(MIN_SOCKET_LEASE_TIME), 
           Integer.valueOf(MAX_SOCKET_LEASE_TIME)}));
 
    m.put(SOCKET_BUFFER_SIZE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SOCKET_BUFFER_SIZE_NAME_0_1_2
        .toLocalizedString(new Object[] {
           Integer.valueOf(DEFAULT_SOCKET_BUFFER_SIZE),
           Integer.valueOf(MIN_SOCKET_BUFFER_SIZE),
           Integer.valueOf(MAX_SOCKET_BUFFER_SIZE)}));

    m.put(CONSERVE_SOCKETS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_CONSERVE_SOCKETS_NAME_0
        .toLocalizedString(Boolean.valueOf(DEFAULT_CONSERVE_SOCKETS)));

    m.put(ROLES_NAME,
      LocalizedStrings.AbstractDistributionConfig_ROLES_NAME_0
        .toLocalizedString(DEFAULT_ROLES));

    m.put(BIND_ADDRESS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_NAME_0
        .toLocalizedString(DEFAULT_BIND_ADDRESS));

    m.put(SERVER_BIND_ADDRESS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SERVER_BIND_ADDRESS_NAME_0
        .toLocalizedString(DEFAULT_BIND_ADDRESS));

    m.put(NAME_NAME, "A name that uniquely identifies a member in its distributed system." +
        " Multiple members in the same distributed system can not have the same name." +
        " Defaults to \"\".");

    m.put(STATISTIC_ARCHIVE_FILE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_STATISTIC_ARCHIVE_FILE_NAME_0
        .toLocalizedString(DEFAULT_STATISTIC_ARCHIVE_FILE));
   
    m.put(STATISTIC_SAMPLE_RATE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_STATISTIC_SAMPLE_RATE_NAME_0_1_2
        .toLocalizedString(new Object[] {
           Integer.valueOf(DEFAULT_STATISTIC_SAMPLE_RATE),
           Integer.valueOf(MIN_STATISTIC_SAMPLE_RATE),
           Integer.valueOf(MAX_STATISTIC_SAMPLE_RATE)}));
 
    m.put(STATISTIC_SAMPLING_ENABLED_NAME, 
      LocalizedStrings.AbstractDistributionConfig_STATISTIC_SAMPLING_ENABLED_NAME_0
        .toLocalizedString(
           Boolean.valueOf(DEFAULT_STATISTIC_SAMPLING_ENABLED)));

    m.put(SSL_ENABLED_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SSL_ENABLED_NAME_0
        .toLocalizedString(
           Boolean.valueOf(DEFAULT_SSL_ENABLED)));

    m.put(SSL_PROTOCOLS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SSL_PROTOCOLS_NAME_0
        .toLocalizedString(DEFAULT_SSL_PROTOCOLS));

    m.put(SSL_CIPHERS_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SSL_CIPHERS_NAME_0
        .toLocalizedString(DEFAULT_SSL_CIPHERS));

    m.put(SSL_REQUIRE_AUTHENTICATION_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SSL_REQUIRE_AUTHENTICATION_NAME
        .toLocalizedString(Boolean.valueOf(DEFAULT_SSL_REQUIRE_AUTHENTICATION)));
    
    m.put(CLUSTER_SSL_ENABLED_NAME, 
        LocalizedStrings.AbstractDistributionConfig_SSL_ENABLED_NAME_0
          .toLocalizedString(
             Boolean.valueOf(DEFAULT_CLUSTER_SSL_ENABLED)));

    m.put(CLUSTER_SSL_PROTOCOLS_NAME, 
        LocalizedStrings.AbstractDistributionConfig_SSL_PROTOCOLS_NAME_0
          .toLocalizedString(DEFAULT_CLUSTER_SSL_PROTOCOLS));

    m.put(CLUSTER_SSL_CIPHERS_NAME, 
        LocalizedStrings.AbstractDistributionConfig_SSL_CIPHERS_NAME_0
          .toLocalizedString(DEFAULT_CLUSTER_SSL_CIPHERS));

    m.put(CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME, 
        LocalizedStrings.AbstractDistributionConfig_SSL_REQUIRE_AUTHENTICATION_NAME
          .toLocalizedString(Boolean.valueOf(DEFAULT_CLUSTER_SSL_REQUIRE_AUTHENTICATION)));
    
    m.put(CLUSTER_SSL_KEYSTORE_NAME,"Location of the Java keystore file containing an distributed member's own certificate and private key.");

    m.put(CLUSTER_SSL_KEYSTORE_TYPE_NAME, 
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(CLUSTER_SSL_KEYSTORE_PASSWORD_NAME,"Password to access the private key from the keystore file specified by javax.net.ssl.keyStore.");

    m.put(CLUSTER_SSL_TRUSTSTORE_NAME,"Location of the Java keystore file containing the collection of CA certificates trusted by distributed member (trust store).");
    
    m.put(CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME,"Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");
    
    m.put(MAX_WAIT_TIME_FOR_RECONNECT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_MAX_WAIT_TIME_FOR_RECONNECT
        .toLocalizedString());

    m.put(MAX_NUM_RECONNECT_TRIES, 
      LocalizedStrings.AbstractDistributionConfig_MAX_NUM_RECONNECT_TRIES
        .toLocalizedString());

    m.put(ASYNC_DISTRIBUTION_TIMEOUT_NAME,
      LocalizedStrings.AbstractDistributionConfig_ASYNC_DISTRIBUTION_TIMEOUT_NAME_0_1_2
        .toLocalizedString( new Object[] {
            Integer.valueOf(DEFAULT_ASYNC_DISTRIBUTION_TIMEOUT),
            Integer.valueOf(MIN_ASYNC_DISTRIBUTION_TIMEOUT),
            Integer.valueOf(MAX_ASYNC_DISTRIBUTION_TIMEOUT)}));
        

    m.put(ASYNC_QUEUE_TIMEOUT_NAME,
      LocalizedStrings.AbstractDistributionConfig_ASYNC_QUEUE_TIMEOUT_NAME_0_1_2   
        .toLocalizedString( new Object[] {
          Integer.valueOf(DEFAULT_ASYNC_QUEUE_TIMEOUT),
          Integer.valueOf(MIN_ASYNC_QUEUE_TIMEOUT),
          Integer.valueOf(MAX_ASYNC_QUEUE_TIMEOUT)}));
    
    m.put(ASYNC_MAX_QUEUE_SIZE_NAME,
      LocalizedStrings.AbstractDistributionConfig_ASYNC_MAX_QUEUE_SIZE_NAME_0_1_2   
        .toLocalizedString( new Object[] {
          Integer.valueOf(DEFAULT_ASYNC_MAX_QUEUE_SIZE),
          Integer.valueOf(MIN_ASYNC_MAX_QUEUE_SIZE),
          Integer.valueOf(MAX_ASYNC_MAX_QUEUE_SIZE)}));       

    m.put(START_LOCATOR_NAME, 
      LocalizedStrings.AbstractDistributionConfig_START_LOCATOR_NAME
        .toLocalizedString());

    m.put(DURABLE_CLIENT_ID_NAME, 
      LocalizedStrings.AbstractDistributionConfig_DURABLE_CLIENT_ID_NAME_0
        .toLocalizedString(DEFAULT_DURABLE_CLIENT_ID));

    m.put(CLIENT_CONFLATION_PROP_NAME, 
      LocalizedStrings.AbstractDistributionConfig_CLIENT_CONFLATION_PROP_NAME
        .toLocalizedString());
    
    m.put(DURABLE_CLIENT_TIMEOUT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_DURABLE_CLIENT_TIMEOUT_NAME_0
        .toLocalizedString(Integer.valueOf(DEFAULT_DURABLE_CLIENT_TIMEOUT)));

    m.put(SECURITY_CLIENT_AUTH_INIT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_AUTH_INIT_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_CLIENT_AUTH_INIT));
    
    m.put(ENABLE_NETWORK_PARTITION_DETECTION_NAME, "Whether network partitioning detection is enabled");
    
    m.put(DISABLE_AUTO_RECONNECT_NAME, "Whether auto reconnect is attempted after a network partition");

    m.put(SECURITY_CLIENT_AUTHENTICATOR_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_AUTHENTICATOR_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_CLIENT_AUTHENTICATOR));

    m.put(SECURITY_CLIENT_DHALGO_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_DHALGO_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_CLIENT_DHALGO));

    m.put(SECURITY_PEER_AUTH_INIT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_PEER_AUTH_INIT_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_PEER_AUTH_INIT));

    m.put(SECURITY_PEER_AUTHENTICATOR_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_PEER_AUTHENTICATOR_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_PEER_AUTHENTICATOR));

    m.put(SECURITY_CLIENT_ACCESSOR_NAME,
      LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_ACCESSOR_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_CLIENT_ACCESSOR));

    m.put(SECURITY_CLIENT_ACCESSOR_PP_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_CLIENT_ACCESSOR_PP_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_CLIENT_ACCESSOR_PP));

    m.put(SECURITY_LOG_LEVEL_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_LOG_LEVEL_NAME_0_1
        .toLocalizedString( new Object[] {
           LogWriterImpl.levelToString(DEFAULT_LOG_LEVEL), 
           LogWriterImpl.allowedLogLevels()}));

    m.put(SECURITY_LOG_FILE_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_LOG_FILE_NAME_0
        .toLocalizedString(DEFAULT_SECURITY_LOG_FILE));

    m.put(SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME, 
      LocalizedStrings.AbstractDistributionConfig_SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME_0
	.toLocalizedString(Integer.valueOf(DEFAULT_SECURITY_PEER_VERIFYMEMBER_TIMEOUT)));

    m.put(SECURITY_PREFIX_NAME,
      LocalizedStrings.AbstractDistributionConfig_SECURITY_PREFIX_NAME
        .toLocalizedString());

    m.put(USERDEFINED_PREFIX_NAME,
        LocalizedStrings.AbstractDistributionConfig_USERDEFINED_PREFIX_NAME
          .toLocalizedString());

    m.put(REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME, 
        LocalizedStrings.AbstractDistributionConfig_REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME_0
          .toLocalizedString(DEFAULT_REMOVE_UNRESPONSIVE_CLIENT));

    m.put(DELTA_PROPAGATION_PROP_NAME, "Whether delta propagation is enabled");
    
    m.put(REMOTE_LOCATORS_NAME, 
        LocalizedStrings.AbstractDistributionConfig_REMOTE_DISTRIBUTED_SYSTEMS_NAME_0
          .toLocalizedString(DEFAULT_REMOTE_LOCATORS));

    m.put(DISTRIBUTED_SYSTEM_ID_NAME, "An id that uniquely idenitifies this distributed system. " +
        "Required when using portable data exchange objects and the WAN." +
    		"Must be the same on each member in this distributed system if set.");
    m.put(ENFORCE_UNIQUE_HOST_NAME, "Whether to require partitioned regions to put " +
    		"redundant copies of data on different physical machines");
    
    m.put(REDUNDANCY_ZONE_NAME, "The zone that this member is in. When this is set, " +
    		"partitioned regions will not put two copies of the same data in the same zone.");

    m.put(GROUPS_NAME, "A comma separated list of all the groups this member belongs to." +
        " Defaults to \"\".");
    
    m.put(USER_COMMAND_PACKAGES, "A comma separated list of the names of the packages containing classes that implement user commands.");
    
    m.put(JMX_MANAGER_NAME, "If true then this member is willing to be a jmx manager. Defaults to false except on a locator.");
    m.put(JMX_MANAGER_START_NAME, "If true then the jmx manager will be started when the cache is created. Defaults to false.");
    m.put(JMX_MANAGER_SSL_NAME, "If true then the jmx manager will only allow SSL clients to connect. Defaults to false. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_SSL_ENABLED_NAME, "If true then the jmx manager will only allow SSL clients to connect. Defaults to false. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_SSL_CIPHERS_NAME, "List of available SSL cipher suites that are to be enabled for JMX Manager. Defaults to \""+DEFAULT_JMX_MANAGER_SSL_CIPHERS+"\" meaning your provider''s defaults.");
    m.put(JMX_MANAGER_SSL_PROTOCOLS_NAME, "List of available SSL protocols that are to be enabled for JMX Manager. Defaults to \""+DEFAULT_JMX_MANAGER_SSL_PROTOCOLS+"\" meaning defaults of your provider.");
    m.put(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME, "If set to false, ciphers and protocols that permit anonymous JMX Clients are allowed. Defaults to \""+DEFAULT_JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION+"\".");
    m.put(JMX_MANAGER_SSL_KEYSTORE_NAME,"Location of the Java keystore file containing jmx manager's own certificate and private key.");
    m.put(JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME, "For Java keystore file format, this property has the value jks (or JKS).");
    m.put(JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME,"Password to access the private key from the keystore file specified by javax.net.ssl.keyStore. ");
    m.put(JMX_MANAGER_SSL_TRUSTSTORE_NAME,"Location of the Java keystore file containing the collection of CA certificates trusted by jmx manager.");
    m.put(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME,"Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");
    
    m.put(JMX_MANAGER_PORT_NAME, "The port the jmx manager will listen on. Default is \"" + DEFAULT_JMX_MANAGER_PORT + "\". Set to zero to disable GemFire's creation of a jmx listening port.");
    m.put(JMX_MANAGER_BIND_ADDRESS_NAME, "The address the jmx manager will listen on for remote connections. Default is \"\" which causes the jmx manager to listen on the host's default address. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME, "The hostname that will be given to clients when they ask a locator for the location of this jmx manager. Default is \"\" which causes the locator to report the jmx manager's actual ip address as its location. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_PASSWORD_FILE_NAME, "The name of the file the jmx manager will use to only allow authenticated clients to connect. Default is \"\" which causes the jmx manager to allow all clients to connect. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_ACCESS_FILE_NAME, "The name of the file the jmx manager will use to define the access level of authenticated clients. Default is \"\" which causes the jmx manager to allow all clients all access. This property is ignored if jmx-manager-port is \"0\".");
    m.put(JMX_MANAGER_HTTP_PORT_NAME, "By default when a jmx-manager is started it will also start an http server on this port. This server is used by the GemFire Pulse application. Setting this property to zero disables the http server. It defaults to 8080. Ignored if jmx-manager is false.");
    m.put(JMX_MANAGER_UPDATE_RATE_NAME, "The rate in milliseconds at which this member will send updates to each jmx manager. Default is " + DEFAULT_JMX_MANAGER_UPDATE_RATE + ". Values must be in the range " + MIN_JMX_MANAGER_UPDATE_RATE + ".." + MAX_JMX_MANAGER_UPDATE_RATE + ".");
    m.put(MEMCACHED_PORT_NAME, "The port GemFireMemcachedServer will listen on. Default is 0. Set to zero to disable GemFireMemcachedServer.");
    m.put(MEMCACHED_PROTOCOL_NAME, "The protocol that GemFireMemcachedServer understands. Default is ASCII. Values may be ASCII or BINARY");
    m.put(MEMCACHED_BIND_ADDRESS_NAME, "The address the GemFireMemcachedServer will listen on for remote connections. Default is \"\" which causes the GemFireMemcachedServer to listen on the host's default address. This property is ignored if memcached-port is \"0\".");
    m.put(REDIS_PORT_NAME, "The port GemFireRedisServer will listen on. Default is 0. Set to zero to disable GemFireRedisServer.");
    m.put(REDIS_BIND_ADDRESS_NAME, "The address the GemFireRedisServer will listen on for remote connections. Default is \"\" which causes the GemFireRedisServer to listen on the host's default address. This property is ignored if redis-port is \"0\".");
    m.put(REDIS_PASSWORD_NAME, "The password which client of GemFireRedisServer must use to authenticate themselves. The default is none and no authentication will be required.");
    m.put(ENABLE_CLUSTER_CONFIGURATION_NAME, LocalizedStrings.AbstractDistributionConfig_ENABLE_SHARED_CONFIGURATION.toLocalizedString());
    m.put(USE_CLUSTER_CONFIGURATION_NAME, LocalizedStrings.AbstractDistributionConfig_USE_SHARED_CONFIGURATION.toLocalizedString());
    m.put(LOAD_CLUSTER_CONFIG_FROM_DIR_NAME, LocalizedStrings.AbstractDistributionConfig_LOAD_SHARED_CONFIGURATION_FROM_DIR.toLocalizedString(SharedConfiguration.CLUSTER_CONFIG_ARTIFACTS_DIR_NAME));
    m.put(CLUSTER_CONFIGURATION_DIR, LocalizedStrings.AbstractDistributionConfig_CLUSTER_CONFIGURATION_DIR.toLocalizedString());
    m.put(
        SERVER_SSL_ENABLED_NAME,
        "If true then the cache server will only allow SSL clients to connect. Defaults to false.");
    m.put(
        SERVER_SSL_CIPHERS_NAME,
        "List of available SSL cipher suites that are to be enabled for CacheServer. Defaults to \""
            + DEFAULT_SERVER_SSL_CIPHERS
            + "\" meaning your provider''s defaults.");
    m.put(
        SERVER_SSL_PROTOCOLS_NAME,
        "List of available SSL protocols that are to be enabled for CacheServer. Defaults to \""
            + DEFAULT_SERVER_SSL_PROTOCOLS
            + "\" meaning defaults of your provider.");
    m.put(
        SERVER_SSL_REQUIRE_AUTHENTICATION_NAME,
        "If set to false, ciphers and protocols that permit anonymous Clients are allowed. Defaults to \""
            + DEFAULT_SERVER_SSL_REQUIRE_AUTHENTICATION + "\".");
    
    m.put(SERVER_SSL_KEYSTORE_NAME,"Location of the Java keystore file containing server's or client's own certificate and private key.");

    m.put(SERVER_SSL_KEYSTORE_TYPE_NAME, 
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(SERVER_SSL_KEYSTORE_PASSWORD_NAME,"Password to access the private key from the keystore file specified by javax.net.ssl.keyStore. ");

    m.put(SERVER_SSL_TRUSTSTORE_NAME,"Location of the Java keystore file containing the collection of CA certificates trusted by server or client(trust store).");
    
    m.put(SERVER_SSL_TRUSTSTORE_PASSWORD_NAME,"Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");
    
    m.put(
        GATEWAY_SSL_ENABLED_NAME,
        "If true then the gateway receiver will only allow SSL gateway sender to connect. Defaults to false.");
    m.put(
        GATEWAY_SSL_CIPHERS_NAME,
        "List of available SSL cipher suites that are to be enabled for Gateway Receiver. Defaults to \""
            + DEFAULT_GATEWAY_SSL_CIPHERS
            + "\" meaning your provider''s defaults.");
    m.put(
        GATEWAY_SSL_PROTOCOLS_NAME,
        "List of available SSL protocols that are to be enabled for Gateway Receiver. Defaults to \""
            + DEFAULT_GATEWAY_SSL_PROTOCOLS
            + "\" meaning defaults of your provider.");
    m.put(
        GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME,
        "If set to false, ciphers and protocols that permit anonymous gateway senders are allowed. Defaults to \""
            + DEFAULT_GATEWAY_SSL_REQUIRE_AUTHENTICATION + "\".");    
    
    m.put(GATEWAY_SSL_KEYSTORE_NAME,"Location of the Java keystore file containing gateway's own certificate and private key.");

    m.put(GATEWAY_SSL_KEYSTORE_TYPE_NAME, 
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(GATEWAY_SSL_KEYSTORE_PASSWORD_NAME,"Password to access the private key from the keystore file specified by javax.net.ssl.keyStore.");

    m.put(GATEWAY_SSL_TRUSTSTORE_NAME,"Location of the Java keystore file containing the collection of CA certificates trusted by gateway.");
    
    m.put(GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME,"Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");
    
    m.put(HTTP_SERVICE_PORT_NAME, "If non zero, then the gemfire developer REST service will be deployed and started when the cache is created. Default value is 0.");
    m.put(HTTP_SERVICE_BIND_ADDRESS_NAME, "The address where gemfire developer REST service will listen for remote REST connections. Default is \"\" which causes the Rest service to listen on the host's default address.");
    
    m.put(
        HTTP_SERVICE_SSL_ENABLED_NAME,
        "If true then the http service like REST dev api and Pulse will only allow SSL enabled clients to connect. Defaults to false.");
    m.put(
        HTTP_SERVICE_SSL_CIPHERS_NAME,
        "List of available SSL cipher suites that are to be enabled for Http Service. Defaults to \""
            + DEFAULT_HTTP_SERVICE_SSL_CIPHERS
            + "\" meaning your provider''s defaults.");
    m.put(
        HTTP_SERVICE_SSL_PROTOCOLS_NAME,
        "List of available SSL protocols that are to be enabled for Http Service. Defaults to \""
            + DEFAULT_HTTP_SERVICE_SSL_PROTOCOLS
            + "\" meaning defaults of your provider.");
    m.put(
        HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME,
        "If set to false, ciphers and protocols that permit anonymous http clients are allowed. Defaults to \""
            + DEFAULT_HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION + "\".");    
    
    m.put(HTTP_SERVICE_SSL_KEYSTORE_NAME,"Location of the Java keystore file containing Http Service's own certificate and private key.");

    m.put(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME, 
        "For Java keystore file format, this property has the value jks (or JKS).");

    m.put(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME,"Password to access the private key from the keystore file specified by javax.net.ssl.keyStore.");

    m.put(HTTP_SERVICE_SSL_TRUSTSTORE_NAME,"Location of the Java keystore file containing the collection of CA certificates trusted by Http Service.");
    
    m.put(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME,"Password to unlock the keystore file (store password) specified by  javax.net.ssl.trustStore.");
    
    m.put(START_DEV_REST_API_NAME, "If true then the developer(API) REST service will be started when the cache is created. Defaults to false.");
    m.put(OFF_HEAP_MEMORY_SIZE_NAME, LocalizedStrings.AbstractDistributionConfig_OFF_HEAP_MEMORY_SIZE_0.toLocalizedString(DEFAULT_OFF_HEAP_MEMORY_SIZE));
    m.put(LOCK_MEMORY_NAME, LocalizedStrings.AbstractDistributionConfig_LOCK_MEMORY.toLocalizedString(DEFAULT_LOCK_MEMORY));
    m.put(DISTRIBUTED_TRANSACTIONS_NAME, "Flag to indicate whether all transactions including JTA should be distributed transactions.  Default is false, meaning colocated transactions.");

    m.put(SECURITY_SHIRO_INIT_NAME, "The name of the shiro configuration file in the classpath, e.g. shiro.ini");

    dcAttDescriptions = Collections.unmodifiableMap(m);

  }
  /**
   * Used by unit tests.
   */
  static public String[] _getAttNames() {
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

  static final InetAddress _getDefaultMcastAddress() {
    String ipLiteral;
    if ( SocketCreator.preferIPv6Addresses() ) {
      ipLiteral = "FF38::1234"; // fix for bug 30014
    } else {
      ipLiteral = "239.192.81.1"; // fix for bug 30014
    }
    try {
      return InetAddress.getByName(ipLiteral);
    } catch (UnknownHostException ex) {
      // this should never happen
      throw new Error(LocalizedStrings.AbstractDistributionConfig_UNEXPECTED_PROBLEM_GETTING_INETADDRESS_0.toLocalizedString(ex), ex);
    }
  }

/**************************** static initializers to gather all the checkers in this class *************************/
  static final Map<String, Method> checkers = new HashMap<String, Method>();
  static{
    for(Method method:AbstractDistributionConfig.class.getDeclaredMethods()) {
      if (method.isAnnotationPresent(ConfigAttributeChecker.class)) {
        ConfigAttributeChecker checker = method.getAnnotation(ConfigAttributeChecker.class);
        checkers.put(checker.name(), method);
      }
    }
  }
}
