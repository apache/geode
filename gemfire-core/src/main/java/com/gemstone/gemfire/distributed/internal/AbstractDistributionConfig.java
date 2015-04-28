/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.File;
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
 * @author Darrel Schneider
 *
 */
@SuppressWarnings("deprecation")
public abstract class AbstractDistributionConfig
  extends AbstractConfig
  implements DistributionConfig
{
  protected void checkName(String value) {
    _checkIfModifiable(NAME_NAME);
  }
  public boolean isNameModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isEnableNetworkPartitionDetectionModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isDisableAutoReconnectModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isDepartureCorrelationWindowModifiable() {
    return _modifiableDefault();
  }

  protected void checkLogFile(File value) {
    _checkIfModifiable(LOG_FILE_NAME);
  }
  public boolean isLogFileModifiable() {
    return _modifiableDefault();
  }

  protected void checkLogLevel(int value) {
    _checkIfModifiable(LOG_LEVEL_NAME);
    if (value < MIN_LOG_LEVEL) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {LOG_LEVEL_NAME, Integer.valueOf(value), Integer.valueOf(MIN_LOG_LEVEL)}));
    }
    if (value > MAX_LOG_LEVEL) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {LOG_LEVEL_NAME, Integer.valueOf(value), Integer.valueOf(MAX_LOG_LEVEL)}));
    }
  }

  protected void checkStartLocator(String value) {
    _checkIfModifiable(START_LOCATOR_NAME);
    if (value != null && value.trim().length() > 0) {
      // throws IllegalArgumentException if string is malformed
      new DistributionLocatorId(value);
    }
  }

  public boolean isStartLocatorModifiable() {
    return _modifiableDefault();
  }

  public boolean isLogLevelModifiable() {
    return _modifiableDefault();
  }

  protected void checkTcpPort(int value) {
    _checkIfModifiable(TCP_PORT_NAME);
    if (value < MIN_TCP_PORT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {TCP_PORT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_TCP_PORT)}));
    }
    if (value > MAX_TCP_PORT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {TCP_PORT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_TCP_PORT)}));
    }
    if ( getSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {TCP_PORT_NAME, Integer.valueOf(value), SSL_ENABLED_NAME}));
    }
    if ( getClusterSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {TCP_PORT_NAME, Integer.valueOf(value), CLUSTER_SSL_ENABLED_NAME}));
    }
  }
  public boolean isTcpPortModifiable() {
    return _modifiableDefault();
  }

  protected void checkMcastPort(int value) {
    _checkIfModifiable(MCAST_PORT_NAME);
    if (value < MIN_MCAST_PORT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {MCAST_PORT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_MCAST_PORT)}));
    }
    if (value > MAX_MCAST_PORT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {MCAST_PORT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_MCAST_PORT)}));
    }
    if ( getSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {MCAST_PORT_NAME, Integer.valueOf(value), SSL_ENABLED_NAME}));
    }
    if ( getClusterSSLEnabled() && value != 0 ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_0_WHEN_2_IS_TRUE.toLocalizedString(new Object[] {MCAST_PORT_NAME, Integer.valueOf(value), CLUSTER_SSL_ENABLED_NAME}));
    }

  }
  public boolean isMcastPortModifiable() {
    return _modifiableDefault();
  }

  protected void checkMcastAddress(InetAddress value) {
    _checkIfModifiable(MCAST_ADDRESS_NAME);
    if (!value.isMulticastAddress()) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_IT_WAS_NOT_A_MULTICAST_ADDRESS.toLocalizedString(new Object[] {MCAST_ADDRESS_NAME, value}));
    }
  }
  public boolean isMcastAddressModifiable() {
    return _modifiableDefault();
  }

  protected void checkMcastTtl(int value) {
    _checkIfModifiable(MCAST_TTL_NAME);
    if (value < MIN_MCAST_TTL) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {MCAST_TTL_NAME, Integer.valueOf(value), Integer.valueOf(MIN_MCAST_TTL)}));
    }
    if (value > MAX_MCAST_TTL) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {MCAST_TTL_NAME, Integer.valueOf(value), Integer.valueOf(MAX_MCAST_TTL)}));
    }
  }
  public boolean isMcastTtlModifiable() {
    return _modifiableDefault();
  }

  protected void checkSocketLeaseTime(int value) {
    _checkIfModifiable(SOCKET_LEASE_TIME_NAME);
    if (value < MIN_SOCKET_LEASE_TIME) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {SOCKET_LEASE_TIME_NAME, Integer.valueOf(value), Integer.valueOf(MIN_SOCKET_LEASE_TIME)}));
    }
    if (value > MAX_SOCKET_LEASE_TIME) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {SOCKET_LEASE_TIME_NAME, Integer.valueOf(value), Integer.valueOf(MAX_SOCKET_LEASE_TIME)}));
    }
  }
  public boolean isSocketLeaseTimeModifiable() {
    return _modifiableDefault();
  }

  protected void checkSocketBufferSize(int value) {
    _checkIfModifiable(SOCKET_BUFFER_SIZE_NAME);
    if (value < MIN_SOCKET_BUFFER_SIZE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {SOCKET_BUFFER_SIZE_NAME, Integer.valueOf(value), Integer.valueOf(MIN_SOCKET_BUFFER_SIZE)}));
    }
    if (value > MAX_SOCKET_BUFFER_SIZE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {SOCKET_BUFFER_SIZE_NAME, Integer.valueOf(value), Integer.valueOf(MAX_SOCKET_BUFFER_SIZE)}));
    }
  }
  public boolean isSocketBufferSizeModifiable() {
    return _modifiableDefault();
  }

  protected void checkConserveSockets(boolean value) {
    _checkIfModifiable(CONSERVE_SOCKETS_NAME);
  }
  public boolean isConserveSocketsModifiable() {
    return _modifiableDefault();
  }

  protected void checkRoles(String value) {
    _checkIfModifiable(ROLES_NAME);
  }
  public boolean isRolesModifiable() {
    return _modifiableDefault();
  }

  protected void checkBindAddress(String value) {
    _checkIfModifiable(BIND_ADDRESS_NAME);
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
  }
  public boolean isBindAddressModifiable() {
    return _modifiableDefault();
  }

  protected void checkServerBindAddress(String value) {
    _checkIfModifiable(SERVER_BIND_ADDRESS_NAME);
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
  }
  public boolean isServerBindAddressModifiable() {
    return _modifiableDefault();
  }

  protected void checkDeployWorkingDir(File value) {
    _checkIfModifiable(DEPLOY_WORKING_DIR);
  }

  public boolean isDeployWorkingDirModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkStatisticSamplingEnabled(boolean value) {
    _checkIfModifiable(STATISTIC_SAMPLING_ENABLED_NAME);
  }
  public boolean isStatisticSamplingEnabledModifiable() {
    return _modifiableDefault();
  }

  
  protected void checkStatisticArchiveFile(File value) {
    _checkIfModifiable(STATISTIC_ARCHIVE_FILE_NAME);
  }
  public boolean isStatisticArchiveFileModifiable() {
    return _modifiableDefault();
  }

  protected void checkCacheXmlFile(File value) {
    _checkIfModifiable(CACHE_XML_FILE_NAME);
  }
  public boolean isCacheXmlFileModifiable() {
    return _modifiableDefault();
  }

  protected void checkSSLEnabled(Boolean value) {
    _checkIfModifiable(SSL_ENABLED_NAME);
    if ( value.booleanValue() && (getMcastPort() != 0) ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_FALSE_WHEN_2_IS_NOT_0.toLocalizedString(new Object[] {SSL_ENABLED_NAME, value, MCAST_PORT_NAME}));
    }
  }
  
  protected void checkClusterSSLEnabled(Boolean value) {
    _checkIfModifiable(CLUSTER_SSL_ENABLED_NAME);
    if ( value.booleanValue() && (getMcastPort() != 0) ) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_MUST_BE_FALSE_WHEN_2_IS_NOT_0.toLocalizedString(new Object[] {CLUSTER_SSL_ENABLED_NAME, value, MCAST_PORT_NAME}));
    }
  }

  protected void checkHttpServicePort(int value) {
    minMaxCheck(HTTP_SERVICE_PORT_NAME, value, 0, 65535);
  }
  
  public boolean isHttpServicePortModifiable() {
    //Need to set "true" for hydra teststo set REST service port
    return true;
  }
  
  protected void checkHttpServiceBindAddress(String value) {
    _checkIfModifiable(HTTP_SERVICE_BIND_ADDRESS_NAME);
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
  }
  
  public boolean isHttpServiceBindAddressModifiable() {
    return _modifiableDefault();
  }
  
  //Add for HTTP Service SSL Start
  public boolean isHttpServiceSSLEnabledModifiable(){
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSL(){
    _checkIfModifiable(HTTP_SERVICE_SSL_ENABLED_NAME);
  }

  public boolean isHttpServiceSSLRequireAuthenticationModifiable(){
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSLRequireAuthentication(){
    _checkIfModifiable(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME);
  }
  
  public boolean isHttpServiceSSLProtocolsModifiable(){
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSLProtocols(){
    _checkIfModifiable(HTTP_SERVICE_SSL_PROTOCOLS_NAME);
  }
  
  public boolean isHttpServiceSSLCiphersModifiable(){
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSLCiphers(){
    _checkIfModifiable(HTTP_SERVICE_SSL_CIPHERS_NAME);
  }
  
  public void checkHttpServiceSSLKeyStore(String value) {
    _checkIfModifiable(HTTP_SERVICE_SSL_KEYSTORE_NAME);
  }
  public boolean isHttpServiceSSLKeyStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSLKeyStoreType(String value) {
    _checkIfModifiable(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME);
  }
  public boolean isHttpServiceSSLKeyStoreTypeModifiable() {
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSLKeyStorePassword(String value) {
    _checkIfModifiable(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME);
  }
  public boolean isHttpServiceSSLKeyStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSLTrustStore(String value) {
    _checkIfModifiable(HTTP_SERVICE_SSL_TRUSTSTORE_NAME);
  }
  public boolean isHttpServiceSSLTrustStoreModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isHttpServiceSSLTrustStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkHttpServiceSSLTrustStorePassword(String value) {
    _checkIfModifiable(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME);
  }
  
//Add for HTTP Service SSL End
  
  
  
  protected void checkStartDevRestApi() {
    _checkIfModifiable(START_DEV_REST_API_NAME);
  }

  @Override
  public boolean isStartDevRestApiModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isSSLEnabledModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isClusterSSLEnabledModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkSSLProtocols(String value) {
    _checkIfModifiable(SSL_PROTOCOLS_NAME);
  }
  protected void checkClusterSSLProtocols(String value) {
    _checkIfModifiable(CLUSTER_SSL_PROTOCOLS_NAME);
  }
  public boolean isSSLProtocolsModifiable() {
    return _modifiableDefault();
  }
  public boolean isClusterSSLProtocolsModifiable() {
    return _modifiableDefault();
  }
  protected void checkSSLCiphers(String value) {
    _checkIfModifiable(SSL_CIPHERS_NAME);
  }
  protected void checkClusterSSLCiphers(String value) {
    _checkIfModifiable(CLUSTER_SSL_CIPHERS_NAME);
  }
  public boolean isSSLCiphersModifiable() {
    return _modifiableDefault();
  }
  public boolean isClusterSSLCiphersModifiable() {
    return _modifiableDefault();
  }
  public void checkSSLRequireAuthentication(Boolean value) {
    _checkIfModifiable(SSL_REQUIRE_AUTHENTICATION_NAME);
  }
  public void checkClusterSSLRequireAuthentication(Boolean value) {
    _checkIfModifiable(CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME);
  }
  public boolean isSSLRequireAuthenticationModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isClusterSSLRequireAuthenticationModifiable() {
    return _modifiableDefault();
  }
  
  public void checkClusterSSLKeyStore(String value) {
    _checkIfModifiable(CLUSTER_SSL_KEYSTORE_NAME);
  }
  public boolean isClusterSSLKeyStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkClusterSSLKeyStoreType(String value) {
    _checkIfModifiable(CLUSTER_SSL_KEYSTORE_TYPE_NAME);
  }
  public boolean isClusterSSLKeyStoreTypeModifiable() {
    return _modifiableDefault();
  }
  
  public void checkClusterSSLKeyStorePassword(String value) {
    _checkIfModifiable(CLUSTER_SSL_KEYSTORE_PASSWORD_NAME);
  }
  public boolean isClusterSSLKeyStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkClusterSSLTrustStore(String value) {
    _checkIfModifiable(CLUSTER_SSL_TRUSTSTORE_NAME);
  }
  public boolean isClusterSSLTrustStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkClusterSSLTrustStorePassword(String value) {
    _checkIfModifiable(CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME);
  }
  
  public boolean isClusterSSLTrustStorePasswordModifiable() {
    return _modifiableDefault();
  }

  public void checkServerSSLKeyStore(String value) {
    _checkIfModifiable(SERVER_SSL_KEYSTORE_NAME);
  }
  public boolean isServerSSLKeyStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkServerSSLKeyStoreType(String value) {
    _checkIfModifiable(SERVER_SSL_KEYSTORE_TYPE_NAME);
  }
  public boolean isServerSSLKeyStoreTypeModifiable() {
    return _modifiableDefault();
  }
  
  public void checkServerSSLKeyStorePassword(String value) {
    _checkIfModifiable(SERVER_SSL_KEYSTORE_PASSWORD_NAME);
  }
  public boolean isServerSSLKeyStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkServerSSLTrustStore(String value) {
    _checkIfModifiable(SERVER_SSL_TRUSTSTORE_NAME);
  }
  public boolean isServerSSLTrustStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkServerSSLTrustStorePassword(String value) {
    _checkIfModifiable(SERVER_SSL_TRUSTSTORE_PASSWORD_NAME);
  }
  
  public boolean isServerSSLTrustStorePasswordModifiable() {
    return _modifiableDefault();
  }

  public void checkJmxManagerSSLKeyStore(String value) {
    _checkIfModifiable(JMX_MANAGER_SSL_KEYSTORE_NAME);
  }
  public boolean isJmxManagerSSLKeyStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkJmxManagerSSLKeyStoreType(String value) {
    _checkIfModifiable(JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME);
  }
  public boolean isJmxManagerSSLKeyStoreTypeModifiable() {
    return _modifiableDefault();
  }
  
  public void checkJmxManagerSSLKeyStorePassword(String value) {
    _checkIfModifiable(JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME);
  }
  public boolean isJmxManagerSSLKeyStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkJmxManagerSSLTrustStore(String value) {
    _checkIfModifiable(JMX_MANAGER_SSL_TRUSTSTORE_NAME);
  }
  public boolean isJmxManagerSSLTrustStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkJmxManagerSSLTrustStorePassword(String value) {
    _checkIfModifiable(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME);
  }
  
  public boolean isJmxManagerSSLTrustStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLKeyStore(String value) {
    _checkIfModifiable(GATEWAY_SSL_KEYSTORE_NAME);
  }
  public boolean isGatewaySSLKeyStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLKeyStoreType(String value) {
    _checkIfModifiable(GATEWAY_SSL_KEYSTORE_TYPE_NAME);
  }
  public boolean isGatewaySSLKeyStoreTypeModifiable() {
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLKeyStorePassword(String value) {
    _checkIfModifiable(GATEWAY_SSL_KEYSTORE_PASSWORD_NAME);
  }
  public boolean isGatewaySSLKeyStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLTrustStore(String value) {
    _checkIfModifiable(GATEWAY_SSL_TRUSTSTORE_NAME);
  }
  public boolean isGatewaySSLTrustStoreModifiable() {
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLTrustStorePassword(String value) {
    _checkIfModifiable(GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME);
  }
  
  public boolean isGatewaySSLTrustStorePasswordModifiable() {
    return _modifiableDefault();
  }
  
  public void checkMcastSendBufferSizeModifiable() {
    _checkIfModifiable(MCAST_SEND_BUFFER_SIZE_NAME);
  }

  public boolean isMcastSendBufferSizeModifiable() {
    return _modifiableDefault();
  }

  public void checkMcastRecvBufferSizeModifiable() {
    _checkIfModifiable(MCAST_RECV_BUFFER_SIZE_NAME);
  }

  public boolean isMcastRecvBufferSizeModifiable() {
    return _modifiableDefault();
  }

  public void checkMcastFlowControlModifiable() {
    _checkIfModifiable(MCAST_FLOW_CONTROL_NAME);
  }

  public boolean isMcastFlowControlModifiable() {
    return _modifiableDefault();
  }

  public void checkUdpSendBufferSizeModifiable() {
    _checkIfModifiable(UDP_SEND_BUFFER_SIZE_NAME);
  }

  public boolean isUdpSendBufferSizeModifiable() {
    return _modifiableDefault();
  }

  public void checkUdpRecvBufferSizeModifiable() {
    _checkIfModifiable(UDP_RECV_BUFFER_SIZE_NAME);
  }

  public boolean isUdpRecvBufferSizeModifiable() {
    return _modifiableDefault();
  }

  public void checkUdpFragmentSizeModifiable() {
    _checkIfModifiable(UDP_FRAGMENT_SIZE_NAME);
  }

  public boolean isUdpFragmentSizeModifiable() {
    return _modifiableDefault();
  }

  public void checkDisableTcpModifiable() {
    _checkIfModifiable(DISABLE_TCP_NAME);
  }

  public boolean isDisableTcpModifiable() {
    return _modifiableDefault();
  }

  public void checkEnableTimeStatisticsModifiable() {
    _checkIfModifiable(ENABLE_TIME_STATISTICS_NAME);
  }

  public boolean isEnableTimeStatisticsModifiable() {
    return _modifiableDefault();
  }

  public void checkMemberTimeoutModifiable() {
    _checkIfModifiable(MEMBER_TIMEOUT_NAME);
  }

  public boolean isMemberTimeoutModifiable() {
    return _modifiableDefault();
  }

  public boolean isMembershipPortRangeModifiable() {
    return _modifiableDefault();
  }

  public boolean isMaxNumberOfTiesModifiable(){
    return _modifiableDefault();
  }

  public boolean isMaxTimeOutModifiable(){
    return _modifiableDefault();
  }

  protected void checkStatisticSampleRate(int value) {
    _checkIfModifiable(STATISTIC_SAMPLE_RATE_NAME);
    if (value < MIN_STATISTIC_SAMPLE_RATE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {STATISTIC_SAMPLE_RATE_NAME, Integer.valueOf(value), Integer.valueOf(MIN_STATISTIC_SAMPLE_RATE)}));
    }
    if (value > MAX_STATISTIC_SAMPLE_RATE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {STATISTIC_SAMPLE_RATE_NAME, Integer.valueOf(value), Integer.valueOf(MAX_STATISTIC_SAMPLE_RATE)}));
    }
  }
  public boolean isStatisticSampleRateModifiable() {
    return _modifiableDefault();
  }

  public boolean isDeltaPropagationModifiable() {
    return _modifiableDefault();
  }

  protected void checkDeltaPropagationModifiable() {
    _checkIfModifiable(DELTA_PROPAGATION_PROP_NAME);
  }
  
  protected String checkRemoteLocators(String value) {
    _checkIfModifiable(REMOTE_LOCATORS_NAME);
    return value ;
  }
  
  public boolean isDistributedSystemIdModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkDistributedSystemId(int value) {
    _checkIfModifiable(DISTRIBUTED_SYSTEM_ID_NAME);
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
  }

  public boolean isEnforceUniqueHostModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkEnforceUniqueHostModifiable() {
    _checkIfModifiable(ENFORCE_UNIQUE_HOST_NAME);
  }
  
  public boolean isRedundancyZoneModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkRedundancyZoneModifiable() {
    _checkIfModifiable(REDUNDANCY_ZONE_NAME);
  }
  
  protected void checkSSLPropertyModifiable() {
    _checkIfModifiable(SSL_SYSTEM_PROPS_NAME);
  }
  
  protected boolean isSSLPropertyModifiable() {
    return _modifiableDefault();
  }

  protected void checkUserCommandPackages(String value) {
    _checkIfModifiable(USER_COMMAND_PACKAGES);
  }

  public boolean isUserCommandPackagesModifiable() {
    return _modifiableDefault();
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
  protected String checkLocators(String value) {
    _checkIfModifiable(LOCATORS_NAME);

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

  public boolean isLocatorsModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isLocatorWaitTimeModifiable() {
    return _modifiableDefault();
  }

  public boolean isRemoteLocatorsModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkAckWaitThreshold(int value) {
    _checkIfModifiable(ACK_WAIT_THRESHOLD_NAME);
    if (value < MIN_ACK_WAIT_THRESHOLD) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {ACK_WAIT_THRESHOLD_NAME, Integer.valueOf(value), Integer.valueOf(MIN_ACK_WAIT_THRESHOLD)}));
    }
    if (value > MAX_ACK_WAIT_THRESHOLD) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {ACK_WAIT_THRESHOLD_NAME, Integer.valueOf(value), Integer.valueOf(MAX_ACK_WAIT_THRESHOLD)}));
    }
  }
  public boolean isAckWaitThresholdModifiable() {
    return _modifiableDefault();
  }


  protected void checkAckSevereAlertThreshold(int value) {
    _checkIfModifiable(ACK_SEVERE_ALERT_THRESHOLD_NAME);
    if (value < MIN_ACK_SEVERE_ALERT_THRESHOLD) {
      throw new IllegalArgumentException("Could not set \"" + ACK_SEVERE_ALERT_THRESHOLD_NAME + "\" to \"" + value + "\" because its value can not be less than \"" + MIN_ACK_SEVERE_ALERT_THRESHOLD + "\".");
    }
    if (value > MAX_ACK_SEVERE_ALERT_THRESHOLD) {
      throw new IllegalArgumentException("Could not set \"" + ACK_SEVERE_ALERT_THRESHOLD_NAME + "\" to \"" + value + "\" because its value can not be greater than \"" + MAX_ACK_SEVERE_ALERT_THRESHOLD + "\".");
    }
  }
  public boolean isAckSevereAlertThresholdModifiable() {
    return _modifiableDefault();
  }


  public boolean isArchiveFileSizeLimitModifiable() {
    return _modifiableDefault();
  }
  protected void checkArchiveFileSizeLimit(int value) {
    _checkIfModifiable(ARCHIVE_FILE_SIZE_LIMIT_NAME);
    if (value < MIN_ARCHIVE_FILE_SIZE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {ARCHIVE_FILE_SIZE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_ARCHIVE_FILE_SIZE_LIMIT)}));
    }
    if (value > MAX_ARCHIVE_FILE_SIZE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {ARCHIVE_FILE_SIZE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_ARCHIVE_FILE_SIZE_LIMIT)}));
    }
  }
  public boolean isArchiveDiskSpaceLimitModifiable() {
    return _modifiableDefault();
  }
  protected void checkArchiveDiskSpaceLimit(int value) {
    _checkIfModifiable(ARCHIVE_DISK_SPACE_LIMIT_NAME);
    if (value < MIN_ARCHIVE_DISK_SPACE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {ARCHIVE_DISK_SPACE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_ARCHIVE_DISK_SPACE_LIMIT)}));
    }
    if (value > MAX_ARCHIVE_DISK_SPACE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {ARCHIVE_DISK_SPACE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_ARCHIVE_DISK_SPACE_LIMIT)}));
    }
  }
  public boolean isLogFileSizeLimitModifiable() {
    return _modifiableDefault();
  }
  protected void checkLogFileSizeLimit(int value) {
    _checkIfModifiable(LOG_FILE_SIZE_LIMIT_NAME);
    if (value < MIN_LOG_FILE_SIZE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {LOG_FILE_SIZE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_LOG_FILE_SIZE_LIMIT)}));
    }
    if (value > MAX_LOG_FILE_SIZE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {LOG_FILE_SIZE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_LOG_FILE_SIZE_LIMIT)}));
    }
  }
  public boolean isLogDiskSpaceLimitModifiable() {
    return _modifiableDefault();
  }
  protected void checkLogDiskSpaceLimit(int value) {
    _checkIfModifiable(LOG_DISK_SPACE_LIMIT_NAME);
    if (value < MIN_LOG_DISK_SPACE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {LOG_DISK_SPACE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_LOG_DISK_SPACE_LIMIT)}));
    }
    if (value > MAX_LOG_DISK_SPACE_LIMIT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {LOG_DISK_SPACE_LIMIT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_LOG_DISK_SPACE_LIMIT)}));
    }
  }

  /** a generic method for checking a new integer setting against a min
      and max value */
  protected void minMaxCheck(String propName, int value, int minValue, int maxValue) {
    _checkIfModifiable(propName);
    if (value < minValue) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {propName, Integer.valueOf(value), Integer.valueOf(minValue)}));
    }
    else if (value > maxValue) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {propName, Integer.valueOf(value), Integer.valueOf(maxValue)}));
    }
  }


  /** check a new multicast send-buffer size setting */
  protected void checkMcastSendBufferSize(int newSize) {
    minMaxCheck(MCAST_SEND_BUFFER_SIZE_NAME, newSize,
      MIN_MCAST_SEND_BUFFER_SIZE, Integer.MAX_VALUE);
  }

  /** check a new multicast recv-buffer size setting */
  protected void checkMcastRecvBufferSize(int newSize) {
    minMaxCheck(MCAST_RECV_BUFFER_SIZE_NAME, newSize,
      MIN_MCAST_RECV_BUFFER_SIZE, Integer.MAX_VALUE);
  }

  /** check a new mcast flow-control setting */
  protected void checkMcastFlowControl(FlowControlParams params) {
    _checkIfModifiable(MCAST_FLOW_CONTROL_NAME);
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
  }

  /** check a new udp message fragment size setting */
  protected void checkUdpFragmentSize(int value) {
    minMaxCheck(UDP_FRAGMENT_SIZE_NAME, value,
      MIN_UDP_FRAGMENT_SIZE, MAX_UDP_FRAGMENT_SIZE);
  }

  /** check a new udp send-buffer size setting */
  protected void checkUdpSendBufferSize(int newSize) {
    minMaxCheck(UDP_SEND_BUFFER_SIZE_NAME, newSize,
      MIN_UDP_SEND_BUFFER_SIZE, Integer.MAX_VALUE);
  }

  /** check a new multicast recv-buffer size setting */
  protected void checkUdpRecvBufferSize(int newSize) {
    minMaxCheck(UDP_RECV_BUFFER_SIZE_NAME, newSize,
      MIN_UDP_RECV_BUFFER_SIZE, Integer.MAX_VALUE);
  }

  /** check a new member-timeout setting */
  protected void checkMemberTimeout(int value) {
    minMaxCheck(MEMBER_TIMEOUT_NAME, value,
      MIN_MEMBER_TIMEOUT, MAX_MEMBER_TIMEOUT);
  }

  protected void checkMembershipPortRange(int[] value) {
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
  }


  public boolean isAsyncDistributionTimeoutModifiable() {
    return _modifiableDefault();
  }
  protected void checkAsyncDistributionTimeout(int value) {
    _checkIfModifiable(ASYNC_DISTRIBUTION_TIMEOUT_NAME);
    if (value < MIN_ASYNC_DISTRIBUTION_TIMEOUT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {ASYNC_DISTRIBUTION_TIMEOUT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_ASYNC_DISTRIBUTION_TIMEOUT)}));
    }
    if (value > MAX_ASYNC_DISTRIBUTION_TIMEOUT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {ASYNC_DISTRIBUTION_TIMEOUT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_ASYNC_DISTRIBUTION_TIMEOUT)}));
    }
  }
  public boolean isAsyncQueueTimeoutModifiable() {
    return _modifiableDefault();
  }
  protected void checkAsyncQueueTimeout(int value) {
    _checkIfModifiable(ASYNC_QUEUE_TIMEOUT_NAME);
    if (value < MIN_ASYNC_QUEUE_TIMEOUT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {ASYNC_QUEUE_TIMEOUT_NAME, Integer.valueOf(value), Integer.valueOf(MIN_ASYNC_QUEUE_TIMEOUT)}));
    }
    if (value > MAX_ASYNC_QUEUE_TIMEOUT) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {ASYNC_QUEUE_TIMEOUT_NAME, Integer.valueOf(value), Integer.valueOf(MAX_ASYNC_QUEUE_TIMEOUT)}));
    }
  }
  public boolean isAsyncMaxQueueSizeModifiable() {
    return _modifiableDefault();
  }
  protected void checkAsyncMaxQueueSize(int value) {
    _checkIfModifiable(ASYNC_MAX_QUEUE_SIZE_NAME);
    if (value < MIN_ASYNC_MAX_QUEUE_SIZE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {ASYNC_MAX_QUEUE_SIZE_NAME, Integer.valueOf(value), Integer.valueOf(MIN_ASYNC_MAX_QUEUE_SIZE)}));
    }
    if (value > MAX_ASYNC_MAX_QUEUE_SIZE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {ASYNC_MAX_QUEUE_SIZE_NAME, Integer.valueOf(value), Integer.valueOf(MAX_ASYNC_MAX_QUEUE_SIZE)}));
    }
  }

  /** @since 5.7 */
  protected void checkClientConflation(String value) {
    if (! (value.equals(CLIENT_CONFLATION_PROP_VALUE_DEFAULT) ||
            value.equals(CLIENT_CONFLATION_PROP_VALUE_ON) ||
              value.equals(CLIENT_CONFLATION_PROP_VALUE_OFF)) ) {
      throw new IllegalArgumentException("Could not set \"" + CLIENT_CONFLATION_PROP_NAME + "\" to \"" + value + "\" because its value is not recognized");
    }
  }
  
  /** @since 5.7 */
  public boolean isClientConflationModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkDurableClientId(String value) {
    _checkIfModifiable(DURABLE_CLIENT_ID_NAME);
  }

  public boolean isDurableClientIdModifiable() {
    return _modifiableDefault();
  }

  protected void checkDurableClientTimeout(int value) {
    _checkIfModifiable(DURABLE_CLIENT_TIMEOUT_NAME);
  }

  public boolean isDurableClientTimeoutModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityClientAuthInit(String value) {
    _checkIfModifiable(SECURITY_CLIENT_AUTH_INIT_NAME);
  }

  public boolean isSecurityClientAuthInitModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityClientAuthenticator(String value) {
    _checkIfModifiable(SECURITY_CLIENT_AUTHENTICATOR_NAME);
  }

  public boolean isSecurityClientAuthenticatorModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityClientDHAlgo(String value) {
    _checkIfModifiable(SECURITY_CLIENT_DHALGO_NAME);
  }

  public boolean isSecurityClientDHAlgoModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityPeerAuthInit(String value) {
    _checkIfModifiable(SECURITY_PEER_AUTH_INIT_NAME);
    if (value != null && value.length() > 0 && getMcastPort() != 0) {
      String mcastInfo = MCAST_PORT_NAME + "[" + getMcastPort() + "]";
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_2_MUST_BE_0_WHEN_SECURITY_IS_ENABLED
          .toLocalizedString(new Object[] { 
             SECURITY_PEER_AUTH_INIT_NAME, value, mcastInfo }));
    }
  }

  public boolean isSecurityPeerAuthInitModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityPeerAuthenticator(String value) {
    _checkIfModifiable(SECURITY_PEER_AUTHENTICATOR_NAME);
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
  }

  public boolean isSecurityPeerAuthenticatorModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityClientAccessor(String value) {
    _checkIfModifiable(SECURITY_CLIENT_ACCESSOR_NAME);
  }

  public boolean isSecurityClientAccessorModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityClientAccessorPP(String value) {
    _checkIfModifiable(SECURITY_CLIENT_ACCESSOR_PP_NAME);
  }

  public boolean isSecurityClientAccessorPPModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityLogLevel(int value) {
    _checkIfModifiable(SECURITY_LOG_LEVEL_NAME);
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
  }

  public boolean isSecurityLogLevelModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityLogFile(File value) {
    _checkIfModifiable(SECURITY_LOG_FILE_NAME);
  }

  public boolean isSecurityLogFileModifiable() {
    return _modifiableDefault();
  }

  protected void checkSecurityPeerMembershipTimeout(int value) {
    _checkIfModifiable(SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME);
    minMaxCheck(SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME, value,
        0, MAX_SECURITY_PEER_VERIFYMEMBER_TIMEOUT);
  }
  
  public boolean isSecurityPeerMembershipTimeoutModifiable() {
    return _modifiableDefault();
  }
  
  protected void checkSecurity(String key, String value) {
    _checkIfModifiable(key);
  }

  public boolean isSecurityModifiable() {
    return _modifiableDefault();
  }

  protected void checkRemoveUnresponsiveClientModifiable() { 
    _checkIfModifiable(REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME); 
  } 

  public boolean isRemoveUnresponsiveClientModifiable() { 
    return _modifiableDefault(); 
  } 
 
  /**
   * @since 7.0
   */
  protected void checkGroups(String value) {
    _checkIfModifiable(GROUPS_NAME);
  }
  /**
   * @since 7.0
   */
  public boolean isGroupsModifiable() {
    return _modifiableDefault();
  }

  protected void checkJmxManager() {
    _checkIfModifiable(JMX_MANAGER_NAME);
  }
  @Override
  public boolean isJmxManagerModifiable() {
    return _modifiableDefault();
  }

  protected void checkJmxManagerStart() {
    _checkIfModifiable(JMX_MANAGER_START_NAME);
  }
  @Override
  public boolean isJmxManagerStartModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxSSLManagerEnabled() {
    _checkIfModifiable(JMX_MANAGER_SSL_NAME);
  }
  protected void checkJmxManagerSSLEnabled() {
    _checkIfModifiable(JMX_MANAGER_SSL_ENABLED_NAME);
  }
  public boolean isJmxManagerSSLEnabledModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerSSLCiphers() {
    _checkIfModifiable(JMX_MANAGER_SSL_CIPHERS_NAME);
  }
  public boolean isJmxManagerSSLCiphersModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerSSLProtocols() {
    _checkIfModifiable(JMX_MANAGER_SSL_PROTOCOLS_NAME);
  }
  public boolean isJmxManagerSSLProtocolsModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerSSLRequireAuthentication() {
    _checkIfModifiable(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME);
  }
  public boolean isJmxManagerSSLRequireAuthenticationModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerPort(int value) {
    minMaxCheck(JMX_MANAGER_PORT_NAME, value, 0, 65535);
  }
  @Override
  public boolean isJmxManagerPortModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerBindAddress(String value) {
    _checkIfModifiable(JMX_MANAGER_BIND_ADDRESS_NAME);
  }
  @Override
  public boolean isJmxManagerBindAddressModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerHostnameForClients(String value) {
    _checkIfModifiable(JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME);
  }
  @Override
  public boolean isJmxManagerHostnameForClientsModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerPasswordFile(String value) {
    _checkIfModifiable(JMX_MANAGER_PASSWORD_FILE_NAME);
  }
  @Override
  public boolean isJmxManagerPasswordFileModifiable() {
    return _modifiableDefault();
  }
  protected void checkJmxManagerAccessFile(String value) {
    _checkIfModifiable(JMX_MANAGER_ACCESS_FILE_NAME);
  }
  @Override
  public boolean isJmxManagerAccessFileModifiable() {
    return _modifiableDefault();
  }
  @Override
  public boolean isJmxManagerHttpPortModifiable() {
    //return _modifiableDefault();
    return isHttpServicePortModifiable();
  }
  protected void checkJmxManagerHttpPort(int value) {
    minMaxCheck(JMX_MANAGER_HTTP_PORT_NAME, value, 0, 65535);
  }
  protected void checkJmxManagerUpdateRate(int value) {
    _checkIfModifiable(JMX_MANAGER_UPDATE_RATE_NAME);
    if (value < MIN_JMX_MANAGER_UPDATE_RATE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_LESS_THAN_2.toLocalizedString(new Object[] {JMX_MANAGER_UPDATE_RATE_NAME, Integer.valueOf(value), Integer.valueOf(MIN_JMX_MANAGER_UPDATE_RATE)}));
    }
    if (value > MAX_JMX_MANAGER_UPDATE_RATE) {
      throw new IllegalArgumentException(LocalizedStrings.AbstractDistributionConfig_COULD_NOT_SET_0_TO_1_BECAUSE_ITS_VALUE_CAN_NOT_BE_GREATER_THAN_2.toLocalizedString(new Object[] {JMX_MANAGER_UPDATE_RATE_NAME, Integer.valueOf(value), Integer.valueOf(MAX_JMX_MANAGER_UPDATE_RATE)}));
    }
  }
  @Override
  public boolean isJmxManagerUpdateRateModifiable() {
    return _modifiableDefault();
  }
  protected void checkMemcachedPort(int value) {
    minMaxCheck(MEMCACHED_PORT_NAME, value, 0, 65535);
  }
  public boolean isMemcachedPortModifiable() {
    return _modifiableDefault();
  }

  protected void checkMemcachedProtocol(String protocol) {
    if (protocol == null
        || (!protocol.equalsIgnoreCase(GemFireMemcachedServer.Protocol.ASCII.name()) &&
            !protocol.equalsIgnoreCase(GemFireMemcachedServer.Protocol.BINARY.name()))) {
      throw new IllegalArgumentException(LocalizedStrings.
          AbstractDistributionConfig_MEMCACHED_PROTOCOL_MUST_BE_ASCII_OR_BINARY.toLocalizedString());
    }
  }

  public boolean isMemcachedProtocolModifiable() {
    return false;
  }
  
  protected void checkMemcachedBindAddress(String value) {
    _checkIfModifiable(MEMCACHED_BIND_ADDRESS_NAME);
    if (value != null && value.length() > 0 &&
        !SocketCreator.isLocalHost(value)) {
      throw new IllegalArgumentException(
        LocalizedStrings.AbstractDistributionConfig_MEMCACHED_BIND_ADDRESS_0_INVALID_MUST_BE_IN_1
          .toLocalizedString(new Object[]{value, SocketCreator.getMyAddresses()
          }));
    }
  }
  public boolean isMemcachedBindAddressModifiable() {
    return _modifiableDefault();
  }

  protected void checkEnableSharedConfiguration() {
    _checkIfModifiable(ENABLE_CLUSTER_CONFIGURATION_NAME);
  }
  
  protected void checkUseSharedConfiguration() {
    _checkIfModifiable(USE_CLUSTER_CONFIGURATION_NAME);
  }
  
  protected void checkLoadSharedConfigFromDir() {
    _checkIfModifiable(LOAD_CLUSTER_CONFIG_FROM_DIR_NAME);
  }
  
  protected void checkClusterConfigDir() {
    _checkIfModifiable(CLUSTER_CONFIGURATION_DIR);
  }
  
  
  public boolean isEnableSharedConfigurationModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isLoadSharedConfigFromDirModifiable() {
    return _modifiableDefault();
  }
  
  public boolean isUseSharedConfigurationModifiable() {
    return _modifiableDefault();
  }

  // AbstractConfig overriding methods
  
  @Override
  protected void checkAttributeName(String attName) {
    if(!attName.startsWith(SECURITY_PREFIX_NAME) && !attName.startsWith(USERDEFINED_PREFIX_NAME)
        && !attName.startsWith(SSL_SYSTEM_PROPS_NAME) && !attName.startsWith(SYS_PROP_NAME)) {
      super.checkAttributeName(attName);
    }
  }
  
  
  
  // AbstractDistributionConfig methods

  static final String[] dcValidAttributeNames;
  static {
    String[] myAtts = new String[] {
      ACK_WAIT_THRESHOLD_NAME,
      ACK_SEVERE_ALERT_THRESHOLD_NAME,
      ARCHIVE_DISK_SPACE_LIMIT_NAME,
      ARCHIVE_FILE_SIZE_LIMIT_NAME,
      BIND_ADDRESS_NAME,
      SERVER_BIND_ADDRESS_NAME,
      CACHE_XML_FILE_NAME,
      DEPLOY_WORKING_DIR,
      LOG_DISK_SPACE_LIMIT_NAME,
      LOG_FILE_NAME,
      LOG_FILE_SIZE_LIMIT_NAME,
      LOG_LEVEL_NAME, LOCATORS_NAME, LOCATOR_WAIT_TIME_NAME, REMOTE_LOCATORS_NAME,
      MCAST_ADDRESS_NAME, MCAST_PORT_NAME, MCAST_TTL_NAME,
      MCAST_SEND_BUFFER_SIZE_NAME, MCAST_RECV_BUFFER_SIZE_NAME,
      MCAST_FLOW_CONTROL_NAME,
      TCP_PORT_NAME,
      SOCKET_LEASE_TIME_NAME, SOCKET_BUFFER_SIZE_NAME, CONSERVE_SOCKETS_NAME,
      NAME_NAME,
      ROLES_NAME,
      STATISTIC_ARCHIVE_FILE_NAME, STATISTIC_SAMPLE_RATE_NAME,
      STATISTIC_SAMPLING_ENABLED_NAME,
      SSL_ENABLED_NAME,
      SSL_PROTOCOLS_NAME,
      SSL_CIPHERS_NAME,
      SSL_REQUIRE_AUTHENTICATION_NAME,
      CLUSTER_SSL_ENABLED_NAME,
      CLUSTER_SSL_PROTOCOLS_NAME,
      CLUSTER_SSL_CIPHERS_NAME,
      CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME,
      CLUSTER_SSL_KEYSTORE_NAME,CLUSTER_SSL_KEYSTORE_TYPE_NAME,CLUSTER_SSL_KEYSTORE_PASSWORD_NAME,CLUSTER_SSL_TRUSTSTORE_NAME,CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME,
      UDP_SEND_BUFFER_SIZE_NAME, UDP_RECV_BUFFER_SIZE_NAME, UDP_FRAGMENT_SIZE_NAME,
      DISABLE_TCP_NAME,
      ENABLE_TIME_STATISTICS_NAME,
      MEMBER_TIMEOUT_NAME,
      MEMBERSHIP_PORT_RANGE_NAME,
      MAX_WAIT_TIME_FOR_RECONNECT_NAME,
      MAX_NUM_RECONNECT_TRIES,
      ASYNC_DISTRIBUTION_TIMEOUT_NAME,
      ASYNC_QUEUE_TIMEOUT_NAME,
      ASYNC_MAX_QUEUE_SIZE_NAME,
      START_LOCATOR_NAME,
      CLIENT_CONFLATION_PROP_NAME,
      DURABLE_CLIENT_ID_NAME,
      DURABLE_CLIENT_TIMEOUT_NAME,
//      DURABLE_CLIENT_KEEP_ALIVE_NAME,
      ENABLE_NETWORK_PARTITION_DETECTION_NAME,
      DISABLE_AUTO_RECONNECT_NAME,
      SECURITY_CLIENT_AUTH_INIT_NAME,
      SECURITY_CLIENT_AUTHENTICATOR_NAME,
      SECURITY_CLIENT_DHALGO_NAME,
      SECURITY_PEER_AUTH_INIT_NAME,
      SECURITY_PEER_AUTHENTICATOR_NAME,
      SECURITY_CLIENT_ACCESSOR_NAME,
      SECURITY_CLIENT_ACCESSOR_PP_NAME,
      SECURITY_LOG_LEVEL_NAME,
      SECURITY_LOG_FILE_NAME,
      SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME,
      SECURITY_PREFIX_NAME,
      REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME,
      DELTA_PROPAGATION_PROP_NAME,
      DISTRIBUTED_SYSTEM_ID_NAME,
      ENFORCE_UNIQUE_HOST_NAME,
      REDUNDANCY_ZONE_NAME,
      GROUPS_NAME,
      JMX_MANAGER_NAME,
      JMX_MANAGER_START_NAME,
      JMX_MANAGER_PORT_NAME,
      JMX_MANAGER_SSL_NAME,
      JMX_MANAGER_SSL_ENABLED_NAME,
      JMX_MANAGER_SSL_PROTOCOLS_NAME,
      JMX_MANAGER_SSL_CIPHERS_NAME,
      JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME,
      JMX_MANAGER_SSL_KEYSTORE_NAME,JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME,JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME,JMX_MANAGER_SSL_TRUSTSTORE_NAME,JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME,
      JMX_MANAGER_BIND_ADDRESS_NAME,
      JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME,
      JMX_MANAGER_PASSWORD_FILE_NAME,
      JMX_MANAGER_ACCESS_FILE_NAME,
      JMX_MANAGER_HTTP_PORT_NAME,
      JMX_MANAGER_UPDATE_RATE_NAME,
      MEMCACHED_PORT_NAME,
      MEMCACHED_PROTOCOL_NAME,
      MEMCACHED_BIND_ADDRESS_NAME,
      USER_COMMAND_PACKAGES,
      ENABLE_CLUSTER_CONFIGURATION_NAME,
      USE_CLUSTER_CONFIGURATION_NAME,
      LOAD_CLUSTER_CONFIG_FROM_DIR_NAME,
      CLUSTER_CONFIGURATION_DIR,
      HTTP_SERVICE_PORT_NAME,
      HTTP_SERVICE_BIND_ADDRESS_NAME,
      START_DEV_REST_API_NAME,
      SERVER_SSL_ENABLED_NAME,
      SERVER_SSL_REQUIRE_AUTHENTICATION_NAME,
      SERVER_SSL_PROTOCOLS_NAME,
      SERVER_SSL_CIPHERS_NAME,
      SERVER_SSL_KEYSTORE_NAME,SERVER_SSL_KEYSTORE_TYPE_NAME,SERVER_SSL_KEYSTORE_PASSWORD_NAME,SERVER_SSL_TRUSTSTORE_NAME,SERVER_SSL_TRUSTSTORE_PASSWORD_NAME,
      GATEWAY_SSL_ENABLED_NAME,
      GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME,
      GATEWAY_SSL_PROTOCOLS_NAME,
      GATEWAY_SSL_CIPHERS_NAME,
      GATEWAY_SSL_KEYSTORE_NAME,GATEWAY_SSL_KEYSTORE_TYPE_NAME,GATEWAY_SSL_KEYSTORE_PASSWORD_NAME,GATEWAY_SSL_TRUSTSTORE_NAME,GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME,
      HTTP_SERVICE_SSL_ENABLED_NAME,
      HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME,
      HTTP_SERVICE_SSL_PROTOCOLS_NAME,
      HTTP_SERVICE_SSL_CIPHERS_NAME,
      HTTP_SERVICE_SSL_KEYSTORE_NAME,HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME,HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME,HTTP_SERVICE_SSL_TRUSTSTORE_NAME,HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME
    };
    List atts = Arrays.asList(myAtts);
    Collections.sort(atts);
    dcValidAttributeNames = (String[])atts.toArray(new String[atts.size()]);
  }

  public static boolean isWellKnownAttribute(String attName) {
    return Arrays.binarySearch(dcValidAttributeNames, attName) >= 0;
  }
  
  public void setAttributeObject(String attName, Object attValue, ConfigSource source) {
    Class validValueClass = getAttributeType(attName);
    if (attValue != null) {
      // null is a "valid" value for any class
      if (!validValueClass.isInstance(attValue)) {
        throw new InvalidValueException(LocalizedStrings.AbstractDistributionConfig_0_VALUE_1_MUST_BE_OF_TYPE_2.toLocalizedString(new Object[] {attName, attValue, validValueClass.getName()}));
      }
    }

    if (attName.equalsIgnoreCase(ACK_WAIT_THRESHOLD_NAME)) {
      this.setAckWaitThreshold(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(ACK_SEVERE_ALERT_THRESHOLD_NAME)) {
      this.setAckSevereAlertThreshold(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(ARCHIVE_DISK_SPACE_LIMIT_NAME)) {
      this.setArchiveDiskSpaceLimit(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(ARCHIVE_FILE_SIZE_LIMIT_NAME)) {
      this.setArchiveFileSizeLimit(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(CACHE_XML_FILE_NAME)) {
      this.setCacheXmlFile((File)attValue);
    } else if (attName.equalsIgnoreCase(DEPLOY_WORKING_DIR)) {
      this.setDeployWorkingDir((File)attValue);
    } else if (attName.equalsIgnoreCase(LOG_DISK_SPACE_LIMIT_NAME)) {
      this.setLogDiskSpaceLimit(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(LOG_FILE_NAME)) {
      this.setLogFile((File)attValue);
    } else if (attName.equalsIgnoreCase(LOG_FILE_SIZE_LIMIT_NAME)) {
      this.setLogFileSizeLimit(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(LOG_LEVEL_NAME)) {
      this.setLogLevel(LogWriterImpl.levelNameToCode((String)attValue));
    } else if (attName.equalsIgnoreCase(LOCATORS_NAME)) {
      this.setLocators((String)attValue);
    } else if (attName.equalsIgnoreCase(LOCATOR_WAIT_TIME_NAME)) {
      this.setLocatorWaitTime(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(REMOTE_LOCATORS_NAME)) {
      this.setRemoteLocators((String)attValue);
    } else if (attName.equalsIgnoreCase(MCAST_ADDRESS_NAME)) {
      this.setMcastAddress((InetAddress)attValue);
    } else if (attName.equalsIgnoreCase(BIND_ADDRESS_NAME)) {
      this.setBindAddress((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_BIND_ADDRESS_NAME)) {
      this.setServerBindAddress((String)attValue);
    } else if (attName.equalsIgnoreCase(TCP_PORT_NAME)) {
      this.setTcpPort(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MCAST_PORT_NAME)) {
      this.setMcastPort(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MCAST_TTL_NAME)) {
      this.setMcastTtl(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(SOCKET_LEASE_TIME_NAME)) {
      this.setSocketLeaseTime(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(SOCKET_BUFFER_SIZE_NAME)) {
      this.setSocketBufferSize(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(CONSERVE_SOCKETS_NAME)) {
      this.setConserveSockets(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(ROLES_NAME)) {
      this.setRoles((String)attValue);
    } else if (attName.equalsIgnoreCase(NAME_NAME)) {
      this.setName((String)attValue);
    } else if (attName.equalsIgnoreCase(STATISTIC_ARCHIVE_FILE_NAME)) {
      this.setStatisticArchiveFile((File)attValue);
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLE_RATE_NAME)) {
      this.setStatisticSampleRate(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLING_ENABLED_NAME)) {
      this.setStatisticSamplingEnabled(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(SSL_ENABLED_NAME)) {
      this.setSSLEnabled(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(SSL_PROTOCOLS_NAME)) {
      this.setSSLProtocols((String)attValue);
    } else if (attName.equalsIgnoreCase(SSL_CIPHERS_NAME)) {
      this.setSSLCiphers((String)attValue);
    } else if (attName.equalsIgnoreCase(SSL_REQUIRE_AUTHENTICATION_NAME)) {
      this.setSSLRequireAuthentication(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_ENABLED_NAME)) {
      this.setClusterSSLEnabled(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_PROTOCOLS_NAME)) {
      this.setClusterSSLProtocols((String)attValue);
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_CIPHERS_NAME)) {
      this.setClusterSSLCiphers((String)attValue);
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      this.setClusterSSLRequireAuthentication(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_NAME)) {
      this.setClusterSSLKeyStore((String)attValue);
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_TYPE_NAME)) {
      this.setClusterSSLKeyStoreType((String)attValue);
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_PASSWORD_NAME)) {
      this.setClusterSSLKeyStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_NAME)) {
      this.setClusterSSLTrustStore((String)attValue);
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      this.setClusterSSLTrustStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(MCAST_SEND_BUFFER_SIZE_NAME)) {
      this.setMcastSendBufferSize(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MCAST_RECV_BUFFER_SIZE_NAME)) {
      this.setMcastRecvBufferSize(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(UDP_SEND_BUFFER_SIZE_NAME)) {
      this.setUdpSendBufferSize(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(UDP_RECV_BUFFER_SIZE_NAME)) {
      this.setUdpRecvBufferSize(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MCAST_FLOW_CONTROL_NAME)) {
      this.setMcastFlowControl((FlowControlParams)attValue);
    } else if (attName.equalsIgnoreCase(UDP_FRAGMENT_SIZE_NAME)) {
      this.setUdpFragmentSize(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(DISABLE_TCP_NAME)) {
      this.setDisableTcp(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(ENABLE_TIME_STATISTICS_NAME)) {
      this.setEnableTimeStatistics(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(MEMBER_TIMEOUT_NAME)) {
      this.setMemberTimeout(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MEMBERSHIP_PORT_RANGE_NAME)) {
      this.setMembershipPortRange((int[])attValue);
    } else if (attName.equalsIgnoreCase(MAX_WAIT_TIME_FOR_RECONNECT_NAME)){
      this.setMaxWaitTimeForReconnect(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MAX_NUM_RECONNECT_TRIES)){
      this.setMaxNumReconnectTries(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(ASYNC_DISTRIBUTION_TIMEOUT_NAME)) {
      this.setAsyncDistributionTimeout(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(ASYNC_QUEUE_TIMEOUT_NAME)) {
      this.setAsyncQueueTimeout(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(ASYNC_MAX_QUEUE_SIZE_NAME)) {
      this.setAsyncMaxQueueSize(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(START_LOCATOR_NAME)) {
      this.setStartLocator((String)attValue);
    } else if (attName.equalsIgnoreCase(CLIENT_CONFLATION_PROP_NAME)) {
      this.setClientConflation((String)attValue);
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_ID_NAME)) {
      this.setDurableClientId((String)attValue);
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_TIMEOUT_NAME)) {
      this.setDurableClientTimeout(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTH_INIT_NAME)) {
      this.setSecurityClientAuthInit((String)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTHENTICATOR_NAME)) {
      this.setSecurityClientAuthenticator((String)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_DHALGO_NAME)) {
      this.setSecurityClientDHAlgo((String)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTH_INIT_NAME)) {
      this.setSecurityPeerAuthInit((String)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTHENTICATOR_NAME)) {
      this.setSecurityPeerAuthenticator((String)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_NAME)) {
      this.setSecurityClientAccessor((String)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_PP_NAME)) {
      this.setSecurityClientAccessorPP((String)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_LEVEL_NAME)) {
      this.setSecurityLogLevel(LogWriterImpl.levelNameToCode((String)attValue));
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_FILE_NAME)) {
      this.setSecurityLogFile((File)attValue);
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME)) {
      this.setSecurityPeerMembershipTimeout(((Integer)attValue).intValue());      
    } else if (attName.startsWith(SECURITY_PREFIX_NAME)) {
      this.setSecurity(attName,(String)attValue);
    } else if (attName.equalsIgnoreCase(ENABLE_NETWORK_PARTITION_DETECTION_NAME)) {
      this.setEnableNetworkPartitionDetection(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(DISABLE_AUTO_RECONNECT_NAME)) {
      this.setDisableAutoReconnect(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME)) {
      this.setRemoveUnresponsiveClient(((Boolean)attValue).booleanValue());
    } else if (attName.startsWith(DELTA_PROPAGATION_PROP_NAME)) {
      this.setDeltaPropagation((((Boolean)attValue).booleanValue()));    
    } else if (attName.startsWith(DISTRIBUTED_SYSTEM_ID_NAME)) {
      this.setDistributedSystemId((Integer)attValue);
    } else if (attName.startsWith(REDUNDANCY_ZONE_NAME)) {
      this.setRedundancyZone((String)attValue);
    } else if (attName.startsWith(ENFORCE_UNIQUE_HOST_NAME)) {
      this.setEnforceUniqueHost(((Boolean)attValue).booleanValue());
    } else if (attName.startsWith(USERDEFINED_PREFIX_NAME)) {
      //Do nothing its user defined property.
    } else if (attName.startsWith(SSL_SYSTEM_PROPS_NAME) || attName.startsWith(SYS_PROP_NAME)) {
      this.setSSLProperty(attName, (String)attValue);
    } else if (attName.equalsIgnoreCase(GROUPS_NAME)) {
      this.setGroups((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_NAME)) {
      this.setJmxManager((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_START_NAME)) {
      this.setJmxManagerStart((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_NAME)) {
      this.setJmxManagerSSL((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_ENABLED_NAME)) {
      this.setJmxManagerSSLEnabled((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      this.setJmxManagerSSLRequireAuthentication((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_PROTOCOLS_NAME)) {
      this.setJmxManagerSSLProtocols((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_CIPHERS_NAME)) {
      this.setJmxManagerSSLCiphers((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_NAME)) {
      this.setJmxManagerSSLKeyStore((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME)) {
      this.setJmxManagerSSLKeyStoreType((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME)) {
      this.setJmxManagerSSLKeyStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_NAME)) {
      this.setJmxManagerSSLTrustStore((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      this.setJmxManagerSSLTrustStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PORT_NAME)) {
      this.setJmxManagerPort(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_BIND_ADDRESS_NAME)) {
      this.setJmxManagerBindAddress((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME)) {
      this.setJmxManagerHostnameForClients((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PASSWORD_FILE_NAME)) {
      this.setJmxManagerPasswordFile((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_ACCESS_FILE_NAME)) {
      this.setJmxManagerAccessFile((String)attValue);
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HTTP_PORT_NAME)) {
      this.setJmxManagerHttpPort(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_UPDATE_RATE_NAME)) {
      this.setJmxManagerUpdateRate(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MEMCACHED_PORT_NAME)) {
      this.setMemcachedPort(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(MEMCACHED_PROTOCOL_NAME)) {
      this.setMemcachedProtocol((String)attValue);
    } else if (attName.equalsIgnoreCase(MEMCACHED_BIND_ADDRESS_NAME)) {
      this.setMemcachedBindAddress((String)attValue);
    } else if (attName.equalsIgnoreCase(USER_COMMAND_PACKAGES)) {
      this.setUserCommandPackages((String)attValue);
    } else if (attName.equalsIgnoreCase(ENABLE_CLUSTER_CONFIGURATION_NAME)) {
      this.setEnableClusterConfiguration(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(LOAD_CLUSTER_CONFIG_FROM_DIR_NAME)) {
      this.setLoadClusterConfigFromDir(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(USE_CLUSTER_CONFIGURATION_NAME)) {
      this.setUseSharedConfiguration(((Boolean)attValue).booleanValue());
    } else if (attName.equalsIgnoreCase(CLUSTER_CONFIGURATION_DIR)) {
      this.setClusterConfigDir((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_SSL_ENABLED_NAME)) {
      this.setServerSSLEnabled((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(SERVER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      this.setServerSSLRequireAuthentication((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(SERVER_SSL_PROTOCOLS_NAME)) {
      this.setServerSSLProtocols((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_SSL_CIPHERS_NAME)) {
      this.setServerSSLCiphers((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_NAME)) {
      this.setServerSSLKeyStore((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_TYPE_NAME)) {
      this.setServerSSLKeyStoreType((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_PASSWORD_NAME)) {
      this.setServerSSLKeyStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_NAME)) {
      this.setServerSSLTrustStore((String)attValue);
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      this.setServerSSLTrustStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_ENABLED_NAME)) {
      this.setGatewaySSLEnabled((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      this.setGatewaySSLRequireAuthentication((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_PROTOCOLS_NAME)) {
      this.setGatewaySSLProtocols((String)attValue);
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_CIPHERS_NAME)) {
      this.setGatewaySSLCiphers((String)attValue);
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_NAME)) {
      this.setGatewaySSLKeyStore((String)attValue);
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_TYPE_NAME)) {
      this.setGatewaySSLKeyStoreType((String)attValue);
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_PASSWORD_NAME)) {
      this.setGatewaySSLKeyStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_NAME)) {
      this.setGatewaySSLTrustStore((String)attValue);
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      this.setGatewaySSLTrustStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_PORT_NAME)) {
       this.setHttpServicePort(((Integer)attValue).intValue());
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_BIND_ADDRESS_NAME)) {
      this.setHttpServiceBindAddress((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_ENABLED_NAME)) {
      this.setHttpServiceSSLEnabled((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      this.setHttpServiceSSLRequireAuthentication((((Boolean)attValue).booleanValue()));
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_PROTOCOLS_NAME)) {
      this.setHttpServiceSSLProtocols((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_CIPHERS_NAME)) {
      this.setHttpServiceSSLCiphers((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_NAME)) {
      this.setHttpServiceSSLKeyStore((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME)) {
      this.setHttpServiceSSLKeyStoreType((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME)) {
      this.setHttpServiceSSLKeyStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_NAME)) {
      this.setHttpServiceSSLTrustStore((String)attValue);
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      this.setHttpServiceSSLTrustStorePassword((String)attValue);
    } else if (attName.equalsIgnoreCase(START_DEV_REST_API_NAME)) {
      this.setStartDevRestApi(((Boolean)attValue).booleanValue());
    } else {
      throw new InternalGemFireException(LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }
    getAttSourceMap().put(attName, source);
  }

  public Object getAttributeObject(String attName) {
    checkAttributeName(attName);
    if (attName.equalsIgnoreCase(ACK_WAIT_THRESHOLD_NAME)) {
      return Integer.valueOf(this.getAckWaitThreshold());
    } else if (attName.equalsIgnoreCase(ACK_SEVERE_ALERT_THRESHOLD_NAME)) {
      return Integer.valueOf(this.getAckSevereAlertThreshold());
    } else if (attName.equalsIgnoreCase(ARCHIVE_DISK_SPACE_LIMIT_NAME)) {
      return Integer.valueOf(this.getArchiveDiskSpaceLimit());
    } else if (attName.equalsIgnoreCase(ARCHIVE_FILE_SIZE_LIMIT_NAME)) {
      return Integer.valueOf(this.getArchiveFileSizeLimit());
    } else if (attName.equalsIgnoreCase(CACHE_XML_FILE_NAME)) {
      return this.getCacheXmlFile();
    } else if (attName.equalsIgnoreCase(DEPLOY_WORKING_DIR)) {
      return this.getDeployWorkingDir();
    } else if (attName.equalsIgnoreCase(LOG_DISK_SPACE_LIMIT_NAME)) {
      return Integer.valueOf(this.getLogDiskSpaceLimit());
    } else if (attName.equalsIgnoreCase(LOG_FILE_NAME)) {
      return this.getLogFile();
    } else if (attName.equalsIgnoreCase(LOG_FILE_SIZE_LIMIT_NAME)) {
      return Integer.valueOf(this.getLogFileSizeLimit());
    } else if (attName.equalsIgnoreCase(LOG_LEVEL_NAME)) {
      return LogWriterImpl.levelToString(this.getLogLevel());
    } else if (attName.equalsIgnoreCase(LOCATORS_NAME)) {
      return this.getLocators();
    } else if (attName.equalsIgnoreCase(LOCATOR_WAIT_TIME_NAME)) {
      return Integer.valueOf(this.getLocatorWaitTime());
    } else if (attName.equalsIgnoreCase(REMOTE_LOCATORS_NAME)) {
      return this.getRemoteLocators();
    } else if (attName.equalsIgnoreCase(MCAST_ADDRESS_NAME)) {
      return this.getMcastAddress();
    } else if (attName.equalsIgnoreCase(BIND_ADDRESS_NAME)) {
      return this.getBindAddress();
    } else if (attName.equalsIgnoreCase(SERVER_BIND_ADDRESS_NAME)) {
      return this.getServerBindAddress();
    } else if (attName.equalsIgnoreCase(TCP_PORT_NAME)) {
      return Integer.valueOf(this.getTcpPort());
    } else if (attName.equalsIgnoreCase(MCAST_PORT_NAME)) {
      return Integer.valueOf(this.getMcastPort());
    } else if (attName.equalsIgnoreCase(MCAST_TTL_NAME)) {
      return Integer.valueOf(this.getMcastTtl());
    } else if (attName.equalsIgnoreCase(SOCKET_LEASE_TIME_NAME)) {
      return Integer.valueOf(this.getSocketLeaseTime());
    } else if (attName.equalsIgnoreCase(SOCKET_BUFFER_SIZE_NAME)) {
      return Integer.valueOf(this.getSocketBufferSize());
    } else if (attName.equalsIgnoreCase(CONSERVE_SOCKETS_NAME)) {
      return Boolean.valueOf(this.getConserveSockets());
    } else if (attName.equalsIgnoreCase(ROLES_NAME)) {
      return this.getRoles();
    } else if (attName.equalsIgnoreCase(NAME_NAME)) {
      return this.getName();
    } else if (attName.equalsIgnoreCase(STATISTIC_ARCHIVE_FILE_NAME)) {
      return this.getStatisticArchiveFile();
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLE_RATE_NAME)) {
      return Integer.valueOf(this.getStatisticSampleRate());
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLING_ENABLED_NAME)) {
      return Boolean.valueOf(this.getStatisticSamplingEnabled());
    } else if (attName.equalsIgnoreCase(SSL_ENABLED_NAME)) {
      return this.getSSLEnabled() ? Boolean.TRUE : Boolean.FALSE;
    } else if (attName.equalsIgnoreCase(SSL_PROTOCOLS_NAME)) {
      return this.getSSLProtocols();
    } else if (attName.equalsIgnoreCase(SSL_CIPHERS_NAME)) {
      return this.getSSLCiphers();
    } else if (attName.equalsIgnoreCase(SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.getSSLRequireAuthentication() ? Boolean.TRUE : Boolean.FALSE;
    }  else if (attName.equalsIgnoreCase(CLUSTER_SSL_ENABLED_NAME)) {
      return this.getClusterSSLEnabled() ? Boolean.TRUE : Boolean.FALSE;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_PROTOCOLS_NAME)) {
      return this.getClusterSSLProtocols();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_CIPHERS_NAME)) {
      return this.getClusterSSLCiphers();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.getClusterSSLRequireAuthentication() ? Boolean.TRUE : Boolean.FALSE;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_NAME)) {
      return this.getClusterSSLKeyStore();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_TYPE_NAME)) {
      return this.getClusterSSLKeyStoreType();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.getClusterSSLKeyStorePassword();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_NAME)) {
      return this.getClusterSSLTrustStore();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.getClusterSSLTrustStorePassword();
    } else if (attName.equalsIgnoreCase(MCAST_SEND_BUFFER_SIZE_NAME)) {
      return Integer.valueOf(this.getMcastSendBufferSize());
    } else if (attName.equalsIgnoreCase(MCAST_RECV_BUFFER_SIZE_NAME)) {
      return Integer.valueOf(this.getMcastRecvBufferSize());
    } else if (attName.equalsIgnoreCase(UDP_SEND_BUFFER_SIZE_NAME)) {
      return Integer.valueOf(this.getUdpSendBufferSize());
    } else if (attName.equalsIgnoreCase(UDP_RECV_BUFFER_SIZE_NAME)) {
      return Integer.valueOf(this.getUdpRecvBufferSize());
    } else if (attName.equalsIgnoreCase(MCAST_FLOW_CONTROL_NAME)) {
      return this.getMcastFlowControl();
    } else if (attName.equalsIgnoreCase(UDP_FRAGMENT_SIZE_NAME)) {
      return Integer.valueOf(this.getUdpFragmentSize());
    } else if (attName.equalsIgnoreCase(DISABLE_TCP_NAME)) {
      return Boolean.valueOf(this.getDisableTcp());
    } else if (attName.equalsIgnoreCase(ENABLE_TIME_STATISTICS_NAME)) {
      return Boolean.valueOf(this.getEnableTimeStatistics());
    } else if (attName.equalsIgnoreCase(MEMBER_TIMEOUT_NAME)) {
      return Integer.valueOf(this.getMemberTimeout());
    } else if (attName.equalsIgnoreCase(MEMBERSHIP_PORT_RANGE_NAME)) {
      return getMembershipPortRange();
    } else if (attName.equalsIgnoreCase(MAX_WAIT_TIME_FOR_RECONNECT_NAME)) {
      return Integer.valueOf(this.getMaxWaitTimeForReconnect());
    } else if (attName.equalsIgnoreCase(MAX_NUM_RECONNECT_TRIES )) {
      return Integer.valueOf(this.getMaxNumReconnectTries());
    } else if (attName.equalsIgnoreCase(ASYNC_DISTRIBUTION_TIMEOUT_NAME)) {
      return Integer.valueOf(this.getAsyncDistributionTimeout());
    } else if (attName.equalsIgnoreCase(ASYNC_QUEUE_TIMEOUT_NAME)) {
      return Integer.valueOf(this.getAsyncQueueTimeout());
    } else if (attName.equalsIgnoreCase(ASYNC_MAX_QUEUE_SIZE_NAME)) {
      return Integer.valueOf(this.getAsyncMaxQueueSize());
    } else if (attName.equalsIgnoreCase(START_LOCATOR_NAME)) {
      return this.getStartLocator();
    } else if (attName.equalsIgnoreCase(CLIENT_CONFLATION_PROP_NAME)) {
      return this.getClientConflation();
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_ID_NAME)) {
      return this.getDurableClientId();
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_TIMEOUT_NAME)) {
      return Integer.valueOf(this.getDurableClientTimeout());
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTH_INIT_NAME)) {
      return this.getSecurityClientAuthInit();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTHENTICATOR_NAME)) {
      return this.getSecurityClientAuthenticator();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_DHALGO_NAME)) {
      return this.getSecurityClientDHAlgo();
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTH_INIT_NAME)) {
      return this.getSecurityPeerAuthInit();
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTHENTICATOR_NAME)) {
      return this.getSecurityPeerAuthenticator();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_NAME)) {
      return this.getSecurityClientAccessor();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_PP_NAME)) {
      return this.getSecurityClientAccessorPP();
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_LEVEL_NAME)) {
      return LogWriterImpl.levelToString(this.getSecurityLogLevel());
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_FILE_NAME)) {
      return this.getSecurityLogFile();
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME)) {
      return Integer.valueOf(this.getSecurityPeerMembershipTimeout());
    } else if (attName.startsWith(SECURITY_PREFIX_NAME)) {
      return this.getSecurity(attName);
    } else if (attName.equalsIgnoreCase(ENABLE_NETWORK_PARTITION_DETECTION_NAME)) {
      return Boolean.valueOf(this.getEnableNetworkPartitionDetection());
    } else if (attName.equalsIgnoreCase(DISABLE_AUTO_RECONNECT_NAME)) {
      return Boolean.valueOf(this.getDisableAutoReconnect());
    } else if (attName.equalsIgnoreCase(REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME)) {
       return Boolean.valueOf(this.getRemoveUnresponsiveClient());
    } else if (attName.equalsIgnoreCase(DELTA_PROPAGATION_PROP_NAME)) {
      return Boolean.valueOf(this.getDeltaPropagation());
    } else if (attName.equalsIgnoreCase(DISTRIBUTED_SYSTEM_ID_NAME)) {
      return Integer.valueOf(this.getDistributedSystemId());
    } else if (attName.equalsIgnoreCase(ENFORCE_UNIQUE_HOST_NAME)) {
      return Boolean.valueOf(this.getEnforceUniqueHost());
    } else if (attName.equalsIgnoreCase(REDUNDANCY_ZONE_NAME)) {
      return this.getRedundancyZone() == null ? "" : this.getRedundancyZone();
    } else if (attName.equalsIgnoreCase(GROUPS_NAME)) {
      return this.getGroups();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_NAME)) {
      return this.getJmxManager();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_START_NAME)) {
      return this.getJmxManagerStart();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_NAME)) {
      return this.getJmxManagerSSL();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_ENABLED_NAME)) {
      return this.getJmxManagerSSLEnabled();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_CIPHERS_NAME)) {
      return this.getJmxManagerSSLCiphers();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_PROTOCOLS_NAME)) {
      return this.getJmxManagerSSLProtocols();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.getJmxManagerSSLRequireAuthentication();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_NAME)) {
      return this.getJmxManagerSSLKeyStore();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME)) {
      return this.getJmxManagerSSLKeyStoreType();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.getJmxManagerSSLKeyStorePassword();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_NAME)) {
      return this.getJmxManagerSSLTrustStore();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.getJmxManagerSSLTrustStorePassword();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PORT_NAME)) {
      return this.getJmxManagerPort();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_BIND_ADDRESS_NAME)) {
      return this.getJmxManagerBindAddress();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME)) {
      return this.getJmxManagerHostnameForClients();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PASSWORD_FILE_NAME)) {
      return this.getJmxManagerPasswordFile();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_ACCESS_FILE_NAME)) {
      return this.getJmxManagerAccessFile();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HTTP_PORT_NAME)) {
      return this.getJmxManagerHttpPort();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_UPDATE_RATE_NAME)) {
      return this.getJmxManagerUpdateRate();
    } else if (attName.equalsIgnoreCase(MEMCACHED_PORT_NAME)) {
      return this.getMemcachedPort();
    } else if (attName.equalsIgnoreCase(MEMCACHED_PROTOCOL_NAME)) {
      return this.getMemcachedProtocol();
    } else if (attName.equalsIgnoreCase(MEMCACHED_BIND_ADDRESS_NAME)) {
      return this.getMemcachedBindAddress();
    } else if (attName.equalsIgnoreCase(USER_COMMAND_PACKAGES)) {
      return this.getUserCommandPackages();
    } else if (attName.equalsIgnoreCase(ENABLE_CLUSTER_CONFIGURATION_NAME)) {
      return this.getEnableClusterConfiguration();
    } else if (attName.equalsIgnoreCase(USE_CLUSTER_CONFIGURATION_NAME)) {
      return this.getUseSharedConfiguration();
    } else if (attName.equalsIgnoreCase(LOAD_CLUSTER_CONFIG_FROM_DIR_NAME)) {
      return this.getLoadClusterConfigFromDir();
    } else if (attName.equalsIgnoreCase(CLUSTER_CONFIGURATION_DIR)) {
      return this.getClusterConfigDir();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_ENABLED_NAME)) {
      return this.getServerSSLEnabled();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_CIPHERS_NAME)) {
      return this.getServerSSLCiphers();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_PROTOCOLS_NAME)) {
      return this.getServerSSLProtocols();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.getServerSSLRequireAuthentication();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_NAME)) {
      return this.getServerSSLKeyStore();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_TYPE_NAME)) {
      return this.getServerSSLKeyStoreType();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.getServerSSLKeyStorePassword();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_NAME)) {
      return this.getServerSSLTrustStore();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.getServerSSLTrustStorePassword();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_ENABLED_NAME)) {
      return this.getGatewaySSLEnabled();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_CIPHERS_NAME)) {
      return this.getGatewaySSLCiphers();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_PROTOCOLS_NAME)) {
      return this.getGatewaySSLProtocols();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.getGatewaySSLRequireAuthentication();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_NAME)) {
      return this.getGatewaySSLKeyStore();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_TYPE_NAME)) {
      return this.getGatewaySSLKeyStoreType();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.getGatewaySSLKeyStorePassword();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_NAME)) {
      return this.getGatewaySSLTrustStore();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.getGatewaySSLTrustStorePassword();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_PORT_NAME)) {
       return this.getHttpServicePort();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_BIND_ADDRESS_NAME)) {
      return this.getHttpServiceBindAddress();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_ENABLED_NAME)) {
      return this.getHttpServiceSSLEnabled();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_CIPHERS_NAME)) {
      return this.getHttpServiceSSLCiphers();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_PROTOCOLS_NAME)) {
      return this.getHttpServiceSSLProtocols();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.getHttpServiceSSLRequireAuthentication();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_NAME)) {
      return this.getHttpServiceSSLKeyStore();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME)) {
      return this.getHttpServiceSSLKeyStoreType();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.getHttpServiceSSLKeyStorePassword();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_NAME)) {
      return this.getHttpServiceSSLTrustStore();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.getHttpServiceSSLTrustStorePassword();
    } else if (attName.equalsIgnoreCase(START_DEV_REST_API_NAME)) {
      return this.getStartDevRestApi();
    } else {
      throw new InternalGemFireException(LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }
  }

  public boolean isAttributeModifiable(String attName) {
    checkAttributeName(attName);
    if (attName.equalsIgnoreCase(ACK_WAIT_THRESHOLD_NAME)) {
      return this.isAckWaitThresholdModifiable();
    } else if (attName.equalsIgnoreCase(ACK_SEVERE_ALERT_THRESHOLD_NAME)) {
      return this.isAckSevereAlertThresholdModifiable();
    } else if (attName.equalsIgnoreCase(ARCHIVE_DISK_SPACE_LIMIT_NAME)) {
      return this.isArchiveDiskSpaceLimitModifiable();
    } else if (attName.equalsIgnoreCase(ARCHIVE_FILE_SIZE_LIMIT_NAME)) {
      return this.isArchiveFileSizeLimitModifiable();
    } else if (attName.equalsIgnoreCase(CACHE_XML_FILE_NAME)) {
      return this.isCacheXmlFileModifiable();
    } else if (attName.equalsIgnoreCase(DEPLOY_WORKING_DIR)) {
      return this.isDeployWorkingDirModifiable();
    } else if (attName.equalsIgnoreCase(LOG_DISK_SPACE_LIMIT_NAME)) {
      return this.isLogDiskSpaceLimitModifiable();
    } else if (attName.equalsIgnoreCase(LOG_FILE_NAME)) {
      return this.isLogFileModifiable();
    } else if (attName.equalsIgnoreCase(LOG_FILE_SIZE_LIMIT_NAME)) {
      return this.isLogFileSizeLimitModifiable();
    } else if (attName.equalsIgnoreCase(LOG_LEVEL_NAME)) {
      return this.isLogLevelModifiable();
    } else if (attName.equalsIgnoreCase(LOCATORS_NAME)) {
      return this.isLocatorsModifiable();
    } else if (attName.equalsIgnoreCase(LOCATOR_WAIT_TIME_NAME)) {
      return this.isLocatorWaitTimeModifiable();
    } else if (attName.equalsIgnoreCase(REMOTE_LOCATORS_NAME)) {
      return this.isRemoteLocatorsModifiable();
    } else if (attName.equalsIgnoreCase(MCAST_ADDRESS_NAME)) {
      return this.isMcastAddressModifiable();
    } else if (attName.equalsIgnoreCase(BIND_ADDRESS_NAME)) {
      return this.isBindAddressModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_BIND_ADDRESS_NAME)) {
      return this.isServerBindAddressModifiable();
    } else if (attName.equalsIgnoreCase(TCP_PORT_NAME)) {
      return this.isTcpPortModifiable();
    } else if (attName.equalsIgnoreCase(MCAST_PORT_NAME)) {
      return this.isMcastPortModifiable();
    } else if (attName.equalsIgnoreCase(MCAST_TTL_NAME)) {
      return this.isMcastTtlModifiable();
    } else if (attName.equalsIgnoreCase(SOCKET_LEASE_TIME_NAME)) {
      return this.isSocketLeaseTimeModifiable();
    } else if (attName.equalsIgnoreCase(SOCKET_BUFFER_SIZE_NAME)) {
      return this.isSocketBufferSizeModifiable();
    } else if (attName.equalsIgnoreCase(CONSERVE_SOCKETS_NAME)) {
      return this.isConserveSocketsModifiable();
    } else if (attName.equalsIgnoreCase(ROLES_NAME)) {
      return this.isRolesModifiable();
    } else if (attName.equalsIgnoreCase(NAME_NAME)) {
      return this.isNameModifiable();
    } else if (attName.equalsIgnoreCase(STATISTIC_ARCHIVE_FILE_NAME)) {
      return this.isStatisticArchiveFileModifiable();
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLE_RATE_NAME)) {
      return this.isStatisticSampleRateModifiable();
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLING_ENABLED_NAME)) {
      return this.isStatisticSamplingEnabledModifiable();
    } else if (attName.equalsIgnoreCase(SSL_ENABLED_NAME)) {
      return this.isSSLEnabledModifiable();
    } else if (attName.equalsIgnoreCase(SSL_PROTOCOLS_NAME)) {
      return this.isSSLProtocolsModifiable();
    } else if (attName.equalsIgnoreCase(SSL_CIPHERS_NAME)) {
      return this.isSSLCiphersModifiable();
    } else if (attName.equalsIgnoreCase(SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.isSSLRequireAuthenticationModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_ENABLED_NAME)) {
      return this.isClusterSSLEnabledModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_PROTOCOLS_NAME)) {
      return this.isClusterSSLProtocolsModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_CIPHERS_NAME)) {
      return this.isClusterSSLCiphersModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.isClusterSSLRequireAuthenticationModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_NAME)) {
      return this.isClusterSSLKeyStoreModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_TYPE_NAME)) {
      return this.isClusterSSLKeyStoreTypeModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.isClusterSSLKeyStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_NAME)) {
      return this.isClusterSSLTrustStoreModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.isClusterSSLTrustStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(MCAST_SEND_BUFFER_SIZE_NAME)) {
      return this.isMcastSendBufferSizeModifiable();
    } else if (attName.equalsIgnoreCase(MCAST_RECV_BUFFER_SIZE_NAME)) {
      return this.isMcastRecvBufferSizeModifiable();
    } else if (attName.equalsIgnoreCase(UDP_SEND_BUFFER_SIZE_NAME)) {
      return this.isUdpSendBufferSizeModifiable();
    } else if (attName.equalsIgnoreCase(UDP_RECV_BUFFER_SIZE_NAME)) {
      return this.isUdpRecvBufferSizeModifiable();
    } else if (attName.equalsIgnoreCase(MCAST_FLOW_CONTROL_NAME)) {
      return this.isMcastFlowControlModifiable();
    } else if (attName.equalsIgnoreCase(UDP_FRAGMENT_SIZE_NAME)) {
      return this.isUdpFragmentSizeModifiable();
    } else if (attName.equalsIgnoreCase(DISABLE_TCP_NAME)) {
      return this.isDisableTcpModifiable();
    } else if (attName.equalsIgnoreCase(ENABLE_TIME_STATISTICS_NAME)) {
      return this.isEnableTimeStatisticsModifiable();
    } else if (attName.equalsIgnoreCase(MEMBER_TIMEOUT_NAME)) {
      return this.isMemberTimeoutModifiable();
    } else if (attName.equalsIgnoreCase(MEMBERSHIP_PORT_RANGE_NAME)) {
      return this.isMembershipPortRangeModifiable();
    } else if (attName.equalsIgnoreCase(MAX_NUM_RECONNECT_TRIES)) {
      return this.isMaxNumberOfTiesModifiable();
    } else if (attName.equalsIgnoreCase(MAX_WAIT_TIME_FOR_RECONNECT_NAME)) {
      return this.isMaxTimeOutModifiable();
    } else if (attName.equalsIgnoreCase(ASYNC_DISTRIBUTION_TIMEOUT_NAME)) {
      return this.isAsyncDistributionTimeoutModifiable();
    } else if (attName.equalsIgnoreCase(ASYNC_QUEUE_TIMEOUT_NAME)) {
      return this.isAsyncQueueTimeoutModifiable();
    } else if (attName.equalsIgnoreCase(ASYNC_MAX_QUEUE_SIZE_NAME)) {
      return this.isAsyncMaxQueueSizeModifiable();
    } else if (attName.equalsIgnoreCase(START_LOCATOR_NAME)) {
      return this.isStartLocatorModifiable();
    } else if (attName.equalsIgnoreCase(CLIENT_CONFLATION_PROP_NAME)) {
      return this.isClientConflationModifiable();
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_ID_NAME)) {
      return this.isDurableClientIdModifiable();
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_TIMEOUT_NAME)) {
      return this.isDurableClientTimeoutModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTH_INIT_NAME)) {
      return this.isSecurityClientAuthInitModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTHENTICATOR_NAME)) {
      return this.isSecurityClientAuthenticatorModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_DHALGO_NAME)) {
      return this.isSecurityClientDHAlgoModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTH_INIT_NAME)) {
      return this.isSecurityPeerAuthInitModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTHENTICATOR_NAME)) {
      return this.isSecurityPeerAuthenticatorModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_NAME)) {
      return this.isSecurityClientAccessorModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_PP_NAME)) {
      return this.isSecurityClientAccessorPPModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_LEVEL_NAME)) {
      return this.isSecurityLogLevelModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_FILE_NAME)) {
      return this.isSecurityLogFileModifiable();
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME)) {
      return this.isSecurityPeerMembershipTimeoutModifiable();
    } else if (attName.startsWith(SECURITY_PREFIX_NAME)) {
      return this.isSecurityModifiable();
    } else if (attName.equalsIgnoreCase(ENABLE_NETWORK_PARTITION_DETECTION_NAME)) {
      return this.isEnableNetworkPartitionDetectionModifiable();
    } else if (attName.equalsIgnoreCase(DISABLE_AUTO_RECONNECT_NAME)) {
      return this.isDisableAutoReconnectModifiable();
    } else if (attName.equalsIgnoreCase(REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME)) {
      return this.isRemoveUnresponsiveClientModifiable();
    } else if (attName.equalsIgnoreCase(DELTA_PROPAGATION_PROP_NAME)) {
      return this.isDeltaPropagationModifiable();
    } else if (attName.equalsIgnoreCase(DISTRIBUTED_SYSTEM_ID_NAME)) {
      return this.isDistributedSystemIdModifiable();
    } else if (attName.equalsIgnoreCase(ENFORCE_UNIQUE_HOST_NAME)) {
      return this.isEnforceUniqueHostModifiable();
    } else if (attName.equalsIgnoreCase(REDUNDANCY_ZONE_NAME)) {
      return this.isRedundancyZoneModifiable();
    } else if (attName.startsWith(SSL_SYSTEM_PROPS_NAME)) {
      return this.isSSLPropertyModifiable();
    } else if (attName.equalsIgnoreCase(GROUPS_NAME)) {
      return this.isGroupsModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_NAME)) {
      return this.isJmxManagerModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_START_NAME)) {
      return this.isJmxManagerStartModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_NAME)) {
      return this.isJmxManagerSSLModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_ENABLED_NAME)) {
      return this.isJmxManagerSSLEnabledModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_CIPHERS_NAME)) {
      return this.isJmxManagerSSLCiphersModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_PROTOCOLS_NAME)) {
      return this.isJmxManagerSSLProtocolsModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.isJmxManagerSSLRequireAuthenticationModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_NAME)) {
      return this.isJmxManagerSSLKeyStoreModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME)) {
      return this.isJmxManagerSSLKeyStoreTypeModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.isJmxManagerSSLKeyStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_NAME)) {
      return this.isJmxManagerSSLTrustStoreModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.isJmxManagerSSLTrustStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PORT_NAME)) {
      return this.isJmxManagerPortModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_BIND_ADDRESS_NAME)) {
      return this.isJmxManagerBindAddressModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME)) {
      return this.isJmxManagerHostnameForClientsModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PASSWORD_FILE_NAME)) {
      return this.isJmxManagerPasswordFileModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_ACCESS_FILE_NAME)) {
      return this.isJmxManagerAccessFileModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HTTP_PORT_NAME)) {
      return this.isJmxManagerHttpPortModifiable();
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_UPDATE_RATE_NAME)) {
      return this.isJmxManagerUpdateRateModifiable();
    } else if (attName.equalsIgnoreCase(MEMCACHED_PORT_NAME)) {
      return this.isMemcachedPortModifiable();
    } else if (attName.equalsIgnoreCase(MEMCACHED_PROTOCOL_NAME)) {
      return this.isMemcachedProtocolModifiable();
    } else if (attName.equalsIgnoreCase(MEMCACHED_BIND_ADDRESS_NAME)) {
      return this.isMemcachedBindAddressModifiable();
    } else if (attName.equalsIgnoreCase(USER_COMMAND_PACKAGES)) {
      return this.isUserCommandPackagesModifiable();
    } else if (attName.equalsIgnoreCase(ENABLE_CLUSTER_CONFIGURATION_NAME)) {
      return this.isEnableSharedConfigurationModifiable();
    } else if (attName.equalsIgnoreCase(LOAD_CLUSTER_CONFIG_FROM_DIR_NAME)) {
      return this.isLoadSharedConfigFromDirModifiable();
    } else if (attName.equalsIgnoreCase(USE_CLUSTER_CONFIGURATION_NAME)) {
      return this.isUseSharedConfigurationModifiable();
    } else if (attName.equalsIgnoreCase(CLUSTER_CONFIGURATION_DIR)) {
      return this._modifiableDefault();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_ENABLED_NAME)) {
      return this.isServerSSLEnabledModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_CIPHERS_NAME)) {
      return this.isServerSSLCiphersModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_PROTOCOLS_NAME)) {
      return this.isServerSSLProtocolsModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.isServerSSLRequireAuthenticationModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_NAME)) {
      return this.isServerSSLKeyStoreModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_TYPE_NAME)) {
      return this.isServerSSLKeyStoreTypeModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.isServerSSLKeyStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_NAME)) {
      return this.isServerSSLTrustStoreModifiable();
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.isServerSSLTrustStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_ENABLED_NAME)) {
      return this.isGatewaySSLEnabledModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_CIPHERS_NAME)) {
      return this.isGatewaySSLCiphersModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_PROTOCOLS_NAME)) {
      return this.isGatewaySSLProtocolsModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.isGatewaySSLRequireAuthenticationModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_NAME)) {
      return this.isGatewaySSLKeyStoreModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_TYPE_NAME)) {
      return this.isGatewaySSLKeyStoreTypeModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.isGatewaySSLKeyStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_NAME)) {
      return this.isGatewaySSLTrustStoreModifiable();
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.isGatewaySSLTrustStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_PORT_NAME)) {
       return this.isHttpServicePortModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_BIND_ADDRESS_NAME)) {
      return this.isHttpServiceBindAddressModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_ENABLED_NAME)) {
      return this.isHttpServiceSSLEnabledModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_CIPHERS_NAME)) {
      return this.isHttpServiceSSLCiphersModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_PROTOCOLS_NAME)) {
      return this.isHttpServiceSSLProtocolsModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return this.isHttpServiceSSLRequireAuthenticationModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_NAME)) {
      return this.isHttpServiceSSLKeyStoreModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME)) {
      return this.isHttpServiceSSLKeyStoreTypeModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME)) {
      return this.isHttpServiceSSLKeyStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_NAME)) {
      return this.isHttpServiceSSLTrustStoreModifiable();
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return this.isHttpServiceSSLTrustStorePasswordModifiable();
    } else if (attName.equalsIgnoreCase(START_DEV_REST_API_NAME)) {
      return this.isStartDevRestApiModifiable();
    } else {
      throw new InternalGemFireException(LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }
  }
  public Class getAttributeType(String attName) {
    checkAttributeName(attName);

    if (attName.equalsIgnoreCase(ACK_WAIT_THRESHOLD_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(ACK_SEVERE_ALERT_THRESHOLD_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(ARCHIVE_DISK_SPACE_LIMIT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(ARCHIVE_FILE_SIZE_LIMIT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(CACHE_XML_FILE_NAME)) {
      return File.class;
    } else if (attName.equalsIgnoreCase(DEPLOY_WORKING_DIR)) {
      return File.class;
    } else if (attName.equalsIgnoreCase(LOG_DISK_SPACE_LIMIT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(LOG_FILE_NAME)) {
      return File.class;
    } else if (attName.equalsIgnoreCase(LOG_FILE_SIZE_LIMIT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(LOG_LEVEL_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(LOCATORS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(LOCATOR_WAIT_TIME_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(REMOTE_LOCATORS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(MCAST_ADDRESS_NAME)) {
      return InetAddress.class;
    } else if (attName.equalsIgnoreCase(BIND_ADDRESS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_BIND_ADDRESS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(TCP_PORT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MCAST_PORT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MCAST_TTL_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(SOCKET_LEASE_TIME_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(SOCKET_BUFFER_SIZE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(CONSERVE_SOCKETS_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(ROLES_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(NAME_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(STATISTIC_ARCHIVE_FILE_NAME)) {
      return File.class;
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLE_RATE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(STATISTIC_SAMPLING_ENABLED_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(SSL_ENABLED_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(SSL_PROTOCOLS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SSL_CIPHERS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_ENABLED_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_PROTOCOLS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_CIPHERS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_TYPE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(MCAST_SEND_BUFFER_SIZE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MCAST_RECV_BUFFER_SIZE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(UDP_SEND_BUFFER_SIZE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(UDP_RECV_BUFFER_SIZE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MCAST_FLOW_CONTROL_NAME)) {
      return FlowControlParams.class;
    } else if (attName.equalsIgnoreCase(UDP_FRAGMENT_SIZE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(DISABLE_TCP_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(ENABLE_TIME_STATISTICS_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(MEMBER_TIMEOUT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MEMBERSHIP_PORT_RANGE_NAME)) {
      return int[].class;
    } else if (attName.equalsIgnoreCase(MAX_WAIT_TIME_FOR_RECONNECT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MAX_NUM_RECONNECT_TRIES)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(ASYNC_DISTRIBUTION_TIMEOUT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(ASYNC_QUEUE_TIMEOUT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(ASYNC_MAX_QUEUE_SIZE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(START_LOCATOR_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(CLIENT_CONFLATION_PROP_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_ID_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(DURABLE_CLIENT_TIMEOUT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTH_INIT_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_AUTHENTICATOR_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_DHALGO_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTH_INIT_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_AUTHENTICATOR_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_CLIENT_ACCESSOR_PP_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_LEVEL_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SECURITY_LOG_FILE_NAME)) {
      return File.class;
    } else if (attName.equalsIgnoreCase(SECURITY_PEER_VERIFYMEMBER_TIMEOUT_NAME)) {
      return Integer.class;
    } else if (attName.startsWith(SECURITY_PREFIX_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(ENABLE_NETWORK_PARTITION_DETECTION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(DISABLE_AUTO_RECONNECT_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(REMOVE_UNRESPONSIVE_CLIENT_PROP_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(DELTA_PROPAGATION_PROP_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(DISTRIBUTED_SYSTEM_ID_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(ENFORCE_UNIQUE_HOST_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(REDUNDANCY_ZONE_NAME)) {
      return String.class;
    } else if (attName.startsWith(USERDEFINED_PREFIX_NAME)) {
      return String.class;
    } else if (attName.startsWith(SSL_SYSTEM_PROPS_NAME) || attName.startsWith(SYS_PROP_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GROUPS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_START_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_ENABLED_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_CIPHERS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_PROTOCOLS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_TYPE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PORT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_BIND_ADDRESS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HOSTNAME_FOR_CLIENTS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_PASSWORD_FILE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_ACCESS_FILE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_HTTP_PORT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(JMX_MANAGER_UPDATE_RATE_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MEMCACHED_PORT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(MEMCACHED_PROTOCOL_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(MEMCACHED_BIND_ADDRESS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(USER_COMMAND_PACKAGES)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(ENABLE_CLUSTER_CONFIGURATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(USE_CLUSTER_CONFIGURATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(LOAD_CLUSTER_CONFIG_FROM_DIR_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(CLUSTER_CONFIGURATION_DIR)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_ENABLED_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_CIPHERS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_PROTOCOLS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_TYPE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_KEYSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(SERVER_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_ENABLED_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_CIPHERS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_PROTOCOLS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_TYPE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_KEYSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(GATEWAY_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_PORT_NAME)) {
      return Integer.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_BIND_ADDRESS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_ENABLED_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_CIPHERS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_PROTOCOLS_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME)) {
      return Boolean.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME)) {
      return String.class;
    } else if (attName.equalsIgnoreCase(START_DEV_REST_API_NAME)) {
      return Boolean.class;
    } else {
      throw new InternalGemFireException(LocalizedStrings.AbstractDistributionConfig_UNHANDLED_ATTRIBUTE_NAME_0.toLocalizedString(attName));
    }
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
  
  
  public boolean isServerSSLEnabledModifiable(){
    return _modifiableDefault();
  }
  
  public void checkServerSSLEnabled(){
    _checkIfModifiable(SERVER_SSL_ENABLED_NAME);
  }

  public boolean isServerSSLRequireAuthenticationModifiable(){
    return _modifiableDefault();
  }
  
  public void checkServerSSLRequireAuthentication(){
    _checkIfModifiable(SERVER_SSL_REQUIRE_AUTHENTICATION_NAME);
  }
  
  public boolean isServerSSLProtocolsModifiable(){
    return _modifiableDefault();
  }
  
  public void checkServerSSLProtocols(){
    _checkIfModifiable(SERVER_SSL_PROTOCOLS_NAME);
  }

  public boolean isServerSSLCiphersModifiable(){
    return _modifiableDefault();
  }
  
  public void checkServerSSLCiphers(){
    _checkIfModifiable(SERVER_SSL_CIPHERS_NAME);
  }
  
  public boolean isGatewaySSLEnabledModifiable(){
    return _modifiableDefault();
  }
  
  public void checkGatewaySSL(){
    _checkIfModifiable(GATEWAY_SSL_ENABLED_NAME);
  }

  public boolean isGatewaySSLRequireAuthenticationModifiable(){
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLRequireAuthentication(){
    _checkIfModifiable(GATEWAY_SSL_REQUIRE_AUTHENTICATION_NAME);
  }
  
  public boolean isGatewaySSLProtocolsModifiable(){
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLProtocols(){
    _checkIfModifiable(GATEWAY_SSL_PROTOCOLS_NAME);
  }

  
  public boolean isGatewaySSLCiphersModifiable(){
    return _modifiableDefault();
  }
  
  public void checkGatewaySSLCiphers(){
    _checkIfModifiable(GATEWAY_SSL_CIPHERS_NAME);
  }
  
}
