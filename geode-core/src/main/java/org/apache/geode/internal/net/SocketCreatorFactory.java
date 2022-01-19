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
package org.apache.geode.internal.net;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class SocketCreatorFactory {

  @MakeNotStatic
  private static SocketCreatorFactory instance = null;
  @MakeNotStatic
  private final Map<SecurableCommunicationChannel, SocketCreator> socketCreators = new HashMap<>();
  private DistributionConfig distributionConfig;

  /**
   * Here we parse the distribution distributionConfig and setup the required SocketCreators
   */
  private void initializeSocketCreators(final DistributionConfig distributionConfig) {
    if (distributionConfig == null) {
      throw new GemFireConfigException(
          "SocketCreatorFactory requires a valid distribution config.");
    } else {
      this.distributionConfig = distributionConfig;
    }
  }

  private DistributionConfig getDistributionConfig() {
    if (distributionConfig == null) {
      throw new GemFireConfigException(
          "SocketCreatorFactory requires a valid distribution config.");
    }
    return distributionConfig;
  }

  private static synchronized SocketCreatorFactory getInstance(boolean closing) {
    if (instance == null && !closing) {
      instance = new SocketCreatorFactory();
    }
    return instance;
  }

  private static synchronized SocketCreatorFactory getInstance() {
    return getInstance(false);
  }

  public static SocketCreator getSocketCreatorForComponent(
      final DistributionConfig distributionConfig,
      final SecurableCommunicationChannel sslEnabledComponent) {
    final SSLConfig sslConfigForComponent =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            sslEnabledComponent);
    return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(sslEnabledComponent,
        sslConfigForComponent);
  }

  public static SocketCreator getSocketCreatorForComponent(
      SecurableCommunicationChannel sslEnabledComponent) {
    return getSocketCreatorForComponent(getInstance().getDistributionConfig(), sslEnabledComponent);
  }

  private SocketCreator getSSLSocketCreator(final SecurableCommunicationChannel sslComponent,
      final SSLConfig sslConfig) {
    if (sslConfig.isEnabled()) {
      if (ArrayUtils.contains(getDistributionConfig().getSecurableCommunicationChannels(),
          SecurableCommunicationChannel.ALL)) {
        return createSSLSocketCreator(SecurableCommunicationChannel.ALL, sslConfig);
        // } else if
        // (ArrayUtils.contains(getDistributionConfig().getSecurableCommunicationChannels(),
        // sslComponent)) {
      } else {
        return createSSLSocketCreator(sslComponent, sslConfig);
      }
    }
    return createSSLSocketCreator(sslComponent, sslConfig);
  }


  private SocketCreator getOrCreateSocketCreatorForSSLEnabledComponent(
      final SecurableCommunicationChannel sslEnabledComponent, final SSLConfig sslConfig) {
    SocketCreator socketCreator = getRegisteredSocketCreatorForComponent(sslEnabledComponent);
    if (socketCreator == null) {
      return getSSLSocketCreator(sslEnabledComponent, sslConfig);
    } else {
      return socketCreator;
    }
  }

  private SocketCreator createSSLSocketCreator(
      final SecurableCommunicationChannel sslEnableComponent, final SSLConfig sslConfig) {
    SocketCreator socketCreator = null;
    if (sslConfig.isEnabled()) {
      socketCreator = new SocketCreator(sslConfig);
      registerSocketCreatorForComponent(sslEnableComponent, socketCreator);
    } else {
      socketCreator = getRegisteredSocketCreatorForComponent(sslEnableComponent);
      if (socketCreator == null) {
        socketCreator = new SocketCreator(sslConfig);
        registerSocketCreatorForComponent(sslEnableComponent, socketCreator);
      }
    }
    return socketCreator;
  }

  private synchronized void registerSocketCreatorForComponent(
      SecurableCommunicationChannel sslEnabledComponent, SocketCreator socketCreator) {
    socketCreators.put(sslEnabledComponent, socketCreator);
  }

  private synchronized SocketCreator getRegisteredSocketCreatorForComponent(
      SecurableCommunicationChannel sslEnabledComponent) {
    return socketCreators.get(sslEnabledComponent);
  }

  /**
   * This a legacy SocketCreator initializer.
   *
   *
   * @return SocketCreator for the defined properties
   *
   * @deprecated as of Geode 1.0
   */
  @Deprecated
  public static SocketCreator createNonDefaultInstance(final boolean useSSL,
      final boolean needClientAuth, final String protocols, final String ciphers,
      final Properties gfsecurityProps) {
    SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(useSSL, needClientAuth,
        protocols, ciphers, gfsecurityProps, null);
    return new SocketCreator(sslConfig);
  }

  public static void close() {
    SocketCreatorFactory socketCreatorFactory = getInstance(true);
    if (socketCreatorFactory != null) {
      socketCreatorFactory.clearSocketCreators();
      socketCreatorFactory.distributionConfig = null;
    }
  }

  private synchronized void clearSocketCreators() {
    socketCreators.clear();
  }

  public static SocketCreatorFactory setDistributionConfig(final DistributionConfig config) {
    getInstance().initializeSocketCreators(config);
    return getInstance();
  }


}
