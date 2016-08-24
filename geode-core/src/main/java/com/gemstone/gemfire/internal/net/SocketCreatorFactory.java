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
package com.gemstone.gemfire.internal.net;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.lang.ArrayUtils;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.admin.SSLConfig;
import com.gemstone.gemfire.internal.security.SecurableComponent;

public class SocketCreatorFactory {

  private static SocketCreatorFactory instance = null;
  private Map<SecurableComponent, SocketCreator> socketCreators = new HashMap<>();
  private DistributionConfig distributionConfig;

  /**
   * Here we parse the distribution distributionConfig and setup the required SocketCreators
   */
  private void initializeSocketCreators(final DistributionConfig distributionConfig) {
    if (distributionConfig == null) {
      throw new GemFireConfigException("SocketCreatorFactory requires a valid distribution config.");
    } else {
      this.distributionConfig = distributionConfig;
    }
    SSLConfigurationFactory.setDistributionConfig(this.distributionConfig);
  }

  private DistributionConfig getDistributionConfig() {
    if (distributionConfig == null) {
      throw new GemFireConfigException("SocketCreatorFactory requires a valid distribution config.");
    }
    return distributionConfig;
  }

  private synchronized static SocketCreatorFactory getInstance(boolean closing) {
    if (instance == null && !closing) {
      instance = new SocketCreatorFactory();
    }
    return instance;
  }

  private synchronized static SocketCreatorFactory getInstance() {
    return getInstance(false);
  }

  public static SocketCreator getSSLSocketCreatorForComponent(SecurableComponent sslEnabledComponent) {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(sslEnabledComponent);
    return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(sslEnabledComponent, sslConfigForComponent);
  }

  private SocketCreator getSSLSocketCreator(final SecurableComponent sslComponent, final SSLConfig sslConfig) {
    if (sslConfig.isEnabled()) {
      if (ArrayUtils.contains(getDistributionConfig().getSSLEnabledComponents(), SecurableComponent.ALL)) {
        return createSSLSocketCreator(SecurableComponent.ALL, sslConfig);
      } else if (ArrayUtils.contains(getDistributionConfig().getSSLEnabledComponents(), sslComponent)) {
        return createSSLSocketCreator(sslComponent, sslConfig);
      }
    }
    return createSSLSocketCreator(SecurableComponent.NONE, sslConfig);
  }


  private SocketCreator getOrCreateSocketCreatorForSSLEnabledComponent(final SecurableComponent sslEnabledComponent, final SSLConfig sslConfig) {
    SocketCreator socketCreator = getSocketCreatorForComponent(sslEnabledComponent);
    if (socketCreator == null) {
      return getSSLSocketCreator(sslEnabledComponent, sslConfig);
    } else {
      return socketCreator;
    }
  }

  private SocketCreator createSSLSocketCreator(final SecurableComponent sslEnableComponent, final SSLConfig sslConfig) {
    SocketCreator socketCreator = null;
    if (sslConfig.isEnabled()) {
      socketCreator = new SocketCreator(sslConfig);
      addSocketCreatorForComponent(sslEnableComponent, socketCreator);
    } else {
      socketCreator = getSocketCreatorForComponent(SecurableComponent.NONE);
      if (socketCreator == null) {
        socketCreator = new SocketCreator(sslConfig);
        addSocketCreatorForComponent(SecurableComponent.NONE, socketCreator);
      }
    }
    return socketCreator;
  }

  private synchronized void addSocketCreatorForComponent(SecurableComponent sslEnabledComponent, SocketCreator socketCreator) {
    socketCreators.put(sslEnabledComponent, socketCreator);
  }

  private synchronized SocketCreator getSocketCreatorForComponent(SecurableComponent sslEnabledComponent) {
    return socketCreators.get(sslEnabledComponent);
  }

  /**
   * Read an array of values from a string, whitespace separated.
   */
  private static String[] createStringArrayFromString(final String text) {
    if (text == null || text.trim().equals("")) {
      return null;
    }

    StringTokenizer st = new StringTokenizer(text);
    Vector v = new Vector();
    while (st.hasMoreTokens()) {
      v.add(st.nextToken());
    }
    return (String[]) v.toArray(new String[v.size()]);
  }

  /**
   * This a legacy SocketCreator initializer.
   * @param useSSL
   * @param needClientAuth
   * @param protocols
   * @param ciphers
   * @param gfsecurityProps
   *
   * @return SocketCreator for the defined properties
   *
   * @deprecated as of Geode 1.0
   */
  @Deprecated
  public static SocketCreator createNonDefaultInstance(final boolean useSSL,
                                                       final boolean needClientAuth,
                                                       final String protocols,
                                                       final String ciphers,
                                                       final Properties gfsecurityProps) {
    SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(useSSL, needClientAuth, protocols, ciphers, gfsecurityProps, null);
    return new SocketCreator(sslConfig);
  }

  public static void close() {
    SocketCreatorFactory socketCreatorFactory = getInstance(true);
    if (socketCreatorFactory != null) {
      socketCreatorFactory.clearSocketCreators();
      socketCreatorFactory.distributionConfig = null;
      SSLConfigurationFactory.close();
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
