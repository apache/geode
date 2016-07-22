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

import com.gemstone.gemfire.distributed.SSLEnabledComponents;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.admin.SSLConfig;

public class SocketCreatorFactory {

  private static SocketCreatorFactory instance = null;
  private static final String NON_SSL = "Non_SSL";
  private Map<SSLEnabledComponent, SocketCreator> socketCreators = new HashMap<>();
  private DistributionConfig distributionConfig;

  /**
   * Here we parse the distribution distributionConfig and setup the required SocketCreators
   */
  private void initializeSocketCreators(final DistributionConfig distributionConfig) {
    if (distributionConfig == null) {
      this.distributionConfig = new DistributionConfigImpl(new Properties());
    } else {
      this.distributionConfig = distributionConfig;
    }
    SSLConfigurationFactory.setDistributionConfig(this.distributionConfig);
    initialize();
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

  private void initialize() {
    //Hack... get a default cluster socket creator initialized
    getClusterSSLSocketCreator();
  }

  public static SocketCreator getClusterSSLSocketCreator() {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(SSLEnabledComponent.CLUSTER);
    return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponent.CLUSTER, sslConfigForComponent);
  }

  public static SocketCreator getServerSSLSocketCreator() {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(SSLEnabledComponent.SERVER);
    return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponent.SERVER, sslConfigForComponent);
  }

  public static SocketCreator getGatewaySSLSocketCreator() {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(SSLEnabledComponent.GATEWAY);
    return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponent.GATEWAY, sslConfigForComponent);
  }

  public static SocketCreator getJMXManagerSSLSocketCreator() {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(SSLEnabledComponent.JMX);
    return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponent.JMX, sslConfigForComponent);
  }

  public static SocketCreator getHTTPServiceSSLSocketCreator() {
    SSLConfig sslConfigForComponent = SSLConfigurationFactory.getSSLConfigForComponent(SSLEnabledComponent.HTTP_SERVICE);
    return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponent.HTTP_SERVICE, sslConfigForComponent);
  }

  private SocketCreator getSSLSocketCreator(final SSLEnabledComponent sslComponent, final DistributionConfig distributionConfig, final SSLConfig sslConfig) {
    if (sslConfig.isEnabled()) {
      if (ArrayUtils.contains(distributionConfig.getSSLEnabledComponents(), SSLEnabledComponents.ALL)) {
        return createSSLSocketCreator(SSLEnabledComponent.ALL, sslConfig);
      } else if (ArrayUtils.contains(distributionConfig.getSSLEnabledComponents(), sslComponent.getConstant())) {
        return createSSLSocketCreator(sslComponent, sslConfig);
      }
    }
    return createSSLSocketCreator(SSLEnabledComponent.NONE, sslConfig);
  }


  private SocketCreator getOrCreateSocketCreatorForSSLEnabledComponent(final SSLEnabledComponent sslEnabledComponent, final SSLConfig sslConfig) {
    SocketCreator socketCreator = getSocketCreatorForComponent(sslEnabledComponent);
    if (socketCreator == null) {
      return getSSLSocketCreator(sslEnabledComponent, distributionConfig, sslConfig);
    } else {
      return socketCreator;
    }
  }

  private SocketCreator createSSLSocketCreator(final SSLEnabledComponent sslEnableComponent, final SSLConfig sslConfig) {
    SocketCreator socketCreator = null;
    if (sslConfig.isEnabled()) {
      socketCreator = new SocketCreator(sslConfig);
      addSocketCreatorForComponent(sslEnableComponent, socketCreator);
    } else {
      socketCreator = new SocketCreator(sslConfig);
      addSocketCreatorForComponent(SSLEnabledComponent.NONE, socketCreator);
    }
    return socketCreator;
  }

  private synchronized void addSocketCreatorForComponent(SSLEnabledComponent sslEnabledComponent, SocketCreator socketCreator) {
    socketCreators.put(sslEnabledComponent, socketCreator);
  }

  private synchronized SocketCreator getSocketCreatorForComponent(SSLEnabledComponent sslEnabledComponent) {
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
      SocketCreatorFactory.instance = null;
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
