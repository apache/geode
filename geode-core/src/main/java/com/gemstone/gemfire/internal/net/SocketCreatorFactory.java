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

public class SocketCreatorFactory {

  private static SocketCreatorFactory instance = new SocketCreatorFactory();
  private static final String NON_SSL = "Non_SSL";
  private Map<String, SocketCreator> socketCreators = new HashMap<>();
  private DistributionConfig distributionConfig;

  private SocketCreatorFactory() {
    this(null);
  }

  private SocketCreatorFactory(DistributionConfig distributionConfig) {
    initializeSocketCreators(distributionConfig);
  }

  /**
   * Here we parse the distribution distributionConfig and setup the required SocketCreators
   */
  private void initializeSocketCreators(final DistributionConfig distributionConfig) {
    if (distributionConfig == null) {
      this.distributionConfig = new DistributionConfigImpl(new Properties());
    } else {
      this.distributionConfig = distributionConfig;
    }
  }

  private static SocketCreatorFactory getInstance() {
    if (instance == null) {
      instance = new SocketCreatorFactory();
    }
    return instance;
  }

  public static SocketCreator getClusterSSLSocketCreator() {
    DistributionConfig distributionConfig = getInstance().distributionConfig;
    if (distributionConfig.getSSLEnabledComponents().length == 0) {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.CLUSTER, null);
    } else {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.CLUSTER, distributionConfig.getClusterSSLAlias());
    }
  }

  public static SocketCreator getServerSSLSocketCreator() {
    DistributionConfig distributionConfig = getInstance().distributionConfig;
    if (distributionConfig.getSSLEnabledComponents().length == 0) {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.SERVER, null, distributionConfig.getServerSSLEnabled(), distributionConfig
        .getServerSSLRequireAuthentication(), createStringArrayFromString(distributionConfig.getServerSSLProtocols()), createStringArrayFromString(distributionConfig
        .getServerSSLCiphers()), distributionConfig.getServerSSLProperties());
    } else {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.SERVER, distributionConfig.getServerSSLAlias());
    }
  }

  public static SocketCreator getGatewaySSLSocketCreator() {
    DistributionConfig distributionConfig = getInstance().distributionConfig;
    if (distributionConfig.getSSLEnabledComponents().length == 0) {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.GATEWAY, null, distributionConfig.getGatewaySSLEnabled(), distributionConfig
        .getGatewaySSLRequireAuthentication(), createStringArrayFromString(distributionConfig.getGatewaySSLProtocols()), createStringArrayFromString(distributionConfig
        .getGatewaySSLCiphers()), distributionConfig.getGatewaySSLProperties());
    } else {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.GATEWAY, distributionConfig.getGatewaySSLAlias());
    }
  }

  public static SocketCreator getJMXManagerSSLSocketCreator() {
    DistributionConfig distributionConfig = getInstance().distributionConfig;
    if (distributionConfig.getSSLEnabledComponents().length == 0) {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.JMX, null, distributionConfig.getJmxManagerSSLEnabled(), distributionConfig
        .getJmxManagerSSLRequireAuthentication(), createStringArrayFromString(distributionConfig.getJmxManagerSSLProtocols()), createStringArrayFromString(distributionConfig
        .getJmxManagerSSLCiphers()), distributionConfig.getJmxSSLProperties());
    } else {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.JMX, distributionConfig.getJMXManagerSSLAlias());
    }
  }

  public static SocketCreator getHTTPServiceSSLSocketCreator() {
    DistributionConfig distributionConfig = getInstance().distributionConfig;
    if (distributionConfig.getSSLEnabledComponents().length == 0) {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.HTTP_SERVICE, null, distributionConfig.getHttpServiceSSLEnabled(), distributionConfig
        .getHttpServiceSSLRequireAuthentication(), createStringArrayFromString(distributionConfig.getHttpServiceSSLProtocols()), createStringArrayFromString(distributionConfig
        .getHttpServiceSSLCiphers()), distributionConfig.getHttpServiceSSLProperties());
    } else {
      return getInstance().getOrCreateSocketCreatorForSSLEnabledComponent(SSLEnabledComponents.HTTP_SERVICE, distributionConfig.getHTTPServiceSSLAlias());
    }
  }

  private SocketCreator getSSLSocketCreator(String sslComponent,
                                            String alias,
                                            DistributionConfig distributionConfig,
                                            final boolean useSSL,
                                            final boolean needClientAuth,
                                            final String[] protocols,
                                            final String[] ciphers,
                                            final Properties props) {
    if (useSSL) {
      if (ArrayUtils.contains(distributionConfig.getSSLEnabledComponents(), SSLEnabledComponents.ALL)) {
        return createSSLSocketCreator(SSLEnabledComponents.ALL, alias, useSSL, needClientAuth, protocols, ciphers, props);
      } else if (distributionConfig.getSSLEnabledComponents().length == 0 || ArrayUtils.contains(distributionConfig.getSSLEnabledComponents(), sslComponent)) {
        return createSSLSocketCreator(sslComponent, alias, useSSL, needClientAuth, protocols, ciphers, props);
      }
    }
    return createSSLSocketCreator(NON_SSL, null, false, false, null, null, null);
  }


  private SocketCreator getOrCreateSocketCreatorForSSLEnabledComponent(String sslEnabledComponent, String alias) {
    return getOrCreateSocketCreatorForSSLEnabledComponent(sslEnabledComponent, alias, distributionConfig.getClusterSSLEnabled(), distributionConfig.getClusterSSLRequireAuthentication(), createStringArrayFromString(distributionConfig
      .getClusterSSLProtocols()), createStringArrayFromString(distributionConfig.getClusterSSLCiphers()), distributionConfig.getClusterSSLProperties());
  }

  private SocketCreator getOrCreateSocketCreatorForSSLEnabledComponent(String sslEnabledComponent,
                                                                       String alias,
                                                                       boolean useSSL,
                                                                       boolean needClientAuth,
                                                                       String[] protocols,
                                                                       String[] ciphers,
                                                                       Properties props) {
    SocketCreator socketCreator = getSocketCreatorForComponent(sslEnabledComponent);
    if (socketCreator == null) {
      return getSSLSocketCreator(sslEnabledComponent, alias, distributionConfig, useSSL, needClientAuth, protocols, ciphers, props);
    } else {
      return socketCreator;
    }
  }

  private SocketCreator createSSLSocketCreator(final String sslEnableComponent,
                                               final String alias,
                                               final boolean useSSL,
                                               final boolean needClientAuth,
                                               final String[] protocols,
                                               final String[] ciphers,
                                               final Properties props) {
    SocketCreator socketCreator = null;
    if (useSSL) {
      socketCreator = new SocketCreator(useSSL, needClientAuth, protocols, ciphers, props, alias);
      addSocketCreatorForComponent(sslEnableComponent, socketCreator);
    } else {
      socketCreator = new SocketCreator();
      addSocketCreatorForComponent(NON_SSL, socketCreator);
    }
    return socketCreator;
  }

  private synchronized void addSocketCreatorForComponent(String sslEnabledComponent, SocketCreator socketCreator) {
    socketCreators.put(sslEnabledComponent, socketCreator);
  }

  private synchronized SocketCreator getSocketCreatorForComponent(String sslEnabledComponent) {
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
   * @return
   *
   * @deprecated as of Geode 1.0
   */
  @Deprecated
  public static SocketCreator createNonDefaultInstance(final boolean useSSL,
                                                       final boolean needClientAuth,
                                                       final String protocols,
                                                       final String ciphers,
                                                       final Properties gfsecurityProps) {
    return new SocketCreator(useSSL, needClientAuth, createStringArrayFromString(protocols), createStringArrayFromString(ciphers), gfsecurityProps, null);
  }

  public static void close() {
    getInstance().clearSocketCreators();
    getInstance().distributionConfig = null;
    instance = null;
  }

  private synchronized void clearSocketCreators() {
    getInstance().socketCreators.clear();
  }

  public static SocketCreatorFactory setDistributionConfig(final DistributionConfig config) {
    getInstance().initializeSocketCreators(config);
    return getInstance();
  }
}
