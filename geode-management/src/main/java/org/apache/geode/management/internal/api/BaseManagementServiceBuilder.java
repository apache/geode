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
 *
 */
package org.apache.geode.management.internal.api;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.NoopHostnameVerifier;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.ConnectionConfig;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.internal.ClientClusterManagementService;

public abstract class BaseManagementServiceBuilder<T extends BaseManagementServiceBuilder> {

  private ClusterManagementServiceTransport transport;

  private String username = System.getProperty("geode.config.cms.connection.username");
  private String password = System.getProperty("geode.config.cms.connection.password");
  private HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();
  private SSLContext sslContext;
  private String authToken;
  private boolean followRedirects =
      Boolean.parseBoolean(System
          .getProperty("geode.config.cms.connection.follow-redirect", "false"));

  /**
   * Build a new {@link ClusterManagementService} instance
   *
   * @return a ClusterManagementService
   */
  public ClusterManagementService build() {
    return new ClientClusterManagementService(configureTransport());
  }

  private ClusterManagementServiceTransport configureTransport() {
    if (transport == null) {
      ConnectionConfig connectionConfig = createConnectionConfig();
      connectionConfig = configureConnectionConfig(connectionConfig);
      return new RestTemplateClusterManagementServiceTransport(connectionConfig);
    }
    return transport;
  }

  public T setTransport(ClusterManagementServiceTransport transport) {
    this.transport = transport;
    return (T) this;
  }

  public T setAuthToken(String token) {
    this.authToken = token;
    return (T) this;
  }

  public T setSslContext(SSLContext sslContext) {
    this.sslContext = sslContext;
    return (T) this;
  }

  public T setUsername(String username) {
    this.username = username;
    return (T) this;
  }

  public T setPassword(String password) {
    this.password = password;
    return (T) this;
  }

  public T setHostnameVerifier(HostnameVerifier hostnameVerifier) {
    this.hostnameVerifier = hostnameVerifier;
    return (T) this;
  }

  public T setFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
    return (T) this;
  }

  protected abstract ConnectionConfig createConnectionConfig();

  protected ConnectionConfig configureConnectionConfig(ConnectionConfig newConnectionConfig) {
    newConnectionConfig.setAuthToken(authToken);
    newConnectionConfig.setFollowRedirects(followRedirects);
    newConnectionConfig.setHostnameVerifier(hostnameVerifier);
    newConnectionConfig.setUsername(username);
    newConnectionConfig.setPassword(password);
    newConnectionConfig.setSslContext(sslContext);
    return newConnectionConfig;
  }
}
