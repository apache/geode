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

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.ConnectionConfig;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.internal.ClientClusterManagementService;

public abstract class BaseManagementServiceBuilder<T extends BaseManagementServiceBuilder<?>> {

  private ClusterManagementServiceTransport transport;

  private String username;
  private String password;
  private HostnameVerifier hostnameVerifier;
  private SSLContext sslContext;
  private String authToken;
  private Boolean followRedirects;

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

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  public T setTransport(ClusterManagementServiceTransport transport) {
    this.transport = transport;
    return self();
  }

  public T setAuthToken(String token) {
    authToken = token;
    return self();
  }

  public T setSslContext(SSLContext sslContext) {
    this.sslContext = sslContext;
    return self();
  }

  public T setUsername(String username) {
    this.username = username;
    return self();
  }

  public T setPassword(String password) {
    this.password = password;
    return self();
  }

  public T setHostnameVerifier(HostnameVerifier hostnameVerifier) {
    this.hostnameVerifier = hostnameVerifier;
    return self();
  }

  public T setFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
    return self();
  }

  protected abstract ConnectionConfig createConnectionConfig();

  protected ConnectionConfig configureConnectionConfig(ConnectionConfig newConnectionConfig) {
    if (authToken != null) {
      newConnectionConfig.setAuthToken(authToken);
    }
    if (followRedirects != null) {
      newConnectionConfig.setFollowRedirects(followRedirects);
    }
    if (hostnameVerifier != null) {
      newConnectionConfig.setHostnameVerifier(hostnameVerifier);
    }
    if (username != null) {
      newConnectionConfig.setUsername(username);
    }
    if (password != null) {
      newConnectionConfig.setPassword(password);
    }
    if (sslContext != null) {
      newConnectionConfig.setSslContext(sslContext);
    }
    return newConnectionConfig;
  }
}
