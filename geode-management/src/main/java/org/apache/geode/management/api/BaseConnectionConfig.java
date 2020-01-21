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

package org.apache.geode.management.api;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.NoopHostnameVerifier;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;

/**
 * Concrete implementation of {@link ConnectionConfig} which can be used
 * where the various connection properties should be set directly as opposed to being derived from
 * another context such as a {@code Cache}. For example
 * {@code GeodeClusterManagementServiceConnectionConfig}.
 *
 * @see ClusterManagementServiceBuilder
 */
@Experimental
public class BaseConnectionConfig
    implements ConnectionConfig {

  private final String host;
  private final int port;
  private String username;
  private String password;
  private HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();
  private SSLContext sslContext;
  private String authToken;
  private boolean followRedirects;

  public BaseConnectionConfig(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public String getAuthToken() {
    return authToken;
  }

  public BaseConnectionConfig setAuthToken(String authToken) {
    this.authToken = authToken;
    return this;
  }

  public BaseConnectionConfig setSslContext(SSLContext sslContext) {
    this.sslContext = sslContext;
    return this;
  }

  @Override
  public SSLContext getSslContext() {
    return sslContext;
  }

  public BaseConnectionConfig setUsername(String username) {
    this.username = username;
    return this;
  }

  @Override
  public String getUsername() {
    return username;
  }

  public BaseConnectionConfig setPassword(String password) {
    this.password = password;
    return this;
  }

  @Override
  public String getPassword() {
    return password;
  }

  public BaseConnectionConfig setHostnameVerifier(
      HostnameVerifier hostnameVerifier) {
    this.hostnameVerifier = hostnameVerifier;
    return this;
  }

  @Override
  public HostnameVerifier getHostnameVerifier() {
    return hostnameVerifier;
  }

  public BaseConnectionConfig setFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
    return this;
  }

  @Override
  public boolean getFollowRedirects() {
    return followRedirects;
  }
}
