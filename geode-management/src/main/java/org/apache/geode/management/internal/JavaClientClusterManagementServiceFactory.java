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

package org.apache.geode.management.internal;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.springframework.http.client.ClientHttpRequestFactory;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;

/**
 * A Service factory which will be used to create {@link ClusterManagementService}
 * instances from pure Java code - i.e. the Cluster Management endpoint URL <b>cannot</b> be
 * inferred from the implied runtime context but needs to be specifically configured using a given
 * URL or {@code ClientHttpRequestFactory}.
 */
public class JavaClientClusterManagementServiceFactory implements ClusterManagementServiceFactory {

  @Override
  public String getContext() {
    return ClusterManagementServiceProvider.JAVA_CLIENT_CONTEXT;
  }

  @Override
  public ClusterManagementService create(String host, int port, SSLContext sslContext,
      HostnameVerifier hostnameVerifier, String username,
      String password) {
    return new ClientClusterManagementService(host, port, sslContext, hostnameVerifier, username,
        password);
  }

  @Override
  public ClusterManagementService create(ClientHttpRequestFactory requestFactory) {
    return new ClientClusterManagementService(requestFactory);
  }

  @Override
  public ClusterManagementService create() {
    throw new IllegalArgumentException("must be invoked with a host and port");
  }

  @Override
  public ClusterManagementService create(String username, String password) {
    throw new IllegalArgumentException("must be invoked with a host and port");
  }
}
