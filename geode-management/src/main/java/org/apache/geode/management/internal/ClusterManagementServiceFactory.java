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

/**
 * A Server Provider interface which should be implemented to create instances of {@link
 * ClusterManagementService} for a given context.
 */
public interface ClusterManagementServiceFactory {

  /**
   * Returns the context of the factory
   */
  String getContext();

  // plain java client side
  ClusterManagementService create(String host, int port, boolean useSSL, String username,
      String password);

  ClusterManagementService create(String host, int port, SSLContext sslContext,
      HostnameVerifier hostnameVerifier, String username, String password);

  ClusterManagementService create(ClientHttpRequestFactory requestFactory);

  // geode server side, we can infer host and port and useSSL properties from the server side
  ClusterManagementService create();

  ClusterManagementService create(String username, String password);
}
