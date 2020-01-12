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

package org.apache.geode.management.client;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.springframework.http.client.ClientHttpRequestFactory;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.PlainClusterManagementServiceBuilder;
import org.apache.geode.management.internal.SpringClusterManagemnetServiceBuilder;

/**
 * this builder allows you to build a ClusterManagementService using host address or
 * an HttpRequestFactory
 */
@Experimental
public class ClusterManagementServiceBuilder {

  public static PlainBuilder buildWithHostAddress() {
    return new PlainClusterManagementServiceBuilder();
  }

  public static HttpRequestFactoryBuilder buildWithRequestFactory() {
    return new SpringClusterManagemnetServiceBuilder();
  }

  public interface Builder {
    ClusterManagementService build();
  }

  public interface PlainBuilder extends Builder {
    /**
     * points to the locator address and http-service-port
     */
    PlainBuilder setHostAddress(String host, int port);

    /**
     * if the web component is ssl-enabled, need to set an SSLContext to connect
     */
    PlainBuilder setSslContext(SSLContext context);

    PlainBuilder setHostnameVerifier(HostnameVerifier hostnameVerifier);

    /**
     * if the cluster has SecurityManager enabled, set the credentials to access the
     * ClusterManagementService
     */
    PlainBuilder setCredentials(String username, String password);

    /**
     * If security-manager is enabled and auth-token-enabled-components includes "management",
     * set the authToken to access the ClusterManagementService
     *
     * Note: if this is set, username/password will be ignored if set
     */
    PlainBuilder setAuthToken(String authToken);
  }

  public interface HttpRequestFactoryBuilder extends Builder {
    HttpRequestFactoryBuilder setRequestFactory(ClientHttpRequestFactory httpRequestFactory);
  }
}
