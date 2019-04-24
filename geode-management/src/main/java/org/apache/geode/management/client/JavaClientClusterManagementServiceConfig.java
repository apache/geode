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

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import org.apache.geode.management.api.ClusterManagementServiceConfig;
import org.apache.geode.management.internal.RestTemplateResponseErrorHandler;

public class JavaClientClusterManagementServiceConfig implements ClusterManagementServiceConfig {

  private static final ResponseErrorHandler DEFAULT_ERROR_HANDLER =
      new RestTemplateResponseErrorHandler();

  private final RestTemplate restTemplate;

  public interface GenericBuilder {
    GenericBuilder setHost(String host);

    GenericBuilder setPort(int port);

    GenericBuilder setSslContext(SSLContext sslContext);

    GenericBuilder setHostnameVerifier(HostnameVerifier hostnameVerifier);

    GenericBuilder setUsername(String username);

    GenericBuilder setPassword(String password);

    ClusterManagementServiceConfig build();
  }

  public interface RequestFactoryBuilder {
    RequestFactoryBuilder setRequestFactory(ClientHttpRequestFactory requestFactory);

    ClusterManagementServiceConfig build();
  }

  public static class Builder implements GenericBuilder, RequestFactoryBuilder {
    private String host;
    private int port;
    private SSLContext sslContext;
    private HostnameVerifier hostnameVerifier;
    private String username;
    private String password;
    private ClientHttpRequestFactory requestFactory;

    private Builder() {}

    @Override
    public GenericBuilder setHost(String host) {
      this.host = host;
      return this;
    }

    @Override
    public GenericBuilder setPort(int port) {
      this.port = port;
      return this;
    }

    @Override
    public GenericBuilder setSslContext(SSLContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    @Override
    public GenericBuilder setHostnameVerifier(HostnameVerifier hostnameVerifier) {
      this.hostnameVerifier = hostnameVerifier;
      return this;
    }

    @Override
    public GenericBuilder setUsername(String username) {
      this.username = username;
      return this;
    }

    @Override
    public GenericBuilder setPassword(String password) {
      this.password = password;
      return this;
    }

    @Override
    public RequestFactoryBuilder setRequestFactory(ClientHttpRequestFactory requestFactory) {
      this.requestFactory = requestFactory;
      return this;
    }

    @Override
    public ClusterManagementServiceConfig build() {
      RestTemplate restTemplate = new RestTemplate();
      restTemplate.setErrorHandler(DEFAULT_ERROR_HANDLER);

      if (requestFactory != null) {
        restTemplate.setRequestFactory(requestFactory);
      } else {
        DefaultUriTemplateHandler templateHandler = new DefaultUriTemplateHandler();
        String schema = (sslContext == null) ? "http" : "https";
        templateHandler.setBaseUrl(schema + "://" + host + ":" + port + "/geode-management");
        restTemplate.setUriTemplateHandler(templateHandler);

        // HttpComponentsClientHttpRequestFactory allows use to preconfigure httpClient for
        // authentication and ssl context
        HttpComponentsClientHttpRequestFactory requestFactory =
            new HttpComponentsClientHttpRequestFactory();

        HttpClientBuilder clientBuilder = HttpClientBuilder.create();
        // configures the clientBuilder
        if (username != null) {
          CredentialsProvider credsProvider = new BasicCredentialsProvider();
          credsProvider.setCredentials(new AuthScope(host, port),
              new UsernamePasswordCredentials(username, password));
          clientBuilder.setDefaultCredentialsProvider(credsProvider);
        }

        clientBuilder.setSSLContext(sslContext);
        clientBuilder.setSSLHostnameVerifier(hostnameVerifier);

        requestFactory.setHttpClient(clientBuilder.build());
        restTemplate.setRequestFactory(requestFactory);
      }

      return new JavaClientClusterManagementServiceConfig(restTemplate);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private JavaClientClusterManagementServiceConfig(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @Override
  public RestTemplate getRestTemplate() {
    return restTemplate;
  }
}
