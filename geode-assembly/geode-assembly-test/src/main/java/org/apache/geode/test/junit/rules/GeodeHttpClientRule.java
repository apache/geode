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
package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.client5.http.impl.DefaultRedirectStrategy;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.net.URIBuilder;
import org.junit.rules.ExternalResource;

import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

/**
 * this rules simplifies creating a httpClient for verification of pulse behaviors or any http
 * client behaviors. Usually after you start up a server/locator with http service, you would want
 * to connect to it through http client and verify some behavior, you would need to use this rule.
 *
 * It uses one httpClient object for the duration of a test.
 *
 * <p>
 * See {@code PulseSecurityTest} for examples
 */
public class GeodeHttpClientRule extends ExternalResource {

  private final String hostName;
  private final Supplier<Integer> portSupplier;
  private HttpHost host;
  private HttpClient httpClient;
  private HttpClient httpClientNoRedirect;
  private boolean useSSL;
  // Jakarta EE migration: Shared context to maintain session cookies across requests
  private HttpClientContext sharedContext;

  public GeodeHttpClientRule(String hostName, Supplier<Integer> portSupplier) {
    this.hostName = hostName;
    this.portSupplier = portSupplier;
  }

  public GeodeHttpClientRule(Supplier<Integer> portSupplier) {
    this("localhost", portSupplier);
  }

  public GeodeHttpClientRule withSSL() {
    useSSL = true;
    return this;
  }

  public ClassicHttpResponse loginToPulse(String username, String password) throws Exception {
    connect();
    // Jakarta EE migration: Use non-redirecting client for login to verify 302 response
    return postNoRedirect("/pulse/login", "username", username, "password", password);
  }

  public void loginToPulseAndVerify(String username, String password) throws Exception {
    ClassicHttpResponse response = loginToPulse(username, password);
    // HttpClient 5.x: getStatusLine() replaced with getCode() and getReasonPhrase()
    assertThat(response.getCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/clusterDetail.html");
  }

  public ClassicHttpResponse logoutFromPulse() throws Exception {
    return get("/pulse/clusterLogout");
  }

  public ClassicHttpResponse get(String uri, String... params) throws Exception {
    connect();
    // Jakarta EE migration: Use shared context to preserve session cookies
    if (sharedContext == null) {
      sharedContext = HttpClientContext.create();
    }
    return (ClassicHttpResponse) httpClient.execute(host, buildHttpGet(uri, params), sharedContext);
  }

  public ClassicHttpResponse post(String uri, String... params) throws Exception {
    connect();
    // Jakarta EE migration: Use shared context to preserve session cookies
    if (sharedContext == null) {
      sharedContext = HttpClientContext.create();
    }
    return (ClassicHttpResponse) httpClient.execute(host, buildHttpPost(uri, params),
        sharedContext);
  }

  private ClassicHttpResponse postNoRedirect(String uri, String... params) throws Exception {
    connect();
    // Jakarta EE migration: Use shared context to preserve session cookies
    if (sharedContext == null) {
      sharedContext = HttpClientContext.create();
    }
    return (ClassicHttpResponse) httpClientNoRedirect.execute(host, buildHttpPost(uri, params),
        sharedContext);
  }

  private void connect() {
    if (httpClient != null) {
      return;
    }

    // Jakarta EE migration: Create lenient redirect strategy for URIs with placeholders
    // Apache HttpComponents 5 is stricter about URI validation than version 4
    // This allows Spring Security OAuth2 redirect URIs with property placeholders
    DefaultRedirectStrategy lenientRedirectStrategy = new DefaultRedirectStrategy() {
      @Override
      public boolean isRedirected(
          org.apache.hc.core5.http.HttpRequest request,
          org.apache.hc.core5.http.HttpResponse response,
          org.apache.hc.core5.http.protocol.HttpContext context)
          throws ProtocolException {
        // Check if this would be a redirect
        if (!super.isRedirected(request, response, context)) {
          return false;
        }
        // Check if the Location header contains property placeholders
        String location = response.getFirstHeader("Location").getValue();
        if (location != null && location.contains("${")) {
          // Don't follow redirects with unresolved property placeholders
          return false;
        }
        return true;
      }
    };

    host = new HttpHost(useSSL ? "https" : "http", hostName, portSupplier.get());
    if (useSSL) {
      HttpClientBuilder clientBuilder = HttpClients.custom();
      SSLContext ctx = SocketCreatorFactory
          .getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).getSslContext();
      // HttpClient 5.x: SSL configuration via connection manager
      HttpClientConnectionManager connectionManager =
          PoolingHttpClientConnectionManagerBuilder.create()
              .setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                  .setSslContext(ctx)
                  .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                  .build())
              .build();
      clientBuilder.setConnectionManager(connectionManager);
      // Jakarta EE migration: Create client with lenient redirect strategy
      clientBuilder.setRedirectStrategy(lenientRedirectStrategy);
      httpClient = clientBuilder.build();
      // Jakarta EE migration: Create client without redirect handling for login verification
      httpClientNoRedirect = HttpClients.custom()
          .setConnectionManager(connectionManager)
          .disableRedirectHandling()
          .build();
    } else {
      // Jakarta EE migration: Create client with lenient redirect strategy
      httpClient = HttpClients.custom()
          .setRedirectStrategy(lenientRedirectStrategy)
          .build();
      // Jakarta EE migration: Create client without redirect handling for login verification
      httpClientNoRedirect = HttpClients.custom()
          .disableRedirectHandling()
          .build();
    }
  }

  private HttpPost buildHttpPost(String uri, String... params) throws Exception {
    HttpPost post = new HttpPost(uri);
    List<NameValuePair> nvps = new ArrayList<>();
    for (int i = 0; i < params.length; i += 2) {
      nvps.add(new BasicNameValuePair(params[i], params[i + 1]));
    }
    post.setEntity(new UrlEncodedFormEntity(nvps));
    return post;
  }

  private HttpGet buildHttpGet(String uri, String... params) throws Exception {
    URIBuilder builder = new URIBuilder();
    builder.setPath(uri);
    for (int i = 0; i < params.length; i += 2) {
      builder.setParameter(params[i], params[i + 1]);
    }
    return new HttpGet(builder.build());
  }

}
