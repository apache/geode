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

package org.apache.geode.redis;

import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.NotSslRecordException;
import org.junit.Rule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.net.filewatch.PollingFileWatcher;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class SSLDUnitTest {

  @Rule
  public RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  private final static String commonPassword = "password";
  private final static SANCapturingHostnameVerifier hostnameVerifier =
      new SANCapturingHostnameVerifier();

  private int redisPort;
  private CertificateMaterial ca;
  private String serverKeyStoreFilename;
  private String serverTrustStoreFilename;

  private void setup() throws Exception {
    setup(p -> {});
  }

  private void setup(Consumer<Properties> propertyConfigurer) throws Exception {
    ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("server")
        .issuedBy(ca)
        .generate();

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCertificate);
    serverStore.trust("ca", ca);
    Properties serverProperties = serverStore.propertiesWith("all", true, false);

    propertyConfigurer.accept(serverProperties);

    MemberVM server = cluster.startRedisVM(0, x -> x.withProperties(serverProperties));

    redisPort = cluster.getRedisPort(server);
    serverKeyStoreFilename = serverProperties.getProperty("ssl-keystore");
    serverTrustStoreFilename = serverProperties.getProperty("ssl-truststore");
  }

  @Test
  public void givenMutualAuthentication_clientCanConnect() throws Exception {
    setup();

    try (Jedis jedis = createClient(true, false)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void givenServerHasCipherAndProtocol_clientCanConnect() throws Exception {
    SSLContext ssl = SSLContext.getInstance("TLSv1.2");
    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getServerSocketFactory().getSupportedCipherSuites();
    String rsaCipher = Arrays.stream(cipherSuites).filter(c -> c.contains("RSA")).findFirst().get();

    setup(p -> {
      p.setProperty("ssl-protocols", "TLSv1.2");
      p.setProperty("ssl-ciphers", rsaCipher);
    });

    try (Jedis jedis = createClient(true, false)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void givenMutualAuthentication_clientErrorsWithoutKeystore() throws Exception {
    setup();

    IgnoredException.addIgnoredException(SSLHandshakeException.class);
    IgnoredException.addIgnoredException("SunCertPathBuilderException");

    // Sometimes the client is created successfully - perhaps this is platform/JDK specific
    assertThatThrownBy(() -> {
      Jedis jedis = createClient(false, false);
      jedis.ping();
    }).satisfiesAnyOf(
        e -> assertThat(e).isInstanceOf(JedisConnectionException.class),
        e -> assertThat(e.getMessage()).contains("SocketException"),
        e -> assertThat(e.getMessage()).contains("SSLException"),
        e -> assertThat(e.getMessage()).contains("SSLHandshakeException"));

    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void givenMutualAuthentication_clientErrorsWithSelfSignedCert() throws Exception {
    setup();

    IgnoredException.addIgnoredException(SSLHandshakeException.class);
    IgnoredException.addIgnoredException("SunCertPathBuilderException");

    // Sometimes the client is created successfully - perhaps this is platform/JDK specific
    assertThatThrownBy(() -> {
      Jedis jedis = createClient(true, true);
      jedis.ping();
    }).satisfiesAnyOf(
        e -> assertThat(e).isInstanceOf(JedisConnectionException.class),
        e -> assertThat(e.getMessage()).contains("SocketException"),
        e -> assertThat(e.getMessage()).contains("SSLException"),
        e -> assertThat(e.getMessage()).contains("SSLHandshakeException"));

    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void givenSslEnabled_clientErrors_whenUsingCleartext() throws Exception {
    setup();

    IgnoredException.addIgnoredException(NotSslRecordException.class);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, redisPort)) {
      assertThatThrownBy(jedis::ping)
          .isInstanceOf(JedisConnectionException.class);
    }

    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void givenServerCertificateIsRotated_clientCanStillConnect() throws Exception {
    setup();

    String newServerName = "updated-server";

    try (Jedis jedis = createClient(true, false)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }

    // create a new certificate for the server
    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName(newServerName)
        .issuedBy(ca)
        .sanDnsName(newServerName)
        .generate();

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCertificate);
    serverStore.trust("ca", ca);

    // Wait for one second since file timestamp granularity may only be seconds depending on the
    // platform.
    Thread.sleep(1000);
    serverStore.createKeyStore(serverKeyStoreFilename, commonPassword);

    // Try long enough for the file change to be detected
    GeodeAwaitility.await().atMost(Duration.ofSeconds(PollingFileWatcher.PERIOD_SECONDS * 3))
        .untilAsserted(() -> {
          try (Jedis jedis = createClient(true, false)) {
            jedis.ping();
            assertThat(hostnameVerifier.getSubjectAltNames()).contains(newServerName);
          }
        });
  }

  @Test
  public void givenServerCAandKeyIsRotated_clientCannotConnect() throws Exception {
    setup();

    try (Jedis jedis = createClient(true, false)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }

    CertificateMaterial newCA = new CertificateBuilder()
        .commonName("New Test CA")
        .isCA()
        .generate();

    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("server")
        .issuedBy(newCA)
        .generate();

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCertificate);
    serverStore.trust("ca", newCA);

    // Wait for one second since file timestamp granularity may only be seconds depending on the
    // platform.
    Thread.sleep(1000);
    serverStore.createKeyStore(serverKeyStoreFilename, commonPassword);
    serverStore.createTrustStore(serverTrustStoreFilename, commonPassword);

    IgnoredException.addIgnoredException(SSLHandshakeException.class);
    IgnoredException.addIgnoredException("SunCertPathBuilderException");

    // Try long enough for the file change to be detected
    GeodeAwaitility.await().atMost(Duration.ofSeconds(PollingFileWatcher.PERIOD_SECONDS * 3))
        .untilAsserted(() -> assertThatThrownBy(() -> createClient(true, false))
              .isInstanceOf(JedisConnectionException.class));

    IgnoredException.removeAllExpectedExceptions();
  }

  private Jedis createClient(boolean mutualAuthentication, boolean isSelfSigned) throws Exception {
    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("redis-client")
        .issuedBy(isSelfSigned ? null : ca)
        .generate();

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate("redis-client", clientCertificate);
    clientStore.trust("ca", ca);

    Properties clientProperties = clientStore.propertiesWith("all");

    KeyManager[] keyManagers = null;

    if (mutualAuthentication) {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      keyStore.load(new FileInputStream(clientProperties.getProperty("ssl-keystore")),
          commonPassword.toCharArray());

      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, commonPassword.toCharArray());
      keyManagers = keyManagerFactory.getKeyManagers();
    }

    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(new FileInputStream(clientProperties.getProperty("ssl-truststore")),
        commonPassword.toCharArray());

    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);

    return new Jedis(BIND_ADDRESS, redisPort, true, sslContext.getSocketFactory(),
        sslContext.getSupportedSSLParameters(), hostnameVerifier);
  }

  /**
   * {@link HostnameVerifier} that captures the Subject Alternative Names (SANs).
   */
  private static class SANCapturingHostnameVerifier implements HostnameVerifier {

    private final List<String> subjectAltNames = new ArrayList<>();

    @Override
    public boolean verify(String s, SSLSession sslSession) {
      subjectAltNames.clear();

      try {
        Certificate[] certs = sslSession.getPeerCertificates();
        X509Certificate x509 = (X509Certificate) certs[0];
        Collection<List<?>> entries = x509.getSubjectAlternativeNames();

        for (List<?> entry : entries) {
          if (entry.size() > 1) {
            subjectAltNames.add((String) entry.get(1));
          }
        }
      } catch (Exception ignored) {
      }

      return true;
    }

    public List<String> getSubjectAltNames() {
      return subjectAltNames;
    }
  }

}
