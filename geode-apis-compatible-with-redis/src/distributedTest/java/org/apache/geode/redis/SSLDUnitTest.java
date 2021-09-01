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
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.NotSslRecordException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public class SSLDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  private static int redisPort;
  private static CertificateMaterial ca;


  @BeforeClass
  public static void setupClass() throws Exception {
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

    MemberVM server = cluster.startRedisVM(0, x -> x.withProperties(serverProperties));

    redisPort = cluster.getRedisPort(server);
  }

  @Test
  public void givenMutualAuthentication_clientCanConnect() throws Exception {
    try (Jedis jedis = createClient(true, false)) {
      assertThat(jedis.ping()).isEqualTo("PONG");
    }
  }

  @Test
  public void givenMutualAuthentication_clientErrorsWithoutKeystore() throws Exception {
    IgnoredException.addIgnoredException(SSLHandshakeException.class);

    // Create the client without a keystore
    try (Jedis jedis = createClient(false, false)) {
      assertThatThrownBy(jedis::ping)
          .hasMessageContaining("readHandshakeRecord");
    }

    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void givenMutualAuthentication_clientErrorsWithSelfSignedCert() throws Exception {
    IgnoredException.addIgnoredException(SSLHandshakeException.class);

    // Create the client with a self-signed certificate
    try (Jedis jedis = createClient(true, true)) {
      assertThatThrownBy(jedis::ping)
          .hasMessageContaining("readHandshakeRecord");
    }

    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void givenSslEnabled_clientErrors_whenUsingCleartext() {
    IgnoredException.addIgnoredException(NotSslRecordException.class);

    try (Jedis jedis = new Jedis(BIND_ADDRESS, redisPort)) {
      assertThatThrownBy(jedis::ping)
          .isInstanceOf(JedisConnectionException.class);
    }

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

    // The generated password is the same everywhere
    char[] password = clientProperties.getProperty("ssl-keystore-password").toCharArray();

    KeyManager[] keyManagers = null;

    if (mutualAuthentication) {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      keyStore.load(new FileInputStream(clientProperties.getProperty("ssl-keystore")), password);

      KeyManagerFactory keyManagerFactory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, password);
      keyManagers = keyManagerFactory.getKeyManagers();
    }

    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(new FileInputStream(clientProperties.getProperty("ssl-truststore")), password);

    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);

    return new Jedis(BIND_ADDRESS, redisPort, true, sslContext.getSocketFactory(),
        sslContext.getSupportedSSLParameters(), null);
  }
}
