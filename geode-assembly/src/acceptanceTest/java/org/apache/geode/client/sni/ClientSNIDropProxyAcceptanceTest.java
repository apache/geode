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
package org.apache.geode.client.sni;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.NoAvailableLocatorsException;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.rules.DockerComposeRule;

public class ClientSNIDropProxyAcceptanceTest {

  private static final URL DOCKER_COMPOSE_PATH =
      ClientSNIDropProxyAcceptanceTest.class.getResource("docker-compose.yml");

  @ClassRule
  public static DockerComposeRule docker = new DockerComposeRule.Builder()
      .file(DOCKER_COMPOSE_PATH.getPath())
      .service("haproxy", 15443)
      .build();

  private ClientCache cache;

  private String trustStorePath;
  private int proxyPort;

  @Before
  public void before() throws IOException, InterruptedException {
    trustStorePath =
        createTempFileFromResource(ClientSNIDropProxyAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();
    docker.execForService("geode", "gfsh", "run", "--file=/geode/scripts/geode-starter.gfsh");
  }

  @After
  public void after() {
    ensureCacheClosed();
  }

  @Test
  public void performSimpleOperationsDropSNIProxy() {
    final Region<String, Integer> region = getRegion();

    region.put("Roy Hobbs", 9);
    assertThat(region.get("Roy Hobbs")).isEqualTo(9);

    pauseProxy();

    assertThatThrownBy(() -> region.get("Roy Hobbs"))
        .isInstanceOf(NoAvailableLocatorsException.class)
        .hasMessageContaining("Unable to connect to any locators in the list");


    unpauseProxy();

    await().untilAsserted(() -> assertThat(region.get("Roy Hobbs")).isEqualTo(9));

    region.put("Bennie Rodriquez", 30);
    assertThat(region.get("Bennie Rodriquez")).isEqualTo(30);

    region.put("Jake Taylor", 7);
    region.put("Crash Davis", 8);

    region.put("Ricky Vaughn", 99);
    region.put("Ebbie Calvin LaLoosh", 37);

  }

  private void pauseProxy() {
    docker.pauseService("haproxy");
  }

  private void unpauseProxy() {
    docker.unpauseService("haproxy");
  }

  public Region<String, Integer> getRegion() {
    Properties gemFireProps = new Properties();
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "all");
    gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    gemFireProps.setProperty(SSL_TRUSTSTORE, trustStorePath);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    gemFireProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    proxyPort = docker.getExternalPortForService("haproxy", 15443);

    ensureCacheClosed();

    cache = new ClientCacheFactory(gemFireProps)
        .addPoolLocator("locator-maeve", 10334)
        .setPoolSocketFactory(ProxySocketFactories.sni("localhost",
            proxyPort))
        .setPoolSubscriptionEnabled(true)
        .create();
    return cache.<String, Integer>createClientRegionFactory(
        ClientRegionShortcut.PROXY)
        .create("jellyfish");
  }

  /**
   * modifies cache field as a side-effect
   */
  private void ensureCacheClosed() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
  }

}
