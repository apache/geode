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
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URL;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.rules.DockerComposeRule;

/**
 * These tests run against a 2-server, 1-locator Geode cluster. The servers and locator run inside a
 * (single) Docker container and are not route-able from the host (where this JUnit test is
 * running). Another Docker container is running the HAProxy image and it's set up as an SNI
 * gateway. The test connects to the gateway via SNI and the gateway (in one Docker container)
 * forwards traffic to Geode members (running in the other Docker container).
 *
 * The two servers, server-dolores, and server-clementine, each are members of their own distinct
 * groups: group-dolores, and group-clementine, respectively. Also each server has a separate
 * REPLICATE region on it: region-dolores, and region-clementine, respectively.
 *
 * This test creates a connection pool to each group in turn. For that group, the test verifies it
 * can update data to the region of interest. There's also a pair of negative tests that verify the
 * correct exception is thrown when an attempt is made to operate on an unreachable region.
 */
public class DualServerSNIAcceptanceTest {

  private static final Logger logger = LogService.getLogger();

  private static final URL DOCKER_COMPOSE_PATH =
      DualServerSNIAcceptanceTest.class.getResource("dual-server-docker-compose.yml");

  @ClassRule
  public static DockerComposeRule docker = new DockerComposeRule.Builder()
      .file(DOCKER_COMPOSE_PATH.getPath())
      .service("haproxy", 15443)
      .build();

  private static Properties clientCacheProperties;
  private ClientCache cache;

  @BeforeClass
  public static void beforeClass() {
    logger.info("[DIAGNOSTIC] Setting up Docker container names...");
    docker.setContainerName("locator-maeve", "locator-maeve");
    docker.setContainerName("server-dolores", "server-dolores");
    docker.setContainerName("server-clementine", "server-clementine");
    logger.info("[DIAGNOSTIC] Container names set successfully");

    logger.info("[DIAGNOSTIC] Starting locator-maeve...");
    docker.loggingExecForService("locator-maeve",
        "gfsh", "run", "--file=/geode/scripts/locator-maeve.gfsh");
    logger.info("[DIAGNOSTIC] Locator-maeve started successfully");

    logger.info("[DIAGNOSTIC] Starting server-dolores...");
    docker.loggingExecForService("server-dolores",
        "gfsh", "run", "--file=/geode/scripts/server-dolores.gfsh");
    logger.info("[DIAGNOSTIC] Server-dolores started successfully");

    logger.info("[DIAGNOSTIC] Starting server-clementine...");
    docker.loggingExecForService("server-clementine",
        "gfsh", "run", "--file=/geode/scripts/server-clementine.gfsh");
    logger.info("[DIAGNOSTIC] Server-clementine started successfully");

    logger.info("[DIAGNOSTIC] Creating regions...");
    docker.loggingExecForService("locator-maeve",
        "gfsh", "run", "--file=/geode/scripts/create-regions.gfsh");
    logger.info("[DIAGNOSTIC] Regions created successfully");

    logger.info("[DIAGNOSTIC] Setting up SSL properties...");
    final String trustStorePath =
        createTempFileFromResource(SingleServerSNIAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();
    logger.info("[DIAGNOSTIC] Trust store path: " + trustStorePath);

    clientCacheProperties = new Properties();
    clientCacheProperties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    clientCacheProperties.setProperty(SSL_KEYSTORE_TYPE, "jks");
    clientCacheProperties.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    clientCacheProperties.setProperty(SSL_TRUSTSTORE, trustStorePath);
    clientCacheProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    clientCacheProperties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    logger.info("[DIAGNOSTIC] SSL properties configured successfully");
  }

  @After
  public void after() {
    ensureCacheClosed();
  }

  @AfterClass
  public static void afterClass() {
    // if you need to capture logs for one of the processes use this pattern:
    // String output =
    // docker.execForService("locator-maeve", "cat", "locator-maeve/locator-maeve.log");
    // System.out.println("Locator log file--------------------------------\n" + output);
  }

  @Test
  public void successfulRoutingTest() {
    logger.info("[DIAGNOSTIC] Starting successfulRoutingTest");
    logger.info("[DIAGNOSTIC] Testing connection to group-dolores, region-dolores");
    try {
      verifyPutAndGet("group-dolores", "region-dolores");
      logger.info("[DIAGNOSTIC] successfulRoutingTest completed successfully");
    } catch (Exception e) {
      logger.error("[DIAGNOSTIC ERROR] successfulRoutingTest failed: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void successfulRoutingTest2() {
    logger.info("[DIAGNOSTIC] Starting successfulRoutingTest2");
    logger.info("[DIAGNOSTIC] Testing connection to group-clementine, region-clementine");
    try {
      verifyPutAndGet("group-clementine", "region-clementine");
      logger.info("[DIAGNOSTIC] successfulRoutingTest2 completed successfully");
    } catch (Exception e) {
      logger.error("[DIAGNOSTIC ERROR] successfulRoutingTest2 failed: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  @Test
  public void unreachabilityTest() {
    verifyUnreachable("group-dolores", "region-clementine");
  }

  @Test
  public void unreachabilityTest2() {
    verifyUnreachable("group-clementine", "region-dolores");
  }

  private void verifyUnreachable(final String groupName, final String regionName) {
    final Region<String, String> region = getRegion(groupName, regionName);
    assertThatThrownBy(() -> region.destroy("hello"))
        .hasCauseInstanceOf(RegionDestroyedException.class)
        .hasStackTraceContaining("was not found during destroy request");
  }

  private void verifyPutAndGet(final String groupName, final String regionName) {
    final Region<String, String> region = getRegion(groupName, regionName);
    region.destroy("hello");
    region.put("hello", "world");
    assertThat(region.get("hello")).isEqualTo("world");
  }

  /**
   * modifies cache field as a side-effect
   */
  private Region<String, String> getRegion(final String groupName, final String regionName) {
    logger.info("[DIAGNOSTIC] Getting region - Group: " + groupName + ", Region: " + regionName);

    final int proxyPort = docker.getExternalPortForService("haproxy", 15443);
    logger.info("[DIAGNOSTIC] HAProxy proxy port: " + proxyPort);

    ensureCacheClosed();
    logger.info("[DIAGNOSTIC] Cache closed, creating new client cache...");

    try {
      cache = new ClientCacheFactory(clientCacheProperties)
          .addPoolLocator("locator-maeve", 10334)
          .setPoolServerGroup(groupName)
          .setPoolSocketFactory(ProxySocketFactories.sni("localhost", proxyPort))
          .create();
      logger.info("[DIAGNOSTIC] Client cache created successfully");

      Region<String, String> region =
          cache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
              .create(regionName);
      logger.info("[DIAGNOSTIC] Client region created successfully");

      return region;
    } catch (Exception e) {
      System.err.println("[DIAGNOSTIC ERROR] Failed to create cache/region: " + e.getMessage());
      System.err.println("[DIAGNOSTIC ERROR] Group: " + groupName + ", Region: " + regionName
          + ", ProxyPort: " + proxyPort);
      e.printStackTrace();
      throw e;
    }
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
