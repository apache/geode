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

import static com.palantir.docker.compose.execution.DockerComposeExecArgument.arguments;
import static com.palantir.docker.compose.execution.DockerComposeExecOption.options;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import com.palantir.docker.compose.DockerComposeRule;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;

/**
 * These tests run against a 2-server, 1-locator Geode cluster. The servers and locator run inside
 * a (single) Docker container and are not route-able from the host (where this JUnit test is
 * running). Another Docker container is running the HAProxy image and it's set up as an SNI
 * gateway. The test connects to the gateway via SNI and the gateway (in one Docker container)
 * forwards traffic to Geode members (running in the other Docker container).
 *
 * The two servers, server-dolores, and server-clementine, each are members of their own distinct
 * groups: group-dolores, and group-clementine, respectively. Also each server has a separate
 * REPLICATE region on it: region-dolores, and region-clementine, respectively.
 *
 * This test creates a connection pool to each group in turn. For that group, the test verifies
 * it can update data to the region of interest. There's also a pair of negative tests that verify
 * the correct exception is thrown when an attempt is made to operate on an unreachable region.
 */
public class DualServerSNIAcceptanceTest {

  private static final URL DOCKER_COMPOSE_PATH =
      SingleServerSNIAcceptanceTest.class.getResource("docker-compose.yml");

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static NotOnWindowsDockerRule docker =
      new NotOnWindowsDockerRule(() -> DockerComposeRule.builder()
          .file(DOCKER_COMPOSE_PATH.getPath()).build());

  private static Properties clientCacheProperties;
  private ClientCache cache;

  @BeforeClass
  public static void beforeClass() throws IOException, InterruptedException {
    docker.get().exec(options("-T"), "geode",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter-2.gfsh"));

    final String trustStorePath =
        createTempFileFromResource(SingleServerSNIAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();

    clientCacheProperties = new Properties();
    clientCacheProperties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    clientCacheProperties.setProperty(SSL_KEYSTORE_TYPE, "jks");
    clientCacheProperties.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    clientCacheProperties.setProperty(SSL_TRUSTSTORE, trustStorePath);
    clientCacheProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    clientCacheProperties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
  }

  @After
  public void after() {
    ensureCacheClosed();
  }

  @Test
  public void successfulRoutingTest() {
    verifyPutAndGet("group-dolores", "region-dolores");
  }

  @Test
  public void successfulRoutingTest2() {
    verifyPutAndGet("group-clementine", "region-clementine");
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
    final int proxyPort = docker.get().containers()
        .container("haproxy")
        .port(15443)
        .getExternalPort();
    ensureCacheClosed();
    cache = new ClientCacheFactory(clientCacheProperties)
        .addPoolLocator("locator-maeve", 10334)
        .setPoolServerGroup(groupName)
        .setPoolSocketFactory(ProxySocketFactories.sni("localhost",
            proxyPort))
        .create();
    return cache.<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(regionName);
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
