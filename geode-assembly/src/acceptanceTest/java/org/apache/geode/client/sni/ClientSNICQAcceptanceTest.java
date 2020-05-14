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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.palantir.docker.compose.DockerComposeRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.proxy.ProxySocketFactories;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class ClientSNICQAcceptanceTest {

  private static final URL DOCKER_COMPOSE_PATH =
      ClientSNICQAcceptanceTest.class.getResource("docker-compose.yml");

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static TestRule ignoreOnWindowsRule = new IgnoreOnWindowsRule();

  @ClassRule
  public static NotOnWindowsDockerRule docker =
      new NotOnWindowsDockerRule(() -> DockerComposeRule.builder()
          .file(DOCKER_COMPOSE_PATH.getPath()).build());

  private CqQuery cqTracker;

  AtomicInteger eventCreateCounter = new AtomicInteger(0);
  AtomicInteger eventUpdateCounter = new AtomicInteger(0);
  private ClientCache cache;
  private Region<String, Integer> region;

  class SNICQListener implements CqListener {


    @Override
    public void onEvent(CqEvent cqEvent) {
      Operation queryOperation = cqEvent.getQueryOperation();


      if (queryOperation.isUpdate()) {
        eventUpdateCounter.incrementAndGet();
      } else if (queryOperation.isCreate()) {
        eventCreateCounter.incrementAndGet();
      }
    }

    @Override
    public void onError(CqEvent aCqEvent) {
      System.out.println("We had an ERROR....");
    }
  }

  private static String trustStorePath;

  @BeforeClass
  public static void beforeClass() throws IOException, InterruptedException {
    trustStorePath =
        createTempFileFromResource(ClientSNICQAcceptanceTest.class,
            "geode-config/truststore.jks")
                .getAbsolutePath();
    docker.get().exec(options("-T"), "geode",
        arguments("gfsh", "run", "--file=/geode/scripts/geode-starter.gfsh"));

  }

  @AfterClass
  public static void afterClass() throws Exception {
    String output =
        docker.get().exec(options("-T"), "geode",
            arguments("cat", "server-dolores/server-dolores.log"));
    System.out.println("Server log file--------------------------------\n" + output);
  }

  @Before
  public void before() throws Exception {
    Properties gemFireProps = new Properties();
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "all");
    gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");

    gemFireProps.setProperty(SSL_TRUSTSTORE, trustStorePath);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");
    gemFireProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");

    int proxyPort = docker.get().containers()
        .container("haproxy")
        .port(15443)
        .getExternalPort();
    cache = new ClientCacheFactory(gemFireProps)
        .addPoolLocator("locator-maeve", 10334)
        .setPoolSocketFactory(ProxySocketFactories.sni("localhost",
            proxyPort))
        .setPoolSubscriptionEnabled(true)
        .create();
    region = cache.<String, Integer>createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("jellyfish");
  }

  @After
  public void after() throws Exception {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void performSimpleCQOverSNIProxy() throws Exception {
    startCQ(region);
    populateRegion(region);
    assertThat(region.get("key0")).isEqualTo(0);
    assertThat(region.get("key1")).isEqualTo(1);
    assertThat(region.get("key2")).isEqualTo(2);
    assertThat(region.get("key99")).isEqualTo(99);

    await().untilAsserted(() -> assertThat(eventCreateCounter.get()).isEqualTo(62));

    updateRegion(region);
    assertThat(region.get("key0")).isEqualTo(10);
    assertThat(region.get("key1")).isEqualTo(11);
    assertThat(region.get("key2")).isEqualTo(12);
    assertThat(region.get("key99")).isEqualTo(109);

    await().untilAsserted(() -> assertThat(eventUpdateCounter.get()).isEqualTo(62));

    // verify that the server cleans up when the client connection to the gateway is destroyed
    ((PoolImpl) cache.getDefaultPool()).killPrimaryEndpoint();
    // since we can't run code in the server let's grab the CQ statistics and verify that
    // the CQ has been closed. StatArchiveReader has a main() that we can use to get a printout
    // of stat values
    await().untilAsserted(() -> {
      String stats = docker.get().exec(options("-T"), "geode",
          arguments("java", "-cp", "/geode/lib/geode-dependencies.jar",
              "org.apache.geode.internal.statistics.StatArchiveReader",
              "stat", "server-dolores/statArchive.gfs", "CqServiceStats.numCqsClosed"));
      // the stat should transition from zero to one at some point
      assertThat(stats).contains("0.0 1.0");
    });

  }

  public void updateRegion(Region<String, Integer> region) {
    for (Integer i = 0; i < 100; ++i) {
      String key = "key" + i;
      region.put(key, (i + 10));
    }
  }

  public void populateRegion(Region<String, Integer> region) {
    for (Integer i = 0; i < 100; ++i) {
      String key = "key" + i;
      region.put(key, i);
    }
  }

  public void startCQ(Region<String, Integer> region)
      throws CqExistsException, CqException, RegionNotFoundException {
    CqAttributesFactory cqf = new CqAttributesFactory();
    cqf.addCqListener(new SNICQListener());
    CqAttributes cqa = cqf.create();

    String cqName = "jellyTracker";

    String queryStr = "SELECT * FROM /jellyfish i where i > 37";

    QueryService queryService = region.getRegionService().getQueryService();
    cqTracker = queryService.newCq(cqName, queryStr, cqa);
    cqTracker.execute();
  }

}
