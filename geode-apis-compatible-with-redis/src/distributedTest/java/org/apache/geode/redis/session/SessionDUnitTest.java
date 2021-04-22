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

package org.apache.geode.redis.session;


import java.net.HttpCookie;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.session.springRedisTestApplication.RedisSpringTestApplication;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public abstract class SessionDUnitTest {

  private static final Logger logger = LogService.getLogger();

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public TestName testName = new TestName();

  // 30 minutes
  protected static final int DEFAULT_SESSION_TIMEOUT = 60 * 30;

  protected static final int LOCATOR = 0;
  protected static final int SERVER1 = 1;
  protected static final int SERVER2 = 2;
  protected static final int APP1 = 3;
  protected static final int APP2 = 4;

  protected static final Map<Integer, Integer> ports = new HashMap<>();
  private static ConfigurableApplicationContext springApplicationContext;

  private RedisClusterClient redisClient;
  private static StatefulRedisClusterConnection<String, String> connection;
  protected static RedisAdvancedClusterCommands<String, String> commands;
  private static Retry retry;

  @BeforeClass
  public static void setup() {
    setupAppPorts();
    setupGeodeRedis();
    setupRetry();
  }

  protected static void setupRetry() {
    RetryConfig config = RetryConfig.custom()
        .maxAttempts(20)
        .retryExceptions(HttpServerErrorException.InternalServerError.class)
        .build();
    RetryRegistry registry = RetryRegistry.of(config);
    retry = registry.retry("sessions");
  }

  protected static void setupAppPorts() {
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    ports.put(APP1, availablePorts[0]);
    ports.put(APP2, availablePorts[1]);
  }

  protected static void setupGeodeRedis() {
    cluster.startLocatorVM(LOCATOR);
    startRedisServer(SERVER1);
    startRedisServer(SERVER2);
    ports.put(SERVER1, cluster.getRedisPort(SERVER1));
    ports.put(SERVER2, cluster.getRedisPort(SERVER2));
  }

  protected static void setupSpringApps(long sessionTimeout) {
    startSpringApp(APP1, sessionTimeout, ports.get(SERVER1), ports.get(SERVER2));
    startSpringApp(APP2, sessionTimeout, ports.get(SERVER2), ports.get(SERVER1));
  }

  @Before
  public void setupClient() {
    redisClient = RedisClusterClient.create("redis://localhost:" + ports.get(SERVER1));

    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .enablePeriodicRefresh(Duration.ofSeconds(5))
            .build();

    redisClient.setOptions(ClusterClientOptions.builder()
        .topologyRefreshOptions(refreshOptions)
        .autoReconnect(true)
        .build());
    connection = redisClient.connect();
    commands = connection.sync();
  }

  @AfterClass
  public static void cleanupAfterClass() {
    stopSpringApp(APP1);
    stopSpringApp(APP2);
  }

  @After
  public void cleanupAfterTest() {
    logger.info(
        "RetryMetrics after test {}: [successfulCallsWithRetries: {} successfulCallsWithoutRetries: {} failedCallsWithRetries: {} failedCallsWithoutRetries: {}]",
        testName.getMethodName(),
        retry.getMetrics().getNumberOfSuccessfulCallsWithRetryAttempt(),
        retry.getMetrics().getNumberOfSuccessfulCallsWithoutRetryAttempt(),
        retry.getMetrics().getNumberOfFailedCallsWithRetryAttempt(),
        retry.getMetrics().getNumberOfFailedCallsWithoutRetryAttempt());

    flushAll();

    try {
      redisClient.shutdown();
    } catch (Exception ignored) {
    }
  }

  protected void flushAll() {
    GeodeAwaitility.await().ignoreExceptions()
        .untilAsserted(() -> cluster.flushAll());
  }

  protected static void startRedisServer(int server) {
    cluster.startRedisVM(server, cluster.getMember(LOCATOR).getPort());

    cluster.getVM(server).invoke("Set logging level to DEBUG", () -> {
      Logger logger = LogManager.getLogger("org.apache.geode.redis.internal");
      Configurator.setAllLevels(logger.getName(), Level.getLevel("DEBUG"));
      FastLogger.setDelegating(true);
    });
  }

  protected static void startSpringApp(int sessionApp, long sessionTimeout, int... serverPorts) {
    int httpPort = ports.get(sessionApp);
    VM host = cluster.getVM(sessionApp).initializeAsClientVM();
    host.invoke("Start a Spring app", () -> {
      System.setProperty("server.port", "" + httpPort);
      System.setProperty("server.servlet.session.timeout", "" + sessionTimeout + "s");

      String[] args = new String[serverPorts.length];
      for (int i = 0; i < serverPorts.length; i++) {
        args[i] = "localhost:" + serverPorts[i];
      }
      springApplicationContext = SpringApplication.run(RedisSpringTestApplication.class, args);
    });
  }

  static void stopSpringApp(int sessionApp) {
    cluster.getVM(sessionApp).invoke(() -> springApplicationContext.close());
  }

  protected String createNewSessionWithNote(int sessionApp, String note) throws Exception {
    return Retry.decorateCallable(retry, () -> createNewSessionWithNote0(sessionApp, note)).call();
  }

  private String createNewSessionWithNote0(int sessionApp, String note) {
    HttpEntity<String> request = new HttpEntity<>(note);
    HttpHeaders resultHeaders = new RestTemplate()
        .postForEntity("http://localhost:" + ports.get(sessionApp) + "/addSessionNote",
            request, String.class)
        .getHeaders();

    return resultHeaders.getFirst("Set-Cookie");
  }

  protected String[] getSessionNotes(int sessionApp, String sessionCookie) throws Exception {
    return Retry.decorateCallable(retry, () -> getSessionNotes0(sessionApp, sessionCookie)).call();
  }

  private String[] getSessionNotes0(int sessionApp, String sessionCookie) {
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionCookie);
    HttpEntity<String> request = new HttpEntity<>("", requestHeaders);

    return new RestTemplate()
        .exchange("http://localhost:" + ports.get(sessionApp) + "/getSessionNotes",
            HttpMethod.GET, request, String[].class)
        .getBody();
  }

  void addNoteToSession(int sessionApp, String sessionCookie, String note) throws Exception {
    Retry.decorateCallable(retry, () -> addNoteToSession0(sessionApp, sessionCookie, note))
        .call();
  }

  private Void addNoteToSession0(int sessionApp, String sessionCookie, String note)
      throws Exception {
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionCookie);
    List<String> notes = new ArrayList<>();
    Collections.addAll(notes, getSessionNotes(sessionApp, sessionCookie));
    HttpEntity<String> request = new HttpEntity<>(note, requestHeaders);
    new RestTemplate()
        .postForEntity("http://localhost:" + ports.get(sessionApp) + "/addSessionNote",
            request, String.class);
    return null;
  }

  protected String getSessionId(String sessionCookie) {
    List<HttpCookie> cookies = HttpCookie.parse(sessionCookie);
    byte[] decodedCookie = Base64.getDecoder().decode(cookies.get(0).getValue());
    return new String(decodedCookie);
  }

}
