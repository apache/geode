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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.session.springRedisTestApplication.RedisSpringTestApplication;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

public class SessionDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();


  protected static final int SESSION_TIMEOUT = 5;

  protected static final int LOCATOR = 0;
  protected static final int SERVER1 = 1;
  protected static final int SERVER2 = 2;
  protected static final int APP1 = 3;
  protected static final int APP2 = 4;


  private static final Map<Integer, Integer> ports = new HashMap<>();
  public static ConfigurableApplicationContext springApplicationContext;

  protected static Jedis jedisConnetedToServer1;
  private static final int JEDIS_TIMEOUT = Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @BeforeClass
  public static void setup() {
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(4);
    ports.put(SERVER1, availablePorts[0]);
    ports.put(SERVER2, availablePorts[1]);
    ports.put(APP1, availablePorts[2]);
    ports.put(APP2, availablePorts[3]);

    cluster.startLocatorVM(LOCATOR);
    startRedisServer(SERVER1);
    startRedisServer(SERVER2);
    startSpringApp(APP1, SERVER1, SERVER2);
    startSpringApp(APP2, SERVER2, SERVER1);

    jedisConnetedToServer1 = new Jedis("localhost", ports.get(SERVER1), JEDIS_TIMEOUT);
  }

  @AfterClass
  public static void cleanupAfterClass() {
    jedisConnetedToServer1.disconnect();
    stopSpringApp(APP1);
    stopSpringApp(APP2);
  }

  @After
  public void cleanupAfterTest() {
    jedisConnetedToServer1.flushAll();
  }

  protected static void startRedisServer(int server1) {
    cluster.startServerVM(server1, redisProperties(server1),
        cluster.getMember(LOCATOR).getPort());
  }

  private static Properties redisProperties(int server2) {
    Properties redisPropsForServer = new Properties();
    redisPropsForServer.setProperty("redis-bind-address", "localHost");
    redisPropsForServer.setProperty("redis-port", "" + ports.get(server2));
    redisPropsForServer.setProperty("log-level", "info");
    return redisPropsForServer;
  }

  static void startSpringApp(int sessionApp, int primaryServer, int secondaryServer) {
    int primaryRedisPort = ports.get(primaryServer);
    int failoverRedisPort = ports.get(secondaryServer);
    int httpPort = ports.get(sessionApp);
    VM host = cluster.getVM(sessionApp);
    host.invoke("start a spring app", () -> {
      System.setProperty("server.port", "" + httpPort);
      System.setProperty("spring.redis.port", "" + primaryRedisPort);
      System.setProperty("server.servlet.session.timeout", "" + SESSION_TIMEOUT + "s");
      springApplicationContext = SpringApplication.run(
          RedisSpringTestApplication.class,
          "" + primaryRedisPort, "" + failoverRedisPort);
    });
  }

  static void stopSpringApp(int sessionApp) {
    cluster.getVM(sessionApp).invoke(() -> springApplicationContext.close());
  }

  protected String createNewSessionWithNote(int sessionApp, String note) {
    HttpEntity<String> request = new HttpEntity<>(note);
    HttpHeaders resultHeaders = new RestTemplate()
        .postForEntity(
            "http://localhost:" + ports.get(sessionApp) + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    return resultHeaders.getFirst("Set-Cookie");
  }

  protected String[] getSessionNotes(int sessionApp, String sessionCookie) {
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionCookie);
    HttpEntity<String> request2 = new HttpEntity<>("", requestHeaders);

    return new RestTemplate()
        .exchange(
            "http://localhost:" + ports.get(sessionApp) + "/getSessionNotes",
            HttpMethod.GET,
            request2,
            String[].class)
        .getBody();
  }

  void addNoteToSession(int sessionApp, String sessionCookie, String note) {
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionCookie);
    HttpEntity<String> request = new HttpEntity<>(note, requestHeaders);
    new RestTemplate()
        .postForEntity(
            "http://localhost:" + ports.get(sessionApp) + "/addSessionNote",
            request,
            String.class)
        .getHeaders();
  }

  protected String getSessionId(String sessionCookie) {
    List<HttpCookie> cookies = HttpCookie.parse(sessionCookie);
    byte[] decodedCookie = Base64.getDecoder().decode(cookies.get(0).getValue());
    return new String(decodedCookie);
  }
}
