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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.net.HttpCookie;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.springRedisTestApplication.RedisSpringTestApplication;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class RedisSessionDistDUnitTest implements Serializable {

  @ClassRule
  public static ClusterStartupRule cluster =
      new ClusterStartupRule(5);

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  public static ConfigurableApplicationContext springApplicationContext;

  private String LOCALHOST = "http://127.0.0.1";

  private static int indexOfLocator = 0;
  private static int indexOfServer1 = 1;
  private static int indexOfServer2 = 2;
  private static int indexOfClient1 = 3;
  private static int indexOfClient2 = 4;

  private static VM client1;
  private static VM client2;

  private static int server1Port;
  private static int server2Port;
  private static int client1Port;
  private static int client2Port;

  private static Properties redisPropsForServer1;
  private static Properties redisPropsForServer2;
  private static MemberVM locator;

  private static RestTemplate restTemplate;

  private static Jedis jedis;

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @BeforeClass
  public static void setup() {

    int[] availablePorts =
        AvailablePortHelper.getRandomAvailableTCPPorts(4);

    server1Port = availablePorts[0];
    server2Port = availablePorts[1];
    client1Port = availablePorts[2];
    client2Port = availablePorts[3];

    int localServer1Port = server1Port;
    int localServer2Port = server2Port;
    int localClient1Port = client1Port;
    int localClient2Port = client2Port;

    locator = cluster.startLocatorVM(indexOfLocator);

    redisPropsForServer1 = new Properties();
    redisPropsForServer1.setProperty("redis-bind-address", "localHost");
    redisPropsForServer1.setProperty("redis-port", Integer.toString(server1Port));
    redisPropsForServer1.setProperty("log-level", "warn");

    redisPropsForServer2 = new Properties();
    redisPropsForServer2.setProperty("redis-bind-address", "localHost");
    redisPropsForServer2.setProperty("redis-port", Integer.toString(server2Port));
    redisPropsForServer2.setProperty("log-level", "warn");

    cluster.startServerVM(indexOfServer1, redisPropsForServer1, locator.getPort());
    cluster.startServerVM(indexOfServer2, redisPropsForServer2, locator.getPort());

    client1 = cluster.getVM(indexOfClient1);
    client2 = cluster.getVM(indexOfClient2);

    client1.invoke("start a spring app", () -> {
      System.setProperty("server.port", Integer.toString(localClient1Port));
      System.setProperty("spring.redis.port", Integer.toString(localServer1Port));
      SpringApplication.run(
          RedisSpringTestApplication.class,
          "" + localServer1Port, "" + localServer2Port);
    });

    client2.invoke("start a spring app", () -> {
      System.setProperty("server.port", Integer.toString(localClient2Port));
      System.setProperty("spring.redis.port", Integer.toString(localServer2Port));
      springApplicationContext = SpringApplication.run(
          RedisSpringTestApplication.class,
          "" + localServer1Port, "" + localServer2Port);
    });

    restTemplate = new RestTemplate();
    jedis = new Jedis("localHost", server1Port, JEDIS_TIMEOUT);
  }

  @After
  public void cleanupAfterTest() {
    jedis.flushAll();
  }

  @AfterClass
  public static void cleanupAfterClass() {
    jedis.disconnect();
  }

  @Test
  public void should_beAbleToCreateASession() {
    HttpEntity<String> request = new HttpEntity<>("note1");
    cluster.getMember(0);
    HttpHeaders resultHeaders = restTemplate
        .postForEntity(
            LOCALHOST + ":" + client1Port + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    assertThat(resultHeaders).isNotNull();

    String cookieString = resultHeaders.get("Set-Cookie").get(0);
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", cookieString);

    List<HttpCookie> cookies = HttpCookie.parse(cookieString);
    byte[] decodedCookie = Base64.getDecoder().decode(cookies.get(0).getValue());

    assertThat(jedis.hgetAll("spring:session:sessions:" + new String(decodedCookie))).isNotEmpty();
  }

  @Test
  public void should_storeSessionDataInRedis() {
    HttpEntity<String> request = new HttpEntity<>("note1");
    HttpHeaders responseHeaders = restTemplate
        .postForEntity(
            LOCALHOST + ":" + client1Port + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    String sessionAsCookie = responseHeaders.get("Set-Cookie").get(0);

    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionAsCookie);
    HttpEntity<String> request2 = new HttpEntity<>("", requestHeaders);

    String[] sessionNotes = restTemplate
        .exchange(
            LOCALHOST + ":" + client2Port + "/getSessionNotes",
            HttpMethod.GET,
            request2,
            String[].class)
        .getBody();

    assertThat(sessionNotes[0]).isEqualTo("note1");

    List<HttpCookie> cookies = HttpCookie.parse(sessionAsCookie);
    byte[] decodedCookie = Base64.getDecoder().decode(cookies.get(0).getValue());

    Map<String, String> sessionInfo =
        jedis.hgetAll("spring:session:sessions:" + new String(decodedCookie));

    assertThat(sessionInfo).isNotNull();
    assertThat(sessionInfo.get("sessionAttr:NOTES")).isNotNull();

    jedis.disconnect();
  }

  @Test
  public void should_propagateSessionData_toOtherServers() {

    HttpEntity<String> request = new HttpEntity<>("noteFromClient1");
    HttpHeaders resultHeaders = restTemplate
        .postForEntity(
            LOCALHOST + ":" + client1Port + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    String sessionAsCookie = resultHeaders.get("Set-Cookie").get(0);
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionAsCookie);

    HttpEntity<String> request2 = new HttpEntity<>("", requestHeaders);

    String[] sessionNotes = restTemplate
        .exchange(
            LOCALHOST + ":" + client2Port + "/getSessionNotes",
            HttpMethod.GET,
            request2,
            String[].class)
        .getBody();

    assertThat(sessionNotes[0]).isEqualTo("noteFromClient1");
  }

  @Test
  public void should_getSessionDataFromOtherGeodeRedisServer_whenOriginalServerGoesDown() {
    HttpEntity<String> request = new HttpEntity<>("noteFromClient2");
    HttpHeaders responseHeaders = restTemplate
        .postForEntity(
            LOCALHOST + ":" + client2Port + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    String sessionAsCookie = responseHeaders.get("Set-Cookie").get(0);
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionAsCookie);
    HttpEntity<String> request2 = new HttpEntity<>("", requestHeaders);

    cluster.crashVM(indexOfServer2);

    String[] sessionNotes = restTemplate
        .exchange(
            LOCALHOST + ":" + client1Port + "/getSessionNotes",
            HttpMethod.GET,
            request2,
            String[].class)
        .getBody();

    assertThat(sessionNotes[0]).isEqualTo("noteFromClient2");

    cluster.startServerVM(indexOfServer2, redisPropsForServer2, locator.getPort());
  }

  @Test
  public void should_getSessionDataFromServer_whenServerGoesDownAndIsRestarted() {
    HttpEntity<String> request = new HttpEntity<>("noteFromClient2");

    HttpHeaders resultHeaders = restTemplate
        .postForEntity(
            LOCALHOST + ":" + client2Port + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    String cookieString = resultHeaders.get("Set-Cookie").get(0);
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", cookieString);
    HttpEntity<String> request2 = new HttpEntity<>("noteFromClient1", requestHeaders);

    cluster.crashVM(indexOfServer2);

    restTemplate
        .postForEntity(
            LOCALHOST + ":" + client1Port + "/addSessionNote",
            request2,
            String.class);

    cluster.startServerVM(indexOfServer2,
        redisPropsForServer2,
        locator.getPort());

    HttpEntity<String> request3 = new HttpEntity<>("", requestHeaders);

    String[] sessionNotes = restTemplate
        .exchange(
            LOCALHOST + ":" + client2Port + "/getSessionNotes",
            HttpMethod.GET,
            request3,
            String[].class)
        .getBody();

    assertThat(sessionNotes).containsExactly("noteFromClient2", "noteFromClient1");
  }

  @Test
  public void should_getCorrectSessionData_whenGeodeRedisServerGoesDown_andClientConnectsToDifferentServer() {
    HttpEntity<String> request = new HttpEntity<>("noteFromClient2");

    HttpHeaders resultHeaders = restTemplate
        .postForEntity(
            LOCALHOST + ":" + client2Port + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    String cookieString = resultHeaders.get("Set-Cookie").get(0);
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", cookieString);
    HttpEntity<String> request2 = new HttpEntity<>("", requestHeaders);

    cluster.crashVM(indexOfServer2);

    String[] sessionNotes = restTemplate
        .exchange(
            LOCALHOST + ":" + client2Port + "/getSessionNotes",
            HttpMethod.GET,
            request2,
            String[].class)
        .getBody();

    assertThat(sessionNotes[0]).isEqualTo("noteFromClient2");

    cluster.startServerVM(indexOfServer2, redisPropsForServer2, locator.getPort());
  }


  @Test
  public void should_getCorrectSessionData_whenAppInstanceGoesDown_andClientConnectsToDifferentAppInstance() {

    HttpEntity<String> request = new HttpEntity<>("noteFromClient2");
    HttpHeaders responseHeaders = restTemplate
        .postForEntity(
            LOCALHOST + ":" + client2Port + "/addSessionNote",
            request,
            String.class)
        .getHeaders();

    String sessionAsCookie = responseHeaders.get("Set-Cookie").get(0);
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionAsCookie);
    HttpEntity<String> request2 = new HttpEntity<>("", requestHeaders);

    client2.invoke(() -> {
      springApplicationContext.close();
    });

    String[] sessionNotes;

    try {
      sessionNotes = restTemplate
          .exchange(
              LOCALHOST + ":" + client1Port + "/getSessionNotes",
              HttpMethod.GET,
              request2,
              String[].class)
          .getBody();

    } finally {

      int localServer1Port = server1Port;
      int localServer2Port = server2Port;
      int localClient2Port = client2Port;

      client2.invoke("start a spring app", () -> {
        System.setProperty("server.port", Integer.toString(localClient2Port));
        System.setProperty("spring.redis.port", Integer.toString(localServer2Port));
        springApplicationContext = SpringApplication.run(
            RedisSpringTestApplication.class,
            "" + localServer1Port, "" + localServer2Port);
      });
    }
    assertThat(sessionNotes[0]).isEqualTo("noteFromClient2");

  }

}
