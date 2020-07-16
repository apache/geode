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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
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
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import redis.clients.jedis.Jedis;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.redis.session.springRedisTestApplication.RedisSpringTestApplication;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;

public abstract class SessionDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  // 30 minutes
  protected static final int DEFAULT_SESSION_TIMEOUT = 60 * 30;

  protected static final int LOCATOR = 0;
  protected static final int SERVER1 = 1;
  protected static final int SERVER2 = 2;
  protected static final int APP1 = 3;
  protected static final int APP2 = 4;

  protected static final Map<Integer, Integer> ports = new HashMap<>();
  public static ConfigurableApplicationContext springApplicationContext;

  protected static Jedis jedisConnetedToServer1;
  protected static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @BeforeClass
  public static void setup() {
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    ports.put(APP1, availablePorts[0]);
    ports.put(APP2, availablePorts[1]);

    cluster.startLocatorVM(LOCATOR);
    startRedisServer(SERVER1);
    startRedisServer(SERVER2);

    ports.put(SERVER1, cluster.getRedisPort(SERVER1));
    ports.put(SERVER2, cluster.getRedisPort(SERVER2));

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
    VM host = cluster.getVM(sessionApp);
    host.invoke("Start a Spring app", () -> {
      System.setProperty("server.port", "" + httpPort);
      System.setProperty("spring.redis.port", "" + serverPorts[0]);
      System.setProperty("server.servlet.session.timeout", "" + sessionTimeout + "s");
      String[] args = new String[serverPorts.length];

      for (int i = 0; i < serverPorts.length; i++) {
        args[i] = "" + serverPorts[i];
      }
      springApplicationContext = SpringApplication.run(
          RedisSpringTestApplication.class, args);
    });
  }

  static void stopSpringApp(int sessionApp) {
    cluster.getVM(sessionApp).invoke(() -> springApplicationContext.close());
  }

  protected String createNewSessionWithNote(int sessionApp, String note) {
    HttpEntity<String> request = new HttpEntity<>(note);
    boolean noteAdded = false;
    String sessionCookie = "";
    do {
      try {
        HttpHeaders resultHeaders = new RestTemplate()
            .postForEntity(
                "http://localhost:" + ports.get(sessionApp)
                    + "/addSessionNote",
                request,
                String.class)
            .getHeaders();
        sessionCookie = resultHeaders.getFirst("Set-Cookie");
        noteAdded = true;
      } catch (HttpServerErrorException e) {
        if (e.getMessage().contains("memberDeparted")) {
          // retry
        } else {
          throw e;
        }
      }
    } while (!noteAdded);

    return sessionCookie;
  }

  protected String[] getSessionNotes(int sessionApp, String sessionCookie) {
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionCookie);
    HttpEntity<String> request = new HttpEntity<>("", requestHeaders);
    boolean sesssionObtained = false;
    String[] responseBody = new String[0];
    do {
      try {
        responseBody = new RestTemplate()
            .exchange(
                "http://localhost:" + ports.get(sessionApp) + "/getSessionNotes",
                HttpMethod.GET,
                request,
                String[].class)
            .getBody();
        sesssionObtained = true;
      } catch (HttpServerErrorException e) {
        if (e.getMessage().contains("Internal Server Error")) {
          // retry
        } else {
          throw e;
        }
      }
    } while (!sesssionObtained);
    return responseBody;
  }

  void addNoteToSession(int sessionApp, String sessionCookie, String note) {
    HttpHeaders requestHeaders = new HttpHeaders();
    requestHeaders.add("Cookie", sessionCookie);
    List<String> notes = new ArrayList<>();
    Collections.addAll(notes, getSessionNotes(sessionApp, sessionCookie));
    HttpEntity<String> request = new HttpEntity<>(note, requestHeaders);
    boolean noteAdded = false;
    do {
      try {
        new RestTemplate()
            .postForEntity(
                "http://localhost:" + ports.get(sessionApp) + "/addSessionNote",
                request,
                String.class)
            .getHeaders();
        noteAdded = true;
      } catch (HttpServerErrorException e) {
        if (e.getMessage().contains("memberDeparted")) {
          List<String> updatedNotes = new ArrayList<>();
          Collections.addAll(updatedNotes, getSessionNotes(sessionApp, sessionCookie));
          if (notes.containsAll(updatedNotes)) {
            noteAdded = true;
          }
          e.printStackTrace();
        } else {
          throw e;
        }
      }
    } while (!noteAdded);
  }

  protected String getSessionId(String sessionCookie) {
    List<HttpCookie> cookies = HttpCookie.parse(sessionCookie);
    byte[] decodedCookie = Base64.getDecoder().decode(cookies.get(0).getValue());
    return new String(decodedCookie);
  }
}
