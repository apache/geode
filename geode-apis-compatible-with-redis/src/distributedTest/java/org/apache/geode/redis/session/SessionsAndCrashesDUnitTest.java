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

import static java.util.Collections.singletonMap;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.session.Session;
import org.springframework.session.SessionRepository;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.session.springRedisTestApplication.RedisSpringTestApplication;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RedisClusterStartupRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class SessionsAndCrashesDUnitTest {

  @ClassRule
  public static RedisClusterStartupRule cluster = new RedisClusterStartupRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private static final int JEDIS_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());
  private static final int NUM_SESSIONS = 100;
  private static final List<String> sessionIds = new ArrayList<>(NUM_SESSIONS);
  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static int[] redisPorts;
  private static JedisCluster jedis;

  private SessionRepository<Session> sessionRepository;
  private ConfigurableApplicationContext springContext;

  @BeforeClass
  public static void classSetup() {
    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(MAX_WAIT_TIME_RECONNECT, "15000");

    locator = cluster.startLocatorVM(0, locatorProperties);

    server1 = startRedisVM(1, 0);
    server2 = startRedisVM(2, 0);
    server3 = startRedisVM(3, 0);

    redisPorts = new int[] {
        cluster.getRedisPort(1),
        cluster.getRedisPort(2),
        cluster.getRedisPort(3)};

    jedis = new JedisCluster(new HostAndPort("localhost", redisPorts[0]), JEDIS_TIMEOUT);
  }

  private static MemberVM startRedisVM(int vmId, Integer redisPort) {
    int locatorPort = locator.getPort();

    return cluster.startRedisVM(vmId, x -> x
        .withProperty(REDIS_PORT, redisPort.toString())
        .withConnectionToLocator(locatorPort));
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    // Focus our connections on server2 and server3 since these are the ones going to be
    // restarted.
    String[] args = new String[] {
        "localhost:" + redisPorts[1],
        "localhost:" + redisPorts[2]};

    int appServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    SpringApplication app = new SpringApplication(RedisSpringTestApplication.class);
    app.setDefaultProperties(singletonMap("server.port", String.valueOf(appServerPort)));
    springContext = app.run(args);
    sessionRepository = springContext.getBean(SessionRepository.class);
    assertThat(sessionRepository).isNotNull();
  }

  @After
  public void teardown() {
    springContext.stop();
    cluster.flushAll(redisPorts[0]);
    sessionIds.clear();
  }

  @Test
  public void sessionOperationsDoNotFail_whileServersAreRestarted() throws Exception {
    createSessions();

    AtomicBoolean running = new AtomicBoolean(true);
    AtomicReference<String> phase = new AtomicReference<>("STARTUP");

    Future<Integer> future1 = executor.submit(() -> sessionUpdater(1, running, phase));
    Future<Integer> future2 = executor.submit(() -> sessionUpdater(2, running, phase));

    GeodeAwaitility.await().during(1, TimeUnit.SECONDS).until(() -> true);

    phase.set("CRASH 1 SERVER2");
    cluster.crashVM(2);
    server2 = startRedisVM(2, redisPorts[1]);
    phase.set("CRASH 1 REBALANCING");
    rebalanceAllRegions(server2);

    phase.set("CRASH 2 SERVER3");
    cluster.crashVM(3);
    server3 = startRedisVM(3, redisPorts[2]);
    phase.set("CRASH 2 REBALANCING");
    rebalanceAllRegions(server3);

    phase.set("CRASH 3 SERVER2");
    cluster.crashVM(2);
    server2 = startRedisVM(2, redisPorts[1]);
    phase.set("CRASH 3 REBALANCING");
    rebalanceAllRegions(server2);

    phase.set("CRASH 4 SERVER3");
    cluster.crashVM(3);
    server3 = startRedisVM(3, redisPorts[2]);
    phase.set("CRASH 4 REBALANCING");
    rebalanceAllRegions(server3);

    phase.set("FINISHING");
    GeodeAwaitility.await().during(20, TimeUnit.SECONDS).until(() -> true);

    running.set(false);
    int updatesThread1 = future1.get();
    int updatesThread2 = future2.get();

    validateSessionAttributes(1, updatesThread1);
    validateSessionAttributes(2, updatesThread2);
  }

  private void validateSessionAttributes(int index, int totalUpdates) {
    SoftAssertions softly = new SoftAssertions();

    for (int i = totalUpdates - NUM_SESSIONS; i < totalUpdates; i++) {
      int sessionIdx = i % NUM_SESSIONS;
      String sessionId = sessionIds.get(sessionIdx);
      Session session = sessionRepository.findById(sessionId);
      String attr = session.getAttribute(String.format("attr-%d-%d", index, sessionIdx));

      softly.assertThat(attr).isEqualTo("value-" + i);
    }

    softly.assertAll();
  }

  private Integer sessionUpdater(int index, AtomicBoolean running, AtomicReference<String> phase) {
    int count = 0;
    while (running.get()) {
      int modCount = count % NUM_SESSIONS;
      String sessionId = sessionIds.get(modCount);
      Session session = findSession(sessionId);
      assertThat(session).as("Session " + sessionId + " not found during phase " + phase.get())
          .isNotNull();

      session.setAttribute(String.format("attr-%d-%d", index, modCount), "value-" + count);
      saveSession(session);
      count++;
    }

    return count;
  }

  private Session findSession(String sessionId) {
    AtomicReference<Session> sessionRef = new AtomicReference<>(null);
    GeodeAwaitility.await().ignoreExceptions()
        .untilAsserted(() -> sessionRef.set(sessionRepository.findById(sessionId)));

    return sessionRef.get();
  }

  private void saveSession(Session session) {
    GeodeAwaitility.await().ignoreExceptions()
        .untilAsserted(() -> sessionRepository.save(session));
  }

  private void createSessions() {
    for (int i = 0; i < NUM_SESSIONS; i++) {
      Session session = sessionRepository.createSession();
      sessionRepository.save(session);
      sessionIds.add(session.getId());
    }
  }

  private static void rebalanceAllRegions(MemberVM vm) {
    vm.invoke(() -> {
      ResourceManager manager = ClusterStartupRule.getCache().getResourceManager();

      RebalanceFactory factory = manager.createRebalanceFactory();
      factory.start().getResults();
    });
  }
}
