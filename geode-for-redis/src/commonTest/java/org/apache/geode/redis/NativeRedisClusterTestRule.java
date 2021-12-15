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
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.REDIS_CLIENT_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.Jedis;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.proxy.HostPort;
import org.apache.geode.redis.internal.proxy.RedisProxy;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class NativeRedisClusterTestRule extends ExternalResource {

  private static final Logger logger = LogService.getLogger();
  private static final String REDIS_COMPOSE_YML = "/redis-cluster-compose.yml";
  private static final int NODE_COUNT = 6;

  private DockerComposeContainer<?> redisCluster;
  private final RuleChain delegate;
  private final int REDIS_PORT = 6379;
  private final List<Integer> exposedPorts = new ArrayList<>();

  public NativeRedisClusterTestRule() {
    delegate = RuleChain
        // Docker compose does not work on windows in CI. Ignore this test on windows
        // Using a RuleChain to make sure we ignore the test before the rule comes into play
        .outerRule(new IgnoreOnWindowsRule());
  }

  public List<Integer> getExposedPorts() {
    return exposedPorts;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    Statement containerStatement = new Statement() {
      @Override
      public void evaluate() throws Throwable {
        URL composeYml = getClass().getResource(REDIS_COMPOSE_YML);
        assertThat(composeYml).as("Cannot load resource " + REDIS_COMPOSE_YML)
            .isNotNull();

        redisCluster =
            new DockerComposeContainer<>("acceptance", new File(composeYml.getFile()));
        for (int i = 0; i < NODE_COUNT; i++) {
          redisCluster.withExposedService("redis-node-" + i, REDIS_PORT);
        }

        redisCluster.waitingFor("redis-cluster-init_1",
            Wait.forLogMessage(".*Ready to accept connections.*", 1));

        redisCluster.start();

        List<ClusterNode> nodes = null;
        for (int i = 0; i < NODE_COUNT; i++) {
          int port = redisCluster.getServicePort("redis-node-" + i, REDIS_PORT);
          nodes = waitForRedisNodes(port, 3);
        }
        assert nodes != null;
        nodes.forEach(logger::info);

        // Used when translating internal redis host:port to the external host:port which is
        // ultimately what command results will return.
        Map<HostPort, HostPort> translationMappings = new HashMap<>();
        List<RedisProxy> proxies = new ArrayList<>();

        for (int i = 0; i < NODE_COUNT; i++) {
          Map<String, ContainerNetwork> networks =
              redisCluster.getContainerByServiceName("redis-node-" + i + "_1").get()
                  .getContainerInfo().getNetworkSettings().getNetworks();
          ContainerNetwork network = networks.values().iterator().next();
          String containerIp = network.getIpAddress();
          int socatPort = redisCluster.getServicePort("redis-node-" + i, REDIS_PORT);

          RedisProxy proxy = new RedisProxy(socatPort);
          Integer exposedPort = proxy.getExposedPort();
          exposedPorts.add(exposedPort);
          translationMappings.put(new HostPort(containerIp, REDIS_PORT),
              new HostPort("127.0.0.1", exposedPort));

          proxies.add(proxy);
        }

        proxies.forEach(p -> p.configure(translationMappings));

        logger.info("Started redis cluster with mapped ports: {}", translationMappings);
        IgnoredException.addIgnoredException("RedisProxy");

        try {
          base.evaluate(); // This will run the test.
        } finally {
          redisCluster.stop();
          proxies.forEach(RedisProxy::stop);
          exposedPorts.clear();
        }
      }
    };

    return delegate.apply(containerStatement, description);
  }

  public void dumpContainerLogs() {
    dumpContainerLogs("redis-cluster-init_1");
    for (int i = 0; i < NODE_COUNT; i++) {
      dumpContainerLogs("redis-node-" + i + "_1");
    }
  }

  private void dumpContainerLogs(String node) {
    System.out.println("=================== Container: " + node + " =====================");
    System.out.println(redisCluster.getContainerByServiceName(node).get().getLogs());
  }

  /**
   * This method assumes that there is a replica for every primary.
   */
  private List<ClusterNode> waitForRedisNodes(int port, int wantedPrimaries) {
    List<ClusterNode> nodes = null;
    int primaryCount = 0;
    int replicaCount = 0;

    try (Jedis jedis = new Jedis(BIND_ADDRESS, port, REDIS_CLIENT_TIMEOUT)) {
      for (int i = 0; i < 60; i++) {
        nodes = ClusterNodes.parseClusterNodes(jedis.clusterNodes()).getNodes();
        primaryCount = nodes.stream().mapToInt(x -> x.primary ? 1 : 0).sum();
        replicaCount = nodes.stream().mapToInt(x -> x.primary ? 0 : 1).sum();
        if (primaryCount == wantedPrimaries && replicaCount == wantedPrimaries) {
          return nodes;
        }

        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
      }
    }

    nodes.forEach(logger::info);

    assertThat(primaryCount)
        .as("Incorrect primary node count")
        .isEqualTo(wantedPrimaries);

    assertThat(replicaCount)
        .as("Incorrect replica node count")
        .isEqualTo(wantedPrimaries);

    return null;
  }

  public void flushAll() {
    ClusterNodes nodes;
    try (Jedis jedis = new Jedis("localhost", exposedPorts.get(0))) {
      nodes = ClusterNodes.parseClusterNodes(jedis.clusterNodes());
    }

    for (ClusterNode node : nodes.getNodes()) {
      if (!node.primary) {
        continue;
      }
      try (Jedis jedis = new Jedis(node.ipAddress, (int) node.port)) {
        jedis.flushAll();
      }
    }
  }
}
