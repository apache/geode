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

package org.apache.geode;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.DockerComposeContainer;
import redis.clients.jedis.Jedis;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class NativeRedisClusterTestRule extends ExternalResource implements Serializable {

  private static final Logger logger = LogService.getLogger();
  private static final Pattern ipPortCportRE = Pattern.compile("([^:]*):([0-9]*)@([0-9]*)");
  private static final String REDIS_COMPOSE_YML = "/redis-cluster-compose.yml";

  private DockerComposeContainer<?> redisCluster;
  private final RuleChain delegate;
  private final int REDIS_PORT = 6379;
  private List<Integer> exposedPorts;

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
            new DockerComposeContainer<>("acceptance", new File(composeYml.getFile()))
                .withExposedService("redis-node-0", REDIS_PORT)
                .withExposedService("redis-node-1", REDIS_PORT)
                .withExposedService("redis-node-2", REDIS_PORT);

        redisCluster.start();

        int port = redisCluster.getServicePort("redis-node-0", REDIS_PORT);
        Jedis jedis = new Jedis("localhost", port);
        List<ClusterNode> nodes = parseClusterNodes(jedis.clusterNodes());

        assertThat(nodes.stream().mapToInt(x -> x.primary ? 1 : 0).sum())
            .as("Incorrect primary node count")
            .isEqualTo(3);

        exposedPorts = Arrays.asList(
            redisCluster.getServicePort("redis-node-0", REDIS_PORT),
            redisCluster.getServicePort("redis-node-1", REDIS_PORT),
            redisCluster.getServicePort("redis-node-2", REDIS_PORT));

        logger.info("Started redis cluster with exposed ports: {}", exposedPorts);

        try {
          base.evaluate(); // This will run the test.
        } finally {
          redisCluster.stop();
        }
      }
    };

    return delegate.apply(containerStatement, description);
  }

  private List<ClusterNode> parseClusterNodes(String rawInput) {
    List<ClusterNode> nodes = new ArrayList<>();

    for (String line : rawInput.split("\\n")) {
      nodes.add(parseOneClusterNodeLine(line));
    }

    return nodes;
  }

  private ClusterNode parseOneClusterNodeLine(String line) {
    String[] parts = line.split(" ");

    Matcher addressMatcher = ipPortCportRE.matcher(parts[1]);
    if (!addressMatcher.matches()) {
      throw new IllegalArgumentException("Unable to extract ip:port@cport from " + line);
    }

    boolean primary = parts[2].contains("master");

    int slotStart = -1;
    int slotEnd = -1;
    if (primary) {
      // Sometimes we see a 'primary' without slots which seems to imply it hasn't yet transitioned
      // to being a 'replica'.
      if (parts.length > 8) {
        String[] startEnd = parts[8].split("-");
        slotStart = Integer.parseInt(startEnd[0]);
        if (startEnd.length > 1) {
          slotEnd = Integer.parseInt(startEnd[1]);
        } else {
          slotEnd = slotStart;
        }
      } else {
        primary = false;
      }
    }

    return new ClusterNode(
        parts[0],
        addressMatcher.group(1),
        Integer.parseInt(addressMatcher.group(2)),
        primary,
        slotStart,
        slotEnd);
  }

  private static class ClusterNode {
    final String guid;
    final String ipAddress;
    final int port;
    final boolean primary;
    final int slotStart;
    final int slotEnd;

    public ClusterNode(String guid, String ipAddress, int port, boolean primary, int slotStart,
        int slotEnd) {
      this.guid = guid;
      this.ipAddress = ipAddress;
      this.port = port;
      this.primary = primary;
      this.slotStart = slotStart;
      this.slotEnd = slotEnd;
    }
  }
}
