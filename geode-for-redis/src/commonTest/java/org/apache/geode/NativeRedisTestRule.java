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

import java.io.Serializable;

import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.GenericContainer;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

public class NativeRedisTestRule extends ExternalResource implements Serializable {

  private static final Logger logger = LogService.getLogger();
  public static final String DEFAULT_REDIS_IMAGE = "redis:6.2.6";

  private GenericContainer<?> redisContainer;
  private final RuleChain delegate;
  private final String dockerImage;
  private final int PORT_TO_EXPOSE = 6379;
  private int max_clients = 10000;

  public NativeRedisTestRule() {
    this(DEFAULT_REDIS_IMAGE);
  }

  public NativeRedisTestRule(String dockerImage) {
    this.dockerImage = dockerImage;
    delegate = RuleChain
        // Docker compose does not work on windows in CI. Ignore this test on windows
        // Using a RuleChain to make sure we ignore the test before the rule comes into play
        .outerRule(new IgnoreOnWindowsRule());
  }

  public int getPort() {
    return redisContainer.getFirstMappedPort();
  }

  public int getExposedPort() {
    return redisContainer.getExposedPorts().get(0);
  }

  public NativeRedisTestRule withMaxConnections(int max_connections) {
    max_clients = max_connections;

    return this;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    Statement containerStatement = new Statement() {
      @Override
      public void evaluate() throws Throwable {

        redisContainer =
            new GenericContainer<>(dockerImage)
                .withExposedPorts(PORT_TO_EXPOSE)
                .withCommand("redis-server --maxclients " + max_clients);

        redisContainer.start();
        int mappedPort = getPort();
        logger.info("Started redis container with exposed port {} -> {}", PORT_TO_EXPOSE,
            mappedPort);
        try {
          base.evaluate(); // This will run the test.
        } finally {
          redisContainer.stop();
          logger.info("Stopped redis container with exposed port {} -> {}", PORT_TO_EXPOSE,
              mappedPort);
        }
      }
    };

    return delegate.apply(containerStatement, description);
  }
}
