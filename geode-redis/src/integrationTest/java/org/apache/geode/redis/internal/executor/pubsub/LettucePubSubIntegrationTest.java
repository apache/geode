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

package org.apache.geode.redis.internal.executor.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class LettucePubSubIntegrationTest {

  private static final String CHANNEL = "best-channel";
  private static RedisClient client;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void beforeClass() {
    client = RedisClient.create("redis://localhost:" + server.getPort());
  }

  @Test
  public void insanity() throws Exception {
    int subscriberCount = 50;
    int publisherCount = 10;
    int publishIterations = 10000;

    for (int i = 0; i < subscriberCount; i++) {
      StatefulRedisPubSubConnection<String, String> subscriber = client.connectPubSub();
      subscriber.sync().subscribe(CHANNEL);
    }

    List<Future<Long>> results = new ArrayList<>();
    for (int i = 0; i < publisherCount; i++) {
      int localI = i;
      results.add(executor.submit(() -> publish(localI, publishIterations)));
    }

    long publishCount = 0;
    for (Future<Long> r : results) {
      publishCount += r.get();
    }

    assertThat(publishCount).isEqualTo(subscriberCount * publisherCount * publishIterations);
  }

  private Long publish(int index, int iterationCount) throws Exception {
    StatefulRedisPubSubConnection<String, String> publisher = client.connectPubSub();
    long publishCount = 0;

    List<RedisFuture<Long>> results = new ArrayList<>();
    for (int i = 0; i < iterationCount; i++) {
      results.add(publisher.async().publish(CHANNEL, "message-" + index + "-" + i));
    }

    for (RedisFuture<Long> r : results) {
      publishCount += r.get();
    }

    return publishCount;
  }
}
