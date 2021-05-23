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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractCommandPipeliningIntegrationTest implements RedisIntegrationTest {
  private Jedis publisher;
  private Jedis subscriber;
  private static final int REDIS_CLIENT_TIMEOUT =
      Math.toIntExact(GeodeAwaitility.getTimeout().toMillis());

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @Before
  public void setUp() {
    subscriber = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    publisher = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @After
  public void tearDown() {
    subscriber.close();
    publisher.close();
  }

  @Test
  public void whenPipelining_commandResponsesAreNotCorrupted() {
    List<String> expectedMessages = Collections.singletonList("hello");

    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "salutations");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    publisher.sadd("foo", "bar");

    // Publish and smembers in a pipeline
    Pipeline pipe = publisher.pipelined();

    pipe.publish("salutations", "hello");
    pipe.smembers("foo");

    pipe.syncAndReturnAll();

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages);
  }

  @Test
  public void should_returnResultsOfPipelinedCommands_inCorrectOrder() {
    Jedis jedis = new Jedis("localhost", getPort(), REDIS_CLIENT_TIMEOUT);
    final int NUMBER_OF_COMMANDS_IN_PIPELINE = 100;
    int numberOfPipeLineRequests = 1000;

    do {
      Pipeline p = jedis.pipelined();
      for (int i = 0; i < NUMBER_OF_COMMANDS_IN_PIPELINE; i++) {
        p.echo(String.valueOf(i));
      }

      List<Object> results = p.syncAndReturnAll();

      verifyResultOrder(NUMBER_OF_COMMANDS_IN_PIPELINE, results);
      numberOfPipeLineRequests--;
    } while (numberOfPipeLineRequests > 0);

    jedis.flushAll();
    jedis.close();
  }

  private void verifyResultOrder(final int numberOfCommandInPipeline, List<Object> results) {
    for (int i = 0; i < numberOfCommandInPipeline; i++) {
      String expected = String.valueOf(i);
      String currentVal = (String) results.get(i);

      assertThat(currentVal).isEqualTo(expected);
    }
  }

  private void waitFor(Callable<Boolean> booleanCallable) {
    GeodeAwaitility.await()
        .ignoreExceptions() // ignoring socket closed exceptions
        .until(booleanCallable);
  }
}
