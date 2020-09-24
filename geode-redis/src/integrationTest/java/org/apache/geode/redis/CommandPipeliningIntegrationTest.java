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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.categories.RedisTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

@Category({RedisTest.class})
public class CommandPipeliningIntegrationTest {
  static Jedis publisher;
  static Jedis subscriber;
  static final int REDIS_CLIENT_TIMEOUT = 100000;

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  @BeforeClass
  public static void setUp() {
    subscriber = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
    publisher = new Jedis("localhost", server.getPort(), REDIS_CLIENT_TIMEOUT);
  }

  @AfterClass
  public static void tearDown() {
    subscriber.close();
    publisher.close();
  }

  public int getPort() {
    return server.getPort();
  }

  @Test
  public void whenPipelining_commandResponsesAreNotCorrupted() {
    List<String> expectedMessages = Arrays.asList("hello");

    MockSubscriber mockSubscriber = new MockSubscriber();

    Runnable runnable = () -> {
      subscriber.subscribe(mockSubscriber, "salutations");
    };

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    publisher.sadd("foo", "bar");

    // Publish and smembers in a pipeline
    Pipeline pipe = publisher.pipelined();

    pipe.publish("salutations", "hello");
    pipe.smembers("foo");

    List<Object> responses = pipe.syncAndReturnAll();

    mockSubscriber.unsubscribe("salutations");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
    waitFor(() -> !subscriberThread.isAlive());

    assertThat(mockSubscriber.getReceivedMessages()).isEqualTo(expectedMessages);
  }


  @Test
  public void should_returnResultsOfPipelinedCommands_inCorrectOrder() {
    Jedis jedis = new Jedis("localhost", server.getPort(), 10000000);
    int NUMBER_OF_COMMANDS_IN_PIPELINE = 100;
    int numberOfPipeLineRequests = 100000;

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

  private void verifyResultOrder(int NUMBER_OF_COMMAND_IN_PIPELINE, List<Object> results) {
    for (int i = 0; i < NUMBER_OF_COMMAND_IN_PIPELINE; i++) {
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
