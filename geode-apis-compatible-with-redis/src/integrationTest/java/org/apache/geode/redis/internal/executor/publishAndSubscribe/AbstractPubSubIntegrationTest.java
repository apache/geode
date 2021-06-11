
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
 *
 */

package org.apache.geode.redis.internal.executor.publishAndSubscribe;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtMostNArgsForSubCommand;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_PUBSUB_SUBCOMMAND;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.PUBSUB_CHANNELS;
import static redis.clients.jedis.Protocol.PUBSUB_NUMSUB;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public abstract class AbstractPubSubIntegrationTest implements RedisIntegrationTest {
  private Jedis subscriber;
  private Jedis introspector;
  private MockSubscriber mockSubscriber;

  @Before
  public void setup() {
    mockSubscriber = new MockSubscriber();
    subscriber = new Jedis("localhost", getPort());
    introspector = new Jedis("localhost", getPort());
  }

  @After
  public void teardown() {
    if (mockSubscriber.getSubscribedChannels() > 0) {
      mockSubscriber.unsubscribe();
    }
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 0);
  }

  @Test
  public void pubsub_shouldError_givenTooFewArguments() {
    assertAtLeastNArgs(introspector, Protocol.Command.PUBSUB, 1);
  }

  @Test
  public void pubsub_shouldReturnError_givenUnknownSubcommand() {
    String expected = String.format(ERROR_UNKNOWN_PUBSUB_SUBCOMMAND, "nonesuch");

    assertThatThrownBy(() -> introspector.sendCommand(Protocol.Command.PUBSUB, "nonesuch"))
        .hasMessageContaining(expected);
  }

  /** -- CHANNELS-- **/

  @Test
  public void channels_shouldError_givenTooManyArguments() {
    assertAtMostNArgsForSubCommand(introspector,
        Protocol.Command.PUBSUB,
        Coder.stringToBytes(PUBSUB_CHANNELS),
        1);
  }

  @Test
  public void channels_shouldReturnListOfAllChannels_withActiveChannelSubscribers_whenCalledWithoutPattern() {
    List<byte[]> expectedChannels = new ArrayList<>();
    expectedChannels.add(Coder.stringToBytes("foo"));
    expectedChannels.add(Coder.stringToBytes("bar"));
    Runnable runnable =
        () -> subscriber.subscribe(mockSubscriber, "foo", "bar");
    Thread subscriberThread = new Thread(runnable);

    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    List<byte[]> result =
        uncheckedCast(introspector.sendCommand(Protocol.Command.PUBSUB, PUBSUB_CHANNELS));

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);
  }

  @Test
  public void channels_shouldNeverReturnPsubscribedChannels_givenNoActiveChannelSubscribers() {

    Runnable runnable = () -> subscriber.psubscribe(mockSubscriber, "f*");
    Thread patternSubscriberThread = new Thread(runnable);

    patternSubscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    List<String> result = introspector.pubsubChannels("f*");

    assertThat(result).isEmpty();

    mockSubscriber.punsubscribe();
  }

  @Test
  public void channels_shouldReturnListOfMatchingChannels_withActiveChannelSubscribers_whenCalledWithPattern() {

    List<String> expectedChannels = new ArrayList<>();
    expectedChannels.add("foo");

    Runnable runnable =
        () -> subscriber.subscribe(mockSubscriber, "foo", "bar");

    Thread subscriberThread = new Thread(runnable);
    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);
    List<String> result = introspector.pubsubChannels("fo*");

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);
  }

  @Test
  public void channels_should_returnEmptyArray_givenPatternWithNoMatches() {
    List<String> result = introspector.pubsubChannels("fo*");

    assertThat(result).isEmpty();
  }

  @Test
  public void channels_shouldOnlyReturnChannelsWithActiveSubscribers() {
    List<byte[]> expectedChannels = new ArrayList<>();
    expectedChannels.add(Coder.stringToBytes("bar"));
    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "foo", "bar");
    Thread subscriberThread = new Thread(runnable);

    subscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);
    mockSubscriber.unsubscribe("foo");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    List<byte[]> result =
        uncheckedCast(introspector.sendCommand(Protocol.Command.PUBSUB, PUBSUB_CHANNELS));

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);
  }

  @Test
  public void channels_shouldNotReturnDuplicates_givenMultipleSubscribersToSameChannel_whenCalledWithoutPattern() {

    Jedis subscriber2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);

    MockSubscriber mockSubscriber2 = new MockSubscriber();
    List<byte[]> expectedChannels = new ArrayList<>();
    expectedChannels.add(Coder.stringToBytes("foo"));

    Runnable runnable = () -> subscriber.subscribe(mockSubscriber, "foo");
    Thread subscriber1Thread = new Thread(runnable);
    subscriber1Thread.start();

    Runnable runnable2 = () -> subscriber2.subscribe(mockSubscriber2, "foo");
    Thread subscriber2Thread = new Thread(runnable2);
    subscriber2Thread.start();

    waitFor(() -> (mockSubscriber.getSubscribedChannels() == 1)
        && (mockSubscriber2.getSubscribedChannels() == 1));

    List<byte[]> result =
        uncheckedCast(introspector
            .sendCommand(Protocol.Command.PUBSUB, PUBSUB_CHANNELS));

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);
    assertThat(result.size()).isEqualTo(1);

    mockSubscriber2.unsubscribe();
    waitFor(() -> mockSubscriber2.getSubscribedChannels() == 0);

    subscriber2.close();
  }

  /** -- NUMSUB-- **/

  @Test
  public void numsub_shouldReturnEmptyList_whenCalledWithOutChannelNames() {
    Runnable fooRunnable =
        () -> subscriber.subscribe(mockSubscriber, "foo");
    Thread fooSubscriberThread = new Thread(fooRunnable);
    fooSubscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    List<Object> result =
        uncheckedCast(
            introspector.sendCommand(Protocol.Command.PUBSUB, PUBSUB_NUMSUB));

    assertThat(result).isEmpty();
  }


  @Test
  public void numsub_shouldReturnListOfChannelsWithSubscriberCount_whenCalledWithActiveChannels() {

    Jedis subscriber2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    MockSubscriber fooAndBarSubscriber = new MockSubscriber();

    Runnable fooRunnable =
        () -> subscriber.subscribe(mockSubscriber, "foo");
    Thread fooSubscriberThread = new Thread(fooRunnable);
    fooSubscriberThread.start();

    Runnable fooAndBarRunnable =
        () -> subscriber2.subscribe(fooAndBarSubscriber, "foo", "bar");
    Thread fooAndBarSubscriberThread = new Thread(fooAndBarRunnable);
    fooAndBarSubscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1
        && fooAndBarSubscriber.getSubscribedChannels() == 2);

    HashMap<String, String> result =
        (HashMap<String, String>) introspector.pubsubNumSub("foo", "bar");

    assertThat(result.containsKey("foo")).isTrue();
    assertThat(result.containsKey("bar")).isTrue();
    assertThat(result.get("foo")).isEqualTo("2");
    assertThat(result.get("bar")).isEqualTo("1");

    fooAndBarSubscriber.unsubscribe();

    waitFor(() -> fooAndBarSubscriber.getSubscribedChannels() == 0);
    subscriber2.close();
  }

  @Test
  public void numsub_shouldReturnChannelNameWithZero_whenCalledWithChannelWithNoSubscribers() {
    Runnable fooRunnable =
        () -> subscriber.subscribe(mockSubscriber, "foo");
    Thread fooSubscriberThread = new Thread(fooRunnable);
    fooSubscriberThread.start();

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    HashMap<String, String> result =
        (HashMap<String, String>) introspector.pubsubNumSub("bar");

    assertThat(result.containsKey("bar")).isTrue();
    assertThat(result.get("bar")).isEqualTo("0");
  }

  @Test
  public void numsub_shouldReturnPatternWithZero_whenCalledWithPatternWithNoChannelSubscribers() {
    Runnable runnable = () -> subscriber.psubscribe(mockSubscriber, "f*");
    Thread patternSubscriberThread = new Thread(runnable);

    patternSubscriberThread.start();
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    HashMap<String, String> result =
        (HashMap<String, String>) introspector.pubsubNumSub("f*");


    assertThat(result.containsKey("f*")).isTrue();
    assertThat(result.get("f*")).isEqualTo("0");
    mockSubscriber.punsubscribe();
  }

  private void waitFor(Callable<Boolean> booleanCallable) {
    GeodeAwaitility.await()
        .ignoreExceptionsInstanceOf(SocketException.class)
        .until(booleanCallable);
  }
}
