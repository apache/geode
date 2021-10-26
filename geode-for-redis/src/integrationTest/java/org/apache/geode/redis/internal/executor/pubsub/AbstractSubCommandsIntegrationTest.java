
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

package org.apache.geode.redis.internal.executor.pubsub;

import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtLeastNArgs;
import static org.apache.geode.redis.RedisCommandArgumentsTestHelper.assertAtMostNArgsForSubCommand;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_UNKNOWN_PUBSUB_SUBCOMMAND;
import static org.apache.geode.redis.internal.executor.pubsub.AbstractSubscriptionsIntegrationTest.REDIS_CLIENT_TIMEOUT;
import static org.apache.geode.test.dunit.rules.RedisClusterStartupRule.BIND_ADDRESS;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.Protocol.PUBSUB_CHANNELS;
import static redis.clients.jedis.Protocol.PUBSUB_NUMSUB;
import static redis.clients.jedis.Protocol.PUBSUB_NUM_PAT;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import org.apache.geode.redis.RedisIntegrationTest;
import org.apache.geode.redis.mocks.MockSubscriber;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public abstract class AbstractSubCommandsIntegrationTest implements RedisIntegrationTest {

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private Jedis subscriber;
  private Jedis introspector;
  private MockSubscriber mockSubscriber;

  @Before
  public void setup() {
    mockSubscriber = new MockSubscriber();
    subscriber = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    introspector = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
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
        PUBSUB_CHANNELS.getBytes(),
        1);
  }

  @Test
  public void channels_shouldNotError_givenMixedCaseArguments() {
    List<byte[]> expectedChannels = new ArrayList<>();
    expectedChannels.add("foo".getBytes());
    expectedChannels.add("bar".getBytes());

    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo", "bar"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    List<byte[]> result =
        uncheckedCast(introspector.sendCommand(Protocol.Command.PUBSUB, "cHaNNEls"));

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);

    unsubscribeWithSuccess(mockSubscriber);
  }

  @Test
  public void channels_shouldReturnListOfAllChannels_withActiveChannelSubscribers_whenCalledWithoutPattern() {
    List<byte[]> expectedChannels = new ArrayList<>();
    expectedChannels.add("foo".getBytes());
    expectedChannels.add("bar".getBytes());

    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo", "bar"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    List<byte[]> result =
        uncheckedCast(introspector.sendCommand(Protocol.Command.PUBSUB, PUBSUB_CHANNELS));

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);

    unsubscribeWithSuccess(mockSubscriber);
  }

  @Test
  public void channels_shouldNeverReturnPsubscribedChannels_givenNoActiveChannelSubscribers() {
    executor.submit(() -> subscriber.psubscribe(mockSubscriber, "f*"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    List<String> result = introspector.pubsubChannels("f*");

    assertThat(result).isEmpty();

    punsubscribeWithSuccess(mockSubscriber);
  }


  @Test
  public void channels_shouldReturnListOfMatchingChannels_withActiveChannelSubscribers_whenCalledWithPattern() {
    List<String> expectedChannels = new ArrayList<>();
    expectedChannels.add("foo");

    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo", "bar"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    List<String> result = introspector.pubsubChannels("fo*");

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);

    unsubscribeWithSuccess(mockSubscriber);
  }

  @Test
  public void channels_should_returnEmptyArray_givenPatternWithNoMatches() {
    List<String> result = introspector.pubsubChannels("fo*");

    assertThat(result).isEmpty();
  }

  @Test
  public void channels_shouldOnlyReturnChannelsWithActiveSubscribers() {
    List<byte[]> expectedChannels = new ArrayList<>();
    expectedChannels.add("bar".getBytes());

    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo", "bar"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);
    mockSubscriber.unsubscribe("foo");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    List<byte[]> result =
        uncheckedCast(introspector.sendCommand(Protocol.Command.PUBSUB, PUBSUB_CHANNELS));

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);

    unsubscribeWithSuccess(mockSubscriber);
  }

  @Test
  public void channels_shouldNotReturnDuplicates_givenMultipleSubscribersToSameChannel_whenCalledWithoutPattern() {
    Jedis subscriber2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);

    MockSubscriber mockSubscriber2 = new MockSubscriber();
    List<byte[]> expectedChannels = new ArrayList<>();
    expectedChannels.add("foo".getBytes());

    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo"));
    executor.submit(() -> subscriber2.subscribe(mockSubscriber2, "foo"));

    waitFor(() -> (mockSubscriber.getSubscribedChannels() == 1)
        && (mockSubscriber2.getSubscribedChannels() == 1));

    List<byte[]> result =
        uncheckedCast(introspector.sendCommand(Protocol.Command.PUBSUB, PUBSUB_CHANNELS));

    assertThat(result).containsExactlyInAnyOrderElementsOf(expectedChannels);
    assertThat(result.size()).isEqualTo(1);

    unsubscribeWithSuccess(mockSubscriber);
    unsubscribeWithSuccess(mockSubscriber2);
    subscriber2.close();
  }

  /** -- NUMSUB-- **/

  @Test
  public void numsub_shouldReturnEmptyList_whenCalledWithOutChannelNames() {
    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    List<Object> result =
        uncheckedCast(introspector.sendCommand(Protocol.Command.PUBSUB, PUBSUB_NUMSUB));

    assertThat(result).isEmpty();

    unsubscribeWithSuccess(mockSubscriber);
  }

  @Test
  public void numsub_shouldReturnListOfChannelsWithSubscriberCount_whenCalledWithActiveChannels() {
    Jedis subscriber2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    MockSubscriber fooAndBarSubscriber = new MockSubscriber();

    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo"));
    executor.submit(() -> subscriber2.subscribe(fooAndBarSubscriber, "foo", "bar"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1
        && fooAndBarSubscriber.getSubscribedChannels() == 2);

    Map<String, String> result = introspector.pubsubNumSub("foo", "bar");

    assertThat(result.containsKey("foo")).isTrue();
    assertThat(result.containsKey("bar")).isTrue();
    assertThat(result.get("foo")).isEqualTo("2");
    assertThat(result.get("bar")).isEqualTo("1");

    unsubscribeWithSuccess(mockSubscriber);
    unsubscribeWithSuccess(fooAndBarSubscriber);
    subscriber2.close();
  }

  @Test
  public void numsub_shouldReturnChannelNameWithZero_whenCalledWithChannelWithNoSubscribers() {
    executor.submit(() -> subscriber.subscribe(mockSubscriber, "foo"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    Map<String, String> result = introspector.pubsubNumSub("bar");

    assertThat(result.containsKey("bar")).isTrue();
    assertThat(result.get("bar")).isEqualTo("0");

    unsubscribeWithSuccess(mockSubscriber);
  }

  @Test
  public void numsub_shouldReturnZero_whenCalledWithPatternWithNoChannelSubscribers() {
    executor.submit(() -> subscriber.psubscribe(mockSubscriber, "f*"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);

    Map<String, String> result = introspector.pubsubNumSub("f*");

    assertThat(result.containsKey("f*")).isTrue();
    assertThat(result.get("f*")).isEqualTo("0");

    punsubscribeWithSuccess(mockSubscriber);
  }

  @Test
  public void numsub_shouldReturnSubscriberCount_whenCalledWithPatternAndSubscribersExist() {
    Jedis subscriber2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    MockSubscriber fooSubscriber = new MockSubscriber();

    executor.submit(() -> subscriber.psubscribe(mockSubscriber, "f*"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    executor.submit(() -> subscriber2.subscribe(fooSubscriber, "foo"));
    waitFor(() -> fooSubscriber.getSubscribedChannels() == 1);

    Map<String, String> result = introspector.pubsubNumSub("f*");

    assertThat(result.containsKey("foo")).isFalse();
    assertThat(result.containsKey("f*")).isTrue();
    assertThat(result.get("f*")).isEqualTo("0");

    result = introspector.pubsubNumSub("foo");

    assertThat(result.containsKey("foo")).isTrue();
    assertThat(result.containsKey("f*")).isFalse();
    assertThat(result.get("foo")).isEqualTo("1");

    punsubscribeWithSuccess(mockSubscriber);
    unsubscribeWithSuccess(fooSubscriber);

    subscriber2.close();
  }

  /** -- NUMPAT-- **/

  @Test
  public void numpat_shouldError_givenTooManyArguments() {
    assertAtMostNArgsForSubCommand(introspector,
        Protocol.Command.PUBSUB,
        PUBSUB_NUM_PAT.getBytes(),
        0);
  }

  @Test
  public void numpat_shouldReturnCountOfAllPatternSubscriptions_ignoringDuplicates() {
    Jedis subscriber2 = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    MockSubscriber mockSubscriber2 = new MockSubscriber();

    executor.submit(() -> subscriber.psubscribe(mockSubscriber, "f*"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    executor.submit(() -> subscriber2.psubscribe(mockSubscriber2, "f*"));
    waitFor(() -> mockSubscriber2.getSubscribedChannels() == 1);

    Long result = introspector.pubsubNumPat();

    assertThat(result).isEqualTo(1);

    punsubscribeWithSuccess(mockSubscriber);
    punsubscribeWithSuccess(mockSubscriber2);

    subscriber2.close();
  }

  @Test
  public void numpat_shouldNotIncludeChannelSubscriptions_forDifferentClient() {
    Jedis patternSubscriberJedis = new Jedis(BIND_ADDRESS, getPort(), REDIS_CLIENT_TIMEOUT);
    MockSubscriber patternSubscriber = new MockSubscriber();

    executor.submit(() -> patternSubscriberJedis.psubscribe(patternSubscriber, "f*"));
    executor.submit(() -> subscriber.psubscribe(mockSubscriber, "b*", "f*"));

    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2
        && patternSubscriber.getSubscribedChannels() == 1);

    Long result = introspector.pubsubNumPat();

    assertThat(result).isEqualTo(2);

    punsubscribeWithSuccess(mockSubscriber);
    punsubscribeWithSuccess(patternSubscriber);

    patternSubscriberJedis.close();
  }

  @Test
  public void numpat_shouldNotIncludeChannelSubscriptions_forSameClient() {
    executor.submit(() -> subscriber.psubscribe(mockSubscriber, "f*"));
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 1);
    mockSubscriber.subscribe("foo");
    waitFor(() -> mockSubscriber.getSubscribedChannels() == 2);

    Long result = introspector.pubsubNumPat();

    assertThat(result).isEqualTo(1);

    unsubscribeWithSuccess(mockSubscriber);
    punsubscribeWithSuccess(mockSubscriber);
  }

  private void unsubscribeWithSuccess(MockSubscriber subscriber) {
    int initialSubscription = subscriber.getSubscribedChannels();
    subscriber.unsubscribe();
    waitFor(() -> subscriber.getSubscribedChannels() < initialSubscription);
  }

  private void punsubscribeWithSuccess(MockSubscriber subscriber) {
    int initialSubscription = subscriber.getSubscribedChannels();
    subscriber.punsubscribe();
    waitFor(() -> subscriber.getSubscribedChannels() < initialSubscription);
  }

  private void waitFor(Callable<Boolean> booleanCallable) {
    GeodeAwaitility.await()
        .ignoreExceptionsInstanceOf(SocketException.class)
        .until(booleanCallable);
  }

}
