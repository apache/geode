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
package org.apache.geode.redis.internal.pubsub;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.pubsub.Subscriptions.PatternSubscriptions;

public class PatternSubscriptionManagerTest extends SubscriptionManagerTestBase {

  @Override
  protected AbstractSubscriptionManager createManager() {
    return new PatternSubscriptionManager();
  }

  @Test
  public void twoPatternsThatMatchSameChannel() {
    Client client1 = createClient();
    Client client2 = createClient();
    AbstractSubscriptionManager manager = createManager();
    byte[] channel = stringToBytes("channel");
    byte[] pattern1 = stringToBytes("ch*");
    byte[] pattern2 = stringToBytes("chan*");

    Object result1 = manager.add(pattern1, client1);
    Object result2 = manager.add(pattern2, client2);

    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
    assertThat(result1).isNotNull();
    assertThat(result2).isNotNull();
    assertThat(manager.getSubscriptionCount(channel)).isEqualTo(2);
    assertThat(manager.getIds()).containsExactlyInAnyOrder(pattern1, pattern2);
    assertThat(manager.getIds(stringToBytes("cha*"))).containsExactlyInAnyOrder(pattern2);
  }

  @Test
  public void emptyManagerReturnsEmptyPatternSubscriptions() {
    PatternSubscriptionManager manager = new PatternSubscriptionManager();
    byte[] channel = stringToBytes("channel");

    List<PatternSubscriptions> subscriptions = manager.getPatternSubscriptions(channel);
    List<PatternSubscriptions> cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(subscriptions).isEmpty();
    assertThat(cachedSubscriptions).isSameAs(subscriptions);
  }

  @Test
  public void emptyManagerDoesNotCachePatternSubscriptions() {
    PatternSubscriptionManager manager = new PatternSubscriptionManager();
    byte[] channel = stringToBytes("channel");

    List<PatternSubscriptions> subscriptions = manager.getPatternSubscriptions(channel);

    assertThat(subscriptions).isEmpty();
    assertThat(manager.cacheSize()).isZero();
  }

  @Test
  public void managerWithOneSubscriptionReturnsIt() {
    PatternSubscriptionManager manager = new PatternSubscriptionManager();
    byte[] pattern = stringToBytes("ch*");
    byte[] channel = stringToBytes("channel");
    byte[] otherChannel = stringToBytes("otherChannel");
    Client client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    Subscription addedSubscription = manager.add(pattern, client);
    PatternSubscriptions expected = new PatternSubscriptions(pattern,
        singletonList(addedSubscription));

    List<PatternSubscriptions> subscriptions = manager.getPatternSubscriptions(channel);
    List<PatternSubscriptions> cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(cachedSubscriptions).isSameAs(subscriptions);
    assertThat(subscriptions).containsExactly(expected);
    assertThat(manager.getPatternSubscriptions(otherChannel)).isEmpty();
    assertThat(manager.cacheSize()).isOne();
  }

  @Test
  public void managerWithOneSubscriptionThatDoesNotMatchChannelDoesNotCache() {
    PatternSubscriptionManager manager = new PatternSubscriptionManager();
    byte[] pattern = stringToBytes("ch*");
    byte[] otherChannel = stringToBytes("otherChannel");
    Client client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    manager.add(pattern, client);

    assertThat(manager.getPatternSubscriptions(otherChannel)).isEmpty();
    assertThat(manager.cacheSize()).isZero();
  }

  @Test
  public void clientsSubscribedToSamePattern() {
    PatternSubscriptionManager manager = new PatternSubscriptionManager();
    byte[] pattern = stringToBytes("ch*");
    byte[] channel = stringToBytes("channel");
    Client client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    Client client2 = mock(Client.class);
    when(client2.addPatternSubscription(eq(pattern))).thenReturn(true);
    Subscription addedSubscription = manager.add(pattern, client);
    Subscription addedSubscription2 = manager.add(pattern, client2);
    PatternSubscriptions expected = new PatternSubscriptions(pattern,
        asList(addedSubscription, addedSubscription2));

    List<PatternSubscriptions> subscriptions = manager.getPatternSubscriptions(channel);
    List<PatternSubscriptions> cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(cachedSubscriptions).isSameAs(subscriptions);
    assertThat(subscriptions).containsExactly(expected);
  }

  @Test
  public void clientSubscribedToTwoPatterns() {
    PatternSubscriptionManager manager = new PatternSubscriptionManager();
    byte[] pattern = stringToBytes("ch*");
    byte[] pattern2 = stringToBytes("*l");
    byte[] channel = stringToBytes("channel");
    Client client = mock(Client.class);
    when(client.addPatternSubscription(eq(pattern))).thenReturn(true);
    when(client.addPatternSubscription(eq(pattern2))).thenReturn(true);
    Subscription addedSubscription = manager.add(pattern, client);
    Subscription addedSubscription2 = manager.add(pattern2, client);
    PatternSubscriptions expected = new PatternSubscriptions(pattern,
        singletonList(addedSubscription));
    PatternSubscriptions expected2 = new PatternSubscriptions(pattern2,
        singletonList(addedSubscription2));

    List<PatternSubscriptions> subscriptions = manager.getPatternSubscriptions(channel);
    List<PatternSubscriptions> cachedSubscriptions = manager.getPatternSubscriptions(channel);

    assertThat(cachedSubscriptions).isSameAs(subscriptions);
    assertThat(subscriptions).containsExactlyInAnyOrder(expected, expected2);
  }
}
