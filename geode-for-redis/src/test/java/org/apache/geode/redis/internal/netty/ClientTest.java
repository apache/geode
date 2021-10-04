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
package org.apache.geode.redis.internal.netty;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.pubsub.PubSub;

public class ClientTest {
  private final Client client = new Client(mockChannel(), mock(PubSub.class));

  private Channel mockChannel() {
    Channel channel = mock(Channel.class);
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    return channel;
  }

  private void verifyClientIsEmpty() {
    assertThat(client.getSubscriptionCount()).isZero();
    assertThat(client.getChannelSubscriptions()).isEmpty();
    assertThat(client.getPatternSubscriptions()).isEmpty();
    assertThat(client.hasSubscriptions()).isFalse();
  }

  @Test
  public void verifyNewClientHasNoSubscriptions() {
    verifyClientIsEmpty();
  }

  @Test
  public void verifyAddingChannelSubscription() {
    final byte[] channel = stringToBytes("channel");

    boolean added = client.addChannelSubscription(channel);

    assertThat(added).isTrue();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.hasSubscriptions()).isTrue();
    assertThat(client.getChannelSubscriptions()).containsExactlyInAnyOrder(channel);
  }

  @Test
  public void verifyAddingPatternSubscription() {
    final byte[] pattern = stringToBytes("pattern");

    boolean added = client.addPatternSubscription(pattern);

    assertThat(added).isTrue();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.hasSubscriptions()).isTrue();
    assertThat(client.getPatternSubscriptions()).containsExactlyInAnyOrder(pattern);
  }

  @Test
  public void verifyAddingDuplicateChannelSubscription() {
    final byte[] channel = stringToBytes("channel");
    client.addChannelSubscription(channel);

    boolean added = client.addChannelSubscription(channel);

    assertThat(added).isFalse();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.hasSubscriptions()).isTrue();
    assertThat(client.getChannelSubscriptions()).containsExactlyInAnyOrder(channel);
  }

  @Test
  public void verifyAddingDuplicatePatternSubscription() {
    final byte[] pattern = stringToBytes("pattern");
    client.addPatternSubscription(pattern);

    boolean added = client.addPatternSubscription(pattern);

    assertThat(added).isFalse();
    assertThat(client.getSubscriptionCount()).isOne();
    assertThat(client.hasSubscriptions()).isTrue();
    assertThat(client.getPatternSubscriptions()).containsExactlyInAnyOrder(pattern);
  }

  @Test
  public void verifyRemovingNonExistentChannelSubscription() {
    final byte[] channel = stringToBytes("channel");

    boolean removed = client.removeChannelSubscription(channel);

    assertThat(removed).isFalse();
  }

  @Test
  public void verifyRemovingNonExistentPatternSubscription() {
    final byte[] pattern = stringToBytes("pattern");

    boolean removed = client.removePatternSubscription(pattern);

    assertThat(removed).isFalse();
  }

  @Test
  public void verifyRemovingExistentChannelSubscription() {
    final byte[] channel = stringToBytes("channel");
    client.addChannelSubscription(channel);

    boolean removed = client.removeChannelSubscription(channel);

    assertThat(removed).isTrue();
    verifyClientIsEmpty();
  }

  @Test
  public void verifyRemovingExistentPatternSubscription() {
    final byte[] pattern = stringToBytes("pattern");
    client.addPatternSubscription(pattern);

    boolean removed = client.removePatternSubscription(pattern);

    assertThat(removed).isTrue();
    verifyClientIsEmpty();
  }

  @Test
  public void verifyAddingTwoChannelSubscriptions() {
    final byte[] channel1 = stringToBytes("channel1");
    final byte[] channel2 = stringToBytes("channel2");

    boolean added1 = client.addChannelSubscription(channel1);
    boolean added2 = client.addChannelSubscription(channel2);

    assertThat(added1).isTrue();
    assertThat(added2).isTrue();
    assertThat(client.getSubscriptionCount()).isEqualTo(2);
    assertThat(client.hasSubscriptions()).isTrue();
    assertThat(client.getChannelSubscriptions()).containsExactlyInAnyOrder(channel1, channel2);
  }

  @Test
  public void verifyAddingTwoPatternSubscriptions() {
    final byte[] pattern1 = stringToBytes("pattern1");
    final byte[] pattern2 = stringToBytes("pattern2");

    boolean added1 = client.addPatternSubscription(pattern1);
    boolean added2 = client.addPatternSubscription(pattern2);

    assertThat(added1).isTrue();
    assertThat(added2).isTrue();
    assertThat(client.getSubscriptionCount()).isEqualTo(2);
    assertThat(client.hasSubscriptions()).isTrue();
    assertThat(client.getPatternSubscriptions()).containsExactlyInAnyOrder(pattern1, pattern2);
  }

  @Test
  public void verifyClearSubscriptionsProducesEmptyClient() {
    final byte[] pattern1 = stringToBytes("pattern1");
    final byte[] pattern2 = stringToBytes("pattern2");
    final byte[] channel1 = stringToBytes("channel1");
    final byte[] channel2 = stringToBytes("channel2");
    client.addChannelSubscription(channel1);
    client.addChannelSubscription(channel2);
    client.addPatternSubscription(pattern1);
    client.addPatternSubscription(pattern2);

    client.clearSubscriptions();

    verifyClientIsEmpty();
  }
}
