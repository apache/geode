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

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bMESSAGE;
import static org.apache.geode.redis.internal.netty.StringBytesGlossary.bPMESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.ChannelFuture;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Client;

public class SubscriptionImplTest {

  private final byte[] subscriptionName = stringToBytes("subscription");
  private final Client client = createClient();
  private final SubscriptionImpl subscription = new SubscriptionImpl();

  private Client createClient() {
    return mock(Client.class);
  }

  @Test
  public void notShutdownByDefault() {
    assertThat(subscription.isShutdown()).isFalse();
  }

  @Test
  public void interruptingPublishShutsSubscriptionDown() throws InterruptedException {
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");

    Thread t =
        new Thread(
            () -> subscription.publishMessage(false, subscriptionName, client, channel, message));
    t.start();
    try {
      t.interrupt();
    } finally {
      t.join();
    }
    assertThat(subscription.isShutdown()).isTrue();
    verify(client, never()).writeToChannel(any());
  }

  @Test
  public void readyToPublishPermitsPublish() throws InterruptedException {
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");
    when(client.writeToChannel(any())).thenReturn(mock(ChannelFuture.class));

    Thread t =
        new Thread(
            () -> subscription.publishMessage(false, subscriptionName, client, channel, message));
    t.start();
    try {
      subscription.readyToPublish();
    } finally {
      t.join();
    }
    assertThat(subscription.isShutdown()).isFalse();
    verify(client, times(1)).writeToChannel(any());
  }

  @Test
  public void publishOfNonPatternWritesExpectedResponse() {
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");
    when(client.writeToChannel(any())).thenReturn(mock(ChannelFuture.class));
    subscription.readyToPublish();
    ArgumentCaptor<RedisResponse> argCaptor = ArgumentCaptor.forClass(RedisResponse.class);

    subscription.publishMessage(false, subscriptionName, client, channel, message);

    verify(client).writeToChannel(argCaptor.capture());
    assertThat(argCaptor.getValue().toString())
        .isEqualTo(RedisResponse.array(bMESSAGE, channel, message).toString());
  }

  @Test
  public void publishOfPatternWritesExpectedResponse() {
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");
    when(client.writeToChannel(any())).thenReturn(mock(ChannelFuture.class));
    subscription.readyToPublish();
    ArgumentCaptor<RedisResponse> argCaptor = ArgumentCaptor.forClass(RedisResponse.class);

    subscription.publishMessage(true, subscriptionName, client, channel, message);

    verify(client).writeToChannel(argCaptor.capture());
    assertThat(argCaptor.getValue().toString())
        .isEqualTo(RedisResponse.array(bPMESSAGE, subscriptionName, channel, message).toString());
  }

}
