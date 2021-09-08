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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public class AbstractSubscriptionTest {
  private static class TestableAbstractSubscription extends AbstractSubscription {
    TestableAbstractSubscription(Client client, byte[] subscriptionName) {
      super(client, subscriptionName);
    }

    @Override
    protected List<Object> createResponse(byte[] channel, byte[] message) {
      return Arrays.asList(channel, message);
    }
  }

  private final byte[] subscriptionName = stringToBytes("subscription");
  private final Client client = createClient();
  private final AbstractSubscription subscription =
      new TestableAbstractSubscription(client, subscriptionName);

  private Client createClient() {
    Client result = mock(Client.class);
    return result;
  }

  @Test
  public void gettors() {
    assertThat(subscription.getSubscriptionName()).isEqualTo(subscriptionName);
    assertThat(subscription.getClient()).isEqualTo(client);
  }

  @Test
  public void notShutdownByDefault() {
    assertThat(subscription.isShutdown()).isFalse();
  }

  @Test
  public void interruptingPublishShutsSubscriptionDown() throws InterruptedException {
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");

    Thread t = new Thread(() -> subscription.publishMessage(channel, message));
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

    Thread t = new Thread(() -> subscription.publishMessage(channel, message));
    t.start();
    try {
      subscription.readyToPublish();
    } finally {
      t.join();
    }
    assertThat(subscription.isShutdown()).isFalse();
    verify(client, times(1)).writeToChannel(any());
  }

}
