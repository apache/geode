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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public class SubscriptionImplTest {

  private final Client client = createClient();
  private final ByteBuf buffer = mock(ByteBuf.class);
  private final SubscriptionImpl subscription = new SubscriptionImpl(client);

  private Client createClient() {
    return mock(Client.class);
  }

  @Test
  public void notShutdownByDefault() {
    assertThat(subscription.isShutdown()).isFalse();
  }

  @Test
  public void interruptingWriteShutsSubscriptionDown() throws InterruptedException {
    Thread t = new Thread(() -> subscription.writeBufferToChannel(buffer));
    t.start();
    try {
      t.interrupt();
    } finally {
      t.join();
    }
    assertThat(subscription.isShutdown()).isTrue();
  }

  @Test
  public void readyToPublishAfterWriteUnblocksWrite() throws InterruptedException {
    when(client.writeBufferToChannel(any())).thenReturn(mock(ChannelFuture.class));
    Thread t = new Thread(() -> subscription.writeBufferToChannel(buffer));
    t.start();
    try {
      subscription.readyToPublish();
    } finally {
      t.join();
    }
    assertThat(subscription.isShutdown()).isFalse();
  }

  @Test
  public void readyToPublishBeforeWriteAllowsWrite() {
    when(client.writeBufferToChannel(any())).thenReturn(mock(ChannelFuture.class));
    subscription.readyToPublish();
    subscription.writeBufferToChannel(buffer);
    assertThat(subscription.isShutdown()).isFalse();
  }
}
