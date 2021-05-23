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

package org.apache.geode.redis.internal.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.netty.channel.ChannelFuture;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PubSubImplJUnitTest {

  @Test
  public void testSubscriptionWithDeadClientIsPruned() {
    Subscriptions subscriptions = new Subscriptions();
    ExecutionHandlerContext mockContext = mock(ExecutionHandlerContext.class);

    ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockFuture.syncUninterruptibly()).thenReturn(mockFuture);
    when(mockFuture.cause()).thenReturn(new Exception());
    when(mockContext.writeToChannel(any())).thenReturn(mockFuture);

    Client deadClient = mock(Client.class);
    when(deadClient.isDead()).thenReturn(true);

    ChannelSubscription subscription =
        spy(new ChannelSubscription(deadClient, "sally".getBytes(), mockContext, subscriptions));
    subscription.readyToPublish();

    subscriptions.add(subscription);

    PubSubImpl subject = new PubSubImpl(subscriptions);

    Long numberOfSubscriptions =
        subject.publishMessageToSubscribers("sally".getBytes(), "message".getBytes());

    assertThat(numberOfSubscriptions).isEqualTo(0);
    assertThat(subscriptions.findSubscriptions(deadClient)).isEmpty();
  }
}
