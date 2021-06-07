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

package org.apache.geode.redis.internal.publishAndSubscribe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;


public class PubSubImplJUnitTest {

  @Test
  public void testSubscriptionWithDeadClientIsPruned() {
    Subscriptions subscriptions = new Subscriptions();
    ExecutionHandlerContext mockContext = mock(ExecutionHandlerContext.class);

    FailingChannelFuture mockFuture = new FailingChannelFuture();
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

  @SuppressWarnings("unchecked")
  static class FailingChannelFuture extends DefaultChannelPromise {
    private GenericFutureListener listener;

    FailingChannelFuture() {
      super(mock(Channel.class));
    }

    @Override
    public ChannelPromise addListener(
        GenericFutureListener<? extends Future<? super Void>> listener) {
      this.listener = listener;
      fail();
      return null;
    }

    @Override
    public ChannelPromise syncUninterruptibly() {
      return this;
    }

    @Override
    public Throwable cause() {
      return new RuntimeException("aeotunhasoen");
    }

    public void fail() {
      try {
        this.listener.operationComplete(this);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
