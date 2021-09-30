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

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.netty.Client;

public class PublisherTest {
  private final RegionProvider regionProvider = mock(RegionProvider.class);
  private final Subscriptions subscriptions = mock(Subscriptions.class);
  private final ExecutorService executorService = createExecutorService();
  private final Publisher publisher = new Publisher(regionProvider, subscriptions, executorService);

  private ExecutorService createExecutorService() {
    // This service will do synchronous execution
    // in the calling thread when execute is called.
    ExecutorService executorService = new ExecutorService() {
      @Override
      public void execute(Runnable command) {
        command.run();
      }

      @Override
      public void shutdown() {}

      @Override
      public List<Runnable> shutdownNow() {
        return null;
      }

      @Override
      public boolean isShutdown() {
        return false;
      }

      @Override
      public boolean isTerminated() {
        return false;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
      }

      @Override
      public <T> Future<T> submit(Callable<T> task) {
        return null;
      }

      @Override
      public <T> Future<T> submit(Runnable task, T result) {
        return null;
      }

      @Override
      public Future<?> submit(Runnable task) {
        return null;
      }

      @Override
      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
          throws InterruptedException {
        return null;
      }

      @Override
      public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout,
          TimeUnit unit) throws InterruptedException {
        return null;
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
          throws InterruptedException, ExecutionException {
        return null;
      }

      @Override
      public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return null;
      }
    };
    return executorService;
  }

  @Test
  public void defaultPublisherHasNoClients() {
    assertThat(publisher.getClientCount()).isZero();
  }

  @Test
  public void doingPublishAddsOneClient() {
    Client client = mock(Client.class);
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");

    publisher.publish(client, channel, message);

    assertThat(publisher.getClientCount()).isOne();
  }

  @Test
  public void doingTwoPublishesFromSameClientAddsOne() {
    Client client = mock(Client.class);
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");

    publisher.publish(client, channel, message);
    publisher.publish(client, channel, message);

    assertThat(publisher.getClientCount()).isOne();
  }

  @Test
  public void doingTwoPublishesFromDifferentClientsAddsBoth() {
    Client client1 = mock(Client.class);
    Client client2 = mock(Client.class);
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");

    publisher.publish(client1, channel, message);
    publisher.publish(client2, channel, message);

    assertThat(publisher.getClientCount()).isEqualTo(2);
  }

  @Test
  public void disconnectingClientsThatPublishedSetCountToZero() {
    Client client1 = mock(Client.class);
    Client client2 = mock(Client.class);
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");

    publisher.publish(client1, channel, message);
    publisher.publish(client2, channel, message);
    publisher.disconnect(client1);
    publisher.disconnect(client2);

    assertThat(publisher.getClientCount()).isZero();
  }

  @Test
  public void closingPublisherDoesNotFail() {
    Client client1 = mock(Client.class);
    Client client2 = mock(Client.class);
    byte[] channel = stringToBytes("channel");
    byte[] message = stringToBytes("message");

    publisher.publish(client1, channel, message);
    publisher.publish(client2, channel, message);
    publisher.close();
  }


  @Test
  public void publishesCallSubscriptionsInCorrectOrder() {
    Client client = mock(Client.class);
    byte[] channel1 = stringToBytes("channel1");
    byte[] message = stringToBytes("message");
    byte[] channel2 = stringToBytes("channel2");
    byte[] channel3 = stringToBytes("channel3");

    publisher.publish(client, channel1, message);
    publisher.publish(client, channel2, message);
    publisher.publish(client, channel3, message);

    ArgumentCaptor<byte[]> channelCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(subscriptions, times(3)).forEachSubscription(channelCaptor.capture(), any());
    assertThat(channelCaptor.getAllValues()).containsExactly(channel1, channel2, channel3);
  }
}
