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

package org.apache.geode.redis.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import io.netty.channel.Channel;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class SubscriptionsIntegrationTest {

  private static final int ITERATIONS = 1000;

  @ClassRule
  public static ExecutorServiceRule executor = new ExecutorServiceRule();

  private Callable<Void> functionSpinner(Consumer<Void> consumer) {
    return () -> {
      for (int i = 0; i < ITERATIONS; i++) {
        consumer.accept(null);
        Thread.yield();
      }
      return null;
    };
  }

  @AfterClass
  public static void after() {
    System.out.println("done");
  }

  @Test
  public void add_doesNotThrowException_whenListIsConcurrentlyModified()
      throws Exception {
    final Subscriptions subscriptions = new Subscriptions();

    Callable<Void> addingCallable1 =
        functionSpinner(x -> subscriptions.add(new DummySubscription()));
    Callable<Void> addingCallable2 =
        functionSpinner(x -> subscriptions.add(new DummySubscription()));

    Future<Void> addingFuture = executor.submit(addingCallable1);
    Future<Void> existsFuture = executor.submit(addingCallable2);

    addingFuture.get();
    existsFuture.get();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS * 2);
  }

  @Test
  public void exists_doesNotThrowException_whenListIsConcurrentlyModified()
      throws Exception {
    final Subscriptions subscriptions = new Subscriptions();

    Callable<Void> addingCallable =
        functionSpinner(x -> subscriptions.add(new DummySubscription()));
    Callable<Void> existsCallable =
        functionSpinner(x -> subscriptions.exists("channel", mock(Client.class)));

    Future<Void> addingFuture = executor.submit(addingCallable);
    Future<Void> existsFuture = executor.submit(existsCallable);

    addingFuture.get();
    existsFuture.get();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void findSubscriptionsByClient_doesNotThrowException_whenListIsConcurrentlyModified()
      throws Exception {
    final Subscriptions subscriptions = new Subscriptions();

    Callable<Void> addingCallable =
        functionSpinner(x -> subscriptions.add(new DummySubscription()));
    Callable<Void> findSubscriptionsCallable =
        functionSpinner(x -> subscriptions.findSubscriptions(mock(Client.class)));

    Future<Void> addingFuture = executor.submit(addingCallable);
    Future<Void> existsFuture = executor.submit(findSubscriptionsCallable);

    addingFuture.get();
    existsFuture.get();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void findSubscriptionsByChannel_doesNotThrowException_whenListIsConcurrentlyModified()
      throws Exception {
    final Subscriptions subscriptions = new Subscriptions();

    Callable<Void> addingCallable =
        functionSpinner(x -> subscriptions.add(new DummySubscription()));
    Callable<Void> findSubscriptionsCallable =
        functionSpinner(x -> subscriptions.findSubscriptions("channel"));

    Future<Void> addingFuture = executor.submit(addingCallable);
    Future<Void> existsFuture = executor.submit(findSubscriptionsCallable);

    addingFuture.get();
    existsFuture.get();

    assertThat(subscriptions.size()).isEqualTo(ITERATIONS);
  }

  @Test
  public void removeByClient_doesNotThrowException_whenListIsConcurrentlyModified()
      throws Exception {
    final Subscriptions subscriptions = new Subscriptions();

    List<Client> clients = new LinkedList<>();
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    for (int i = 0; i < ITERATIONS; i++) {
      Client client = new Client(mock(Channel.class));
      clients.add(client);
      subscriptions.add(new ChannelSubscription(client, "channel", context));
    }

    Callable<Void> removeCallable = () -> {
      clients.forEach(c -> subscriptions.remove(c));
      return null;
    };
    Callable<Void> existsCallable = () -> {
      clients.forEach(c -> subscriptions.exists("channel", c));
      return null;
    };

    Future<Void> removeFuture = executor.submit(removeCallable);
    Future<Void> existsFuture = executor.submit(existsCallable);

    removeFuture.get();
    existsFuture.get();

    assertThat(subscriptions.size()).isEqualTo(0);
  }

  @Test
  public void removeByChannelAndClient_doesNotThrowException_whenListIsConcurrentlyModified()
      throws Exception {
    final Subscriptions subscriptions = new Subscriptions();

    List<Client> clients = new LinkedList<>();
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    for (int i = 0; i < ITERATIONS; i++) {
      Client client = new Client(mock(Channel.class));
      clients.add(client);
      subscriptions.add(new ChannelSubscription(client, "channel", context));
    }

    Callable<Void> removeCallable = () -> {
      clients.forEach(c -> subscriptions.remove("channel", c));
      return null;
    };
    Callable<Void> existsCallable = () -> {
      clients.forEach(c -> subscriptions.exists("channel", c));
      return null;
    };

    Future<Void> removeFuture = executor.submit(removeCallable);
    Future<Void> existsFuture = executor.submit(existsCallable);

    removeFuture.get();
    existsFuture.get();

    assertThat(subscriptions.size()).isEqualTo(0);
  }
}
