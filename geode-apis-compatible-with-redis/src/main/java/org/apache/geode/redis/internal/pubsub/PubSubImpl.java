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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.netty.Client;

/**
 * Concrete class that manages publish and subscribe functionality. Since Redis subscriptions
 * require a persistent connection we need to have a way to track the existing clients that are
 * expecting to receive published messages.
 */
public class PubSubImpl implements PubSub {
  private final Subscriptions subscriptions;
  private final Publisher publisher;

  public PubSubImpl(Subscriptions subscriptions, RegionProvider regionProvider) {
    this(subscriptions, new Publisher(regionProvider, subscriptions));
  }

  @VisibleForTesting
  PubSubImpl(Subscriptions subscriptions, Publisher publisher) {
    this.subscriptions = subscriptions;
    this.publisher = publisher;
  }

  @VisibleForTesting
  public int getSubscriptionCount() {
    return subscriptions.size();
  }

  @Override
  public long publish(byte[] channel, byte[] message, Client client) {
    publisher.publish(client, channel, message);
    return subscriptions.getAllSubscriptionCount(channel);
  }

  @Override
  public SubscribeResult subscribe(byte[] channel, Client client) {
    return subscriptions.subscribe(channel, client);
  }

  @Override
  public SubscribeResult psubscribe(byte[] pattern, Client client) {
    return subscriptions.psubscribe(pattern, client);
  }

  @Override
  public Collection<Collection<?>> unsubscribe(List<byte[]> channels, Client client) {
    return subscriptions.unsubscribe(channels, client);
  }

  @Override
  public Collection<Collection<?>> punsubscribe(List<byte[]> patterns, Client client) {
    return subscriptions.punsubscribe(patterns, client);
  }

  @Override
  public List<byte[]> findChannelNames() {
    return subscriptions.findChannelNames();
  }

  @Override
  public List<byte[]> findChannelNames(byte[] pattern) {
    return subscriptions.findChannelNames(pattern);
  }

  @Override
  public List<Object> findNumberOfSubscribersPerChannel(List<byte[]> names) {
    List<Object> result = new ArrayList<>(names.size() * 2);
    names.forEach(name -> {
      result.add(name);
      result.add(subscriptions.getChannelSubscriptionCount(name));
    });
    return result;
  }

  @Override
  public long findNumberOfSubscribedPatterns() {
    return subscriptions.getPatternSubscriptionCount();
  }

  @Override
  public void clientDisconnect(Client client) {
    publisher.disconnect(client);
    subscriptions.remove(client);
  }

  @Override
  public void close() {
    publisher.close();
  }
}
