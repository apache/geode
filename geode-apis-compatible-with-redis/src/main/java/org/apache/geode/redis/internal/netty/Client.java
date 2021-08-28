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

package org.apache.geode.redis.internal.netty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;


public class Client {
  private final Channel channel;
  private final Set<byte[]> channelSubscriptions =
      new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);
  private final Set<byte[]> patternSubscriptions =
      new ObjectOpenCustomHashSet<>(ByteArrays.HASH_STRATEGY);

  public Client(Channel remoteAddress) {
    this.channel = remoteAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Client client = (Client) o;
    return Objects.equals(channel, client.channel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(channel);
  }

  public void addShutdownListener(
      GenericFutureListener<? extends Future<? super Void>> shutdownListener) {
    channel.closeFuture().addListener(shutdownListener);
  }

  public String toString() {
    return channel.toString();
  }

  public boolean hasSubscriptions() {
    return !channelSubscriptions.isEmpty() || !patternSubscriptions.isEmpty();
  }

  public long getSubscriptionCount() {
    return channelSubscriptions.size() + patternSubscriptions.size();
  }

  public void clearSubscriptions() {
    channelSubscriptions.clear();
    patternSubscriptions.clear();
  }

  public boolean addChannelSubscription(byte[] channel) {
    return channelSubscriptions.add(channel);
  }

  public boolean addPatternSubscription(byte[] pattern) {
    return patternSubscriptions.add(pattern);
  }

  public boolean removeChannelSubscription(byte[] channel) {
    return channelSubscriptions.remove(channel);
  }

  public boolean removePatternSubscription(byte[] pattern) {
    return patternSubscriptions.remove(pattern);
  }

  public List<byte[]> getChannelSubscriptions() {
    if (channelSubscriptions.isEmpty()) {
      return Collections.emptyList();
    }
    return new ArrayList<>(channelSubscriptions);
  }

  public List<byte[]> getPatternSubscriptions() {
    if (patternSubscriptions.isEmpty()) {
      return Collections.emptyList();
    }
    return new ArrayList<>(patternSubscriptions);
  }
}
