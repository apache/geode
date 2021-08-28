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

import java.util.function.Consumer;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.netty.Client;

class ChannelSubscriptionManager
    extends AbstractSubscriptionManager<ChannelSubscription> {
  @Override
  protected ClientSubscriptionManager<ChannelSubscription> emptyClientManager() {
    return EMPTY_CHANNEL_MANAGER;
  }

  @Override
  protected ClientSubscriptionManager<ChannelSubscription> createClientManager(
      ChannelSubscription subscription) {
    return new ChannelClientSubscriptionManager(subscription);
  }

  @Override
  public int getSubscriptionCount(byte[] channel) {
    return getClientManager(channel).getSubscriptionCount();
  }

  @Override
  public void foreachSubscription(byte[] channel, Consumer<Subscription> action) {
    getClientManager(channel).forEachSubscription(null, action);
  }

  @Override
  public void remove(Client client) {
    client.getChannelSubscriptions().forEach(
        channel -> remove(channel, client));
  }


  @Immutable
  private static final ClientSubscriptionManager<ChannelSubscription> EMPTY_CHANNEL_MANAGER =
      new ClientSubscriptionManager<ChannelSubscription>() {
        @Override
        public void forEachSubscription(String channel, Consumer<Subscription> action) {}

        @Override
        public int getSubscriptionCount() {
          return 0;
        }

        @Override
        public int getSubscriptionCount(String channel) {
          return 0;
        }

        @Override
        public boolean add(ChannelSubscription subscription) {
          return true;
        }

        @Override
        public boolean remove(Client client) {
          return true;
        }
      };

}
