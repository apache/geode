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

import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.function.Consumer;
import java.util.regex.PatternSyntaxException;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.redis.internal.netty.Client;

class PatternSubscriptionManager
    extends AbstractSubscriptionManager<PatternSubscription> {
  @Override
  protected boolean addToClient(Client client, byte[] pattern) {
    return client.addPatternSubscription(pattern);
  }

  @Override
  protected PatternSubscription createSubscription(byte[] pattern, Client client) {
    return new PatternSubscription(pattern, client);
  }

  @Override
  protected ClientSubscriptionManager<PatternSubscription> emptyClientManager() {
    return EMPTY_PATTERN_MANAGER;
  }

  @Override
  protected ClientSubscriptionManager<PatternSubscription> createClientManager(
      PatternSubscription subscription) {
    try {
      return new PatternClientSubscriptionManager(subscription);
    } catch (PatternSyntaxException ex) {
      subscription.getClient().removePatternSubscription(subscription.getSubscriptionName());
      throw ex;
    }
  }

  @Override
  public int getSubscriptionCount(byte[] channel) {
    int result = 0;
    final String channelString = bytesToString(channel);
    for (ClientSubscriptionManager<PatternSubscription> manager : clientManagers.values()) {
      result += manager.getSubscriptionCount(channelString);
    }
    return result;
  }

  @Override
  public void foreachSubscription(byte[] channel, Consumer<Subscription> action) {
    final String channelString = bytesToString(channel);
    for (ClientSubscriptionManager<PatternSubscription> manager : clientManagers.values()) {
      manager.forEachSubscription(channelString, action);
    }
  }

  @Override
  public void remove(Client client) {
    client.getPatternSubscriptions().forEach(
        channel -> remove(channel, client));
  }


  @Immutable
  private static final ClientSubscriptionManager<PatternSubscription> EMPTY_PATTERN_MANAGER =
      new ClientSubscriptionManager<PatternSubscription>() {
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
        public boolean add(PatternSubscription subscription) {
          return true;
        }

        @Override
        public boolean remove(Client client) {
          return true;
        }
      };

}
