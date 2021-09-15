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



import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.pubsub.Subscriptions.ForEachConsumer;

class PatternSubscriptionManager
    extends AbstractSubscriptionManager {

  @Override
  protected ClientSubscriptionManager createClientManager(
      Client client, byte[] patternBytes, Subscription subscription) {
    return new PatternClientSubscriptionManager(client, patternBytes, subscription);
  }

  @Override
  public int getSubscriptionCount(byte[] channel) {
    int result = 0;
    for (ClientSubscriptionManager manager : clientManagers.values()) {
      result += manager.getSubscriptionCount(channel);
    }
    return result;
  }

  @Override
  public void foreachSubscription(byte[] channel, ForEachConsumer action) {
    clientManagers.forEach((id, manager) -> manager.forEachSubscription(id.getSubscriptionIdBytes(),
        channel, action));
  }

  @Override
  protected boolean addToClient(Client client, byte[] pattern) {
    return client.addPatternSubscription(pattern);
  }

  @Override
  public void remove(Client client) {
    client.getPatternSubscriptions().forEach(
        channel -> remove(channel, client));
  }
}
