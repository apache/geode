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

import java.util.Collection;

import org.apache.geode.redis.internal.netty.Client;

class ChannelSubscriptionManager
    extends AbstractSubscriptionManager {

  @Override
  public int getSubscriptionCount(byte[] channel) {
    return getClientManager(channel).getSubscriptionCount();
  }

  @Override
  protected boolean addToClient(Client client, byte[] channel) {
    return client.addChannelSubscription(channel);
  }

  @Override
  public void remove(Client client) {
    client.getChannelSubscriptions().forEach(
        channel -> remove(channel, client));
  }

  public Collection<Subscription> getChannelSubscriptions(byte[] channel) {
    return getClientManager(channel).getSubscriptions();
  }
}
