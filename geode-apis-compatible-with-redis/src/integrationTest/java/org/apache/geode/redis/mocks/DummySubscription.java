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

package org.apache.geode.redis.mocks;

import java.util.List;

import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.pubsub.Subscription;

public class DummySubscription implements Subscription {

  @Override
  public Type getType() {
    return Type.CHANNEL;
  }

  @Override
  public boolean isEqualTo(Object channelOrPattern, Client client) {
    return false;
  }

  @Override
  public void publishMessage(byte[] channel, byte[] message) {}

  @Override
  public boolean matchesClient(Client client) {
    return false;
  }

  @Override
  public boolean matches(byte[] channel) {
    return false;
  }

  @Override
  public List<Object> createResponse(byte[] channel, byte[] message) {
    return null;
  }

  @Override
  public byte[] getSubscriptionName() {
    return null;
  }

  @Override
  public void shutdown() {}
}
