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

import java.util.List;
import java.util.function.Consumer;

import org.apache.geode.redis.internal.netty.Client;

/**
 * Keeps track of subscriptions of a particular type S.
 */
interface SubscriptionManager<S extends Subscription> {

  /**
   * returns the number of subscriptions for the given channel.
   */
  int getSubscriptionCount(byte[] channel);

  /**
   * For each subscription on the given channel invoke the given action.
   */
  void foreachSubscription(byte[] channel, Consumer<Subscription> action);

  /**
   * returns the ids (channel or pattern) for all the subscriptions added
   * to this manager.
   */
  List<byte[]> getIds();

  /**
   * returns the ids (channel or pattern) for all the subscriptions added
   * to this manager that match the given glob pattern.
   */
  List<byte[]> getIds(byte[] pattern);

  /**
   * returns the number of subscriptions added to this manager
   */
  int getSubscriptionCount();

  /**
   * add the given subscription to this manager. If the manager already
   * has a subscription with the same id and client then the old one is
   * replaced by this one.
   */
  void add(S subscription);

  /**
   * remove and subscriptions that have been added to this manager by the
   * given client.
   */
  void remove(Client client);

  /**
   * remove a subscription with the given id (channel or pattern) and client
   * from this manager.
   */
  void remove(byte[] channelOrPattern, Client client);
}
