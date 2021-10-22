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

/**
 * An instance of this interface keeps track of all the clients
 * that have active subscriptions to the channel or pattern that
 * this instance represents.
 * In a given manager all of its subscriptions will have the same name.
 */
interface ClientSubscriptionManager {
  /**
   * return how many subscriptions this manager has.
   */
  int getSubscriptionCount();

  /**
   * Adds the given subscription for the given client to this manager.
   * If this manager already has a subscription
   * for that client then the old subscription is
   * removed and the new one is added.
   *
   * @return true if added or already added; false if caller should retry
   */
  boolean add(Client client, Subscription subscription);

  /**
   * Remove any subscription added for the given client.
   *
   * @return true if removed false if not removed
   */
  boolean remove(Client client);

  /**
   * The returned collection MUST be a live view of the subscriptions
   * in the manager. This means that if a subscription is added or
   * removed to the manager in the future then the returned Collection
   * will know about that change.
   */
  Collection<Subscription> getSubscriptions();
}
