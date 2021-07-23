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

import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.GlobPattern;
import org.apache.geode.redis.internal.netty.Client;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

/**
 * Interface that represents the ability to Publish, Subscribe and Unsubscribe from channels.
 */
public interface PubSub {

  /**
   * Publish a message on a channel
   *
   * @param channel to publish to
   * @param message to publish
   */
  long publish(RegionProvider regionProvider, byte[] channel, byte[] message);

  /**
   * Subscribe to a channel
   *
   * @param channel to subscribe to
   * @param context ExecutionHandlerContext which will handle the client response
   * @param client a Client instance making the request
   * @return the result of the subscribe
   */
  SubscribeResult subscribe(byte[] channel, ExecutionHandlerContext context, Client client);

  /**
   * Subscribe to a pattern
   *
   * @param pattern glob pattern to subscribe to
   * @param context ExecutionHandlerContext which will handle the client response
   * @param client a Client instance making the request
   * @return the result of the subscribe
   */
  SubscribeResult psubscribe(byte[] pattern, ExecutionHandlerContext context, Client client);

  /**
   * Unsubscribe a client from a channel
   *
   * @param channel the channel to unsubscribe from
   * @param client the Client which is to be unsubscribed
   * @return the number of channels still subscribed to by the client
   */
  long unsubscribe(byte[] channel, Client client);

  /**
   * Unsubscribe from a previously subscribed pattern
   *
   * @param pattern the channel to unsubscribe from
   * @param client the Client which is to be unsubscribed
   * @return the number of channels still subscribed to by the client
   */
  long punsubscribe(GlobPattern pattern, Client client);

  /**
   * Return a list of channel names or patterns that a client has subscribed to
   *
   * @param client the Client which is to be queried
   * @return the list of channels or patterns
   */
  List<byte[]> findSubscriptionNames(Client client, Subscription.Type type);

  /**
   * Return a list of channel names and patterns that a client has subscribed to
   *
   * @param client the Client which is to be queried
   * @return the list of channels and patterns
   */
  List<byte[]> findSubscriptionNames(Client client);

  /**
   * Return a list of all subscribed channel names (not including subscribed patterns).
   */
  List<byte[]> findChannelNames();

  /**
   * Return a list of all subscribed channels that match a pattern. This pattern is only applied to
   * channel names and not to actual subscribed patterns. For example, given that the following
   * subscriptions exist: "foo", "foobar" and "fo*" then calling this method with {@code f*} will
   * return {@code foo} and {@code foobar}.
   *
   * @param pattern the glob pattern to search for
   */
  List<byte[]> findChannelNames(byte[] pattern);

  /**
   * Return a list consisting of pairs {@code channelName, subscriptionCount}.
   *
   * @param names a list of the names to consider. This should not include any patterns.
   */
  List<Object> findNumberOfSubscribersPerChannel(List<byte[]> names);

  /**
   * Return a count of all pattern subscriptions including duplicates.
   */
  Long findNumberOfSubscribedPatterns();
}
