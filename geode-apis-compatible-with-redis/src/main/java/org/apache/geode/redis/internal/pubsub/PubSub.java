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
import java.util.List;

import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.netty.Client;

/**
 * Interface that represents the ability to Publish, Subscribe and Unsubscribe from channels.
 */
public interface PubSub {

  /**
   * Publish a message on a channel and return a count of (local) clients that received the message.
   * This command is asynchronous and the caller may receive a response before subscribers
   * receive the message.
   * <p/>
   * The returned value is somewhat arbitrary and mimics what Redis does in a clustered environment.
   * Since subscribers and publishers can be connected to any member, the publish command is
   * distributed to all members of the cluster but remote subscribers are not counted in the
   * returned value.
   *
   * @param channel to publish to
   * @param message to publish
   * @return the number of subscribers to this channel that are connected to the server on which
   *         the command is executed.
   */
  long publish(RegionProvider regionProvider, byte[] channel, byte[] message, Client client);

  /**
   * Subscribe to a channel
   *
   * @param channel to subscribe to
   * @param client which will handle the client response
   * @return the result of the subscribe
   */
  SubscribeResult subscribe(byte[] channel, Client client);

  /**
   * Subscribe to a pattern
   *
   * @param pattern glob pattern to subscribe to
   * @param client which will handle the client response
   * @return the result of the subscribe
   */
  SubscribeResult psubscribe(byte[] pattern, Client client);

  /**
   * Unsubscribe a client from a channel
   *
   * @param channels the channels to unsubscribe from
   * @param client the Client which is to be unsubscribed
   * @return result will contain a nested Collection for each channel unsubscribed from.
   */
  Collection<Collection<?>> unsubscribe(List<byte[]> channels, Client client);

  /**
   * Unsubscribe from a previously subscribed pattern
   *
   * @param patterns the patterns to unsubscribe from
   * @param client the Client which is to be unsubscribed
   * @return result will contain a nested Collection for each pattern unsubscribed from.
   */
  Collection<Collection<?>> punsubscribe(List<byte[]> patterns, Client client);

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
  long findNumberOfSubscribedPatterns();
}
