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

package org.apache.geode.redis.internal;

import org.apache.geode.redis.internal.org.apache.hadoop.fs.GlobPattern;

/**
 * Interface that represents the ability to Publish, Subscribe and Unsubscribe from channels.
 */
public interface PubSub {

  /**
   * Publish a message on a channel
   *
   * @param channel to publish to
   * @param message to publish
   * @return the number of messages published
   */
  long publish(String channel, byte[] message);

  /**
   * Subscribe to a channel
   *
   * @param channel to subscribe to
   * @param context ExecutionHandlerContext which will handle the client response
   * @param client a Client instance making the request
   * @return the number of channels subscribed to
   */
  long subscribe(String channel, ExecutionHandlerContext context, Client client);

  /**
   * Subscribe to a pattern
   *
   * @param pattern glob pattern to subscribe to
   * @param context ExecutionHandlerContext which will handle the client response
   * @param client a Client instance making the request
   * @return the number of channels subscribed to
   */
  long psubscribe(GlobPattern pattern, ExecutionHandlerContext context, Client client);

  /**
   * Unsubscribe a client from a channel
   *
   * @param channel the channel to unsubscribe from
   * @param client the Client which is to be unsubscribed
   * @return the number of channels still subscribed to by the client
   */
  long unsubscribe(String channel, Client client);

  /**
   * Unsubscribe from a previously subscribed pattern
   *
   * @param pattern the channel to unsubscribe from
   * @param client the Client which is to be unsubscribed
   * @return the number of channels still subscribed to by the client
   */
  long punsubscribe(GlobPattern pattern, Client client);
}
