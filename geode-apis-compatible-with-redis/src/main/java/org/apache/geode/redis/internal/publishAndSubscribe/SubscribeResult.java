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

package org.apache.geode.redis.internal.publishAndSubscribe;

public class SubscribeResult {
  private final Subscription subscription;
  private final long channelCount;
  private final byte[] channel;

  public SubscribeResult(Subscription subscription, long channelCount, byte[] channel) {
    this.subscription = subscription;
    this.channelCount = channelCount;
    this.channel = channel;
  }

  /**
   * Returns the Subscription instance this subscribe operations created; possibly null.
   */
  public Subscription getSubscription() {
    return subscription;
  }

  /**
   * returns the number of channels this subscribe operation subscribed to.
   */
  public long getChannelCount() {
    return channelCount;
  }

  public byte[] getChannel() {
    return channel;
  }
}
