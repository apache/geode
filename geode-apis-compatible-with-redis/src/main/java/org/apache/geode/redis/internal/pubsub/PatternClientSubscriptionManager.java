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

import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.geode.redis.internal.executor.GlobPattern;

class PatternClientSubscriptionManager
    extends AbstractClientSubscriptionManager<PatternSubscription> {
  private final Pattern pattern;

  public PatternClientSubscriptionManager(PatternSubscription subscription) {
    super(subscription);
    byte[] patternBytes = subscription.getSubscriptionName();
    pattern = GlobPattern.createPattern(patternBytes);
  }

  private boolean matches(String channel) {
    return GlobPattern.matches(pattern, channel);
  }

  @Override
  public int getSubscriptionCount(String channel) {
    int result = super.getSubscriptionCount();
    if (result > 0 && !matches(channel)) {
      result = 0;
    }
    return result;
  }

  @Override
  public void forEachSubscription(String channel, Consumer<Subscription> action) {
    if (getSubscriptionCount() > 0 && matches(channel)) {
      super.forEachSubscription(channel, action);
    }
  }

}
