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

import java.util.Arrays;
import java.util.List;

import org.apache.geode.redis.internal.org.apache.hadoop.fs.GlobPattern;

/**
 * This class represents a pattern subscription as created by the PSUBSCRIBE command
 */
class PatternSubscription extends AbstractSubscription {
  final GlobPattern pattern;

  public PatternSubscription(Client client, GlobPattern pattern, ExecutionHandlerContext context) {
    super(client, context);

    if (pattern == null) {
      throw new IllegalArgumentException("pattern cannot be null");
    }
    this.pattern = pattern;
  }

  @Override
  public List<String> createResponse(String channel, String message) {
    return Arrays.asList("pmessage", pattern.globPattern(), channel, message);
  }

  @Override
  public boolean isEqualTo(Object channelOrPattern, Client client) {
    return this.pattern != null && this.pattern.equals(channelOrPattern)
        && this.getClient().equals(client);
  }

  @Override
  public boolean matches(String channel) {
    return pattern.matches(channel);
  }

}
