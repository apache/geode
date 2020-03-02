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

/**
 * This class represents a single channel subscription as created by the SUBSCRIBE command
 */
class ChannelSubscription extends AbstractSubscription {
  private String channel;

  public ChannelSubscription(Client client, String channel, ExecutionHandlerContext context) {
    super(client, context);

    if (channel == null) {
      throw new IllegalArgumentException("channel cannot be null");
    }
    this.channel = channel;
  }

  @Override
  public List<String> createResponse(String channel, String message) {
    return Arrays.asList("message", channel, message);
  }

  @Override
  public boolean isEqualTo(Object channelOrPattern, Client client) {
    return this.channel != null && this.channel.equals(channelOrPattern)
        && this.getClient().equals(client);
  }

  @Override
  public boolean matches(String channel) {
    return this.channel.equals(channel);
  }

}
