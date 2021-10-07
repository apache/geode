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
 */

package org.apache.geode.redis.internal.proxy;

import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.util.CharsetUtil;

import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.CoderException;

public class ClusterNodesResponseProcessor implements RedisResponseProcessor {

  private final Map<HostPort, HostPort> mappings;

  public ClusterNodesResponseProcessor(Map<HostPort, HostPort> mappings) {
    this.mappings = mappings;
  }

  @Override
  public Object process(Object message, Channel channel) {
    if (message instanceof ErrorRedisMessage) {
      return message;
    }

    ByteBuf buf = ((FullBulkStringRedisMessage) message).content();
    String input = buf.toString(CharsetUtil.UTF_8);

    for (Map.Entry<HostPort, HostPort> entry : mappings.entrySet()) {
      String findHostPort = entry.getKey().getHost() + ":" + entry.getKey().getPort();
      String replaceHostPort = entry.getValue().getHost() + ":" + entry.getValue().getPort();

      input = input.replace(findHostPort, replaceHostPort);
    }

    buf.release();

    ByteBuf response;
    try {
      response = Coder.getStringResponse(channel.alloc().buffer(), input, true);
    } catch (CoderException e) {
      throw new RuntimeException(e);
    }

    return response;
  }

}
