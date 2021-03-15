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

package org.apache.geode.redis.internal.netty;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.redis.ArrayHeaderRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;

public class MessageToCommandDecoder extends MessageToMessageDecoder<RedisMessage> {

  private static final ThreadLocal<LocalRedisArray> currentCommand =
      ThreadLocal.withInitial(LocalRedisArray::new);

  @Override
  protected void decode(ChannelHandlerContext ctx, RedisMessage msg, List<Object> out)
      throws Exception {
    LocalRedisArray array = currentCommand.get();

    if (msg instanceof ArrayHeaderRedisMessage) {
      array.count = ((ArrayHeaderRedisMessage) msg).length();
      return;
    }

    if (msg instanceof FullBulkStringRedisMessage) {
      ByteBuf buffer = ((FullBulkStringRedisMessage) msg).content();
      byte[] data = new byte[buffer.readableBytes()];
      buffer.readBytes(data);
      array.list.add(data);
      array.count--;

      if (array.count == 0) {
        out.add(new Command(array.list));
        currentCommand.remove();
      }
    }
  }

  private static class LocalRedisArray {
    List<byte[]> list = new ArrayList<>();
    long count = 0;
  }
}
