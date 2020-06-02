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
package org.apache.geode.redis.internal.executor;

import java.util.Collection;

import io.netty.buffer.ByteBuf;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisResponse;

/**
 * The AbstractExecutor is the base of all {@link Executor} types for the {@link GeodeRedisServer}.
 */
public abstract class AbstractExecutor implements Executor {

  protected long getBoundedStartIndex(long index, long size) {
    if (size < 0L) {
      throw new IllegalArgumentException("Size < 0, really?");
    }
    if (index >= 0L) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, 0);
    }
  }

  protected long getBoundedEndIndex(long index, long size) {
    if (size < 0L) {
      throw new IllegalArgumentException("Size < 0, really?");
    }
    if (index >= 0L) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, -1);
    }
  }

  protected void respondBulkStrings(Command command, ExecutionHandlerContext context,
      Object message) {
    ByteBuf rsp;
    try {
      if (message instanceof Collection) {
        rsp = Coder.getArrayResponse(context.getByteBufAllocator(),
            (Collection<?>) message);
      } else {
        rsp = Coder.getBulkStringResponse(context.getByteBufAllocator(), message);
      }
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
      return;
    }

    command.setResponse(rsp);
  }

  protected RedisResponse respondBulkStrings(Object message) {
    if (message instanceof Collection) {
      return RedisResponse.array((Collection<?>) message);
    } else {
      return RedisResponse.string((String) message);
    }
  }

  protected RedisKeyCommands getRedisKeyCommands(ExecutionHandlerContext context) {
    return new RedisKeyCommandsFunctionExecutor(context.getRegionProvider().getDataRegion());
  }

  protected Region<ByteArrayWrapper, RedisData> getDataRegion(ExecutionHandlerContext context) {
    return context.getRegionProvider().getDataRegion();
  }
}
