/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.redis.executor.list;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.Command;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Extendable;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.Coder;

public abstract class PushExecutor extends PushXExecutor implements Extendable {

  private final int START_VALUES_INDEX = 2;
  static volatile AtomicInteger puts = new AtomicInteger(0);

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<Integer, ByteArrayWrapper> keyRegion = getOrCreateRegion(context, key, RedisDataType.REDIS_LIST);
    pushElements(key, commandElems, START_VALUES_INDEX, commandElems.size(), keyRegion, pushType(), context);
    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), listSize));
  }

  protected abstract ListDirection pushType();

}
