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
package org.apache.geode.redis.internal.executor.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;

public class MSetNXExecutor extends StringExecutor {

  private final int SET = 1;

  private final int NOT_SET = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> region =
        context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 3 || commandElems.size() % 2 == 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.MSETNX));
      return;
    }

    boolean hasEntry = false;

    Map<ByteArrayWrapper, ByteArrayWrapper> map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(keyArray);
      try {
        checkDataType(key, RedisDataType.REDIS_STRING, context);
      } catch (RedisDataTypeMismatchException e) {
        hasEntry = true;
        break;
      }
      byte[] value = commandElems.get(i + 1);
      map.put(key, new ByteArrayWrapper(value));
      if (region.containsKey(key)) {
        hasEntry = true;
        break;
      }
    }
    boolean successful = false;
    if (!hasEntry) {
      successful = true;
      for (ByteArrayWrapper k : map.keySet()) {
        try {
          checkAndSetDataType(k, context);
        } catch (RedisDataTypeMismatchException e) {
          successful = false;
          break;
        }
      }
      region.putAll(map);
    }
    if (successful) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), SET));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_SET));
    }

  }

}
