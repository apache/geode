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
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataTypeMismatchException;

public class MSetExecutor extends StringExecutor {

  private final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();


    if (commandElems.size() < 3 || commandElems.size() % 2 == 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.MSET));
      return;
    }

    Region<ByteArrayWrapper, ByteArrayWrapper> region =
        context.getRegionProvider().getStringsRegion();

    Map<ByteArrayWrapper, ByteArrayWrapper> map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    for (int i = 1; i < commandElems.size(); i += 2) {
      byte[] keyArray = commandElems.get(i);
      ByteArrayWrapper key = new ByteArrayWrapper(keyArray);
      try {
        checkAndSetDataType(key, context);
      } catch (RedisDataTypeMismatchException e) {
        continue;
      }
      byte[] value = commandElems.get(i + 1);
      map.put(key, new ByteArrayWrapper(value));
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      region.putAll(map);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), "Thread interrupted."));
      return;
    } catch (TimeoutException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          "Timeout acquiring lock. Please try again."));
      return;
    }
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
  }

}
