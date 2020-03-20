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

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class GetSetExecutor extends StringExecutor {

  private final int VALUE_INDEX = 2;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() != 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.GETSET));
      return;
    }

    Region<ByteArrayWrapper, ByteArrayWrapper> region =
        context.getRegionProvider().getStringsRegion();

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);

    byte[] newCharValue = commandElems.get(VALUE_INDEX);
    ByteArrayWrapper newValueWrapper = new ByteArrayWrapper(newCharValue);
    ByteArrayWrapper oldValueWrapper;
    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      oldValueWrapper = region.get(key);
      if (oldValueWrapper != null) {
        try {
          checkDataType(oldValueWrapper, RedisDataType.REDIS_STRING, context);
        } catch (Exception e) {
          command.setResponse(
              Coder.getErrorResponse(context.getByteBufAllocator(),
                  RedisConstants.ERROR_WRONG_TYPE));
          return;
        }
      }
      region.put(key, newValueWrapper);
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
    respondBulkStrings(command, context, oldValueWrapper);
  }
}
