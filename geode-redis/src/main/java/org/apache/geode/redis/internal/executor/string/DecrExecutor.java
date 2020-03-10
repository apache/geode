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
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RegionProvider;

public class DecrExecutor extends StringExecutor {

  private final String ERROR_VALUE_NOT_USABLE =
      "The value at this key cannot be decremented numerically";

  private final String ERROR_OVERFLOW = "This decrementation cannot be performed due to overflow";

  private final byte[] INIT_VALUE_BYTES = Coder.stringToBytes("-1");

  private final int INIT_VALUE_INT = -1;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();
    long value;

    RegionProvider rC = context.getRegionProvider();
    Region<ByteArrayWrapper, ByteArrayWrapper> r = rC.getStringsRegion();

    if (commandElems.size() < 2) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.DECR));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);

    try (AutoCloseableLock regionLock = withRegionLock(context, key)) {
      ByteArrayWrapper valueWrapper = r.get(key);

      /*
       * Value does not exist
       */

      if (valueWrapper == null) {
        byte[] newValue = INIT_VALUE_BYTES;
        r.put(key, new ByteArrayWrapper(newValue));
        command
            .setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), INIT_VALUE_INT));
        return;
      }

      /*
       * Value exists
       */

      String stringValue = valueWrapper.toString();
      try {
        value = Long.parseLong(stringValue);
      } catch (NumberFormatException e) {
        command.setResponse(
            Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE_NOT_USABLE));
        return;
      }

      if (value == Long.MIN_VALUE) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_OVERFLOW));
        return;
      }

      value--;

      stringValue = "" + value;

      r.put(key, new ByteArrayWrapper(Coder.stringToBytes(stringValue)));
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
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), value));
  }
}
