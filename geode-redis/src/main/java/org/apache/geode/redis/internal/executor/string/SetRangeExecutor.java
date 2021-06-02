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
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class SetRangeExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The number provided must be numeric";

  private final String ERROR_ILLEGAL_OFFSET =
      "The offset is out of range, must be greater than or equal to 0 and the offset added to the length of the value must be less than 536870911 (512MB), the maximum allowed size";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SETRANGE));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper wrapper = r.get(key);

    int offset;
    byte[] value = commandElems.get(3);
    try {
      byte[] offAr = commandElems.get(2);
      offset = Coder.bytesToInt(offAr);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
      return;
    }

    int totalLength = offset + value.length;
    if (offset < 0 || totalLength > 536870911) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_ILLEGAL_OFFSET));
      return;
    } else if (value.length == 0) {
      int length = wrapper == null ? 0 : wrapper.toBytes().length;
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), length));
      if (wrapper == null) {
        context.getRegionProvider().removeKey(key);
      }
      return;
    }

    if (wrapper == null) {
      byte[] bytes = new byte[totalLength];
      System.arraycopy(value, 0, bytes, offset, value.length);
      r.put(key, new ByteArrayWrapper(bytes));
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), bytes.length));
    } else {

      byte[] bytes = wrapper.toBytes();
      int returnLength;
      if (totalLength < bytes.length) {
        System.arraycopy(value, 0, bytes, offset, value.length);
        r.put(key, new ByteArrayWrapper(bytes));
        returnLength = bytes.length;
      } else {
        byte[] newBytes = new byte[totalLength];
        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
        System.arraycopy(value, 0, newBytes, offset, value.length);
        returnLength = newBytes.length;
        r.put(key, new ByteArrayWrapper(newBytes));
      }

      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), returnLength));
    }
  }

}
