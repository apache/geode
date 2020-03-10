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

import java.util.Arrays;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class GetRangeExecutor extends StringExecutor {

  private static final String ERROR_NOT_INT = "value is not an integer or out of range";
  private static final int startIndex = 2;
  private static final int stopIndex = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() != 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.GETRANGE));
      return;
    }

    long start;
    long end;

    try {
      byte[] startI = commandElems.get(startIndex);
      byte[] stopI = commandElems.get(stopIndex);
      start = Coder.bytesToLong(startI);
      end = Coder.bytesToLong(stopI);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
      return;
    }

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();
    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_STRING, context);
    ByteArrayWrapper valueWrapper = r.get(key);

    if (valueWrapper == null) {
      command.setResponse(Coder.getEmptyStringResponse(context.getByteBufAllocator()));
      return;
    }

    byte[] value = valueWrapper.toBytes();
    int length = value.length;

    start = getBoundedStartIndex(start, length);
    end = getBoundedEndIndex(end, length);

    /*
     * Can't 'start' at end of value
     */
    if (start > end || start == length) {
      command.setResponse(Coder.getEmptyStringResponse(context.getByteBufAllocator()));
      return;
    }
    /*
     * 1 is added to end because the end in copyOfRange is exclusive but in Redis it is inclusive
     */
    if (end != length) {
      end++;
    }
    byte[] returnRange = Arrays.copyOfRange(value, (int) start, (int) end);
    if (returnRange == null || returnRange.length == 0) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    respondBulkStrings(command, context, returnRange);
  }
}
