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

public class SetBitExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The number provided must be numeric";

  private final String ERROR_VALUE = "The value is out of range, must be 0 or 1";

  private final String ERROR_ILLEGAL_OFFSET =
      "The offset is out of range, must be greater than or equal to 0  and at most 4294967295 (512MB)";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SETBIT));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkAndSetDataType(key, context);
    ByteArrayWrapper wrapper = r.get(key);

    long offset;
    int value;
    int returnBit = 0;
    try {
      byte[] offAr = commandElems.get(2);
      byte[] valAr = commandElems.get(3);
      offset = Coder.bytesToLong(offAr);
      value = Coder.bytesToInt(valAr);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_INT));
      return;
    }

    if (value != 0 && value != 1) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_VALUE));
      return;
    }

    if (offset < 0 || offset > 4294967295L) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_ILLEGAL_OFFSET));
      return;
    }

    int byteIndex = (int) (offset / 8);
    offset %= 8;

    if (wrapper == null) {
      byte[] bytes = new byte[byteIndex + 1];
      if (value == 1) {
        bytes[byteIndex] = (byte) (0x80 >> offset);
      }
      r.put(key, new ByteArrayWrapper(bytes));
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), 0));
    } else {

      byte[] bytes = wrapper.toBytes();
      if (byteIndex < bytes.length) {
        returnBit = (bytes[byteIndex] & (0x80 >> offset)) >> (7 - offset);
      } else {
        returnBit = 0;
      }

      if (byteIndex < bytes.length) {
        bytes[byteIndex] = value == 1 ? (byte) (bytes[byteIndex] | (0x80 >> offset))
            : (byte) (bytes[byteIndex] & ~(0x80 >> offset));
        r.put(key, new ByteArrayWrapper(bytes));
      } else {
        byte[] newBytes = new byte[byteIndex + 1];
        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
        newBytes[byteIndex] = value == 1 ? (byte) (newBytes[byteIndex] | (0x80 >> offset))
            : (byte) (newBytes[byteIndex] & ~(0x80 >> offset));
        r.put(key, new ByteArrayWrapper(newBytes));
      }

      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), returnBit));
    }

  }

}
