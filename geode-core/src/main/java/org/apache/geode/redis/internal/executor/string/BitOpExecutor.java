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

public class BitOpExecutor extends StringExecutor {

  private static final String ERROR_NO_SUCH_OP = "Please specify a legal operation";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    Region<ByteArrayWrapper, ByteArrayWrapper> r = context.getRegionProvider().getStringsRegion();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.BITOP));
      return;
    }

    String operation = command.getStringKey().toUpperCase();
    ByteArrayWrapper destKey = new ByteArrayWrapper(commandElems.get(2));
    checkDataType(destKey, context);

    byte[][] values = new byte[commandElems.size() - 3][];
    int maxLength = 0;
    for (int i = 3; i < commandElems.size(); i++) {
      ByteArrayWrapper key = new ByteArrayWrapper(commandElems.get(i));
      checkDataType(key, context);
      ByteArrayWrapper value = r.get(key);
      if (value == null) {
        values[i - 3] = null;
        continue;
      }

      byte[] val = value.toBytes();
      values[i - 3] = val;
      if (val.length > maxLength) {
        maxLength = val.length;
        byte[] tmp = values[0];
        values[0] = val;
        values[i - 3] = tmp;
      }
      if (i == 3 && operation.equalsIgnoreCase("NOT"))
        break;
    }


    if (operation.equals("AND"))
      and(context, r, destKey, values, maxLength);
    else if (operation.equals("OR"))
      or(context, r, destKey, values, maxLength);
    else if (operation.equals("XOR"))
      xor(context, r, destKey, values, maxLength);
    else if (operation.equals("NOT"))
      not(context, r, destKey, values, maxLength);
    else {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NO_SUCH_OP));
      return;
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), maxLength));
  }

  private void and(ExecutionHandlerContext context, Region<ByteArrayWrapper, ByteArrayWrapper> r,
      ByteArrayWrapper destKey, byte[][] values, int max) {
    byte[] dest = new byte[max];
    outer: for (int i = 0; i < max; i++) {
      byte b = values[0][i];
      for (int j = 1; j < values.length; j++) {
        if (values[j] == null) {
          break outer;
        } else if (i < values[j].length)
          b &= values[j][i];
        else
          b &= 0;
      }
      dest[i] = b;
    }
    checkAndSetDataType(destKey, context);
    r.put(destKey, new ByteArrayWrapper(dest));
  }

  private void or(ExecutionHandlerContext context, Region<ByteArrayWrapper, ByteArrayWrapper> r,
      ByteArrayWrapper destKey, byte[][] values, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = values[0][i];
      for (int j = 1; j < values.length; j++) {
        byte[] cA = values[j];
        if (cA != null && i < cA.length)
          b |= cA[i];
        else
          b |= 0;
      }
      dest[i] = b;
    }
    checkAndSetDataType(destKey, context);
    r.put(destKey, new ByteArrayWrapper(dest));
  }

  private void xor(ExecutionHandlerContext context, Region<ByteArrayWrapper, ByteArrayWrapper> r,
      ByteArrayWrapper destKey, byte[][] values, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = values[0][i];
      for (int j = 1; j < values.length; j++) {
        byte[] cA = values[j];
        if (cA != null && i < cA.length)
          b ^= cA[i];
        else
          b ^= 0;
      }
      dest[i] = b;
    }
    checkAndSetDataType(destKey, context);
    r.put(destKey, new ByteArrayWrapper(dest));
  }

  private void not(ExecutionHandlerContext context, Region<ByteArrayWrapper, ByteArrayWrapper> r,
      ByteArrayWrapper destKey, byte[][] values, int max) {
    byte[] dest = new byte[max];
    byte[] cA = values[0];
    for (int i = 0; i < max; i++) {
      if (cA == null)
        dest[i] = ~0;
      else
        dest[i] = (byte) (~cA[i] & 0xFF);
    }
    checkAndSetDataType(destKey, context);
    r.put(destKey, new ByteArrayWrapper(dest));
  }

}
