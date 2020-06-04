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

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisResponse;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;

public class GetBitExecutor extends StringExecutor {

  private final String ERROR_NOT_INT = "The offset provided must be numeric";

  @Override
  public RedisResponse executeCommandWithResponse(Command command,
      ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      return RedisResponse.error(ArityDef.GETBIT);
    }

    ByteArrayWrapper key = command.getKey();

    ByteArrayWrapper wrapper = getRedisStringCommands(context).get(key);
    if (wrapper == null) {
      return RedisResponse.integer(0);
    }

    int bit = 0;
    byte[] bytes = wrapper.toBytes();
    int offset;
    try {
      byte[] offAr = commandElems.get(2);
      offset = Coder.bytesToInt(offAr);
    } catch (NumberFormatException e) {
      return RedisResponse.error(ERROR_NOT_INT);
    }
    if (offset < 0) {
      offset += bytes.length * 8;
    }

    if (offset < 0 || offset > bytes.length * 8) {
      return RedisResponse.integer(0);
    }

    int byteIndex = offset / 8;
    offset %= 8;

    if (byteIndex >= bytes.length) {
      return RedisResponse.integer(0);
    }

    bit = (bytes[byteIndex] & (0x80 >> offset)) >> (7 - offset);
    return RedisResponse.integer(bit);
  }

}
