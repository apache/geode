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
package org.apache.geode.redis.internal.executor.hash;

import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;

public class HGetExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HGET));
      return;
    }

    ByteArrayWrapper regioName = toEntryKey(command.getKey());

    checkDataType(regioName, RedisDataType.REDIS_HASH, context);
    Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> keyRegion =
        getRegion(context, regioName);

    if (keyRegion == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    ByteArrayWrapper key = toEntryKey(command.getKey());

    Map<ByteArrayWrapper, ByteArrayWrapper> entry = keyRegion.get(key);
    if (entry == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    ByteArrayWrapper valueWrapper = entry.get(field);

    if (valueWrapper != null) {
      command.setResponse(
          Coder.getBulkStringResponse(context.getByteBufAllocator(), valueWrapper.toBytes()));
    } else
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));

  }

}
