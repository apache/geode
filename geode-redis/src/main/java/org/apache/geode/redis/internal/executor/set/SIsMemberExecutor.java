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
package org.apache.geode.redis.internal.executor.set;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class SIsMemberExecutor extends SetExecutor {

  private final int EXISTS = 1;

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.SISMEMBER));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    ByteArrayWrapper member = new ByteArrayWrapper(commandElems.get(2));

    checkDataType(key, RedisDataType.REDIS_SET, context);
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, Boolean> keyRegion =
        (Region<ByteArrayWrapper, Boolean>) context.getRegionProvider().getRegion(key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }

    if (keyRegion.containsKey(member)) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), EXISTS));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
    }
  }

}
