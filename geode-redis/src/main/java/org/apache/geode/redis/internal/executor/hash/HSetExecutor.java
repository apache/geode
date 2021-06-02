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

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class HSetExecutor extends HashExecutor implements Extendable {

  private final int EXISTING_FIELD = 0;

  private final int NEW_FIELD = 1;

  private final int VALUE_INDEX = 3;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion =
        getOrCreateRegion(context, key, RedisDataType.REDIS_HASH);

    byte[] byteField = commandElems.get(FIELD_INDEX);
    ByteArrayWrapper field = new ByteArrayWrapper(byteField);

    byte[] value = commandElems.get(VALUE_INDEX);

    Object oldValue;

    if (onlySetOnAbsent()) {
      oldValue = keyRegion.putIfAbsent(field, new ByteArrayWrapper(value));
    } else {
      oldValue = keyRegion.put(field, new ByteArrayWrapper(value));
    }

    if (oldValue == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NEW_FIELD));
    } else {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), EXISTING_FIELD));
    }

  }

  protected boolean onlySetOnAbsent() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.HSET;
  }
}
