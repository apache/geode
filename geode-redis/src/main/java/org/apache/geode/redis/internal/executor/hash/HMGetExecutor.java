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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;

public class HMGetExecutor extends HashExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.HMGET));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getRegion(context, key);
    checkDataType(key, RedisDataType.REDIS_HASH, context);

    if (keyRegion == null) {
      command.setResponse(
          Coder.getArrayOfNils(context.getByteBufAllocator(), commandElems.size() - 2));
      return;
    }

    ArrayList<ByteArrayWrapper> fields = new ArrayList<ByteArrayWrapper>();
    for (int i = 2; i < commandElems.size(); i++) {
      byte[] fieldArray = commandElems.get(i);
      ByteArrayWrapper field = new ByteArrayWrapper(fieldArray);
      fields.add(field);
    }

    Map<ByteArrayWrapper, ByteArrayWrapper> results = keyRegion.getAll(fields);

    ArrayList<ByteArrayWrapper> values = new ArrayList<ByteArrayWrapper>();

    /*
     * This is done to preserve order in the output
     */
    for (ByteArrayWrapper field : fields) {
      values.add(results.get(field));
    }

    respondBulkStrings(command, context, values);
  }
}
