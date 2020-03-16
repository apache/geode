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
package org.apache.geode.redis.internal.executor.list;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.ListQuery;

public class LIndexExecutor extends ListExecutor {

  private static final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LINDEX));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] indexArray = commandElems.get(2);

    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Object, Object> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;

    int redisIndex;

    try {
      redisIndex = Coder.bytesToInt(indexArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    /*
     * Now the fun part, converting the redis index into our index. The redis index is 0 based but
     * negative values count from the tail
     */

    if (redisIndex < 0)
    // Since the redisIndex is negative here, this will reset it to be a standard 0 based index
    {
      redisIndex = listSize + redisIndex;
    }

    /*
     * If the index is still less than 0 that means the index has shot off back past the beginning,
     * which means the index isn't real and a nil is returned
     */
    if (redisIndex < 0) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    /*
     * Now we must get that element from the region
     */
    Struct entry;
    try {
      entry = getEntryAtIndex(context, key, redisIndex);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (entry == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    Object[] entryArray = entry.getFieldValues();
    ByteArrayWrapper valueWrapper = (ByteArrayWrapper) entryArray[1];
    respondBulkStrings(command, context, valueWrapper);
  }

  private Struct getEntryAtIndex(ExecutionHandlerContext context, ByteArrayWrapper key, int index)
      throws Exception {

    Query query = getQuery(key, ListQuery.LINDEX, context);

    Object[] params = {index + 1};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    if (results == null || results.size() == 0 || results.size() <= index) {
      return null;
    } else {
      return (Struct) results.asList().get(index);
    }
  }
}
