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
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.ListQuery;

public class LSetExecutor extends ListExecutor {

  private static final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  private static final String ERROR_INDEX =
      "The index provided is not within range of this list or the key does not exist";

  private static final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LSET));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] indexArray = commandElems.get(2);
    byte[] value = commandElems.get(3);

    int index;

    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Object, Object> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INDEX));
      return;
    }

    try {
      index = Coder.bytesToInt(indexArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;
    if (index < 0) {
      index += listSize;
    }
    if (index < 0 || index > listSize) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INDEX));
      return;
    }

    Integer indexKey;
    try {
      indexKey = getIndexKey(context, key, index);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (indexKey == null) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INDEX));
      return;
    }
    if (index == listSize) {
      indexKey++;
    }
    keyRegion.put(indexKey, new ByteArrayWrapper(value));
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
  }

  private Integer getIndexKey(ExecutionHandlerContext context, ByteArrayWrapper key, int index)
      throws Exception {
    Query query = getQuery(key, ListQuery.LSET, context);

    Object[] params = {index + 1};

    @SuppressWarnings("unchecked")
    SelectResults<Integer> results = (SelectResults<Integer>) query.execute(params);
    int size = results.size();
    if (size == 0) {
      return null;
    }

    return results.asList().get(size - 1);
  }
}
