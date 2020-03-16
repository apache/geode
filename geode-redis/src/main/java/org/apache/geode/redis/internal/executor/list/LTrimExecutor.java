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

public class LTrimExecutor extends ListExecutor {

  private static final String ERROR_KEY_NOT_EXISTS = "The key does not exists on this server";

  private static final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  private static final String SUCCESS = "OK";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LTRIM));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] startArray = commandElems.get(2);
    byte[] stopArray = commandElems.get(3);

    int redisStart;
    int redisStop;

    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Object, Object> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_KEY_NOT_EXISTS));
      return;
    }

    int listSize = keyRegion.size() - LIST_EMPTY_SIZE;
    if (listSize == 0) {
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
      return;
    }

    try {
      redisStart = Coder.bytesToInt(startArray);
      redisStop = Coder.bytesToInt(stopArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    redisStart = getBoundedStartIndex(redisStart, listSize);
    redisStop = getBoundedEndIndex(redisStop, listSize);
    redisStart = Math.min(redisStart, listSize - 1);
    redisStop = Math.min(redisStop, listSize - 1);

    if (redisStart == 0 && redisStop == listSize - 1) {
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
      return;
    } else if (redisStart == 0 && redisStop < redisStart) {
      context.getRegionProvider().removeKey(key, RedisDataType.REDIS_LIST);
      command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
      return;
    }

    List<Integer> keepList;
    try {
      keepList = getRange(context, key, redisStart, redisStop);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    for (Object keyElement : keyRegion.keySet()) {
      if (!keepList.contains(keyElement) && keyElement instanceof Integer) {
        keyRegion.remove(keyElement);
      }
    }

    // Reset indexes in meta data region
    keyRegion.put("head", keepList.get(0));
    keyRegion.put("tail", keepList.get(keepList.size() - 1));
    command.setResponse(Coder.getSimpleStringResponse(context.getByteBufAllocator(), SUCCESS));
  }

  private List<Integer> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, int start,
      int stop) throws Exception {
    Query query = getQuery(key, ListQuery.LTRIM, context);

    Object[] params = {stop + 1};

    @SuppressWarnings("unchecked")
    SelectResults<Integer> results = (SelectResults<Integer>) query.execute(params);
    if (results == null || results.size() <= start) {
      return null;
    }

    return results.asList().subList(start, results.size());
  }
}
