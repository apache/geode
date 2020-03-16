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

public class LRemExecutor extends ListExecutor {

  private static final String ERROR_NOT_NUMERIC = "The count provided is not numeric";

  private static final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.LREM));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    byte[] countArray = commandElems.get(2);
    byte[] value = commandElems.get(3);

    int count;

    checkDataType(key, RedisDataType.REDIS_LIST, context);
    Region<Object, Object> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }

    try {
      count = Coder.bytesToInt(countArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    List<Struct> removeList;
    try {
      removeList = getRemoveList(context, key, new ByteArrayWrapper(value), count);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int numRemoved = 0;

    if (removeList == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
      return;
    }

    for (Struct entry : removeList) {
      Integer removeKey = (Integer) entry.getFieldValues()[0];
      Object oldVal = keyRegion.remove(removeKey);
      if (oldVal != null) {
        numRemoved++;
      }
    }
    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }

  private List<Struct> getRemoveList(ExecutionHandlerContext context, ByteArrayWrapper key,
      ByteArrayWrapper value, int count) throws Exception {
    Object[] params;
    Query query;
    if (count > 0) {
      query = getQuery(key, ListQuery.LREMG, context);
      params = new Object[] {value, count};
    } else if (count < 0) {
      query = getQuery(key, ListQuery.LREML, context);
      params = new Object[] {value, -count};
    } else {
      query = getQuery(key, ListQuery.LREME, context);
      params = new Object[] {value};
    }

    @SuppressWarnings("unchecked")
    SelectResults<Struct> results = (SelectResults<Struct>) query.execute(params);

    if (results == null || results.isEmpty()) {
      return null;
    }

    return results.asList();
  }
}
