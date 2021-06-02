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
package org.apache.geode.redis.internal.executor.sortedset;

import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.DoubleWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Extendable;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.SortedSetQuery;

public class ZRangeExecutor extends SortedSetExecutor implements Extendable {

  private final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    boolean withScores = false;

    if (commandElems.size() >= 5) {
      byte[] fifthElem = commandElems.get(4);
      withScores = Coder.bytesToString(fifthElem).equalsIgnoreCase("WITHSCORES");

    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }


    int start;
    int stop;
    int sSetSize = keyRegion.size();

    try {
      byte[] startArray = commandElems.get(2);
      byte[] stopArray = commandElems.get(3);
      start = Coder.bytesToInt(startArray);
      stop = Coder.bytesToInt(stopArray);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    start = getBoundedStartIndex(start, sSetSize);
    stop = getBoundedEndIndex(stop, sSetSize);

    if (start > stop || start == sSetSize) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }
    if (stop == sSetSize) {
      stop--;
    }
    List<?> list;
    try {
      list = getRange(context, key, start, stop);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    command.setResponse(Coder.zRangeResponse(context.getByteBufAllocator(), list, withScores));
  }

  private List<?> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, int start,
      int stop) throws Exception {
    Query query;

    if (isReverse()) {
      query = getQuery(key, SortedSetQuery.ZRANGE, context);
    } else {
      query = getQuery(key, SortedSetQuery.ZREVRANGE, context);
    }

    Object[] params = {stop + 1};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    List<?> list = results.asList();

    return list.subList(start, stop + 1);

  }

  protected boolean isReverse() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.ZRANGE;
  }
}
