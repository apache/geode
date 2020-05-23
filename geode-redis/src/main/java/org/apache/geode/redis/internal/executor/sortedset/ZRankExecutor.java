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

public class ZRankExecutor extends SortedSetExecutor implements Extendable {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 3) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), getArgsError()));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    ByteArrayWrapper member = new ByteArrayWrapper(commandElems.get(2));

    DoubleWrapper value = keyRegion.get(member);

    if (value == null) {
      command.setResponse(Coder.getNilResponse(context.getByteBufAllocator()));
      return;
    }

    int rank;
    try {
      rank = getRange(context, key, member, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), rank));
  }

  private int getRange(ExecutionHandlerContext context, ByteArrayWrapper key,
      ByteArrayWrapper member, DoubleWrapper valueWrapper) throws Exception {
    Query query;
    if (isReverse()) {
      query = getQuery(key, SortedSetQuery.ZREVRANK, context);
    } else {
      query = getQuery(key, SortedSetQuery.ZRANK, context);
    }

    Object[] params = {valueWrapper.score, valueWrapper.score, member};

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    return (Integer) results.asList().get(0);

  }

  protected boolean isReverse() {
    return false;
  }

  @Override
  public String getArgsError() {
    return ArityDef.ZRANK;
  }

}
