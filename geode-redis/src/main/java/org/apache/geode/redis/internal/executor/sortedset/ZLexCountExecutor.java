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
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.SortedSetQuery;

public class ZLexCountExecutor extends SortedSetExecutor {

  private final String ERROR_ILLEGAL_SYNTAX =
      "The min and max strings must either start with a (, [ or be - or +";

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZLEXCOUNT));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);
    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }

    boolean minInclusive = false;
    boolean maxInclusive = false;

    byte[] minArray = commandElems.get(2);
    byte[] maxArray = commandElems.get(3);
    String startString = Coder.bytesToString(minArray);
    String stopString = Coder.bytesToString(maxArray);

    if (minArray[0] == Coder.OPEN_BRACE_ID) {
      startString = startString.substring(1);
      minInclusive = false;
    } else if (minArray[0] == Coder.OPEN_BRACKET_ID) {
      startString = startString.substring(1);
      minInclusive = true;
    } else if (minArray[0] != Coder.HYPHEN_ID) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_ILLEGAL_SYNTAX));
      return;
    }

    if (maxArray[0] == Coder.OPEN_BRACE_ID) {
      stopString = stopString.substring(1);
      maxInclusive = false;
    } else if (maxArray[0] == Coder.OPEN_BRACKET_ID) {
      stopString = stopString.substring(1);
      maxInclusive = true;
    } else if (maxArray[0] != Coder.PLUS_ID) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_ILLEGAL_SYNTAX));
      return;
    }


    int count;
    try {
      count = getCount(key, keyRegion, context, Coder.stringToByteArrayWrapper(startString),
          Coder.stringToByteArrayWrapper(stopString), minInclusive, maxInclusive);
    } catch (Exception e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), e.toString()));
      return;
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), count));
  }

  private int getCount(ByteArrayWrapper key, Region<ByteArrayWrapper, DoubleWrapper> keyRegion,
      ExecutionHandlerContext context, ByteArrayWrapper start, ByteArrayWrapper stop,
      boolean startInclusive, boolean stopInclusive) throws Exception {
    if (start.equals(minus) && stop.equals(plus)) {
      return keyRegion.size();
    } else if (start.equals(plus) || stop.equals(minus)) {
      return 0;
    }

    Query query;
    Object[] params;
    if (start.equals(minus)) {
      if (stopInclusive) {
        query = getQuery(key, SortedSetQuery.ZLEXCOUNTNINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZLEXCOUNTNINF, context);
      }
      params = new Object[] {stop};
    } else if (stop.equals(plus)) {
      if (startInclusive) {
        query = getQuery(key, SortedSetQuery.ZLEXCOUNTPINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZLEXCOUNTPINF, context);
      }
      params = new Object[] {start};
    } else {
      if (startInclusive) {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZLEXCOUNTSTISI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZLEXCOUNTSTI, context);
        }
      } else {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZLEXCOUNTSI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZLEXCOUNT, context);
        }
      }
      params = new Object[] {start, stop};
    }

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    return (Integer) results.asList().get(0);
  }
}
