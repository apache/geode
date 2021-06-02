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

import java.util.ArrayList;
import java.util.Collection;
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

public class ZRemRangeByLexExecutor extends SortedSetExecutor {

  private final int ERROR_NOT_EXISTS = 0;

  private final String ERROR_ILLEGAL_SYNTAX =
      "The min and max strings must either start with a (, [ or be - or +";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZREMRANGEBYLEX));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command
          .setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), ERROR_NOT_EXISTS));
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

    Collection<ByteArrayWrapper> removeList;
    try {
      removeList = getRange(key, keyRegion, context, Coder.stringToByteArrayWrapper(startString),
          Coder.stringToByteArrayWrapper(stopString), minInclusive, maxInclusive);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int numRemoved = 0;

    for (ByteArrayWrapper entry : removeList) {
      Object oldVal = keyRegion.remove(entry);
      if (oldVal != null) {
        numRemoved++;
      }
    }

    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), numRemoved));
  }

  private Collection<ByteArrayWrapper> getRange(ByteArrayWrapper key,
      Region<ByteArrayWrapper, DoubleWrapper> keyRegion, ExecutionHandlerContext context,
      ByteArrayWrapper start, ByteArrayWrapper stop, boolean startInclusive, boolean stopInclusive)
      throws Exception {
    if (start.equals(minus) && stop.equals(plus)) {
      return new ArrayList<ByteArrayWrapper>(keyRegion.keySet());
    } else if (start.equals(plus) || stop.equals(minus)) {
      return null;
    }

    Query query;
    Object[] params;
    if (start.equals(minus)) {
      if (stopInclusive) {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXNINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXNINF, context);
      }
      params = new Object[] {stop, INFINITY_LIMIT};
    } else if (stop.equals(plus)) {
      if (startInclusive) {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXPINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZRANGEBYLEXPINF, context);
      }
      params = new Object[] {start, INFINITY_LIMIT};
    } else {
      if (startInclusive) {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEXSTISI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEXSTI, context);
        }
      } else {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEXSI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZRANGEBYLEX, context);
        }
      }
      params = new Object[] {start, stop, INFINITY_LIMIT};
    }

    @SuppressWarnings("unchecked")
    SelectResults<ByteArrayWrapper> results =
        (SelectResults<ByteArrayWrapper>) query.execute(params);

    return results.asList();
  }

}
