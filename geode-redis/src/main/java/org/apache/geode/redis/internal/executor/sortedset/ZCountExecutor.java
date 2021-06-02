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
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.DoubleWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisConstants.ArityDef;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.SortedSetQuery;

public class ZCountExecutor extends SortedSetExecutor {

  private final String ERROR_NOT_NUMERIC = "The number provided is not numeric";

  private final int NOT_EXISTS = 0;

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZCOUNT));
      return;
    }

    ByteArrayWrapper key = command.getKey();

    Region<ByteArrayWrapper, DoubleWrapper> keyRegion = getRegion(context, key);
    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);

    if (keyRegion == null) {
      command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), NOT_EXISTS));
      return;
    }

    boolean startInclusive = true;
    boolean stopInclusive = true;
    double start;
    double stop;

    byte[] startArray = commandElems.get(2);
    byte[] stopArray = commandElems.get(3);
    String startString = Coder.bytesToString(startArray);
    String stopString = Coder.bytesToString(stopArray);

    if (startArray[0] == Coder.OPEN_BRACE_ID) {
      startString = startString.substring(1);
      startInclusive = false;
    }
    if (stopArray[0] == Coder.OPEN_BRACE_ID) {
      stopString = stopString.substring(1);
      stopInclusive = false;
    }

    try {
      start = Coder.stringToDouble(startString);
      stop = Coder.stringToDouble(stopString);
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }


    int count;
    try {
      count = getCount(key, keyRegion, context, start, stop, startInclusive, stopInclusive);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }


    command.setResponse(Coder.getIntegerResponse(context.getByteBufAllocator(), count));
  }

  private int getCount(ByteArrayWrapper key, Region<ByteArrayWrapper, DoubleWrapper> keyRegion,
      ExecutionHandlerContext context, double start, double stop, boolean startInclusive,
      boolean stopInclusive) throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    if (start == Double.NEGATIVE_INFINITY && stop == Double.POSITIVE_INFINITY) {
      return keyRegion.size();
    } else if (start == Double.POSITIVE_INFINITY || stop == Double.NEGATIVE_INFINITY) {
      return 0;
    }

    Query query;
    Object[] params;
    if (start == Double.NEGATIVE_INFINITY) {
      if (stopInclusive) {
        query = getQuery(key, SortedSetQuery.ZCOUNTNINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZCOUNTNINF, context);
      }
      params = new Object[] {stop};
    } else if (stop == Double.POSITIVE_INFINITY) {
      if (startInclusive) {
        query = getQuery(key, SortedSetQuery.ZCOUNTPINFI, context);
      } else {
        query = getQuery(key, SortedSetQuery.ZCOUNTPINF, context);
      }
      params = new Object[] {start};
    } else {
      if (startInclusive) {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZCOUNTSTISI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZCOUNTSTI, context);
        }
      } else {
        if (stopInclusive) {
          query = getQuery(key, SortedSetQuery.ZCOUNTSI, context);
        } else {
          query = getQuery(key, SortedSetQuery.ZCOUNT, context);
        }
      }
      params = new Object[] {start, stop};
    }

    SelectResults<?> results = (SelectResults<?>) query.execute(params);

    return (Integer) results.asList().get(0);
  }

}
