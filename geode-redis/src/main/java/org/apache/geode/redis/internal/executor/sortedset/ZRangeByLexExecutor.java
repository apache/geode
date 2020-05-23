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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.netty.buffer.ByteBuf;

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

public class ZRangeByLexExecutor extends SortedSetExecutor {

  private static final String ERROR_NOT_NUMERIC = "The index provided is not numeric";

  private static final String ERROR_ILLEGAL_SYNTAX =
      "The min and max strings must either start with a (, [ or be - or +";

  private static final String ERROR_LIMIT = "The offset and count cannot be negative";

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 4) {
      command
          .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ArityDef.ZRANGEBYLEX));
      return;
    }

    boolean existsLimit = false;

    if (commandElems.size() >= 7) {
      byte[] fifthElem = commandElems.get(4);
      existsLimit = Coder.bytesToString(fifthElem).equalsIgnoreCase("LIMIT");
    }

    int offset = 0;
    int limit = 0;

    if (existsLimit) {
      try {
        byte[] offsetArray = commandElems.get(5);
        byte[] limitArray = commandElems.get(6);
        offset = Coder.bytesToInt(offsetArray);
        limit = Coder.bytesToInt(limitArray);
      } catch (NumberFormatException e) {
        command
            .setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
        return;
      }
    }

    if (offset < 0 || limit < 0) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_LIMIT));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    Region<ByteArrayWrapper, DoubleWrapper> keyRegion =
        getOrCreateRegion(context, key, RedisDataType.REDIS_SORTEDSET);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
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
    Collection<ByteArrayWrapper> list = null;
    if (!(existsLimit && limit == 0)) {
      try {
        list = getRange(key, keyRegion, context, Coder.stringToByteArrayWrapper(startString),
            Coder.stringToByteArrayWrapper(stopString), minInclusive, maxInclusive, offset, limit);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    if (list == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
    } else {
      command.setResponse(getCustomBulkStringArrayResponse(list, context));
    }
  }

  private List<ByteArrayWrapper> getRange(ByteArrayWrapper key,
      Region<ByteArrayWrapper, DoubleWrapper> keyRegion,
      ExecutionHandlerContext context,
      ByteArrayWrapper start, ByteArrayWrapper stop,
      boolean startInclusive, boolean stopInclusive,
      int offset, int limit)
      throws FunctionDomainException, TypeMismatchException,
      NameResolutionException, QueryInvocationTargetException {
    if (start.equals(minus) && stop.equals(plus)) {
      List<ByteArrayWrapper> l = new ArrayList<>(keyRegion.keySet());
      int size = l.size();
      Collections.sort(l);
      if (limit == 0) {
        limit += size;
      }
      l = l.subList(Math.min(size, offset), Math.min(offset + limit, size));
      return l;
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
    if (limit > 0) {
      params[params.length - 1] = (limit + offset);
    }
    @SuppressWarnings("unchecked")
    SelectResults<ByteArrayWrapper> results =
        (SelectResults<ByteArrayWrapper>) query.execute(params);
    List<ByteArrayWrapper> list = results.asList();
    int size = list.size();
    return list.subList(Math.min(size, offset), size);

  }

  private ByteBuf getCustomBulkStringArrayResponse(Collection<ByteArrayWrapper> items,
      ExecutionHandlerContext context) {
    Iterator<ByteArrayWrapper> it = items.iterator();
    ByteBuf response = context.getByteBufAllocator().buffer();
    response.writeByte(Coder.ARRAY_ID);
    response.writeBytes(Coder.intToBytes(items.size()));
    response.writeBytes(Coder.CRLFar);

    while (it.hasNext()) {
      ByteArrayWrapper next = it.next();
      byte[] byteAr = next.toBytes();
      response.writeByte(Coder.BULK_STRING_ID);
      response.writeBytes(Coder.intToBytes(byteAr.length));
      response.writeBytes(Coder.CRLFar);
      response.writeBytes(byteAr);
      response.writeBytes(Coder.CRLFar);
    }

    return response;
  }

}
