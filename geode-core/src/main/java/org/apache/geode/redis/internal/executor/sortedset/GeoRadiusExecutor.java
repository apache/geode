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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.HashNeighbors;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.StringWrapper;
import org.apache.geode.redis.internal.executor.SortedSetQuery;

import java.util.List;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_NUMERIC;

public class GeoRadiusExecutor extends GeoSortedSetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 6) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ""));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, StringWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    double lon;
    double lat;
    double radius;
    try {
      byte[] lonArray = commandElems.get(2);
      byte[] latArray = commandElems.get(3);
      byte[] radArray = commandElems.get(4);
      lon = Coder.bytesToDouble(lonArray);
      lat = Coder.bytesToDouble(latArray);
      radius = Coder.bytesToDouble(radArray) * 1000.0;
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    HashNeighbors hn;
    try {
      hn = GeoCoder.geoHashGetAreasByRadius(lon, lat, radius);
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ""));
      return;
    }

    List<?> eastKeys;
    try {
      eastKeys = getRange(context, key, hn.north);
    } catch (Exception e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), e.getMessage()));
      return;
    }

    command.setResponse(Coder.georadiusResponse(context.getByteBufAllocator(), eastKeys, false));
  }

  private List<?> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, String hash) throws Exception {
    Query query = getQuery(key, SortedSetQuery.GEORADIUS, context);
    Object[] params = {hash + "%"};
    SelectResults<?> results = (SelectResults<?>) query.execute(params);
    return results.asList();
  }

}
