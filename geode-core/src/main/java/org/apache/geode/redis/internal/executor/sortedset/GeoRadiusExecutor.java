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

import java.util.ArrayList;
import java.util.List;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_DIST_UNIT;
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
    double radius = 0.0;
    String unit;
    try {
      byte[] lonArray = commandElems.get(2);
      byte[] latArray = commandElems.get(3);
      byte[] radArray = commandElems.get(4);
      unit = new String(commandElems.get(5));
      lon = Coder.bytesToDouble(lonArray);
      lat = Coder.bytesToDouble(latArray);
      switch(unit) {
        case "km":
          radius = Coder.bytesToDouble(radArray) * 1000.0;
          break;
        case "m":
          radius = Coder.bytesToDouble(radArray);
          break;
        case "ft":
          radius = Coder.bytesToDouble(radArray) * 3.28084;
        break;
        case "mi":
          radius = Coder.bytesToDouble(radArray) * 0.000621371;
          break;
        default:
          throw new IllegalArgumentException();
      }
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    } catch (IllegalArgumentException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INVALID_DIST_UNIT));
      return;
    }

    HashNeighbors hn;
    try {
      hn = GeoCoder.geoHashGetAreasByRadius(lon, lat, radius);
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ""));
      return;
    }

    List<String> hashes = new ArrayList<>();
    for (String neighbor : hn.get()) {
      try {
          hashes.addAll(getRange(context, key, neighbor));
      } catch (Exception e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), e.getMessage()));
        return;
      }
    }

    command.setResponse(Coder.georadiusResponse(context.getByteBufAllocator(), hashes, false));
  }

  private List<String> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, String hash) throws Exception {
    Query query = getQuery(key, SortedSetQuery.GEORADIUS, context);
    Object[] params = {hash + "%"};
    SelectResults<String> results = (SelectResults<String>) query.execute(params);
    return results.asList();
  }

}
