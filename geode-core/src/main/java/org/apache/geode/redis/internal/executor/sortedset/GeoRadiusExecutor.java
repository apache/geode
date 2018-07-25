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
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.GeoRadiusElement;
import org.apache.geode.redis.internal.HashNeighbors;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.StringWrapper;
import org.apache.geode.redis.internal.executor.SortedSetQuery;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    double radius;
    String unit;

    boolean withDist = false;
    boolean withCoord = false;
    boolean withHash = false;
    Double distScale = 1.0;
    char[] centerHashPrecise;

    try {
      byte[] lonArray = commandElems.get(2);
      byte[] latArray = commandElems.get(3);
      byte[] radArray = commandElems.get(4);
      unit = new String(commandElems.get(5));
      centerHashPrecise = GeoCoder.geoHashBits(lonArray, latArray, GeoCoder.LEN_GEOHASH);
      lon = Coder.bytesToDouble(lonArray);
      lat = Coder.bytesToDouble(latArray);
      switch(unit) {
        case "km":
          distScale = 1000.0;
          break;
        case "m":
          break;
        case "ft":
          distScale = 3.28084;
        break;
        case "mi":
          distScale = 0.000621371;
          break;
        default:
          throw new IllegalArgumentException();
      }

      radius = Coder.bytesToDouble(radArray) * distScale;
    } catch (NumberFormatException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    } catch (IllegalArgumentException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INVALID_DIST_UNIT));
      return;
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ""));
      return;
    }

    for (int i = 6; i < commandElems.size(); i++) {
      if (new String(commandElems.get(i)).equals("withdist")) withDist = true;
      if (new String(commandElems.get(i)).equals("withcoord")) withCoord = true;
      if (new String(commandElems.get(i)).equals("withhash")) withHash = true;
    }

    HashNeighbors hn;
    try {
      hn = GeoCoder.geoHashGetAreasByRadius(lon, lat, radius);
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ""));
      return;
    }

    List<GeoRadiusElement> results = new ArrayList<>();
    for (String neighbor : hn.get()) {
      try {
          List<StructImpl> range = getRange(context, key, neighbor);
          for (StructImpl point : range) {
            String name = point.get("key").toString();
            char[] hashBits = point.get("value").toString().toCharArray();

            Optional<Double> dist = withDist ?
                    Optional.of(GeoCoder.geoDist(centerHashPrecise, hashBits) / distScale) :
                    Optional.empty();
            Optional<GeoCoord> coord = withCoord ?
                    Optional.of(GeoCoder.geoPos(hashBits)) : Optional.empty();
            Optional<String> hash = withHash ? Optional.of(GeoCoder.bitsToHash(hashBits)) : Optional.empty();

            results.add(new GeoRadiusElement(name, coord, dist, hash));
          }
      } catch (Exception e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), e.getMessage()));
        return;
      }
    }

    command.setResponse(GeoCoder.georadiusResponse(context.getByteBufAllocator(), results));
  }

  private List<StructImpl> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, String hash) throws Exception {
    Query query = getQuery(key, SortedSetQuery.GEORADIUS, context);
    Object[] params = {hash + "%"};
    SelectResults<StructImpl> results = (SelectResults<StructImpl>) query.execute(params);
    return results.asList();
  }

}
