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
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.StringWrapper;
import org.apache.geode.redis.internal.executor.SortedSetQuery;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_DIST_UNIT;
import static org.apache.geode.redis.internal.RedisConstants.ERROR_NOT_NUMERIC;

public class GeoRadiusByMemberExecutor extends GeoSortedSetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 5) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEORADIUS));
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
    String member;

    boolean withDist = false;
    boolean withCoord = false;
    boolean withHash = false;
    Integer count = null;
    Boolean ascendingOrder = null;

    Double distScale = 1.0;
    char[] centerHashPrecise;

    try {
      byte[] memberArray = commandElems.get(2);
      byte[] radArray = commandElems.get(3);
      unit = new String(commandElems.get(4));

      member = new String(memberArray);
      StringWrapper hashWrapper = keyRegion.get(new ByteArrayWrapper(memberArray));
      centerHashPrecise = hashWrapper.toString().toCharArray();
      GeoCoord pos = GeoCoder.geoPos(centerHashPrecise);
      lon = pos.getLongitude();
      lat = pos.getLatitude();
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
    }

    int i;
    for (i = 6; i < commandElems.size() && (new String(commandElems.get(i))).contains("with"); i++) {
      String elem = new String(commandElems.get(i));

      if (elem.equals("withdist")) withDist = true;
      if (elem.equals("withcoord")) withCoord = true;
      if (elem.equals("withhash")) withHash = true;
    }

    if (i < commandElems.size() && (new String(commandElems.get(i))).equals("count")) {
      try {
        count = Coder.bytesToInt(commandElems.get(++i));
        i++;
      } catch (NumberFormatException e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
        return;
      }
    }

    if (i < commandElems.size() && (new String(commandElems.get(i))).contains("sc")) {
      String elem = new String(commandElems.get(i++));

      if (elem.equals("asc")) ascendingOrder = true;
      else if (elem.equals("desc")) ascendingOrder = false;
    }

    if (i < commandElems.size()) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEORADIUS));
      return;
    }

    HashNeighbors hn;
    try {
      hn = GeoCoder.geoHashGetAreasByRadius(lon, lat, radius);
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_NOT_NUMERIC));
      return;
    }

    List<GeoRadiusElement> results = new ArrayList<>();
    for (String neighbor : hn.get()) {
      try {
          List<StructImpl> range = getRange(context, key, neighbor);
          for (StructImpl point : range) {
            String name = point.get("key").toString();
            char[] hashBits = point.get("value").toString().toCharArray();

            Double dist = GeoCoder.geoDist(centerHashPrecise, hashBits) / distScale;
            Optional<GeoCoord> coord = withCoord ?
                    Optional.of(GeoCoder.geoPos(hashBits)) : Optional.empty();
            Optional<String> hash = withHash ? Optional.of(GeoCoder.bitsToHash(hashBits)) : Optional.empty();

            // Because of the way hashing works, sometimes you can get the requested member back in the results
            if (!name.equals(member))
              results.add(new GeoRadiusElement(name, coord, dist, withDist, hash));
          }
      } catch (Exception e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), e.getMessage()));
        return;
      }
    }

    if (ascendingOrder != null && ascendingOrder) {
      Collections.sort(results, Comparator.comparing(GeoRadiusElement::getDistFromCenter));
    } else if (ascendingOrder != null && !ascendingOrder) {
      Collections.sort(results, Comparator.comparing((GeoRadiusElement x) -> -1.0 * x.getDistFromCenter()));
    }

    if (count != null && count < results.size()) {
      results = results.subList(0, count);
    }

    command.setResponse(GeoCoder.geoRadiusResponse(context.getByteBufAllocator(), results));
  }

  private List<StructImpl> getRange(ExecutionHandlerContext context, ByteArrayWrapper key, String hash) throws Exception {
    Query query = getQuery(key, SortedSetQuery.GEORADIUS, context);
    Object[] params = {hash + "%"};
    SelectResults<StructImpl> results = (SelectResults<StructImpl>) query.execute(params);
    return results.asList();
  }

}
