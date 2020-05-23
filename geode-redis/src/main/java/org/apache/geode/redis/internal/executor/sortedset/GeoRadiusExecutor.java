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

import static org.apache.geode.redis.internal.RedisConstants.ERROR_INVALID_ARGUMENT_UNIT_NUM;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.github.davidmoten.geo.LatLong;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.internal.StructImpl;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.GeoRadiusResponseElement;
import org.apache.geode.redis.internal.MemberNotFoundException;
import org.apache.geode.redis.internal.RedisCommandParserException;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;

public class GeoRadiusExecutor extends GeoSortedSetExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    List<byte[]> commandElems = command.getProcessedCommand();

    if (commandElems.size() < 6) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), RedisConstants.ArityDef.GEORADIUS));
      return;
    }

    ByteArrayWrapper key = command.getKey();
    checkDataType(key, RedisDataType.REDIS_SORTEDSET, context);
    Region<ByteArrayWrapper, ByteArrayWrapper> keyRegion = getRegion(context, key);

    if (keyRegion == null) {
      command.setResponse(Coder.getEmptyArrayResponse(context.getByteBufAllocator()));
      return;
    }

    GeoRadiusParameters params;
    try {
      params = new GeoRadiusParameters(keyRegion, commandElems,
          GeoRadiusParameters.CommandType.GEORADIUS);
    } catch (IllegalArgumentException e) {
      command.setResponse(
          Coder.getErrorResponse(context.getByteBufAllocator(), ERROR_INVALID_ARGUMENT_UNIT_NUM));
      return;
    } catch (RedisCommandParserException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.ArityDef.GEORADIUS));
      return;
    } catch (MemberNotFoundException e) {
      /* Not possible for GEORADIUS */
      return;
    }

    Set<String> hn = GeoCoder.geohashSearchAreas(params.lon, params.lat, params.radius);

    List<GeoRadiusResponseElement> results = new ArrayList<>();
    for (String neighbor : hn) {
      try {
        List<StructImpl> range = getGeoRadiusRange(context, key, neighbor);
        for (StructImpl point : range) {
          String name = point.get("key").toString();
          String hash = point.get("value").toString();

          double dist = GeoCoder.geoDist(params.centerHashPrecise, hash) * params.distScale;

          // Post-filter for accuracy
          if (dist > (params.radius * params.distScale)) {
            continue;
          }

          Optional<LatLong> coord =
              params.withCoord ? Optional.of(GeoCoder.geoPos(hash)) : Optional.empty();
          Optional<String> hashOpt =
              params.withHash ? Optional.of(hash) : Optional.empty();

          results.add(new GeoRadiusResponseElement(name, coord, dist, params.withDist, hashOpt));
        }
      } catch (Exception e) {
        command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(), e.getMessage()));
        return;
      }
    }

    if (params.order == GeoRadiusParameters.SortOrder.ASC) {
      GeoRadiusResponseElement.sortByDistanceAscending(results);
    } else if (params.order == GeoRadiusParameters.SortOrder.DESC) {
      GeoRadiusResponseElement.sortByDistanceDescending(results);
    }

    if (params.count != null && params.count < results.size()) {
      results = results.subList(0, params.count);
    }

    respondGeoRadius(command, context, results);
  }

}
