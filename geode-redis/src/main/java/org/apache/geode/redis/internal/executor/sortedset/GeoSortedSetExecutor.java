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

import com.github.davidmoten.geo.LatLong;
import io.netty.buffer.ByteBuf;

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
import org.apache.geode.redis.internal.GeoRadiusResponseElement;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.SortedSetQuery;

public abstract class GeoSortedSetExecutor extends AbstractExecutor {

  @Override
  protected Region<ByteArrayWrapper, ByteArrayWrapper> getOrCreateRegion(
      ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, ByteArrayWrapper> r =
        (Region<ByteArrayWrapper, ByteArrayWrapper>) context
            .getRegionProvider().getOrCreateRegion(key, type, context);
    return r;
  }

  protected Region<ByteArrayWrapper, ByteArrayWrapper> getRegion(ExecutionHandlerContext context,
      ByteArrayWrapper key) {
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, ByteArrayWrapper> r =
        (Region<ByteArrayWrapper, ByteArrayWrapper>) context.getRegionProvider().getRegion(key);
    return r;
  }

  protected List<StructImpl> getGeoRadiusRange(ExecutionHandlerContext context,
      ByteArrayWrapper key, String hash) throws Exception {
    Query query = getQuery(key, SortedSetQuery.GEORADIUS, context);
    Object[] params = {hash + "%"};
    @SuppressWarnings("unchecked")
    SelectResults<StructImpl> results = (SelectResults<StructImpl>) query.execute(params);
    return results.asList();
  }

  protected void respondGeoRadius(Command command, ExecutionHandlerContext context,
      List<GeoRadiusResponseElement> results) {
    ByteBuf rsp;
    try {
      rsp = GeoCoder.geoRadiusResponse(context.getByteBufAllocator(), results);
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
      return;
    }

    command.setResponse(rsp);
  }

  protected void respondGeoCoordinates(Command command, ExecutionHandlerContext context,
      List<LatLong> positions) {
    ByteBuf rsp;
    try {
      rsp = GeoCoder.getBulkStringGeoCoordinateArrayResponse(context.getByteBufAllocator(),
          positions);
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
      return;
    }

    command.setResponse(rsp);
  }
}
