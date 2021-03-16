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

package org.apache.geode.redis.internal.cluster;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.size.ObjectGraphSizer;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;

public class RedisRegionSizer implements Function<Void> {

  private static final String REDIS_REGION_SIZER_ID = "redis-region-size";

  public static void register() {
    FunctionService.registerFunction(new RedisRegionSizer());
  }

  @Override
  public void execute(FunctionContext<Void> context) {
    Region<RedisKey, RedisData> region =
        context.getCache().getRegion(RegionProvider.REDIS_DATA_REGION);

    long size;
    try {
      size = ObjectGraphSizer.size(region);
    } catch (IllegalAccessException e) {
      throw new FunctionException(e);
    }

    context.getResultSender().lastResult(size);
  }

  @Override
  public String getId() {
    return REDIS_REGION_SIZER_ID;
  }

}
