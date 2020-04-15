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

package org.apache.geode.redis.internal.executor.set;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.internal.ByteArrayWrapper;

public class GeodeRedisSetWithFunctions implements RedisSet {
  private static final Logger logger = LogService.getLogger();

  private final ByteArrayWrapper key;
  private final Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region;

  static {
    FunctionService.registerFunction(new SaddFunction());
  }

  public GeodeRedisSetWithFunctions(ByteArrayWrapper key,
      Region<ByteArrayWrapper, Set<ByteArrayWrapper>> region) {

    this.key = key;
    this.region = region;
  }

  @Override
  public long sadd(Collection<ByteArrayWrapper> membersToAdd) {
    logger.info("DEBUG: GeodeRedisSetWithFunctions in sadd");
    ResultCollector<Collection<ByteArrayWrapper>, List<Long>> results = FunctionService
        .onRegion(region)
        .withFilter(Collections.singleton(key))
        .setArguments(membersToAdd)
        .execute(SaddFunction.ID);

    return results.getResult().get(0);
  }

  @Override
  public long srem(Collection<ByteArrayWrapper> membersToAdd) {
    return 0;
  }

  @Override
  public Set<ByteArrayWrapper> members() {
    return null;
  }

}
