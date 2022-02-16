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

package org.apache.geode.redis.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.commands.RedisCommandType;

public class GeodeRedisStatsIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Test
  public void checkGeodeRedisStatsExist() {
    Cache cache = CacheFactory.getAnyInstance();
    InternalDistributedSystem internalSystem =
        (InternalDistributedSystem) cache.getDistributedSystem();
    StatisticsManager statisticsManager = internalSystem.getStatisticsManager();

    List<String> statDescriptions = statisticsManager.getStatsList()
        .stream().map(Statistics::getTextId).collect(Collectors.toList());

    assertThat(statDescriptions).contains(GeodeRedisStats.STATS_BASENAME);

    for (RedisCommandType.Category category : RedisCommandType.Category.values()) {
      String name = GeodeRedisStats.STATS_BASENAME + ":" + category.name();
      assertThat(statDescriptions).contains(name);
    }
  }

}
