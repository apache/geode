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

package org.apache.geode.redis.internal.executor.server;

import static org.apache.geode.distributed.ConfigurationProperties.GEODE_FOR_REDIS_REPLICA_COUNT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.redis.GeodeRedisServerRule;

public class InfoIntegrationTest extends AbstractInfoIntegrationTest {

  @ClassRule
  public static GeodeRedisServerRule server =
      new GeodeRedisServerRule().withProperty(GEODE_FOR_REDIS_REPLICA_COUNT, "0")
          .withProperty(LOG_LEVEL, "info");

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Test
  public void shouldReturnRedisVersion() {
    String expectedResult = "redis_version:5.0.6";

    String actualResult = jedis.info();

    assertThat(actualResult).contains(expectedResult);
  }

}
