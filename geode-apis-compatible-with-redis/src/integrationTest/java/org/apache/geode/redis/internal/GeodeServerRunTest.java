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
package org.apache.geode.redis.internal;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class GeodeServerRunTest {
  @ClassRule
  public static GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Test
  @Ignore("This is a no-op test to conveniently run redis api for geode server for local development/testing purposes")
  public void runGeodeServer() {
    LogService.getLogger().warn("Server running on port: " + server.getPort());
    while (true) {
    }
  }
}
