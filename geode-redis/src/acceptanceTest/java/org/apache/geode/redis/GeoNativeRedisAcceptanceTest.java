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
package org.apache.geode.redis;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.testcontainers.containers.GenericContainer;
import redis.clients.jedis.Jedis;

import org.apache.geode.test.junit.categories.RedisTest;
import org.apache.geode.test.junit.rules.IgnoreOnWindowsRule;

@Category({RedisTest.class})
public class GeoNativeRedisAcceptanceTest extends GeoIntegrationTest {

  // Docker compose does not work on windows in CI. Ignore this test on windows
  // Using a RuleChain to make sure we ignore the test before the rule comes into play
  @ClassRule
  public static TestRule ignoreOnWindowsRule = new IgnoreOnWindowsRule();

  @BeforeClass
  public static void setUp() {
    GenericContainer redisContainer = new GenericContainer<>("redis:5.0.6").withExposedPorts(6379);
    redisContainer.start();
    jedis = new Jedis("localhost", redisContainer.getFirstMappedPort(), 10000000);
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
  }

  // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis

  @Test
  public void testGeoHash() {
    // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis
  }

  @Test
  public void testGeoRadiusByMemberWithCoord() {
    // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis
  }

  @Test
  public void testGeoRadiusByMemberFull() {
    // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis
  }

  @Test
  public void testGeoRadiusByMemberBasic() {
    // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis
  }

  @Test
  public void testGeoRadiusByMemberNorth() {
    // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis
  }

  @Test
  public void testGeoPos() {
    // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis
  }

  @Test
  public void testGeoRadiusByMemberWithDist() {
    // TODO: See JIRA GEODE-7909 Update Geo* commands in Geode Redis to match native Redis
  }
}
