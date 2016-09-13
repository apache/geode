/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class CacheServiceJUnitTest {
  
  private GemFireCacheImpl cache;

  @Before
  public void setUp() {
    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
  }
  
  @After
  public void tearDown() {
    if(cache != null) {
      cache.close();
    }
  }

  @Test
  public void test() {
    MockCacheService service = cache.getService(MockCacheService.class);
    assertEquals(cache, service.getCache());
  }

}
