/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.cache.configuration;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.config.JAXBService;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class CacheConfigTest {

  private CacheConfig cacheConfig;
  private JAXBService service;

  @Before
  public void setUp() throws Exception {
    cacheConfig = new CacheConfig("1.0");
    service = new JAXBService(CacheConfig.class);
  }

  @Test
  public void invalidIndexType() {
    RegionConfig regionConfig = new RegionConfig();
    cacheConfig.getRegion().add(regionConfig);
    regionConfig.setName("regionA");
    regionConfig.setRefid("REPLICATE");
    RegionConfig.Index index = new RegionConfig.Index();
    index.setName("indexName");
    index.setKeyIndex(true);
    index.setExpression("expression");
    regionConfig.getIndex().add(index);

    System.out.println(service.marshall(cacheConfig));
  }
}
