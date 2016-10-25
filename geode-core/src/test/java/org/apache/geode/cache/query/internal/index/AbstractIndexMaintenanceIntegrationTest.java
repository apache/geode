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
package org.apache.geode.cache.query.internal.index;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public abstract class AbstractIndexMaintenanceIntegrationTest {


  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void whenRemovingRegionEntryFromIndexIfEntryDestroyedIsThrownCorrectlyRemoveFromIndexAndNotThrowException() throws Exception {
    CacheUtils.startCache();
    Cache cache = CacheUtils.getCache();
    LocalRegion region = (LocalRegion)cache.createRegionFactory(RegionShortcut.REPLICATE).create("portfolios");
    QueryService qs = cache.getQueryService();
    AbstractIndex statusIndex = createIndex(qs, "statusIndex", "value.status", "/portfolios.entrySet()");

    PortfolioPdx p = new PortfolioPdx(1);
    region.put("KEY-1", p);
    RegionEntry entry = region.getRegionEntry("KEY-1");
    region.destroy("KEY-1");

    statusIndex.removeIndexMapping(entry, IndexProtocol.OTHER_OP);
  }

  protected abstract AbstractIndex createIndex(final QueryService qs, String name, String indexExpression, String regionPath)
    throws IndexNameConflictException, IndexExistsException, RegionNotFoundException ;
}
