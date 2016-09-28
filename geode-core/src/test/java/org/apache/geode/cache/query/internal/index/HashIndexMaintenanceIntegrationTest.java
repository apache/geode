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

import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class HashIndexMaintenanceIntegrationTest extends AbstractIndexMaintenanceIntegrationTest {

  @Override
  protected AbstractIndex createIndex(final QueryService qs, String name, String indexExpression, String regionPath)
    throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    return (HashIndex)qs.createHashIndex(name, indexExpression, regionPath);
  }

  @Test
  public void testInvalidTokensForHash() throws Exception {
    CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("exampleRegion");
    QueryService qs = CacheUtils.getCache().getQueryService();
    Region region = CacheUtils.getCache().getRegion("/exampleRegion");
    region.put("0", new Portfolio(0));
    region.invalidate("0");
    Index index = qs.createHashIndex("hash index index", "p.status", "/exampleRegion p");
    SelectResults results = (SelectResults) qs.newQuery("Select * from /exampleRegion r where r.status='active'").execute();
    //the remove should have happened
    assertEquals(0, results.size());

    results = (SelectResults) qs.newQuery("Select * from /exampleRegion r where r.status!='inactive'").execute();
    assertEquals(0, results.size());

    HashIndex cindex = (HashIndex)index;
    Iterator iterator =  cindex.entriesSet.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      count++;
      iterator.next();
    }
    assertEquals("incorrect number of entries in collection", 0, count);
  }
}
