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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.PortfolioPdx;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class PdxCopyOnReadQueryJUnitTest {

  private Cache cache = null;

  @Test
  public void testCopyOnReadPdxSerialization() throws Exception {
    List<String> classes = new ArrayList<>();
    classes.add(PortfolioPdx.class.getCanonicalName());
    ReflectionBasedAutoSerializer serializer =
        new ReflectionBasedAutoSerializer(classes.toArray(new String[0]));

    CacheFactory cf = new CacheFactory();
    cf.setPdxSerializer(serializer);
    cf.setPdxReadSerialized(false);
    cf.set(MCAST_PORT, "0");
    cache = cf.create();
    cache.setCopyOnRead(true);

    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("SimpleObjects");
    Region duplicates =
        cache.createRegionFactory(RegionShortcut.REPLICATE).create("SimpleObjects_Duplicates");

    for (int i = 0; i < 10; i++) {
      PortfolioPdx t = new PortfolioPdx(i);
      region.put(i, t);
      duplicates.put(i, t);
    }

    QueryService qs = cache.getQueryService();
    SelectResults rs =
        (SelectResults) qs.newQuery("select * from " + SEPARATOR + "SimpleObjects").execute();
    assertEquals(10, rs.size());
    Query query =
        qs.newQuery("select * from " + SEPARATOR + "SimpleObjects_Duplicates s where s in ($1)");
    SelectResults finalResults = (SelectResults) query.execute(new Object[] {rs});
    assertEquals(10, finalResults.size());
  }

}
