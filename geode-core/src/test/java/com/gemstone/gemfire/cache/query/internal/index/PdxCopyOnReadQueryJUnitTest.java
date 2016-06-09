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
package com.gemstone.gemfire.cache.query.internal.index;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class PdxCopyOnReadQueryJUnitTest {
  
  private Cache cache = null;
  
  @Test
  public void testCopyOnReadPdxSerialization() throws Exception {
    List<String> classes = new ArrayList<String>();
    classes.add(PortfolioPdx.class.getCanonicalName());
    ReflectionBasedAutoSerializer serializer = new ReflectionBasedAutoSerializer(classes.toArray(new String[0]));
    
    CacheFactory cf = new CacheFactory();
    cf.setPdxSerializer(serializer);
    cf.setPdxReadSerialized(false);
    cf.set(MCAST_PORT, "0");
    cache = cf.create();
    cache.setCopyOnRead(true);
    
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("SimpleObjects");
    Region duplicates = cache.createRegionFactory(RegionShortcut.REPLICATE).create("SimpleObjects_Duplicates");

    for (int i = 0; i < 10; i++) {
      PortfolioPdx t = new PortfolioPdx(i);
      region.put(i,t);
      duplicates.put(i, t);
    }
    
    QueryService qs = cache.getQueryService();
    SelectResults rs = (SelectResults)qs.newQuery("select * from /SimpleObjects").execute();
    assertEquals(10, rs.size());
    Query query = qs.newQuery("select * from /SimpleObjects_Duplicates s where s in ($1)");
    SelectResults finalResults = (SelectResults)query.execute(new Object[]{rs});
    assertEquals(10, finalResults.size());
  }

}
