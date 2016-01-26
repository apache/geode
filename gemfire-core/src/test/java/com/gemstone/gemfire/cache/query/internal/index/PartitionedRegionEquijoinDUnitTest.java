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

import java.util.ArrayList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class PartitionedRegionEquijoinDUnitTest extends EquijoinDUnitTest {
 
  @Override
  protected void createRegions() {
    region1 = createPartitionRegion("region1");
    region2 = createColocatedPartitionRegion("region2", "region1");
    FunctionService.registerFunction(equijoinTestFunction);
  }
  
  @Override
  protected void createAdditionalRegions() throws Exception {
    region3 = createColocatedPartitionRegion("region3", "region1");
    region4 = createColocatedPartitionRegion("region4", "region1");
  }
 
  @Test
  public void testSingleFilterWithSingleEquijoinNestedQuery() throws Exception {
    createRegions();

    String[] queries = new String[]{
        "select * from /region1 c, /region2 s where c.pkid=1 and c.pkid = s.pkid or c.pkid in set (1,2,3,4)",
    };
    
    for (int i = 0; i < 1000; i++) {
      region1.put( i, new Customer(i, i));
      region2.put( i, new Customer(i, i));
    }
    
    executeQueriesWithIndexCombinations(queries);
  }

  public Region createPartitionRegion(String regionName) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    RegionFactory factory = CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(paf.create());
    return factory.create(regionName);
  }
 
  public Region createColocatedPartitionRegion(String regionName, final String colocatedRegion) {
     PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setColocatedWith(colocatedRegion);
    RegionFactory factory = CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(paf.create());
    return factory.create(regionName);
  }
  

  @Override
  protected Object[] executeQueries(String[] queries) {
    ResultCollector collector = FunctionService.onRegion(region1).withArgs(queries).execute(equijoinTestFunction.getId());
    Object result = collector.getResult();
    return (Object[])((ArrayList)result).get(0);
  }
  
  Function equijoinTestFunction = new Function(){
    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public void execute(FunctionContext context) {
      try {
        String[] queries = (String[]) context.getArguments();
        QueryService qs = CacheUtils.getCache().getQueryService();
        
        Object[] results = new SelectResults[queries.length];
        for (int i = 0; i < queries.length; i++) {
          results[i] = qs.newQuery(queries[i]).execute((RegionFunctionContext)context);
        }
        context.getResultSender().lastResult(results);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public String getId() {
      return "Equijoin Query";
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  };
}
