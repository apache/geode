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
/*
 * TestNewFunction.java
 *
 * Created on June 16, 2005, 3:55 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import java.util.ArrayList;
import java.util.Collection;
import junit.framework.TestCase;

/**
 *
 * @author Ketand
 */
public class TestNewFunctionSSorRS extends TestCase {
    
    /** Creates a new instance of TestNewFunction */
    public TestNewFunctionSSorRS(String testName) {
        super(testName);
    }
    protected void setUp() throws java.lang.Exception {
        CacheUtils.startCache();
    }
    
    protected void tearDown() throws java.lang.Exception {
        CacheUtils.closeCache();
    }
    
    public void testNewFunc() throws Exception {
        Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
        for(int i=0; i<4; i++){
            region.put(""+i, new Portfolio(i));
            // CacheUtils.log(new Portfolio(i));
        }
        
        Object r[][] = new Object[2][2];
        QueryService qs;
        qs = CacheUtils.getQueryService();
        
        String queries[] = {
            "SELECT DISTINCT * from /portfolios pf , pf.positions.values pos where status = 'inactive'",
            "select distinct * from /portfolios where ID > 1 ",
  
        };
        
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                QueryObserverImpl observer1 = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer1);
                r[i][0] = q.execute();
                if(!observer1.isIndexesUsed){
                    CacheUtils.log("NO INDEX IS USED!");
                } 
                CacheUtils.log(Utils.printResult(r[i][0]));
            } catch(Exception e) {
                fail("Caught exception");
                e.printStackTrace();
            }
        }
        
        qs.createIndex("sIndex", IndexType.FUNCTIONAL,"status","/portfolios");
        qs.createIndex("iIndex", IndexType.FUNCTIONAL,"ID","/portfolios");
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                QueryObserverImpl observer2 = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer2);
                r[i][1] = q.execute();
                if(observer2.isIndexesUsed){
                    CacheUtils.log("YES INDEX IS USED!");
                } else {
                    fail("Index NOT Used");
                }
                 CacheUtils.log(Utils.printResult(r[i][1]));
            } catch(Exception e) {
                fail("Caught exception");
                e.printStackTrace();
            }
        }
        
        StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
        ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,queries);
    }
     class QueryObserverImpl extends QueryObserverAdapter{
        boolean isIndexesUsed = false;
        ArrayList indexesUsed = new ArrayList();
        
        public void beforeIndexLookup(Index index, int oper, Object key) {
            indexesUsed.add(index.getName());
        }
        
        public void afterIndexLookup(Collection results) {
            if(results != null){
                isIndexesUsed = true;
            }
        }
    }
      
}