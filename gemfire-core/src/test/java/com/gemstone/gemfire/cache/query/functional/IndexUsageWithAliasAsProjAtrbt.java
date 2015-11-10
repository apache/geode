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
 * IndexUsageWithAliasAsProjAtrbt.java
 *
 * Created on May 4, 2005, 11:10 AM
 *@author vikramj
 */
package com.gemstone.gemfire.cache.query.functional;

import com.gemstone.gemfire.cache.Region;
import junit.framework.TestCase;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import java.util.*;

public class IndexUsageWithAliasAsProjAtrbt extends TestCase{
    ObjectType resType1=null;
    ObjectType resType2= null;
    
    
    int resSize1=0;
    int resSize2=0;
    
    Iterator itert1=null;
    Iterator itert2=null;
    
    Set set1=null;
    Set set2=null;
    
    String s1;
    String s2;
    
    public IndexUsageWithAliasAsProjAtrbt(String testName){
        super(testName);
    }
    
    protected void setUp() throws java.lang.Exception {
        CacheUtils.startCache();
    }
    
    protected void tearDown() throws java.lang.Exception {
        CacheUtils.closeCache();
    }
    
    public void testComparisonBetnWithAndWithoutIndexCreation() throws Exception {
        //TASK IUM 7
        Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
        for(int i=0;i<4;i++){
            region.put(""+i, new Portfolio(i));
        }
        QueryService qs;
        qs = CacheUtils.getQueryService();
        String queries[] = {
            // IUM 7
            "Select distinct security from /portfolios, secIds security where length > 1",
            // IUM 8         
            "Select distinct security from /portfolios , secIds security where length > 2 AND (intern <> 'SUN' OR intern <> 'DELL' )",
            // IUM 9 
            "Select distinct  security from /portfolios  pos , secIds security where length > 2 and pos.ID > 0"         
                    
        };
        SelectResults r[][]= new SelectResults[queries.length][2];
        
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
               r[i][0] = (SelectResults)q.execute();
                
                QueryObserverImpl observer = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer);
                if(!observer.isIndexesUsed){
                    CacheUtils.log("NO INDEX USED");
                }else {
                  fail("If index were not there how did they get used ???? ");
                }
                
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        
        //  Create an Index on status and execute the same query again.
        
        qs = CacheUtils.getQueryService();
        qs.createIndex("lengthIndex", IndexType.FUNCTIONAL,"length","/portfolios,secIds, positions.values");

        String queries2[] = {
            // IUM 7
            "Select distinct security from /portfolios, secIds security where length > 1", 
            // IUM 8
            "Select distinct security from /portfolios , secIds security where length > 2 AND (intern <> 'SUN' OR intern <> 'DELL' )",        
            // IUM 9 
          "Select distinct  security from /portfolios  pos , secIds security where length > 2 and pos.ID > 0"         
                    
        };
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
               r[i][1] = (SelectResults) q.execute();
                
                QueryObserverImpl observer2 = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer2);
                if(observer2.isIndexesUsed == true){
                    CacheUtils.log("YES INDEX IS USED!");
                }else {
                  fail("Index should have been used!!! ");
                }
                
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        
       CacheUtils.compareResultsOfWithAndWithoutIndex(r,this);
    }
    public void testNestedQResultComposition() throws Exception{
        Region region = CacheUtils.createRegion("pos", Portfolio.class);
        for(int i=0;i<4;i++){
            region.put(""+i, new Portfolio(i));
        }
        CacheUtils.getQueryService();
        String queries[] = {
          //"select distinct * from /pos, positions where value!=null",
          //"select distinct intern from /pos,names where length>=3",
            "select distinct nm from /pos prt,names nm where ID>0",
            "select distinct prt from /pos prt, names where names[3]='ddd'"
        };
        try{
            for(int i=0;i<queries.length;i++){
                Query q = CacheUtils.getQueryService().newQuery(queries[i]);
                q.execute();
             //   CacheUtils.log(Utils.printResult(result));
                
            }
        }catch (Exception e){
            e.printStackTrace();
            fail();
        }
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
