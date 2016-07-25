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
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class CustomerOptimizationsJUnitTest
{
  public CustomerOptimizationsJUnitTest() {
  }
  
  @Test
  public void testProjectionEvaluationDuringIndexResults() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    String[] queries = new String[] {
        "select  p.status from /pos p where p.ID > 0 " ,
        "select  p.status from /pos p, p.positions pos where p.ID > 0 " ,
        "select  p.status from /pos p  where p.ID > 0 and p.createTime > 0" ,
        "select  p.status as sts, p as pos from /pos p  where p.ID > 0 and p.createTime > 0" ,
        "select  p.status as sts, p as pos from /pos p  where p.ID IN  SET( 0,1,2,3) and p.createTime > 0", 
        "select  p.status as sts, p as pos from /pos p  where ( p.ID IN  SET( 0,1,2,3) and p.createTime > 0L) OR (p.ID IN  SET( 2,3) and p.createTime > 5L)"  
          
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");
    
    
    ObjectType[] expectedTypes = new ObjectType[] {
        new ObjectTypeImpl(String.class),
        new ObjectTypeImpl(String.class),
        new ObjectTypeImpl(String.class),
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)}),
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)}),
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)}),
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)})
    };

    final boolean [] expectedCallback = {false, true, false, false,false,true};
    final boolean [] actualCallback = new boolean[queries.length];
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedCallback[i],actualCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }

  @Ignore
  @Test
  public void testProjectionEvaluationDuringIndexResults_UNIMPLEMENTED() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    String[] queries = new String[] {      
        "select  p.status from /pos p, p.positions pos where p.ID > 0 " ,
        "select  p.status as sts, p as pos from /pos p  where ( p.ID IN  SET( 0,1,2,3) and p.createTime > 0L) OR (p.ID IN  SET( 2,3) and p.createTime > 5L)",  
        "select  p.status as sts, p as pos from /pos p  where  p.ID IN  SET( 0,1,2,3) and p.createTime > 0L"
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    
    
    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class),
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)}),
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)})
    };

    final boolean [] expectedCallback = {false,false, false};
    final boolean [] actualCallback = new boolean[queries.length];
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedCallback[i],actualCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testUnionDuringIndexEvaluationForIN() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  p.status as sts, p as pos from /pos p  where  p.ID IN  SET( 0,1,2,3,4,5) ",
        "select  p.status as sts, p as pos from /pos p  where  p.ID IN  SET( 0,1,2,3,4,5,101,102,103,104,105) AND p.createTime > 9l"        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    final boolean [] expectedIndexUsed = new boolean[]{true,true};
    final boolean [] actualIndexUsed = new boolean[]{false,false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false,false};
    final boolean [] actualProjectionCallback = new boolean[]{false,false};
 
    final boolean [] expectedUnionCallback = {false,false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false,false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)}),
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)})
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testBug39851() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {        
        "select  p.status as sts, p as pos from /pos p  where  p.ID IN  ( Select x.ID from /pos x where x.ID > 10) "
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    final boolean [] expectedIndexUsed = new boolean[]{true,true};
    final boolean [] actualIndexUsed = new boolean[]{false,false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false,false};
    final boolean [] actualProjectionCallback = new boolean[]{false,false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)})
        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testUnionDuringIndexEvaluationWithMultipleFilters() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  p.status as sts, p as pos from /pos p  where  p.ID IN  SET( 0,1,2,3,4,5,101,102,103,104,105) AND p.createTime > 9l"
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)})        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }

  @Ignore
  @Test
  public void testProjectionEvaluationDuringIndexResultsWithComplexWhereClause_UNIMPLEMENTED_1() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  p.status as sts, p as pos from /pos p  where   (p.ID IN  SET( 0,1,2,3,4,5,101,102,103,104,105) AND p.createTime > 9l) OR (p.ID IN  SET( 20,30,110,120) AND p.createTime > 7l)"
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    //qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {true};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)})        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }

  @Ignore
  @Test
  public void testProjectionEvaluationDuringIndexResultsWithComplexWhereClause_UNIMPLEMENTED_2() throws QueryException {
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  p.status as sts, p as pos from /pos p  where   (p.ID IN  SET( 0,1,2,3,4,5,101,102,103,104,105) AND p.createTime > 9l) OR (p.ID IN  SET( 20,30,110,120) AND p.createTime > 7l)"
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {true};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new StructTypeImpl(new String[]{"sts","pos"}, new ObjectType[]{new ObjectTypeImpl(String.class),new ObjectTypeImpl(Portfolio.class)})        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testSuspectedBug_1() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where   p.ID IN  SET( 0) AND p.createTime IN SET( 4l ) AND  p.\"type\" IN SET( 'type0') AND p.status IN SET( 'active')" 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testNestedJunction() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 10000; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  (p.createTime IN SET( 10l ) OR  p.status IN SET( 'active') )AND  p.ID >  0 " 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    final List indexUsed = new ArrayList(); 
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{true};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {true};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexUsed.size(),3);
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testRangeQuery() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  p.createTime > 0 AND p.createTime <11 AND  p.ID IN  SET( 0) " 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    final List indexesUsed = new ArrayList();
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexesUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexesUsed.size(),1);
    assertEquals( ((Index)indexesUsed.iterator().next()).getName(),"PortFolioID");
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testInAndEqualityCombination() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  p.ID = 11 AND   p.createTime IN  SET( 10L) " 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    final List indexesUsed = new ArrayList();
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexesUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexesUsed.size(),1);
    assertEquals( ((Index)indexesUsed.iterator().next()).getName(),"PortFolioID");
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testRangeAndNotEqualCombination() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  p.ID > 11 AND  p.ID < 20 AND  p.createTime <>9L " 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{true};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    final List indexesUsed = new ArrayList();
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexesUsed.add(index);
      }
      public void beforeIndexLookup(Index index, int lowerBoundOperator,
          Object lowerBoundKey, int upperBoundOperator, Object upperBoundKey,
          Set NotEqualKeys) {
        actualIndexUsed[i] = true;
        indexesUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexesUsed.size(),1);
    assertEquals( ((Index)indexesUsed.iterator().next()).getName(),"PortFolioID");
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testInAndRangeCombination() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  p.ID > 11 AND  p.ID < 19 and  p.createTime IN  SET( 10L) " 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    final List indexesUsed = new ArrayList();
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexesUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexesUsed.size(),1);
    assertEquals( ((Index)indexesUsed.iterator().next()).getName(),"CreateTime");
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testNotFilterableNestedJunction() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 10000; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  (p.createTime IN SET( 10l ) OR  p.status IN SET( 'active') )AND  p.ID >  0 AND  p.createTime = 10l" 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    final List indexUsed = new ArrayList(); 
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    //qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexUsed.size(),1);
    assertEquals(((Index)indexUsed.iterator().next()).getName(),"CreateTime");
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  //ideally rojection should have been evaluated while collecting index results
  @Ignore
  @Test
  public void testProjectionEvaluationOnORJunction_NOT_IMPLEMENTED() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 10000; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  p.createTime IN SET( 10l ) OR  p.status IN SET( 'active') OR p.ID >  0" 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    final List indexUsed = new ArrayList(); 
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexUsed.size(),3);
    assertEquals(((Index)indexUsed.iterator().next()).getName(),"CreateTime");
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Test
  public void testLiteralBehaviour_1() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  true" 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    final List indexUsed = new ArrayList(); 
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{false};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexUsed.size(),0);
    
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  @Test
  public void testLiteralBheaviour_2() throws Exception{
    QueryService qs = CacheUtils.getQueryService();
    Region rgn = CacheUtils.getRegion("/pos");
    for(int i =100; i < 1000; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.setCreateTime(10l);
      rgn.put(""+i, pf);
    }
    String[] queries = new String[] {       
        "select  distinct p.status  from /pos p  where  p.createTime = 10l AND  p.status IN SET( 'active') AND  true" 
        
        };
    SelectResults[][] sr = new SelectResults[queries.length][2];
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);      
      sr[i][0] = (SelectResults)q.execute();      
    }
    final List indexUsed = new ArrayList(); 
    qs.createIndex("PortFolioID", IndexType.FUNCTIONAL,"ID", "/pos");    
    qs.createIndex("CreateTime", IndexType.FUNCTIONAL,"createTime", "/pos");
    qs.createIndex("Status", IndexType.FUNCTIONAL,"status", "/pos");
    qs.createIndex("Type", IndexType.FUNCTIONAL,"\"type\"", "/pos");
    final boolean [] expectedIndexUsed = new boolean[]{true};
    final boolean [] actualIndexUsed = new boolean[]{false};
    
    final boolean [] expectedProjectionCallabck = new boolean[]{false};
    final boolean [] actualProjectionCallback = new boolean[]{false};
 
    final boolean [] expectedUnionCallback = {false};
    final boolean [] actualUnionCallback = new boolean[queries.length];
    
    
    final boolean [] expectedIntersectionCallback = {false};
    final boolean [] actualIntersectionCallback = new boolean[queries.length];

    ObjectType[] expectedTypes = new ObjectType[] {  
        new ObjectTypeImpl(String.class)        
    };
    QueryObserverHolder.setInstance(new QueryObserverAdapter() {
      private int i =0;
      public void invokedQueryUtilsUnion(SelectResults r1, SelectResults r2)
      {    
         actualUnionCallback[i] = true;
      }
      
      public void invokedQueryUtilsIntersection(SelectResults r1, SelectResults r2)
      {    
         actualIntersectionCallback[i] = true;
      }
      
      public void beforeIndexLookup(Index index, int oper, Object key) {
        actualIndexUsed[i] = true;
        indexUsed.add(index);
      }
      
      public void beforeApplyingProjectionOnFilterEvaluatedResults(Object preProjectionApplied)
      {    
         actualProjectionCallback[i] = true;
      }
      
      public void afterQueryEvaluation(Object result)
      {    
         ++i;
      }
    });
    
    for(int i=0; i <queries.length;++i) {
      Query q = qs.newQuery(queries[i]);
      sr[i][1] = (SelectResults)q.execute();
      assertEquals(expectedUnionCallback[i],actualUnionCallback[i]);
      assertEquals(expectedTypes[i],sr[i][1].getCollectionType().getElementType());
      assertEquals(expectedIndexUsed[i],actualIndexUsed[i]);      
      assertEquals(expectedIntersectionCallback[i],actualIntersectionCallback[i]);
      assertEquals(expectedProjectionCallabck[i],actualProjectionCallback[i]);
    }
    assertEquals(indexUsed.size(),1);
    assertEquals(((Index)indexUsed.iterator().next()).getName(),"Status");
    CacheUtils.compareResultsOfWithAndWithoutIndex(sr, this);
  }
  
  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    Cache cache = CacheUtils.getCache();
    Region region = CacheUtils.createRegion("pos", Portfolio.class);
    for(int i =0; i < 100; ++i) {
      region.put(""+i, new Portfolio(i));
    }
    
  }
  
  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

}
