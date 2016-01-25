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
package com.gemstone.gemfire.cache.query.dunit;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.functional.StructSetOrResultsSet;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.LocalDataSet;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.execute.PRClientServerTestBase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This tests the querying using a RegionFunctionContext which provides a filter
 * (routing keys) to run the query on subset of buckets "locally". If query
 * includes buckets
 *
 * @author shobhit
 *
 */
public class QueryUsingFunctionContextDUnitTest extends CacheTestCase {

  private static final int cnt = 0;

  private static final int cntDest = 100;

  static VM server1 = null;

  static VM server2 = null;

  static VM server3 = null;

  static VM client = null;

  static Cache cache = null;

  static Function function = null;
  // PR 2 is co-located with 1 and 3 is co-located with 2
  // PR 5 is co-located with 4
  static String PartitionedRegionName1 = "TestPartitionedRegion1"; //default name
  static String PartitionedRegionName2 = "TestPartitionedRegion2"; //default name
  static String PartitionedRegionName3 = "TestPartitionedRegion3"; //default name
  static String PartitionedRegionName4 = "TestPartitionedRegion4"; //default name
  static String PartitionedRegionName5 = "TestPartitionedRegion5"; //default name

  static String repRegionName = "TestRepRegion"; //default name
  static String localRegionName = "TestLocalRegion"; //default name

  static Integer serverPort1 = null;

  static Integer serverPort2 = null;

  static Integer serverPort3 = null;

  public static int numOfBuckets = 20;

  public static String[] queries = new String[] {
      "select * from /" + PartitionedRegionName1 + " where ID>=0",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2 where r1.ID = r2.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2 where r1.ID = r2.ID AND r1.status = r2.status",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2, /" + PartitionedRegionName3 + " r3 where r1.ID = r2.ID and r2.ID = r3.ID",
      "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName2 + " r2, /" + PartitionedRegionName3
          + " r3  , /" + repRegionName + " r4 where r1.ID = r2.ID and r2.ID = r3.ID and r3.ID = r4.ID",
     // "Select * from /" + PartitionedRegionName4 + " r4 , /" + PartitionedRegionName5 + " r5 where r4.ID = r5.ID"
      };

  public static String[] nonColocatedQueries = new String[]{
    "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName4 + " r4 where r1.ID = r4.ID",
    "Select * from /" + PartitionedRegionName1 + " r1, /" + PartitionedRegionName4 + " r4 , /" + PartitionedRegionName5 + " r5 where r1.ID = r4.ID and r4.ID = r5.ID"
  };
  
  public static String[] queriesForRR = new String[]{"<trace> select * from /"+repRegionName+" where ID>=0"};

  private static PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");
  /**
   * @param name
   */
  public QueryUsingFunctionContextDUnitTest(String name) {
    super(name);

  }

  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(CacheTestCase.class, "disconnectFromDS");
    super.tearDown2();
    invokeInEveryVM(QueryObserverHolder.class, "reset");
    cache = null;
    invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    server1 = host.getVM(0);
    server2 = host.getVM(1);
    server3 = host.getVM(2);
    client = host.getVM(3);
    createServersWithRegions();
    fillValuesInRegions();
    registerFunctionOnServers();
  }


  /**
   * Test on Replicated Region.
   */
  public void testQueriesWithFilterKeysOnReplicatedRegion() {
    addExpectedException("IllegalArgumentException");

    Object[][] r = new Object[queriesForRR.length][2];

    client.invoke(new CacheSerializableRunnable("Run function on RR") {
      @Override
      public void run2() throws CacheException {

        ResultCollector rcollector = null;

        for (int i = 0; i < queriesForRR.length; i++) {

          try {
            function = new TestQueryFunction("queryFunctionOnRR");

            rcollector = FunctionService
                .onRegion(
                    CacheFactory.getAnyInstance().getRegion(repRegionName))
                .withArgs(queriesForRR[i]).execute(function);

            //Should not come here, an exception is expected from above function call.
            fail("Function call did not fail for query with function context");
          } catch (FunctionException ex) {
            if (!(ex.getCause() instanceof IllegalArgumentException)) {
              fail("Should have received an IllegalArgumentException");
            }
          }
        }//For loop ends here.
      }
    });
  }

  /**
   * Test on PR on one server only using filter.
   */
  public void testQueriesWithFilterKeysOnPRLocal() {
    
    client.invoke(new CacheSerializableRunnable("Test query on client and server") {
      
      @Override
      public void run2() throws CacheException {
        Set filter =  new HashSet();
        filter.add(0);

        for (int i=0; i< queries.length; i++) {
          Object[][] r = new Object[1][2];

          TestServerQueryFunction func = new TestServerQueryFunction("LDS Server function-1");
          function = new TestQueryFunction("queryFunction-1");
          QueryUsingFunctionContextDUnitTest test = new QueryUsingFunctionContextDUnitTest("test");
          ArrayList queryResults2 = test.runQueryOnClientUsingFunc(function, PartitionedRegionName1, filter, queries[i]);  
          if (queryResults2 == null)
            fail(queries[i] +"result is null from client function");
          
          ArrayList queryResults1 = test.runLDSQueryOnClientUsingFunc(func, filter, queries[i]);
          if (queryResults1 == null)
            fail(queries[i] +"result is null from LDS function");
          
          r[0][0] = queryResults1;
          r[0][1] = queryResults2;
          StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
          ssORrs.CompareQueryResultsAsListWithoutAndWithIndexes(r, 1,false, new String[]{queries[i]});
        }   
      }
    });
    
  }

  public void testInvalidQueries() {
    
    addExpectedException("Syntax error");
    client.invoke(new CacheSerializableRunnable("Test query on client and server") {
      
      @Override
      public void run2() throws CacheException {
        Set filter =  new HashSet();
        filter.add(0);
        String query = "select * from / " + repRegionName + " where ID>=0";
        TestServerQueryFunction func = new TestServerQueryFunction("LDS Server function-1");
        function = new TestQueryFunction("queryFunction-1");
        QueryUsingFunctionContextDUnitTest test = new QueryUsingFunctionContextDUnitTest("test");
        try {
          test.runQueryOnClientUsingFunc(function, repRegionName, filter, query);  
          fail("Query execution should have failed.");
        } catch (FunctionException ex) {
          assertTrue("The exception message should mention QueryInvalidException. ", ex.getLocalizedMessage().contains("QueryInvalidException"));
        }
        
        query = "select * from / " + PartitionedRegionName1 + " where ID>=0";
        func = new TestServerQueryFunction("LDS Server function-1");
        function = new TestQueryFunction("queryFunction-1");
        test = new QueryUsingFunctionContextDUnitTest("test");
        try {
          test.runQueryOnClientUsingFunc(function, PartitionedRegionName1, filter, query);  
          fail("Query execution should have failed.");
        } catch (FunctionException ex) {
          assertTrue("The exception message should mention QueryInvalidException. ", ex.getLocalizedMessage().contains("QueryInvalidException"));
        }
      }
    });
    
  }
  
  /**
   *
   */
  public void testQueriesWithFilterKeysOnPRLocalAndRemote() {
    
    client.invoke(new CacheSerializableRunnable("Test query on client and server") {
      
      @Override
      public void run2() throws CacheException {
        Set filter = getFilter(0, 1);

        TestServerQueryFunction func = new TestServerQueryFunction("LDS Server function-1");
        function = new TestQueryFunction("queryFunction-2");

        for (int i=0; i< queries.length; i++) {
          Object[][] r = new Object[1][2];

          QueryUsingFunctionContextDUnitTest test = new QueryUsingFunctionContextDUnitTest("test");
          ArrayList queryResults2 = test.runQueryOnClientUsingFunc(function, PartitionedRegionName1, filter, queries[i]);
          if (queryResults2 == null)
            fail(queries[i] +"result is null from client function");
          ArrayList queryResults1 = test.runLDSQueryOnClientUsingFunc(func, filter, queries[i]);
          if (queryResults1 == null)
            fail(queries[i] +"result is null from LDS function");
          

          r[0][0] = queryResults1;
          r[0][1] = queryResults2;
          StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
          ssORrs.CompareQueryResultsAsListWithoutAndWithIndexes(r, 1,false, new String[]{queries[i]});
        }    
      }
    });
  }

  /**
  *
  */
 public void testQueriesWithFilterKeysOnPRLocalAndRemoteWithBucketDestroy() {
   
   // Set Query Observer in cache on server1
   server1.invoke(new CacheSerializableRunnable("Set QueryObserver in cache on server1") {
     @Override
     public void run2() throws CacheException {

       class MyQueryObserver extends IndexTrackingQueryObserver {

         @Override
         public void startQuery(Query query) {
           //Destroy only for first query.
           if (query.getQueryString().contains("ID>=0")){
             Region pr = CacheFactory.getAnyInstance().getRegion(PartitionedRegionName1);
             Region KeyRegion = null;
             for (int i=3; i<7; i++) {
               KeyRegion = ((PartitionedRegion)pr).getBucketRegion(i/*key*/);
               if (KeyRegion != null)
               KeyRegion.destroyRegion();
             }
           }
         }
       };

       QueryObserverHolder.setInstance(new MyQueryObserver());
     }
   });
   
   client.invoke(new CacheSerializableRunnable("Test query on client and server") {
     
     @Override
     public void run2() throws CacheException {
       Set filter = getFilter(0, 2);
       
       TestServerQueryFunction func = new TestServerQueryFunction("LDS Server function-2");
       function = new TestQueryFunction("queryFunction");

       for (int i=0; i< queries.length; i++) {
         
         QueryUsingFunctionContextDUnitTest test = new QueryUsingFunctionContextDUnitTest("test");
         ArrayList queryResults2 = test.runQueryOnClientUsingFunc(function, PartitionedRegionName1, filter, queries[i]);  
         
         // The Partition Region has 20 buckets with 100 values and key i goes in bucket j=(i%20)
         // So each bucket has 5 keys so for 3 buckets 3*5.
         if (i==0 && queryResults2.size() != 3*5) {
           fail("Result size should have been 15 but was"+queryResults2.size());
         }
       }    
     }
   });
   // Reset Query Observer in cache on server1
   server1.invoke(new CacheSerializableRunnable("Reset Query Observer on server1") {
     @Override
     public void run2() throws CacheException {
       QueryObserverHolder.reset();
     }
   });

 }

  /**
   *
   */
  public void testQueriesWithFilterKeysOnPRWithBucketDestroy() {
    addExpectedException("QueryInvocationTargetException");
    Object[][] r = new Object[queries.length][2];
    Set filter =  new HashSet();

    // Close cache on server1
    server1.invoke(new CacheSerializableRunnable("Set QueryObserver in cache on server1") {
      @Override
      public void run2() throws CacheException {

        class MyQueryObserver extends IndexTrackingQueryObserver {

          @Override
          public void startQuery(Query query) {
            Region pr = CacheFactory.getAnyInstance().getRegion(PartitionedRegionName1);
            Region KeyRegion = null;
            for (int i=0; i<7; i++) {
              KeyRegion = ((PartitionedRegion)pr).getBucketRegion(i/*key*/);
              if (KeyRegion != null)
              KeyRegion.destroyRegion();
            }
          }
        };

        QueryObserverHolder.setInstance(new MyQueryObserver());
      }
    });

    client.invoke(new CacheSerializableRunnable("Run function on PR") {
      @Override
      public void run2() throws CacheException {
        Set filter =  new HashSet();
        ResultCollector rcollector = null;
        filter.addAll(getFilter(0, 19));

        for (int i = 0; i < queries.length; i++) {

          try {
            function = new TestQueryFunction("queryFunctionBucketDestroy");

            rcollector = FunctionService
                .onRegion(
                    CacheFactory.getAnyInstance().getRegion(PartitionedRegionName1))
                .withArgs(queries[i])
                .withFilter(filter)
                .execute(function);

            //Should not come here, an exception is expected from above function call.
            fail("Function call did not fail for query with function context");
          } catch (FunctionException ex) {
            //ex.printStackTrace();
            if (!(ex.getCause() instanceof QueryInvocationTargetException)) {
              fail("Should have received an QueryInvocationTargetException but recieved"+ex.getMessage());
            }
          }
        }//For loop ends here.
      }
    });

 // Close cache on server1
    server1.invoke(new CacheSerializableRunnable("Reset Query Observer on server1") {
      @Override
      public void run2() throws CacheException {
        QueryObserverHolder.reset();
      }
    });

  }

  /**
  *
  */
 public void testQueriesWithFilterKeysOnPRWithRebalancing() {
   addExpectedException("QueryInvocationTargetException");
   addExpectedException("java.net.SocketException");
   addExpectedException("ServerConnectivityException");
   addExpectedException("FunctionException");
   addExpectedException("IOException");

   // Close cache on server1
   server1.invoke(new CacheSerializableRunnable("Set QueryObserver in cache on server1") {
     @Override
     public void run2() throws CacheException {

       class MyQueryObserver extends IndexTrackingQueryObserver {

         @Override
         public void startQuery(Query query) {
           Region pr = CacheFactory.getAnyInstance().getRegion(PartitionedRegionName1);
           Region KeyRegion = null;
           for (int i=6; i<9; i++) {
             KeyRegion = ((PartitionedRegion)pr).getBucketRegion(i/*key*/);
             if (KeyRegion != null)
             KeyRegion.destroyRegion();
           }
         }
       };

       QueryObserverHolder.setInstance(new MyQueryObserver());
     }
   });

   client.invoke(new CacheSerializableRunnable("Run function on PR") {
     @Override
     public void run2() throws CacheException {
       Set filter =  new HashSet();
       ResultCollector rcollector = null;
       filter.addAll(getFilter(6, 9));

       for (int i = 0; i < queries.length; i++) {

         try {
           function = new TestQueryFunction("queryFunction");

           rcollector = FunctionService
               .onRegion(
                   CacheFactory.getAnyInstance().getRegion(PartitionedRegionName1))
               .withArgs(queries[i])
               .withFilter(filter)
               .execute(function);

           //Should not come here, an exception is expected from above function call.
           fail("Function call did not fail for query with function context");
         } catch (FunctionException ex) {
           if (!((ex.getCause() instanceof QueryInvocationTargetException) || (ex.getCause() instanceof ServerConnectivityException))) {
             if (ex.getCause() instanceof FunctionException) {
               FunctionException fe = (FunctionException)ex.getCause();
               if (!fe.getMessage().startsWith("IOException")) {
                 fail("Should have received an QueryInvocationTargetException but recieved"+ex.getMessage());
               }
             }
             else {
               fail("Should have received an QueryInvocationTargetException but recieved"+ex.getMessage());
             }
           }
         }
       }//For loop ends here.
     }
   });

// Close cache on server1
   server1.invoke(new CacheSerializableRunnable("Reset Query Observer on server1") {
     @Override
     public void run2() throws CacheException {
       QueryObserverHolder.reset();
     }
   });

 }

 
 public void testNonColocatedRegionQueries() {
   addExpectedException("UnsupportedOperationException");
   client.invoke(new CacheSerializableRunnable("Test query on non-colocated regions on server") {
     @Override
     public void run2() throws CacheException {
       Set filter =  new HashSet();
       filter.add(0);

       for (int i=0; i< nonColocatedQueries.length; i++) {
         function = new TestQueryFunction("queryFunction-1");
         QueryUsingFunctionContextDUnitTest test = new QueryUsingFunctionContextDUnitTest("test");
         try {
          ArrayList queryResults2 = test.runQueryOnClientUsingFunc(function, PartitionedRegionName1, filter, nonColocatedQueries[i]);
          fail("Function call did not fail for query with function context");
         } catch (FunctionException e) {
          if (!(e.getCause() instanceof UnsupportedOperationException)) {
            fail("Should have received an UnsupportedOperationException but received", e);
          }
         }  
        }   
     }
   });
   
 }
 
 public void testJoinQueryPRWithMultipleIndexes(){
   
   server1.invoke(new CacheSerializableRunnable("Test query with indexes") {
     
     @Override
     public void run2() throws CacheException {
       Set filter = getFilter(0, 1);
       function = new TestQueryFunction("queryFunction-2");
       Object[][] r = new Object[2][2];
       QueryUsingFunctionContextDUnitTest test = new QueryUsingFunctionContextDUnitTest("test");
       int j = 0;
       for (int i=3; i< 5; i++) {
         ArrayList queryResults2 = test.runQueryOnClientUsingFunc(function, PartitionedRegionName1, filter, queries[i]);
         if (queryResults2 == null)
           fail(queries[i] +"result is null from client function");
         r[j++][1] = queryResults2;
       }
       createIndex();
       j = 0;
       for (int i=3; i< 5; i++) {
         ArrayList queryResults1 = test.runQueryOnClientUsingFunc(function, PartitionedRegionName1, filter, queries[i]);
         if (queryResults1 == null)
           fail(queries[i] +"result is null from client function");
         r[j++][0] = queryResults1;
       }
       
       StructSetOrResultsSet ssORrs = new  StructSetOrResultsSet();
       ssORrs.CompareQueryResultsAsListWithoutAndWithIndexes(r, 2,false, queries);

     }
   });
 }
 

  //Helper classes and function
  public class TestQueryFunction extends FunctionAdapter {

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    private final String id;


    public TestQueryFunction(String id) {
      super();
      this.id = id;
    }

    @Override
    public void execute(FunctionContext context) {
      Cache cache = CacheFactory.getAnyInstance();
      QueryService queryService = cache.getQueryService();
      ArrayList allQueryResults = new ArrayList();
      String qstr = (String) context.getArguments();
      try {
        Query query = queryService.newQuery(qstr);
        context.getResultSender().lastResult((ArrayList) ((SelectResults) query
                .execute((RegionFunctionContext) context)).asList());
      } catch (Exception e) {
        throw new FunctionException(e);
      }
    }

    @Override
    public String getId() {
      return this.id;
    }
  }
  
  public class TestServerQueryFunction extends FunctionAdapter {

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    private final String id;


    public TestServerQueryFunction(String id) {
      super();
      this.id = id;
    }

    @Override
    public void execute(FunctionContext context) {
      Cache cache = CacheFactory.getAnyInstance();
      QueryService queryService = cache.getQueryService();
      ArrayList allQueryResults = new ArrayList();
      Object[] args = (Object[])context.getArguments();
      Set buckets = getBucketsForFilter((Set)args[1]);
      Region localDataSet = new LocalDataSet((PartitionedRegion)CacheFactory.getAnyInstance().getRegion(PartitionedRegionName1), buckets);

      try {
        Query query = queryService.newQuery((String)args[0]);
        context.getResultSender().lastResult((ArrayList) ((SelectResults) ((LocalDataSet) localDataSet).executeQuery(
            (DefaultQuery) query, null, buckets)).asList());
      } catch (Exception e) {
        throw new FunctionException(e);
      }
    }

    @Override
    public String getId() {
      return this.id;
    }
    
    private Set getBucketsForFilter(Set filter) {
      Set bucketids = new HashSet();
      for (Object key: filter) {
        int intKey = ((Integer)key).intValue();
        bucketids.add(intKey%numOfBuckets);
      }
      return bucketids;
    }
  }

  public void fillValuesInRegions() {
    //Create common Portflios and NewPortfolios
    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest);

    //Fill local region
    server1.invoke(getCacheSerializableRunnableForPRPuts(localRegionName,
        portfolio, cnt, cntDest));

    //Fill replicated region
    server1.invoke(getCacheSerializableRunnableForPRPuts(repRegionName,
        portfolio, cnt, cntDest));

    //Fill Partition Region
    server1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName1,
        portfolio, cnt, cntDest));
    server1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName2,
        portfolio, cnt, cntDest));
    server1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName3,
        portfolio, cnt, cntDest));
    server1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName4,
        portfolio, cnt, cntDest));
    server1.invoke(getCacheSerializableRunnableForPRPuts(PartitionedRegionName5,
        portfolio, cnt, cntDest));
  }

  private void registerFunctionOnServers() {
    function = new TestQueryFunction("queryFunction");
    server1.invoke(PRClientServerTestBase.class,
        "registerFunction", new Object []{function});

    server2.invoke(PRClientServerTestBase.class,
        "registerFunction", new Object []{function});

    server3.invoke(PRClientServerTestBase.class,
        "registerFunction", new Object []{function});
  }

  private void createServersWithRegions() {
    //Create caches
    Properties props = new Properties();
    server1.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });
    server2.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });
    server3.invoke(PRClientServerTestBase.class, "createCacheInVm",
        new Object[] { props });

    //Create Cache Servers
    Integer port1 = (Integer)server1.invoke(PRClientServerTestBase.class,
    "createCacheServer");
    Integer port2 = (Integer)server2.invoke(PRClientServerTestBase.class,
        "createCacheServer");
    Integer port3 = (Integer)server3.invoke(PRClientServerTestBase.class,
        "createCacheServer");
    serverPort1 = port1;
    serverPort2 = port2;
    serverPort3 = port3;

    //Create client cache without regions
    client.invoke(QueryUsingFunctionContextDUnitTest.class, "createCacheClientWithoutRegion",
        new Object[] { getServerHostName(server1.getHost()), port1, port2,
            port3 });

    //Create proxy regions on client.
    client.invoke(QueryUsingFunctionContextDUnitTest.class, "createProxyRegions");

    //Create local Region on servers
    server1.invoke(QueryUsingFunctionContextDUnitTest.class, "createLocalRegion");

    //Create ReplicatedRegion on servers
    server1.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    server2.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");
    server3.invoke(QueryUsingFunctionContextDUnitTest.class, "createReplicatedRegion");

    //Create two colocated PartitionedRegions On Servers.
    server1.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");
    server2.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");
    server3.invoke(QueryUsingFunctionContextDUnitTest.class, "createColocatedPR");

  }

  public static void createProxyRegions() {
    new QueryUsingFunctionContextDUnitTest("temp").createProxyRegs();
  }

  private void createProxyRegs() {
    ClientCache cache = (ClientCache)CacheFactory.getAnyInstance();
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(repRegionName);

    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(localRegionName);

    cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
      .create(PartitionedRegionName1);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
      .create(PartitionedRegionName2);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
    .create(PartitionedRegionName3);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
    .create(PartitionedRegionName4);
    cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
    .create(PartitionedRegionName5);
  }

  public static void createLocalRegion() {
    new QueryUsingFunctionContextDUnitTest("temp").createLocalReg();
  }
  public void createLocalReg() {
    cache = CacheFactory.getAnyInstance();
    cache.createRegionFactory(RegionShortcut.LOCAL).create(localRegionName);
  }

  public static void createReplicatedRegion() {
    new QueryUsingFunctionContextDUnitTest("temp").createRR();
  }
  public void createRR() {
    cache = CacheFactory.getAnyInstance();
    cache.createRegionFactory(RegionShortcut.REPLICATE).create(repRegionName);
  }

  public static void createColocatedPR() {
    new QueryUsingFunctionContextDUnitTest("temp").createColoPR();
  }
  public void createColoPR() {
    PartitionResolver testKeyBasedResolver = new QueryAPITestPartitionResolver();
    cache = CacheFactory.getAnyInstance();
    cache
        .createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(
            new PartitionAttributesFactory()
                .setTotalNumBuckets(numOfBuckets)
                .setPartitionResolver(testKeyBasedResolver)
                .create())
        .create(PartitionedRegionName1);

    cache
        .createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(
            new PartitionAttributesFactory()
                .setTotalNumBuckets(numOfBuckets)
                .setPartitionResolver(testKeyBasedResolver)
                .setColocatedWith(PartitionedRegionName1)
                .create())
        .create(PartitionedRegionName2);
    
    cache
    .createRegionFactory(RegionShortcut.PARTITION)
    .setPartitionAttributes(
        new PartitionAttributesFactory()
            .setTotalNumBuckets(numOfBuckets)
            .setPartitionResolver(testKeyBasedResolver)
            .setColocatedWith(PartitionedRegionName2)
            .create())
    .create(PartitionedRegionName3);
    
    cache
    .createRegionFactory(RegionShortcut.PARTITION)
    .setPartitionAttributes(
        new PartitionAttributesFactory()
            .setTotalNumBuckets(numOfBuckets)
            .setPartitionResolver(testKeyBasedResolver)
             .create())
    .create(PartitionedRegionName4); // not collocated
    
    cache
    .createRegionFactory(RegionShortcut.PARTITION)
    .setPartitionAttributes(
        new PartitionAttributesFactory()
            .setTotalNumBuckets(numOfBuckets)
            .setPartitionResolver(testKeyBasedResolver)
            .setColocatedWith(PartitionedRegionName4)
             .create())
    .create(PartitionedRegionName5); // collocated with 4 
  }

  public static void createCacheClientWithoutRegion(String host, Integer port1, Integer port2, Integer port3) {
    new QueryUsingFunctionContextDUnitTest("temp").createCacheClientWithoutReg(host, port1, port2, port3);
  }
  private void createCacheClientWithoutReg(String host, Integer port1, Integer port2, Integer port3) {
    this.disconnectFromDS();
    ClientCache cache = new ClientCacheFactory()
      .addPoolServer(host, port1)
      .addPoolServer(host, port2)
      .addPoolServer(host, port3)
      .create();
  }

  /**
   * Run query on server using LocalDataSet.executeQuery() to compare results
   * received from client function execution.
   * @param queries
   * @param filter
   * @return
   */
  public static ArrayList runQueryOnServerLocalDataSet(String query, Set filter) {
    return new QueryUsingFunctionContextDUnitTest("temp").runQueryOnServerLDS(query, filter);
  }

  protected ArrayList runQueryOnServerLDS(String queryStr, Set filter) {

    Set buckets = getBucketsForFilter(filter);
    Region localDataSet = new LocalDataSet((PartitionedRegion)CacheFactory.getAnyInstance().getRegion(PartitionedRegionName1), buckets);

    QueryService qservice = CacheFactory.getAnyInstance().getQueryService();

    Query query = qservice.newQuery(queryStr);
    SelectResults results;
    try {
      results = (SelectResults) ((LocalDataSet) localDataSet).executeQuery(
          (DefaultQuery) query, null, buckets);

      return (ArrayList)results.asList();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Run query on server to compare the results recieved from client function execution.
   * @param queries
   * @return ArrayList of results
   */
  public static ArrayList runQueryOnServerRegion(String query) {
    return new QueryUsingFunctionContextDUnitTest("temp").runQueryOnServerReg(query);
  }
  protected ArrayList runQueryOnServerReg(String queryStr) {

    QueryService qservice = CacheFactory.getAnyInstance().getQueryService();

    Query query = qservice.newQuery(queryStr);
    SelectResults results = null;
    try {
      results = (SelectResults) query.execute();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return results!= null ? (ArrayList)results.asList():null;
  }

  /**
   * Run query using a function executed by client on a region on server with filter.
   * @param function
   * @param regionName
   * @param filter
   * @return ArrayList of results
   */
  public static ArrayList runQueryOnClientUsingFunction(Function function, String regionName, Set filter, String query) {
    return new QueryUsingFunctionContextDUnitTest("temp").runQueryOnClientUsingFunc(function, regionName, filter, query);
  }

  private ArrayList runQueryOnClientUsingFunc(Function func, String regionName, Set filter, String query) {
    ResultCollector rcollector = null;

    // Filter can not be set as null if withFilter() is called.
    if (filter != null) {
      rcollector = FunctionService.onRegion(CacheFactory.getAnyInstance().getRegion(regionName))
        .withArgs(query)
        .withFilter(filter)
        .execute(func);
    } else {
      rcollector = FunctionService.onRegion(CacheFactory.getAnyInstance().getRegion(regionName))
      .withArgs(query)
      .execute(func);
    }
    Object result = rcollector.getResult();
    assertTrue(result instanceof ArrayList);

    //Results from multiple nodes.
    ArrayList resultList = (ArrayList)result;
    resultList.trimToSize();
    List queryResults = null;

    if (resultList.size()!=0 && resultList.get(0) instanceof ArrayList) {
      queryResults = new ArrayList();
      for (Object obj: resultList) {
        if (obj != null) {
          queryResults.addAll((ArrayList)obj);
        }
      }
    }

    return (ArrayList) queryResults;
  }

  /**
   * Runs a {@link LocalDataSet} query on a single server. 
   * @param func
   * @param filter
   * @param query
   * @return results in a List
   */
  private ArrayList runLDSQueryOnClientUsingFunc(Function func, Set filter, String query) {
    ResultCollector rcollector = null;

    // Filter can not be set as null if withFilter() is called.
    rcollector = FunctionService
        .onServer(ClientCacheFactory.getAnyInstance())
        .withArgs(new Object[]{query, filter}).execute(func);
    Object result = rcollector.getResult();
    assertTrue(result instanceof ArrayList);

    //Results from multiple nodes.
    ArrayList resultList = (ArrayList)result;
    resultList.trimToSize();
    List queryResults = new ArrayList();
    
    if (resultList.size()!=0 && resultList.get(0) instanceof ArrayList) {
      for (Object obj: resultList) {
        if (obj != null) {
          queryResults.addAll((ArrayList)obj);
        }
      }
    }

    return (ArrayList) queryResults;
  }

  private Set getFilter(int start, int end) {
    Set filter = new HashSet();
    for (int j=start; j<=end; j++) {
      filter.add(j);
    }
    return filter;
  }


  private Set getBucketsForFilter(Set filter) {
    Set bucketids = new HashSet();
    for (Object key: filter) {
      int intKey = ((Integer)key).intValue();
      bucketids.add(intKey%numOfBuckets);
    }
    return bucketids;
  }

  /**
   * This function puts portfolio objects into the created Region (PR or Local) *
   *
   * @param regionName
   * @param portfolio
   * @param to
   * @param from
   * @return cacheSerializable object
   */
  public CacheSerializableRunnable getCacheSerializableRunnableForPRPuts(
      final String regionName, final Object[] portfolio, final int from,
      final int to)
  {
    SerializableRunnable puts = new CacheSerializableRunnable("Region Puts") {
      @Override
      public void run2() throws CacheException
      {
        Cache cache = CacheFactory.getAnyInstance();
        Region region = cache.getRegion(regionName);
        for (int j = from; j < to; j++)
          region.put(new Integer(j), portfolio[j]);
        getLogWriter()
            .info(
                "PRQueryDUnitHelper#getCacheSerializableRunnableForPRPuts: Inserted Portfolio data on Region "
                    + regionName);
      }
    };
    return (CacheSerializableRunnable)puts;
  }
  
 
  public void createIndex() {
    QueryService qs = CacheFactory.getAnyInstance().getQueryService();
    try {
      qs.createIndex("ID1", "ID", "/"+ PartitionedRegionName1);
      qs.createIndex("ID2", "ID", "/"+ PartitionedRegionName2);
      qs.createIndex("ID3", "ID", "/"+ PartitionedRegionName3);
    } catch (Exception e) {
        fail("Index creation failed " + e);
    }
  }

}
