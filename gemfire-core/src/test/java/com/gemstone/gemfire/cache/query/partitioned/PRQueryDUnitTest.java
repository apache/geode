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

package com.gemstone.gemfire.cache.query.partitioned;

/**
 * This tests creates partition regions across VM's and a local Region across
 * one of the VM executes Queries both on Local Region & PR's & compare
 * ResultSets
 *
 * @author pbatra
 */

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.internal.cache.PartitionedRegionQueryEvaluator;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.VM;

public class PRQueryDUnitTest extends PartitionedRegionDUnitTestCase

{
  /**
   * constructor *
   *
   * @param name
   */

  public PRQueryDUnitTest(String name) {

    super(name);
  }

  static Properties props = new Properties();

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  int totalNumBuckets = 100;

  int totalDataSize = 90;
  int cnt=0;

  int stepSize = 20;

  final String name = "Portfolios";

  final String localName = "LocalPortfolios";

  final int i = 0;

  final int redundancy = 0;

  /**
   * This test <br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verfies the size ,type , contents of both the resultSets Obtained
   *
   * @throws Exception
   */

  public void testPRDAckCreationAndQuerying() throws Exception
  {
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Creating PR's on the participating VM's
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
    // creating a local region on one of the JVM's
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        i, stepSize));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        stepSize, (2 * stepSize)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        (2 * stepSize), (3 * stepSize)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        (3 * (stepSize)), totalDataSize));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, i, totalDataSize));

    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
  }


   /**
    * This test does the following using full queries with projections and drill-down<br>
    * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
    * 2. Creates a Local region on one of the VM's <br>
    * 3. Puts in the same data both in PR region & the Local Region <br>
    * 4. Queries the data both in local & PR <br>
    * 5. Verfies the size ,type , contents of both the resultSets Obtained
    *
    * @throws Exception
    */

  public void testPRDAckCreationAndQueryingFull() throws Exception
  {
    getLogWriter()
    .info(
          "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

    Class valueConstraint = Portfolio.class;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Creating PR's on the participating VM's
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
    // creating a local region on one of the JVM's
    vm0.invoke(PRQHelp
               .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfoliosAndPositions,
                                                             0, stepSize));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfoliosAndPositions,
                                                             stepSize, (2 * stepSize)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfoliosAndPositions,
                                                             (2 * stepSize), (3 * stepSize)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfoliosAndPositions,
                                                             (3 * (stepSize)), totalDataSize));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
                                                             portfoliosAndPositions, i, totalDataSize));

    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
                                                                               name, localName, true));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
  }


  /**
   * This test <br>
   * 1. Creates PR regions across with scope = DACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR using Query Constants like NULL ,
   * UNDEFINED , TRUE, FALSE<br>
   * 5. Verfies the size ,type , contents of both the resultSets Obtained
   *
   * @throws Exception
   */

  public void testPRDAckCreationAndQueryingWithConstants() throws Exception
  {
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : Querying PR Test with DACK Started*****");

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);

    VM vm1 = host.getVM(1);

    VM vm2 = host.getVM(2);

    VM vm3 = host.getVM(3);

    // Creating PR's on the participating VM's
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : Creating PR's on VM0, VM1 , VM2 , VM3");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : Creating Local region on VM0 to compare result Sets");
    // creating a local region on one of the JVM's
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        i, stepSize));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        stepSize, (2 * stepSize)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        (2 * stepSize), (3 * stepSize)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        (3 * (stepSize)), totalDataSize));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : Inserted Portfolio data across PR's");

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, i, totalDataSize));

    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : Inserted Portfolio data over Local Region on VM0");

    // querying the VM for data
    vm0
        .invoke(PRQHelp
            .getCacheSerializableRunnableForPRQueryWithConstantsAndComparingResults(
                name, localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQueryingWithConstants : *Querying PR's with DACK Test ENDED*****");
  }

  /**
   * Test data loss (bucket 0) while the PRQueryEvaluator is processing
   * the query loop
   * @throws Exception
   */
  public void testDataLossDuringQueryProcessor() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore1 = host.getVM(2);
    final VM datastore2 = host.getVM(3);
    final int totalBuckets = 11;
    final int redCop = 0;

    CacheSerializableRunnable createPR = new CacheSerializableRunnable("Create PR") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory attr = new AttributesFactory();
        attr.setValueConstraint(String.class);
        PartitionAttributes prAttr =
          new PartitionAttributesFactory().setRedundantCopies(redCop).setTotalNumBuckets(totalBuckets).create();
        attr.setPartitionAttributes(prAttr);
        getCache().createRegion(rName, attr.create());
      }
    };
    datastore1.invoke(createPR);
    datastore2.invoke(createPR);

    AttributesFactory attr = new AttributesFactory();
    attr.setValueConstraint(String.class);
    PartitionAttributes prAttr =
      new PartitionAttributesFactory().setRedundantCopies(redCop)
      .setTotalNumBuckets(totalBuckets).setLocalMaxMemory(0).create();
    attr.setPartitionAttributes(prAttr);
    PartitionedRegion pr = (PartitionedRegion) getCache().createRegion(rName, attr.create());
    // Create bucket zero, one and two
    pr.put(new Integer(0), "zero");
    pr.put(new Integer(1), "one");
    pr.put(new Integer(2), "two");

    class MyTestHook implements PartitionedRegionQueryEvaluator.TestHook {
      public boolean done = false;
      public void hook(int spot) throws RuntimeException {
        if (spot == 4) {
          synchronized(this) {
            if (done) {
              return;
            }
            this.done = true;
          }
          datastore1.invoke(disconnectVM());
          datastore2.invoke(disconnectVM());
        }
      }
    };
    final MyTestHook th = new MyTestHook();

    // add expected exception strings
    final ExpectedException ex = addExpectedException("Data loss detected");
    try {
      Object[] params = new Object[0];
      final DefaultQuery query = (DefaultQuery)getCache().getQueryService()
          .newQuery("select distinct * from " + pr.getFullPath());
      final SelectResults results = query.getSimpleSelect().getEmptyResultSet(
          params, getCache(), query);

      // TODO assert this is the correct set of bucket Ids,
      final HashSet<Integer> buckets = new HashSet<Integer>();
      for (int i=0; i < 3; i++){
        buckets.add(new Integer(i));
      }
      PartitionedRegionQueryEvaluator qe = new PartitionedRegionQueryEvaluator(
          pr.getSystem(), pr, query, params, results, buckets);

      qe.queryBuckets(th);
      assertTrue(th.done);
      assertTrue(false);
    } catch (QueryException expected) {
      assertTrue(th.done);
    } finally {
      ex.remove();
      getCache().close();
    }
  }

  public void testQueryResultsFromMembers() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore1 = host.getVM(2);
    final VM datastore2 = host.getVM(3);
    final int totalBuckets = 10;
    final int redCop = 0;

    CacheSerializableRunnable createPR = new CacheSerializableRunnable("Create PR") {
      public void run2() throws CacheException {
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributes prAttr =
          new PartitionAttributesFactory().setRedundantCopies(redCop).setTotalNumBuckets(totalBuckets).create();
        attr.setPartitionAttributes(prAttr);
        getCache().createRegion(rName, attr.create());
      }
    };
    datastore1.invoke(createPR);
    datastore2.invoke(createPR);

    
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributes prAttr =
      new PartitionAttributesFactory().setRedundantCopies(redCop)
      .setTotalNumBuckets(totalBuckets).create();
    attr.setPartitionAttributes(prAttr);
    PartitionedRegion pr = (PartitionedRegion) getCache().createRegion(rName, attr.create());
    
    
    // Create bucket zero, one and two
    int numEntries = 100;
    
    for (int i=1; i <= numEntries; i++) {
      pr.put(new Integer(i), new Portfolio(i));  
    }

    int[] limit = new int[] {
        10, 
        15,
        30,
        0,
        1,
        9
    };
    
    String[] queries = new String[] {
        "select * from " + pr.getFullPath() + " LIMIT " + limit[0],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[1],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[2],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[3],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[4],
        "select * from " + pr.getFullPath() + " where ID > 10 LIMIT " + limit[5],
    };

    try {
      for (int q=0; q < queries.length; q++) {
        Object[] params = new Object[0];
        final DefaultQuery query = (DefaultQuery)getCache().getQueryService().newQuery(queries[q]);
        final SelectResults results = query.getSimpleSelect().getEmptyResultSet(params, getCache(), query);

        // TODO assert this is the correct set of bucket Ids,
        final HashSet<Integer> buckets = new HashSet<Integer>();
        for (int i=0; i < totalBuckets; i++){
          buckets.add(new Integer(i));
        }


        final PartitionedRegionQueryEvaluator qe =
          new PartitionedRegionQueryEvaluator(pr.getSystem(), pr, query, params, results, buckets);

        class MyTestHook implements PartitionedRegionQueryEvaluator.TestHook {
          public HashMap resultsPerMember = new HashMap();
          public void hook(int spot) throws RuntimeException {
            int size = 0;
            if (spot == 3) {
              for (Object mr: qe.getResultsPerMember().entrySet()){
                Map.Entry e = (Map.Entry)mr;
                Collection<Collection> results = (Collection<Collection>)e.getValue();
                for (Collection<Object> r : results) {
                  if (this.resultsPerMember.containsKey(e.getKey())) {
                    this.resultsPerMember.put(e.getKey(), new Integer(r.size() + 
                        ((Integer)this.resultsPerMember.get(e.getKey())).intValue()));
                  } else {
                    this.resultsPerMember.put(e.getKey(), new Integer(r.size()));
                  }
                }
              }
            }
          }
        };

        final MyTestHook th = new MyTestHook();
        qe.queryBuckets(th);
        for (Object r: th.resultsPerMember.entrySet()){
          Map.Entry e = (Map.Entry)r;
          Integer res = (Integer)e.getValue();
          getLogWriter().info("PRQueryDUnitTest#testQueryResultsFromMembers : \n" +
              "Query [" + queries[q] + "] Member : " + e.getKey() + " results size :" + res.intValue());
          assertEquals("Query [" + queries[q] + "]: The results returned by the member does not match the query limit size : Member : " + e.getKey(), limit[q], res.intValue());
        }
      }
    } finally {
      getCache().close();
    }
  }

  public void testQueryResultsFromMembersWithAccessor() throws Exception {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    final VM datastore1 = host.getVM(2);
    final VM datastore2 = host.getVM(3);
    final int totalBuckets = 20;
    final int redCop = 0;

    CacheSerializableRunnable createPR = new CacheSerializableRunnable("Create PR") {
      public void run2() throws CacheException {
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributes prAttr =
          new PartitionAttributesFactory().setRedundantCopies(redCop).setTotalNumBuckets(totalBuckets).create();
        attr.setPartitionAttributes(prAttr);
        getCache().createRegion(rName, attr.create());
      }
    };
    datastore1.invoke(createPR);
    datastore2.invoke(createPR);

    
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributes prAttr =
      new PartitionAttributesFactory().setRedundantCopies(redCop)
      .setTotalNumBuckets(totalBuckets).setLocalMaxMemory(0).create();
    attr.setPartitionAttributes(prAttr);
    PartitionedRegion pr = (PartitionedRegion) getCache().createRegion(rName, attr.create());
    
    
    // Create bucket zero, one and two
    int numEntries = 100;
    
    for (int i=1; i <= numEntries; i++) {
      pr.put(new Integer(i), new Portfolio(i));  
    }

    int[] limit = new int[] {
        10, 
        15,
        30,
        0,
        1,
        9
    };
    
    String[] queries = new String[] {
        "select * from " + pr.getFullPath() + " LIMIT " + limit[0],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[1],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[2],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[3],
        "select * from " + pr.getFullPath() + " LIMIT " + limit[4],
        "select * from " + pr.getFullPath() + " where ID > 10 LIMIT " + limit[5],
    };

    try {
      for (int q=0; q < queries.length; q++) {
        Object[] params = new Object[0];
        final DefaultQuery query = (DefaultQuery)getCache().getQueryService().newQuery(queries[q]);
        final SelectResults results = query.getSimpleSelect().getEmptyResultSet(params, getCache(), query);

        // TODO assert this is the correct set of bucket Ids,
        final HashSet<Integer> buckets = new HashSet<Integer>();
        for (int b = 0; b < totalBuckets; b++) {
          buckets.add(b);
        }
        


        final PartitionedRegionQueryEvaluator qe =
          new PartitionedRegionQueryEvaluator(pr.getSystem(), pr, query, params, results, buckets);

        class MyTestHook implements PartitionedRegionQueryEvaluator.TestHook {
          public HashMap resultsPerMember = new HashMap();
          public void hook(int spot) throws RuntimeException {
            if (spot == 3) {
              for (Object mr: qe.getResultsPerMember().entrySet()){
                Map.Entry e = (Map.Entry)mr;
                Collection<Collection> results = (Collection<Collection>)e.getValue();
                for (Collection<Object> r : results) {
                  if (this.resultsPerMember.containsKey(e.getKey())) {
                    this.resultsPerMember.put(e.getKey(), new Integer(r.size() + 
                        ((Integer)this.resultsPerMember.get(e.getKey())).intValue()));
                  } else {
                    this.resultsPerMember.put(e.getKey(), new Integer(r.size()));
                  }
                }
              }
            }
          }
        };

        final MyTestHook th = new MyTestHook();
        qe.queryBuckets(th);

        for (Object r: th.resultsPerMember.entrySet()){
          Map.Entry e = (Map.Entry)r;
          Integer res = (Integer)e.getValue();
          getLogWriter().info("PRQueryDUnitTest#testQueryResultsFromMembers : \n" +
              "Query [" + queries[q] + "] Member : " + e.getKey() + " results size :" + res.intValue());
          if (res.intValue() != 0 /* accessor member */ || res.intValue() != limit[q]) {
            assertEquals("Query [" + queries[q] + "]: The results returned by the member does not match the query limit size : Member : " + e.getKey(), limit[q], res.intValue());
          }
        }
      }
    } finally {
      getCache().close();
    }
  }

  /**
   * Simulate a data loss (buckets 0 and 2) before the PRQueryEvaluator begins
   * the query loop
   * @throws Exception
   */
  public void testSimulatedDataLossBeforeQueryProcessor() throws Exception
  {
    final String rName = getUniqueName();
    Host host = Host.getHost(0);
    VM accessor = host.getVM(1);
    VM datastore1 = host.getVM(2);
    VM datastore2 = host.getVM(3);
    final int totalBuckets = 11;

    CacheSerializableRunnable createPR = new CacheSerializableRunnable("Create PR") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory attr = new AttributesFactory();
        attr.setValueConstraint(String.class);
        PartitionAttributes prAttr =
          new PartitionAttributesFactory().setRedundantCopies(1).setTotalNumBuckets(totalBuckets).create();
        attr.setPartitionAttributes(prAttr);
        getCache().createRegion(rName, attr.create());
      }
    };
    datastore1.invoke(createPR);
    datastore2.invoke(createPR);
    accessor.invoke(new CacheSerializableRunnable("Create accessor PR") {
      @Override
      public void run2() throws CacheException {
        AttributesFactory attr = new AttributesFactory();
        attr.setValueConstraint(String.class);
        PartitionAttributes prAttr =
          new PartitionAttributesFactory().setRedundantCopies(1)
          .setTotalNumBuckets(totalBuckets).setLocalMaxMemory(0).create();
        attr.setPartitionAttributes(prAttr);
        getCache().createRegion(rName, attr.create());
      }
    });

    // add expected exception strings
    final ExpectedException ex = addExpectedException("Data loss detected",
        accessor);
    accessor.invoke(new SerializableCallable(
        "Create bucket and test dataloss query") {
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion)getCache().getRegion(rName);
        // Create bucket one
        pr.put(new Integer(1), "one");

        Object[] params = new Object[0];
        final DefaultQuery query = (DefaultQuery)getCache().getQueryService()
            .newQuery("select distinct * from " + pr.getFullPath());
        final SelectResults results = query.getSimpleSelect()
            .getEmptyResultSet(params, getCache(), query);

        // Fake data loss
        final HashSet<Integer> buckets = new HashSet<Integer>();
        for (int i=0; i < 3; i++){
          buckets.add(new Integer(i));
        }
     
        try {
          PartitionedRegionQueryEvaluator qe = new PartitionedRegionQueryEvaluator(
              pr.getSystem(), pr, query, params, results, buckets);
          qe.queryBuckets(null);
          assertTrue(false);
        } catch (QueryException expected) {
        }
//        assertEquals(1, results.size());
//        getLogWriter().info("Select results are: " + results);
        return Boolean.TRUE;
      }
    });
    ex.remove();
  }

  /**
   * This test <pr> 1. Creates PR regions across with scope = DACK , with one VM
   * as the accessor Node & others as Datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verfies the size , type , contents of both the resultSets Obtained
   *
   * @throws Exception
   */

  public void testPRAccessorCreationAndQuerying() throws Exception
  {
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Querying PR Test with DACK Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);

    VM vm1 = host.getVM(1);

    VM vm2 = host.getVM(2);

    VM vm3 = host.getVM(3);

    // Creting PR's on the participating VM's

    // Creating Accessor node on the VM
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Creating the Accessor node in the PR");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        0));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM's
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully Created the Datastore node in the PR");

    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully Created PR's across all VM's");
    // creating a local region on one of the JVM's
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        i, stepSize));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        stepSize, (2 * stepSize)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        (2 * stepSize), (3 * stepSize)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        (3 * (stepSize)), totalDataSize));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Inserted Portfolio data across PR's");

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, i, totalDataSize));

    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Querying PR's Test ENDED*****");
  }

  /**
   * This test does the following using full queries with projections and drill-down<br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verfies the size ,type , contents of both the resultSets Obtained
   *
   * @throws Exception
   */

 public void testPRDAckCreationAndQueryingWithOrderBy() throws Exception
 {
   int dataSize = 10;
   int step = 2;
   
   getLogWriter()
   .info(
         "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);

   // Creating PR's on the participating VM's
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
              .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (3 * (step)), dataSize));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
                                                            portfoliosAndPositions, i, dataSize));

   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
                                                                              name, localName));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
 }

 /**
  * This test does the following using full queries with projections and drill-down<br>
  * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
  * 2. Creates a Local region on one of the VM's <br>
  * 3. Puts in the same data both in PR region & the Local Region <br>
  * 4. Queries the data both in local & PR <br>
  * 5. Verfies the size ,type , contents of both the resultSets Obtained
  *
  * @throws Exception
  */

 public void testPRDAckCreationAndQueryingWithOrderByVerifyOrder() throws Exception
 {
   int dataSize = 10;
   int step = 2;

   getLogWriter()
   .info(
   "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);

   // Creating PR's on the participating VM's
   getLogWriter()
   .info(
   "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
       .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       (3 * (step)), dataSize));
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
       portfoliosAndPositions, i, dataSize));

   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndVerifyOrder(
       name, localName));
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
 }
 
  /**
   * This test <pr> 1. Creates PR regions across with scope = DACK , with one VM
   * as the accessor Node & others as Datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verfies the size , type , contents of both the resultSets Obtained
   *
   * @throws Exception
   */
  public void testPRAccessorCreationAndQueryWithOrderBy() throws Exception
  {
    int dataSize = 10;
    int step = 2;
    
    Class valueConstraint = Portfolio.class;
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Querying PR Test with DACK Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);

    VM vm1 = host.getVM(1);

    VM vm2 = host.getVM(2);

    VM vm3 = host.getVM(3);

    // Creting PR's on the participating VM's

    // Creating Accessor node on the VM
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Creating the Accessor node in the PR");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        0, valueConstraint));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM's
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, valueConstraint));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, valueConstraint));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, valueConstraint));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully Created the Datastore node in the PR");

    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully Created PR's across all VM's");
    // creating a local region on one of the JVM's
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions
    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfolio,
        i, step));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfolio,
        step, (2 * step)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfolio,
        (2 * step), (3 * step)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfolio,
        (3 * (step)), dataSize));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Inserted Portfolio data across PR's");

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
        portfolio, i, dataSize));

    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
        name, localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQuerying : Querying PR's Test ENDED*****");
  }

  public void testPRDAckCreationAndQueryingWithOrderByLimit() throws Exception
  {
    int dataSize = 10;
    int step = 2;
    
    getLogWriter()
    .info(
          "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

    Class valueConstraint = Portfolio.class;
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Creating PR's on the participating VM's
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                               redundancy, valueConstraint));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
    // creating a local region on one of the JVM's
    vm0.invoke(PRQHelp
               .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                             0, step));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        step, (2 * step)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                             (2 * step), (3 * step)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                             (3 * (step)), dataSize));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
                                                             portfoliosAndPositions, i, dataSize));

    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryWithLimit(
                                                                               name, localName));
    getLogWriter()
      .info(
            "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
  }


  /**
   * This test <pr> 1. Creates PR regions across with scope = DACK , with one VM
   * as the accessor Node & others as Datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in no data in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verfies the size , type , contents of both the resultSets Obtained
   *
   * @throws Exception
   */

  public void testPRAccessorCreationAndQueryingWithNoData() throws Exception
  {
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Querying PR Test with No Data  Started*****");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);

    VM vm1 = host.getVM(1);

    VM vm2 = host.getVM(2);

    VM vm3 = host.getVM(3);

    // Creting PR's on the participating VM's

    // Creating Accessor node on the VM
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Creating the Accessor node in the PR");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        0));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM's
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Successfully Created the Datastore node in the PR");

    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Successfully Created PR's across all VM's");
    // creating a local region on one of the JVM's
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Successfully Created Local Region on VM0");

    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    getLogWriter()
        .info(
            "PRQueryDUnitTest#testPRAccessorCreationAndQueryingWithNoData : Querying PR's Test No Data ENDED*****");
  }
}
