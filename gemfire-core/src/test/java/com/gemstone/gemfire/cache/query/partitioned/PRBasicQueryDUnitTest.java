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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This tests creates partition regions with 1 Datastore & 1 Accessor node, 
 * firing a simple query and validating the ResultSet size and Contents. 
 * 
 * @author pbatra
 */
public class PRBasicQueryDUnitTest extends PartitionedRegionDUnitTestCase

{
  /**
   * constructor
   * 
   * @param name
   */

  public PRBasicQueryDUnitTest(String name) {
    super(name);
  }

  int totalNumBuckets = 100;

  int queryTestCycle = 10;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  final String name = "Portfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 50;

  final int redundancy = 0;

  /**
   * A very basic dunit test that <br>
   * 1. Creates a PR Accessor and Data Store with redundantCopies = 0.
   * 2. Populates the region with test data.
   * 3. Fires a query on accessor VM and verifies the result. 
   * @throws Exception
   */
  public void testPRBasicQuerying() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); 
    VM vm1 = host.getVM(1);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Accessor node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));
    // Creating local region on vm0 to compare the results of query.
//    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(localName,
//        Scope.DISTRIBUTED_ACK, redundancy));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName));
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest:testPRBasicQuerying ----- Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created the Datastore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName,
        portfolio, cnt, cntDest));

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }
  
  public void testPRCountStarQuery() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); 
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Creating the Accessor node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, Portfolio.class));
    // Creating local region on vm0 to compare the results of query.
//    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(localName,
//        Scope.DISTRIBUTED_ACK, redundancy));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest:testPRCountStarQuery ----- Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));

    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully Created the Datastore node in the PR");

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = PRQHelp.createPortfoliosAndPositions(cntDest+100);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest+100));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest+100));
    
    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Inserted Portfolio data across PR's");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest+100));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName, portfolio,
        cnt, cntDest+100));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest+100));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName, portfolio,
        cnt, cntDest+100));
    
    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCountStarQueries(
        name, localName));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCountStarQueries(
        name, localName));

    getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Querying PR's Test ENDED");
  }
  
  public void testPROrderBy() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));

    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    vm1.invoke(PRQHelp
        .getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));

    vm1.invoke(new CacheSerializableRunnable("Adding portfolios") {
      public void run2() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        for (int j = 0; j < 100; j++)
          region.put(new Integer(j), new Portfolio(j));
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Running Query") {
      public void run2() {
        try {
          QueryService qs = null;
          qs = getCache().getQueryService();

          Index d = qs.createIndex("index", IndexType.FUNCTIONAL, "ID", "/"
              + name);
          for (int i = 0; i < 100; i++) {
            Query query = qs.newQuery("SELECT DISTINCT * FROM /" + name
                + " WHERE ID >= " + i + " ORDER BY ID asc LIMIT 1");
            SelectResults results = (SelectResults) query.execute();
            int expectedValue = i;
            for (Object o : results) {
              Portfolio p = (Portfolio) o;
              assert (p.getID() == expectedValue++);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("exception:" + e);
        }
      }
    });
  }

}
