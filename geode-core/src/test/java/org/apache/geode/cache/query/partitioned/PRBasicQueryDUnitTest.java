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
package org.apache.geode.cache.query.partitioned;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import static org.apache.geode.cache.query.Utils.*;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;

/**
 * This tests creates partition regions with 1 Datastore & 1 Accessor node, 
 * firing a simple query and validating the ResultSet size and Contents. 
 * 
 */
@Category(DistributedTest.class)
public class PRBasicQueryDUnitTest extends PartitionedRegionDUnitTestCase

{
  /**
   * constructor
   * 
   * @param name
   */

  public PRBasicQueryDUnitTest() {
    super();
  }

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

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
  @Test
  public void testPRBasicQuerying() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); 
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Accessor node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));
    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest:testPRBasicQuerying ----- Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));

    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created the Datastore node in the PR");

    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest));
    
    LogWriterUtils.getLogWriter()
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

    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }
  
  @Test
  public void testPRCountStarQuery() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0); 
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    setCacheInVMs(vm0, vm1, vm2);
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Creating the Accessor node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, Portfolio.class));
    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest:testPRCountStarQuery ----- Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));

    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, Portfolio.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    
    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully Created the Datastore node in the PR");

    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest+100);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest+100));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest+100));
    
    LogWriterUtils.getLogWriter()
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

    LogWriterUtils.getLogWriter()
        .info(
            "PRQBasicQueryDUnitTest#testPRCountStarQuery: Querying PR's Test ENDED");
  }
  
  @Test
  public void testPROrderBy() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));

    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
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
