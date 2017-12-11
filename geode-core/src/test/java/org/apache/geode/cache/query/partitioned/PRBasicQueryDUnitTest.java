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
package org.apache.geode.cache.query.partitioned;

import static org.apache.geode.cache.query.Utils.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This tests creates partition regions with 1 Datastore & 1 Accessor node, firing a simple query
 * and validating the ResultSet size and Contents.
 *
 */
@Category(DistributedTest.class)
public class PRBasicQueryDUnitTest extends PartitionedRegionDUnitTestCase

{
  public PRBasicQueryDUnitTest() {
    super();
  }

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  private static final int MAX_SYNC_WAIT = 30 * 1000;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  final String name = "Portfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 50;

  final int redundancy = 0;

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.cache.query.data.*");
    return properties;
  }

  /**
   * A very basic dunit test that <br>
   * 1. Creates a PR Accessor and Data Store with redundantCopies = 0. 2. Populates the region with
   * test data. 3. Fires a query on accessor VM and verifies the result.
   *
   * @throws Exception
   */
  @Test
  public void testPRBasicQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Creating the Accessor node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));
    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest:testPRBasicQuerying ----- Creating the Datastore node in the PR");
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created the Datastore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRBasicQuerying: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Inserted Portfolio data across PR's");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName, portfolio, cnt, cntDest));

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRBasicQuerying: Querying PR's Test ENDED");
  }

  /**
   * A basic dunit test that <br>
   * 1. Creates a PR and colocated child region Accessor and Data Store with redundantCopies = 0. 2.
   * Populates the region with test data. 3. Fires a query on accessor VM and verifies the result.
   * 4. Shuts down the caches, then restarts them asynchronously 5. Attempt the query while the
   * regions are being recovered
   *
   * @throws Exception
   */
  @Test
  public void testColocatedPRQueryDuringRecovery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Creating the Accessor node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));
    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest:testColocatedPRBasicQuerying ----- Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully Created the Datastore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Inserted Portfolio data across PR's");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName, portfolio, cnt, cntDest));

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Querying PR's 1st pass ENDED");

    // Shut everything down and then restart to test queries during recovery
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForCloseCache());
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForCloseCache());

    // Re-create the regions - only create the parent regions on the datastores
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Creating the Accessor node in the PR");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForColocatedParentCreate(name, redundancy,
        PortfolioData.class, true));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully created the Accessor node in the PR");
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest:testColocatedPRBasicQuerying: re-creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForColocatedParentCreate(name, redundancy,
        PortfolioData.class, true));

    // Now start the child regions asynchronously so queries will happen during persistent recovery
    AsyncInvocation vm0PR =
        vm0.invokeAsync(PRQHelp.getCacheSerializableRunnableForColocatedChildCreate(name,
            redundancy, PortfolioData.class, true));
    AsyncInvocation vm1PR =
        vm1.invokeAsync(PRQHelp.getCacheSerializableRunnableForColocatedChildCreate(name,
            redundancy, PortfolioData.class, true));

    // delay the query to let the recovery get underway
    Thread.sleep(100);

    try {
      // This is a repeat of the original query from before closing and restarting the datastores.
      // This time
      // it should fail due to persistent recovery that has not completed.
      vm0.invoke(
          PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName, true));
      fail(
          "Expected PartitionOfflineException when queryiong a region with offline colocated child");
    } catch (Exception e) {
      if (!(e.getCause() instanceof PartitionOfflineException)) {
        e.printStackTrace();
        throw e;
      }
    }
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Querying PR's 2nd pass (after restarting regions) ENDED");
  }

  /**
   * A basic dunit test that <br>
   * 1. Creates a PR and colocated child region Accessor and Data Store with redundantCopies = 0. 2.
   * Populates the region with test data. 3. Fires a query on accessor VM and verifies the result.
   * 4. Shuts down the caches, then restarts them asynchronously, but don't restart the child region
   * 5. Attempt the query while the region offline because of the missing child region
   *
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  @Test
  public void testColocatedPRQueryDuringRecoveryWithMissingColocatedChild() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Creating the Accessor node in the PR");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));
    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest:testColocatedPRBasicQuerying ----- Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully Created the Datastore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio, cnt, cntDest));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Inserted Portfolio data across PR's");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName, portfolio, cnt, cntDest));

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Querying PR's 1st pass ENDED");

    // Shut everything down and then restart to test queries during recovery
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForCloseCache());
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForCloseCache());

    // Re-create the only the parent region
    setCacheInVMs(vm0, vm1);
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Creating the Accessor node in the PR");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForColocatedParentCreate(name, redundancy,
        PortfolioData.class, true));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Successfully created the Accessor node in the PR");
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest:testColocatedPRBasicQuerying ----- re-creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForColocatedParentCreate(name, redundancy,
        PortfolioData.class, true));

    try {
      // This is a repeat of the original query from before closing and restarting the datastores.
      // This time
      // it should fail due to persistent recovery that has not completed.
      vm0.invoke(
          PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName, true));
      fail(
          "Expected PartitionOfflineException when queryiong a region with offline colocated child");
    } catch (Exception e) {
      if (!(e.getCause() instanceof PartitionOfflineException)) {
        throw e;
      }
    }
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testColocatedPRBasicQuerying: Querying PR's 2nd pass (after restarting regions) ENDED");
  }

  @Test
  public void testPRCountStarQuery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    setCacheInVMs(vm0, vm1, vm2);
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRCountStarQuery: Querying PR Test with DACK Started");

    // Creting PR's on the participating VM's
    // Creating Accessor node on the VM0.
    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRCountStarQuery: Creating the Accessor node in the PR");

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy, Portfolio.class));
    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully created the Accessor node in the PR");

    // Creating the Datastores Nodes in the VM1.
    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest:testPRCountStarQuery ----- Creating the Datastore node in the PR");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));

    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, Portfolio.class));

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully Created the Datastore node in the PR");

    LogWriterUtils.getLogWriter().info(
        "PRQBasicQueryDUnitTest#testPRCountStarQuery: Successfully Created PR's across all VM's");

    // Generating portfolio object array to be populated across the PR's & Local
    // Regions

    final Portfolio[] portfolio = createPortfoliosAndPositions(cntDest + 100);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest + 100));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio, cnt,
        cntDest + 100));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRCountStarQuery: Inserted Portfolio data across PR's");
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest + 100));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName, portfolio, cnt,
        cntDest + 100));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest + 100));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRDuplicatePuts(localName, portfolio, cnt,
        cntDest + 100));

    // querying the VM for data and comparing the result with query result of
    // local region.
    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCountStarQueries(name, localName));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCountStarQueries(name, localName));

    LogWriterUtils.getLogWriter()
        .info("PRQBasicQueryDUnitTest#testPRCountStarQuery: Querying PR's Test ENDED");
  }

  @Test
  public void testPROrderBy() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, Portfolio.class));

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

          Index d = qs.createIndex("index", IndexType.FUNCTIONAL, "ID", "/" + name);
          for (int i = 0; i < 100; i++) {
            Query query = qs.newQuery("SELECT DISTINCT * FROM /" + name + " WHERE ID >= " + i
                + " ORDER BY ID asc LIMIT 1");
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
