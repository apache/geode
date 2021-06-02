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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.query.Utils.createPortfolioData;
import static org.apache.geode.cache.query.Utils.createPortfoliosAndPositions;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * This tests creates partition regions with 1 Datastore & 1 Accessor node, firing a simple query
 * and validating the ResultSet size and Contents.
 */
@Category({OQLQueryTest.class})
public class PRBasicQueryDUnitTest extends CacheTestCase {

  private static final String name = "Portfolios";
  private static final String localName = "LocalPortfolios";

  private static final int cnt = 0;
  private static final int cntDest = 50;
  private static final int redundancy = 0;

  private final PRQueryDUnitHelper prQueryDUnitHelper = new PRQueryDUnitHelper();

  @After
  public void tearDown() {
    disconnectAllFromDS();
    invokeInEveryVM(() -> PRQueryDUnitHelper.setCache(null));
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*");
    return config;
  }

  /**
   * 1. Creates a PR Accessor and Data Store with redundantCopies = 0.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a query on accessor VM and verifies the result.
   */
  @Test
  public void testPRBasicQuerying() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    // Creating PR's on the participating VM's
    // Creating Accessor node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        PortfolioData.class));

    // Creating the Datastores Nodes in the VM1.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(localName,
        portfolio, cnt, cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(name,
        localName));
  }

  /**
   * 1. Creates a PR and colocated child region Accessor and Data Store with redundantCopies = 0.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a query on accessor VM and verifies the result.
   *
   * <p>
   * 4. Shuts down the caches, then restarts them asynchronously
   *
   * <p>
   * 5. Attempt the query while the regions are being recovered
   */
  @Test
  public void testColocatedPRQueryDuringRecovery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    // Creating PR's on the participating VM's
    // Creating Accessor node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        PortfolioData.class));

    // Creating the Datastores Nodes in the VM1.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(localName,
        portfolio, cnt, cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(name,
        localName));

    // Shut everything down and then restart to test queries during recovery
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForCloseCache());
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForCloseCache());

    // Re-create the regions - only create the parent regions on the datastores
    setCacheInVMs(vm0, vm1);
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedParentCreate(name,
        redundancy, PortfolioData.class, true));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        PortfolioData.class));

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedParentCreate(name,
        redundancy, PortfolioData.class, true));

    // Now start the child regions asynchronously so queries will happen during persistent recovery
    AsyncInvocation async0 =
        vm0.invokeAsync(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedChildCreate(name,
            redundancy, PortfolioData.class, true));
    AsyncInvocation async1 =
        vm1.invokeAsync(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedChildCreate(name,
            redundancy, PortfolioData.class, true));

    // delay the query to let the recovery get underway
    Thread.sleep(100);

    try {
      // This is a repeat of the original query from before closing and restarting the datastores.
      // This time
      // it should fail due to persistent recovery that has not completed.
      vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(name,
          localName, true));
      fail(
          "Expected PartitionOfflineException when querying a region with offline colocated child");
    } catch (Exception e) {
      if (!(e.getCause() instanceof PartitionOfflineException)) {
        throw e;
      }
    }

    async0.await();
    async1.await();
  }

  /**
   * 1. Creates a PR and colocated child region Accessor and Data Store with redundantCopies = 0.
   *
   * <p>
   * 2. Populates the region with test data.
   *
   * <p>
   * 3. Fires a query on accessor VM and verifies the result.
   *
   * <p>
   * 4. Shuts down the caches, then restarts them asynchronously, but don't restart the child region
   *
   * <p>
   * 5. Attempt the query while the region offline because of the missing child region
   */
  @Test
  public void testColocatedPRQueryDuringRecoveryWithMissingColocatedChild() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    // Creating PR's on the participating VM's
    // Creating Accessor node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        PortfolioData.class));

    // Creating the Datastores Nodes in the VM1.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedPRCreate(name, redundancy,
        PortfolioData.class, true));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(localName,
        portfolio, cnt, cntDest));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(name,
        localName));

    // Shut everything down and then restart to test queries during recovery
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForCloseCache());
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForCloseCache());

    // Re-create the only the parent region
    setCacheInVMs(vm0, vm1);
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedParentCreate(name,
        redundancy, PortfolioData.class, true));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForColocatedParentCreate(name,
        redundancy, PortfolioData.class, true));

    try {
      // This is a repeat of the original query from before closing and restarting the datastores.
      // This time
      // it should fail due to persistent recovery that has not completed.
      vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(name,
          localName, true));
      fail(
          "Expected PartitionOfflineException when queryiong a region with offline colocated child");
    } catch (Exception e) {
      if (!(e.getCause() instanceof PartitionOfflineException)) {
        throw e;
      }
    }
  }

  @Test
  public void testPRCountStarQuery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    setCacheInVMs(vm0, vm1, vm2);

    // Creating PR's on the participating VM's
    // Creating Accessor node on the VM0.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        Portfolio.class));

    // Creating local region on vm0 to compare the results of query.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    // Creating the Datastores Nodes in the VM1.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));

    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        Portfolio.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    Portfolio[] portfolio = createPortfoliosAndPositions(cntDest + 100);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt,
        cntDest + 100));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(name, portfolio,
        cnt, cntDest + 100));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest + 100));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(localName,
        portfolio, cnt, cntDest + 100));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt,
        cntDest + 100));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRDuplicatePuts(localName,
        portfolio, cnt, cntDest + 100));

    // querying the VM for data and comparing the result with query result of local region.
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRCountStarQueries(name, localName));
    vm2.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRCountStarQueries(name, localName));
  }

  @Test
  public void testPROrderBy() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(localName,
        PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        Portfolio.class));

    vm1.invoke(new CacheSerializableRunnable("Adding portfolios") {
      @Override
      public void run2() {
        Cache cache = getCache();
        Region region = cache.getRegion(name);
        for (int i = 0; i < 100; i++) {
          region.put(i, new Portfolio(i));
        }
      }
    });

    vm1.invoke(() -> {
      QueryService qs = getCache().getQueryService();
      qs.createIndex("index", IndexType.FUNCTIONAL, "ID", SEPARATOR + name);
      for (int i = 0; i < 100; i++) {
        Query query = qs.newQuery(
            "SELECT DISTINCT * FROM " + SEPARATOR + name + " WHERE ID >= " + i
                + " ORDER BY ID asc LIMIT 1");
        SelectResults results = (SelectResults) query.execute();
        int expectedValue = i;
        for (Object o : results) {
          Portfolio p = (Portfolio) o;
          assertEquals(expectedValue++, p.getID());
        }
      }
    });
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

}
