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

import static org.apache.geode.cache.query.Utils.createPortfolioData;
import static org.apache.geode.cache.query.Utils.createPortfoliosAndPositions;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PRQueryPortfolioDUnitTest extends CacheTestCase {

  private static final Class PORTFOLIO_CLASS = Portfolio.class;
  private static final Class PORTFOLIO_DATA_CLASS = PortfolioData.class;

  private static final String PARTITIONED_REGION_NAME = "Portfolios";
  private static final String LOCAL_REGION_NAME = "LocalPortfolios";
  private static final int TOTAL_DATA_SIZE = 90;
  private static final int STEP_SIZE = 20;
  private static final int CNT = 0;
  private static final int I = 0;
  private static final int REDUNDANCY = 0;

  private int dataSize;
  private int step;
  private PortfolioData[] portfolio;
  private Portfolio[] portfoliosAndPositions;
  private PRQueryDUnitHelper prQueryDUnitHelper;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    dataSize = 10;
    step = 2;
    portfolio = createPortfolioData(CNT, TOTAL_DATA_SIZE);
    portfoliosAndPositions = createPortfoliosAndPositions(TOTAL_DATA_SIZE);
    prQueryDUnitHelper = new PRQueryDUnitHelper();
  }

  @After
  public void tearDown() throws Exception {
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
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Verifies the size ,type , contents of both the resultSets Obtained
   */
  @Test
  public void testPRDAckCreationAndQuerying() throws Exception {
    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, PORTFOLIO_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, I, STEP_SIZE));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, STEP_SIZE, (2 * STEP_SIZE)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, (2 * STEP_SIZE), (3 * STEP_SIZE)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, (3 * (STEP_SIZE)), TOTAL_DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, I, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * This test does the following using full queries with projections and drill-down
   * <p>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Verifies the size ,type , contents of both the resultSets Obtained
   */
  @Test
  public void testPRDAckCreationAndQueryingFull() throws Exception {
    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, PORTFOLIO_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfoliosAndPositions, 0, STEP_SIZE));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfoliosAndPositions, STEP_SIZE, (2 * STEP_SIZE)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfoliosAndPositions, (2 * STEP_SIZE), (3 * STEP_SIZE)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfoliosAndPositions, (3 * (STEP_SIZE)), TOTAL_DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfoliosAndPositions, I, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME, true));
  }

  /**
   * 1. Creates PR regions across with scope = DACK
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR using Query Constants like NULL, UNDEFINED, TRUE, FALSE
   * <p>
   * 5. Verifies the size, type, contents of both the resultSets Obtained
   */
  @Test
  public void testPRDAckCreationAndQueryingWithConstants() throws Exception {
    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PORTFOLIO_DATA_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, I, STEP_SIZE));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, STEP_SIZE, (2 * STEP_SIZE)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, (2 * STEP_SIZE), (3 * STEP_SIZE)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, (3 * (STEP_SIZE)), TOTAL_DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, I, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryWithConstantsAndComparingResults(
            PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * 1. Creates PR regions across with scope = DACK, with one VM as the accessor Node & others as
   * Datastores
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Verifies the size, type, contents of both the resultSets Obtained
   */
  @Test
  public void testPRAccessorCreationAndQuerying() throws Exception {
    // Creating Accessor node on the VM
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, 0, PORTFOLIO_DATA_CLASS));

    // Creating the Datastores Nodes in the VM's
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PORTFOLIO_DATA_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, I, STEP_SIZE));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, STEP_SIZE, (2 * STEP_SIZE)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, (2 * STEP_SIZE), (3 * STEP_SIZE)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, (3 * (STEP_SIZE)), TOTAL_DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, I, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * This test does the following using full queries with projections and drill-down
   * <p>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Verifies the size ,type , contents of both the resultSets Obtained
   */
  @Test
  public void testPRDAckCreationAndQueryingWithOrderBy() throws Exception {
    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, PORTFOLIO_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, step));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, step, (2 * step)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (2 * step), (3 * step)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, I, dataSize));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * This test does the following using full queries with projections and drill-down
   * <p>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Verifies the size ,type , contents of both the resultSets Obtained
   */
  @Test
  public void testPRDAckCreationAndQueryingWithOrderByVerifyOrder() throws Exception {
    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, PORTFOLIO_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, step));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, step, (2 * step)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (2 * step), (3 * step)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, I, dataSize));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryAndVerifyOrder(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * 1. Creates PR regions across with scope = DACK, with one VM as the accessor Node & others as
   * Datastores
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Verifies the size , type , contents of both the resultSets Obtained
   */
  @Test
  public void testPRAccessorCreationAndQueryWithOrderBy() throws Exception {
    // Creating Accessor node on the VM
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, 0, PORTFOLIO_CLASS));

    // Creating the Datastores Nodes in the VM's
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, PORTFOLIO_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfolio, I, step));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfolio, step, (2 * step)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfolio, (2 * step), (3 * step)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfolio, (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfolio, I, dataSize));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  @Test
  public void testPRDAckCreationAndQueryingWithOrderByLimit() throws Exception {
    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, PORTFOLIO_CLASS));

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, step));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, step, (2 * step)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (2 * step), (3 * step)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, I, dataSize));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryWithLimit(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * 1. Creates PR regions across with scope = DACK, with one VM as the accessor Node & others as
   * Datastores
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in no data in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Verifies the size , type , contents of both the resultSets Obtained
   */
  @Test
  public void testPRAccessorCreationAndQueryingWithNoData() throws Exception {
    // Creating Accessor node on the VM
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, 0, PORTFOLIO_DATA_CLASS));

    // Creating the Datastores Nodes in the VM's
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PORTFOLIO_DATA_CLASS));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PORTFOLIO_DATA_CLASS));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }
}
