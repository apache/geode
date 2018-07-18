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
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
@SuppressWarnings("serial")
public class PRQueryPortfolioDUnitTest extends CacheTestCase {

  private static final Class PORTFOLIO_CLASS = Portfolio.class;
  private static final Class PORTFOLIO_DATA_CLASS = PortfolioData.class;

  private static final String PARTITIONED_REGION_NAME = "Portfolios";
  private static final String LOCAL_REGION_NAME = "LocalPortfolios";

  private static final int START_PORTFOLIO_DATA_INDEX = 0;
  private static final int TOTAL_DATA_SIZE = 90;
  private static final int DATA_SIZE = 10;
  private static final int START_KEY = 0;
  private static final int START_KEY_STEP = 2;
  private static final int STEP_SIZE = 20;
  private static final int REDUNDANCY = 0;

  private PortfolioData[] portfolioData;
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

    vm0.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    vm1.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    vm2.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    vm3.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));

    portfolioData = createPortfolioData(START_PORTFOLIO_DATA_INDEX, TOTAL_DATA_SIZE);
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
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
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
        portfoliosAndPositions, START_KEY, STEP_SIZE));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfoliosAndPositions, STEP_SIZE, (2 * STEP_SIZE)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfoliosAndPositions, (2 * STEP_SIZE), (3 * STEP_SIZE)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfoliosAndPositions, (3 * (STEP_SIZE)), TOTAL_DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfoliosAndPositions, START_KEY, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * This test does the following using full queries with projections and drill-down <br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
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
        portfoliosAndPositions, START_KEY, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME, true));
  }

  /**
   * 1. Creates PR regions across with scope = DACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR using Query Constants like NULL, UNDEFINED, TRUE, FALSE
   * <br>
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
        portfolioData, START_KEY, STEP_SIZE));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolioData, STEP_SIZE, (2 * STEP_SIZE)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolioData, (2 * STEP_SIZE), (3 * STEP_SIZE)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolioData, (3 * (STEP_SIZE)), TOTAL_DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolioData, START_KEY, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryWithConstantsAndComparingResults(
            PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * 1. Creates PR regions across with scope = DACK, with one VM as the accessor Node & others as
   * Datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
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
        portfolioData, START_KEY, STEP_SIZE));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolioData, STEP_SIZE, (2 * STEP_SIZE)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolioData, (2 * STEP_SIZE), (3 * STEP_SIZE)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolioData, (3 * (STEP_SIZE)), TOTAL_DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolioData, START_KEY, TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * This test does the following using full queries with projections and drill-down <br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
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
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, START_KEY_STEP));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, START_KEY_STEP, (2 * START_KEY_STEP)));
    vm2.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(PARTITIONED_REGION_NAME,
            portfoliosAndPositions, (2 * START_KEY_STEP), (3 * START_KEY_STEP)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (START_KEY_STEP)), DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, START_KEY, DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * This test does the following using full queries with projections and drill-down <br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
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
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, START_KEY_STEP));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, START_KEY_STEP, (2 * START_KEY_STEP)));
    vm2.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(PARTITIONED_REGION_NAME,
            portfoliosAndPositions, (2 * START_KEY_STEP), (3 * START_KEY_STEP)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (START_KEY_STEP)), DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, START_KEY, DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryAndVerifyOrder(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * 1. Creates PR regions across with scope = DACK, with one VM as the accessor Node & others as
   * Datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
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
        PARTITIONED_REGION_NAME, portfoliosAndPositions, START_KEY, START_KEY_STEP));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, START_KEY_STEP, (2 * START_KEY_STEP)));
    vm2.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(PARTITIONED_REGION_NAME,
            portfoliosAndPositions, (2 * START_KEY_STEP), (3 * START_KEY_STEP)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (START_KEY_STEP)), DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, START_KEY, DATA_SIZE));

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
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, START_KEY_STEP));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, START_KEY_STEP, (2 * START_KEY_STEP)));
    vm2.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(PARTITIONED_REGION_NAME,
            portfoliosAndPositions, (2 * START_KEY_STEP), (3 * START_KEY_STEP)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (START_KEY_STEP)), DATA_SIZE));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, START_KEY, DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryWithLimit(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * 1. Creates PR regions across with scope = DACK, with one VM as the accessor Node & others as
   * Datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in no data in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
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
}
