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
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
@SuppressWarnings("serial")
public class PRQueryRegionDestroyedDUnitTest extends CacheTestCase {

  private static final String PARTITIONED_REGION_NAME = "Portfolios";
  private static final String LOCAL_REGION_NAME = "LocalPortfolios";
  private static final int START_PORTFOLIO_INDEX = 0;
  private static final int END_PORTFOLIO_INDEX = 50;
  private static final int REDUNDANCY = 1;

  private PortfolioData[] portfolio;
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

    portfolio = createPortfolioData(START_PORTFOLIO_INDEX, END_PORTFOLIO_INDEX);
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
    config.put(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.**");
    return config;
  }

  /**
   * 1. Creates PR regions across with scope = DACK, one accessor node & 2 datastores <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Also calls Region.close() randomly on one of the datastore VM's with delay <br>
   * 6. then recreates the PR on the same VM <br>
   * 7. Verifies the size, type and contents of both the resultSets obtained
   */
  @Test
  public void testPRWithRegionDestroyInOneDatastoreWithDelay() throws Exception {
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, REDUNDANCY, PortfolioData.class));

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        REDUNDANCY, PortfolioData.class));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));

    // Putting the data into the accessor node
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, START_PORTFOLIO_INDEX, END_PORTFOLIO_INDEX));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, START_PORTFOLIO_INDEX, END_PORTFOLIO_INDEX));

    // Execute query first time. This is to make sure all the buckets are created
    // (lazy bucket creation).
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));

    // Now execute the query. And while query execution in process destroy the region
    // on one of the node.
    AsyncInvocation async0 =
        vm0.invokeAsync(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
            PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));

    Wait.pause(5);

    // skip the use of random and just close region in vm2
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRegionClose(
        PARTITIONED_REGION_NAME, REDUNDANCY, PortfolioData.class));

    async0.await();
  }
}
