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
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
@SuppressWarnings("serial")
public class PRInvalidQueryDUnitTest extends CacheTestCase {

  private static final String REGION_NAME = "Portfolios";

  private static final int START_PORTFOLIO_DATA_INDEX = 0;
  private static final int TOTAL_DATA_SIZE = 90;
  private static final int START_KEY = 0;
  private static final int START_KEY_STEP = 20;
  private static final int REDUNDANCY = 0;

  private PRQueryDUnitHelper prQueryDUnitHelper;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    vm0.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    vm1.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    vm2.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    vm3.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));

    prQueryDUnitHelper = new PRQueryDUnitHelper();
  }

  @After
  public void tearDown() {
    disconnectAllFromDS();
    invokeInEveryVM(() -> PRQueryDUnitHelper.setCache(null));
  }

  /**
   * 1. Creates PR regions across with scope = DACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the data in PR region & the Local Region <br>
   * 4. Queries the PR with an Invalid Query Syntax <br>
   * 5. Verifies that there is an QueryInvalidException
   */
  @Test
  public void testPRDAckCreationAndQueryingWithInvalidQuery() throws Exception {
    // Creating Accessor node on the VM
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(REGION_NAME,
        REDUNDANCY, PortfolioData.class));

    // Creating the Datastores Nodes in the VM's
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(REGION_NAME, REDUNDANCY,
        PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(REGION_NAME, REDUNDANCY,
        PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(REGION_NAME, REDUNDANCY,
        PortfolioData.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    PortfolioData[] portfolioData =
        createPortfolioData(START_PORTFOLIO_DATA_INDEX, TOTAL_DATA_SIZE);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(REGION_NAME, portfolioData,
        START_KEY, START_KEY + START_KEY_STEP));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(REGION_NAME, portfolioData,
        START_KEY + START_KEY_STEP, START_KEY + (2 * START_KEY_STEP)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(REGION_NAME, portfolioData,
        START_KEY + (2 * START_KEY_STEP), START_KEY + (3 * START_KEY_STEP)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(REGION_NAME, portfolioData,
        START_KEY + (3 * START_KEY_STEP), TOTAL_DATA_SIZE));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRInvalidQuery(REGION_NAME));
  }
}
