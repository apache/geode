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
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PRInvalidQueryDUnitTest extends CacheTestCase {

  private static final String name = "Portfolios";

  private static final int i = 0;
  private static final int step = 20;
  private static final int cnt = 0;
  private static final int cntDest = 90;
  private static final int redundancy = 0;

  private final PRQueryDUnitHelper prq = new PRQueryDUnitHelper();

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
    setCacheInVMs(vm0, vm1, vm2, vm3);
  }

  @After
  public void tearDown() {
    disconnectAllFromDS();
    invokeInEveryVM(() -> PRQueryDUnitHelper.setCache(null));
  }

  /**
   * 1. Creates PR regions across with scope = DACK
   *
   * <p>
   * 2. Creates a Local region on one of the VM's
   *
   * <p>
   * 3. Puts in the data in PR region & the Local Region
   *
   * <p>
   * 4. Queries the PR qith an Invalid Query Syntax
   *
   * <p>
   * 5. Verifies that there is an QueryInvalidException
   */
  @Test
  public void testPRDAckCreationAndQueryingWithInvalidQuery() throws Exception {
    // Creating Accessor node on the VM
    vm0.invoke(
        prq.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy, PortfolioData.class));

    // Creating the Datastores Nodes in the VM's
    vm1.invoke(prq.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(prq.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(prq.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    // Generating portfolio object array to be populated across the PR's & Local Regions
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i, i + step));
    vm1.invoke(
        prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i + step, i + (2 * step)));
    vm2.invoke(
        prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i + (2 * step), i + (3 * step)));
    vm3.invoke(prq.getCacheSerializableRunnableForPRPuts(name, portfolio, i + (3 * step), cntDest));

    // querying the VM for data
    vm0.invoke(prq.getCacheSerializableRunnableForPRInvalidQuery(name));
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }
}
