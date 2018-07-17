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
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;

/**
 * Basic functional test for removing index from a partitioned region system.
 */
@Category({OQLIndexTest.class})
public class PRBasicRemoveIndexDUnitTest extends CacheTestCase {

  private static final String name = "PartitionedPortfolios";

  private static final int start = 0;
  private static final int end = 1003;
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
   * Remove index test to remove all the indexes in a given partitioned region
   */
  @Test
  public void testPRBasicIndexRemove() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(start, end);

    // Putting the data into the PR's created
    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, start, end));

    // create all the indexes.

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid", null, "p"));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status", null, "p"));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "PrIndexOnId",
        "p.ID", null, "p"));

    // remove indexes
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRemoveIndex(name, false));
  }

  /**
   * Test removing single index on a pr.
   */
  @Test
  public void testPRBasicRemoveParticularIndex() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(name, redundancy,
        PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(start, end);

    // Putting the data into the PR's created
    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(name, portfolio, start, end));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid", null, "p"));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status", null, "p"));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(name, "PrIndexOnId",
        "p.ID", null, "p"));

    // remove indexes
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRemoveIndex(name, true));
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }
}
