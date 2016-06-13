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

import static com.gemstone.gemfire.cache.query.Utils.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Basic functional test for removing index from a partitioned region system.
 */
@Category(DistributedTest.class)
public class PRBasicRemoveIndexDUnitTest extends PartitionedRegionDUnitTestCase {

  private final PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  private final int start = 0;

  private final int end = 1003;

  /**
   * Name of the partitioned region for the test.
   */
  private final String name = "PartitionedPortfolios";

  /**
   * Redundancy level for the pr.
   */
  private final int redundancy = 0;

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
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
    LogWriterUtils.getLogWriter().info(
        "PRBasicRemoveIndexDUnitTest.testPRBasicIndexCreate test now starts ....");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    
    final PortfolioData[] portfolio = createPortfolioData(start, end);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        start, end));
    
    // create all the indexes.
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid",null, "p"));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    
    //remove indexes
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForRemoveIndex(name, false));
    
    LogWriterUtils.getLogWriter().info(
    "PRBasicRemoveIndexDUnitTest.testPRBasicRemoveIndex test now  ends sucessfully");
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
    LogWriterUtils.getLogWriter().info(
        "PRBasicRemoveIndexDUnitTest.testPRBasicIndexCreate test now starts ....");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    final PortfolioData[] portfolio = createPortfolioData(start, end);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        start, end));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid",null, "p"));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    
//  remove indexes
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForRemoveIndex(name, true));
  }
}
