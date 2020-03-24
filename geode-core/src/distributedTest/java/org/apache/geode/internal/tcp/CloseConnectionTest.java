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
package org.apache.geode.internal.tcp;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class CloseConnectionTest implements Serializable {
  private VM vm0;
  private VM vm1;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(getController(), vm0, vm1)) {
      vm.invoke(() -> cacheRule.closeAndNullCache());
    }

    disconnectAllFromDS();
  }

  @Test
  public void sharedSenderShouldRecoverFromClosedSocket() {
    // Create a region in each member. VM0 has a proxy region, so state must be in VM1
    vm0.invoke(() -> {
      getCache().createRegionFactory(RegionShortcut.REPLICATE_PROXY).create("region");
    });
    vm1.invoke(() -> {
      getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
    });


    // Force VM1 to close it's connections.
    vm1.invoke(() -> {
      ConnectionTable conTable = getConnectionTable();
      assertThat(conTable.getNumberOfReceivers()).isEqualTo(2);
      conTable.closeReceivers(false);
      await().untilAsserted(() -> assertThat(conTable.getNumberOfReceivers()).isEqualTo(0));
    });

    // See if VM0 noticed the closed connections. Try to do a couple of region
    // operations
    vm0.invoke(() -> {
      Region<Object, Object> region = getCache().getRegion("region");
      region.put("1", "1");

      assertThat(region.get("1")).isEqualTo("1");
    });

    // Make sure connections were reestablished
    vm1.invoke(() -> {
      ConnectionTable conTable = getConnectionTable();
      await().untilAsserted(() -> assertThat(conTable.getNumberOfReceivers()).isEqualTo(2));
    });

    // Make sure the Sender connection has a reader thread
    vm1.invoke(() -> {
      ConnectionTable conTable = getConnectionTable();
      InternalDistributedSystem distributedSystem = getCache().getInternalDistributedSystem();
      InternalDistributedMember otherMember = distributedSystem.getDistributionManager()
          .getOtherNormalDistributionManagerIds().iterator().next();
      Connection connection = conTable.getConduit().getConnection(otherMember, true, false,
          System.currentTimeMillis(), 15000, 0);
      assertThat(connection.hasResidualReaderThread()).isTrue();
    });
  }

  private ConnectionTable getConnectionTable() {
    ClusterDistributionManager cdm =
        (ClusterDistributionManager) getCache().getDistributionManager();
    DistributionImpl distribution = (DistributionImpl) cdm.getDistribution();
    return distribution.getDirectChannel().getConduit().getConTable();
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }
}
