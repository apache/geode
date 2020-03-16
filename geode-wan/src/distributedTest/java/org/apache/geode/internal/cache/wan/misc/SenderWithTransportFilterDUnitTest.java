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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class SenderWithTransportFilterDUnitTest extends WANTestBase {

  @Test
  public void testSerialSenderWithTransportFilter() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> SenderWithTransportFilterDUnitTest.createReceiverWithTransportFilters(nyPort));
    vm2.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", null, isOffHeap()));

    vm3.invoke(() -> WANTestBase.createCache(lnPort));

    vm3.invoke(() -> SenderWithTransportFilterDUnitTest.createSenderWithTransportFilter("ln", 2,
        false, 100, 1, false, false, true));

    vm3.invoke(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName() + "_RR", "ln", isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_RR", 100));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_RR", 100));
  }

  @Test
  public void testParallelSenderWithTransportFilter() {
    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));

    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> SenderWithTransportFilterDUnitTest.createReceiverWithTransportFilters(nyPort));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", null, 0, 10,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.createCache(lnPort));

    vm3.invoke(() -> SenderWithTransportFilterDUnitTest.createSenderWithTransportFilter("ln", 2,
        true, 100, 1, false, false, true));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(getTestMethodName() + "_PR", "ln", 0, 10,
        isOffHeap()));

    vm3.invoke(() -> WANTestBase.startSender("ln"));

    vm3.invoke(() -> WANTestBase.doPuts(getTestMethodName() + "_PR", 100));

    vm2.invoke(() -> WANTestBase.validateRegionSize(getTestMethodName() + "_PR", 100));
  }

  public static int createReceiverWithTransportFilters(int locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = createCache(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<>();
    transportFilters.add(new CheckSumTransportFilter("CheckSumTransportFilter"));
    if (!transportFilters.isEmpty()) {
      for (GatewayTransportFilter filter : transportFilters) {
        fact.addGatewayTransportFilter(filter);
      }
    }
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      fail("Test " + test.getName() + " failed to start GatewayReceiver on port " + port, e);
    }
    return port;
  }

  public static void createSenderWithTransportFilter(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, boolean isManualStart) {
    File persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getVMId());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};

    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<>();
      transportFilters.add(new CheckSumTransportFilter("CheckSumTransportFilter"));
      if (!transportFilters.isEmpty()) {
        for (GatewayTransportFilter filter : transportFilters) {
          gateway.addGatewayTransportFilter(filter);
        }
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    } else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      deprecatedSetManualStart(gateway, isManualStart);
      ((InternalGatewaySenderFactory) gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<>();
      transportFilters.add(new CheckSumTransportFilter("CheckSumTransportFilter"));
      if (!transportFilters.isEmpty()) {
        for (GatewayTransportFilter filter : transportFilters) {
          gateway.addGatewayTransportFilter(filter);
        }
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
  }

  @SuppressWarnings("deprecation")
  private static void deprecatedSetManualStart(GatewaySenderFactory gateway,
      boolean isManualStart) {
    gateway.setManualStart(isManualStart);
  }

  static class CheckSumTransportFilter implements GatewayTransportFilter {

    Adler32 checker = new Adler32();

    private String name;

    public CheckSumTransportFilter(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }

    @Override
    public InputStream getInputStream(InputStream stream) {
      return new CheckedInputStream(stream, checker);
    }

    @Override
    public OutputStream getOutputStream(OutputStream stream) {
      return new CheckedOutputStream(stream, checker);
    }

    @Override
    public void close() {}
  }
}
