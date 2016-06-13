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
package com.gemstone.gemfire.internal.cache.wan.misc;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;

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

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.InternalGatewaySenderFactory;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class SenderWithTransportFilterDUnitTest extends WANTestBase {

  @Test
  public void testSerialSenderWithTansportFilter() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> SenderWithTransportFilterDUnitTest.createReceiverWithTransportFilters( nyPort ));
    vm2.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", null, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createCache( lnPort ));

    vm3.invoke(() -> SenderWithTransportFilterDUnitTest.createSenderWithTransportFilter( "ln", 2, false, 100,
            1, false, false, true ));

    vm3.invoke(() -> WANTestBase.createReplicatedRegion(
        getTestMethodName() + "_RR", "ln", isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_RR", 100 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_RR", 100 ));
  }

  @Test
  public void testParallelSenderWithTansportFilter() {
    Integer lnPort = (Integer)vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId( 1 ));

    Integer nyPort = (Integer)vm1.invoke(() -> WANTestBase.createFirstRemoteLocator( 2, lnPort ));

    vm2.invoke(() -> SenderWithTransportFilterDUnitTest.createReceiverWithTransportFilters( nyPort ));
    vm2.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", null, 0, 10, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.createCache( lnPort ));

    vm3.invoke(() -> SenderWithTransportFilterDUnitTest.createSenderWithTransportFilter( "ln", 2, true, 100,
            1, false, false, true ));

    vm3.invoke(() -> WANTestBase.createPartitionedRegion(
        getTestMethodName() + "_PR", "ln", 0, 10, isOffHeap() ));

    vm3.invoke(() -> WANTestBase.startSender( "ln" ));

    vm3.invoke(() -> WANTestBase.doPuts( getTestMethodName() + "_PR", 100 ));

    vm2.invoke(() -> WANTestBase.validateRegionSize(
        getTestMethodName() + "_PR", 100 ));
  }
  
  public static int createReceiverWithTransportFilters(int locPort) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort
        + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<GatewayTransportFilter>();
    transportFilters.add(new CheckSumTranportFilter("CheckSumTranportFilter"));
    if (!transportFilters.isEmpty()) {
      for (GatewayTransportFilter filter : transportFilters) {
        fact.addGatewayTransportFilter(filter);
      }
    }
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    }
    catch (IOException e) {
      fail("Test " + test.getName() + " failed to start GatewayRecevier on port " + port, e);
    }
    return port;
  }

  public static void createSenderWithTransportFilter(String dsName,
      int remoteDsId, boolean isParallel, Integer maxMemory, Integer batchSize,
      boolean isConflation, boolean isPersistent, boolean isManulaStart) {
    File persistentDirectory = new File(dsName + "_disk_"
        + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] { persistentDirectory };

    if (isParallel) {
      GatewaySenderFactory gateway = cache
          .createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      ((InternalGatewaySenderFactory)gateway).setLocatorDiscoveryCallback(new MyLocatorCallback());
      ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<GatewayTransportFilter>();
      transportFilters.add(new CheckSumTranportFilter("CheckSumTranportFilter"));
      if (!transportFilters.isEmpty()) {
        for (GatewayTransportFilter filter : transportFilters) {
          gateway.addGatewayTransportFilter(filter);
        }
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    }
    else {
      GatewaySenderFactory gateway = cache
          .createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      ((InternalGatewaySenderFactory)gateway)
          .setLocatorDiscoveryCallback(new MyLocatorCallback());
      ArrayList<GatewayTransportFilter> transportFilters = new ArrayList<GatewayTransportFilter>();
      transportFilters.add(new CheckSumTranportFilter("CheckSumTranportFilter"));
      if (!transportFilters.isEmpty()) {
        for (GatewayTransportFilter filter : transportFilters) {
          gateway.addGatewayTransportFilter(filter);
        }
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName)
            .getName());
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
  }

  static class CheckSumTranportFilter implements GatewayTransportFilter {

    Adler32 checker = new Adler32();
    
    private String name;
    
    public CheckSumTranportFilter(String name){
      this.name = name;
    }

    @Override
    public String toString(){
      return this.name;
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
    public void close() {
    }
  }
}
