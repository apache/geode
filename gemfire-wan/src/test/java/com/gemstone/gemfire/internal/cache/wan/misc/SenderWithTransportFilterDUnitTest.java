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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.InternalGatewaySenderFactory;
import com.gemstone.gemfire.internal.cache.wan.WANTestBase;
import com.gemstone.gemfire.test.dunit.VM;

public class SenderWithTransportFilterDUnitTest extends WANTestBase {

  private static final long serialVersionUID = 1L;

  public SenderWithTransportFilterDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testSerialSenderWithTansportFilter() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(SenderWithTransportFilterDUnitTest.class,
        "createReceiverWithTransportFilters", new Object[] { nyPort });
    vm2.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", null, isOffHeap() });

    vm3.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm3.invoke(SenderWithTransportFilterDUnitTest.class,
        "createSenderWithTransportFilter", new Object[] { "ln", 2, false, 100,
            1, false, false, true });

    vm3.invoke(WANTestBase.class, "createReplicatedRegion", new Object[] {
        getTestMethodName() + "_RR", "ln", isOffHeap() });

    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_RR", 100 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_RR", 100 });
  }

  public void testParallelSenderWithTansportFilter() {
    Integer lnPort = (Integer)vm0.invoke(WANTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Integer nyPort = (Integer)vm1.invoke(WANTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm2.invoke(SenderWithTransportFilterDUnitTest.class,
        "createReceiverWithTransportFilters", new Object[] { nyPort });
    vm2.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", null, 0, 10, isOffHeap() });

    vm3.invoke(WANTestBase.class, "createCache", new Object[] { lnPort });

    vm3.invoke(SenderWithTransportFilterDUnitTest.class,
        "createSenderWithTransportFilter", new Object[] { "ln", 2, true, 100,
            1, false, false, true });

    vm3.invoke(WANTestBase.class, "createPartitionedRegion", new Object[] {
        getTestMethodName() + "_PR", "ln", 0, 10, isOffHeap() });

    vm3.invoke(WANTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANTestBase.class, "doPuts",
        new Object[] { getTestMethodName() + "_PR", 100 });

    vm2.invoke(WANTestBase.class, "validateRegionSize", new Object[] {
        getTestMethodName() + "_PR", 100 });
  }
  
  public static int createReceiverWithTransportFilters(int locPort) {
    WANTestBase test = new WANTestBase(getTestMethodName());
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
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
      e.printStackTrace();
      fail("Test " + test.getName()
          + " failed to start GatewayRecevier on port " + port);
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
    
    public String toString(){
      return this.name;
    }
    public InputStream getInputStream(InputStream stream) {
      return new CheckedInputStream(stream, checker);
      // return new ZipInputStream(stream);
    }

    public OutputStream getOutputStream(OutputStream stream) {
      return new CheckedOutputStream(stream, checker);
      // return new ZipOutputStream(stream);
    }

    public void close() {
      // TODO Auto-generated method stub
    }

  }
  
}
