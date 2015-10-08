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
package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.File;
import java.io.IOException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.FileUtil;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * A class for testing the recovery after restart for GemFire cluster that has
 * HDFS regions
 * 
 * @author Hemant Bhanawat
 */
@SuppressWarnings({ "serial", "deprecation", "rawtypes" })
public class RegionRecoveryDUnitTest extends CacheTestCase {
  public RegionRecoveryDUnitTest(String name) {
    super(name);
  }

  private static String homeDir = null;

  public void tearDown2() throws Exception {
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      SerializableCallable cleanUp = cleanUpStores();
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(cleanUp);
      }
    }
    super.tearDown2();
  }

  public SerializableCallable cleanUpStores() throws Exception {
    SerializableCallable cleanUp = new SerializableCallable() {
      public Object call() throws Exception {
        if (homeDir != null) {
          // Each VM will try to delete the same directory. But that's okay as
          // the subsequent invocations will be no-ops.
          FileUtil.delete(new File(homeDir));
          homeDir = null;
        }
        return 0;
      }
    };
    return cleanUp;
  }

  /**
   * Tests a basic restart of the system. Events if in HDFS should be read back.
   * The async queue is not persisted so we wait until async queue persists the
   * items to HDFS.
   * 
   * @throws Exception
   */
  public void testBasicRestart() throws Exception {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Going two level up to avoid home directories getting created in
    // VM-specific directory. This avoids failures in those tests where
    // datastores are restarted and bucket ownership changes between VMs.
    homeDir = "../../testBasicRestart";
    String uniqueName = "testBasicRestart";

    createServerRegion(vm0, 11, 1, 500, 500, homeDir, uniqueName);
    createServerRegion(vm1, 11, 1, 500, 500, homeDir, uniqueName);
    createServerRegion(vm2, 11, 1, 500, 500, homeDir, uniqueName);
    createServerRegion(vm3, 11, 1, 500, 500, homeDir, uniqueName);

    doPuts(vm0, uniqueName, 1, 50);
    doPuts(vm1, uniqueName, 40, 100);
    doPuts(vm2, uniqueName, 40, 100);
    doPuts(vm3, uniqueName, 90, 150);

    cacheClose(vm0, true);
    cacheClose(vm1, true);
    cacheClose(vm2, true);
    cacheClose(vm3, true);

    createServerRegion(vm0, 11, 1, 500, 500, homeDir, uniqueName);
    createServerRegion(vm1, 11, 1, 500, 500, homeDir, uniqueName);
    createServerRegion(vm2, 11, 1, 500, 500, homeDir, uniqueName);
    createServerRegion(vm3, 11, 1, 500, 500, homeDir, uniqueName);

    verifyGetsForValue(vm0, uniqueName, 1, 50, false);
    verifyGetsForValue(vm1, uniqueName, 40, 100, false);
    verifyGetsForValue(vm2, uniqueName, 40, 100, false);
    verifyGetsForValue(vm3, uniqueName, 90, 150, false);

    cacheClose(vm0, false);
    cacheClose(vm1, false);
    cacheClose(vm2, false);
    cacheClose(vm3, false);

    disconnectFromDS();

  }

  /**
   * Servers are stopped and restarted. Disabled due to bug 48067.
   */
  public void testPersistedAsyncQueue_Restart() throws Exception {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Going two level up to avoid home directories getting created in
    // VM-specific directory. This avoids failures in those tests where
    // datastores are restarted and bucket ownership changes between VMs.
    homeDir = "../../testPersistedAsyncQueue_Restart";
    String uniqueName = "testPersistedAsyncQueue_Restart";

    // create cache and region
    createPersistedServerRegion(vm0, 11, 1, 2000, 5, homeDir, uniqueName);
    createPersistedServerRegion(vm1, 11, 1, 2000, 5, homeDir, uniqueName);
    createPersistedServerRegion(vm2, 11, 1, 2000, 5, homeDir, uniqueName);
    createPersistedServerRegion(vm3, 11, 1, 2000, 5, homeDir, uniqueName);

    // do some puts
    AsyncInvocation a0 = doAsyncPuts(vm0, uniqueName, 1, 50);
    AsyncInvocation a1 = doAsyncPuts(vm1, uniqueName, 40, 100);
    AsyncInvocation a2 = doAsyncPuts(vm2, uniqueName, 40, 100);
    AsyncInvocation a3 = doAsyncPuts(vm3, uniqueName, 90, 150);

    a3.join();
    a2.join();
    a1.join();
    a0.join();

    // close the cache
    cacheClose(vm0, true);
    cacheClose(vm1, true);
    cacheClose(vm2, true);
    cacheClose(vm3, true);

    // recreate the cache and regions
    a3 = createAsyncPersistedServerRegion(vm3, 11, 1, 2000, 5, homeDir, uniqueName);
    a2 = createAsyncPersistedServerRegion(vm2, 11, 1, 2000, 5, homeDir, uniqueName);
    a1 = createAsyncPersistedServerRegion(vm1, 11, 1, 2000, 5, homeDir, uniqueName);
    a0 = createAsyncPersistedServerRegion(vm0, 11, 1, 2000, 5, homeDir, uniqueName);

    a3.join();
    a2.join();
    a1.join();
    a0.join();

    // these gets should probably fetch the data from async queue
    verifyGetsForValue(vm0, uniqueName, 1, 50, false);
    verifyGetsForValue(vm1, uniqueName, 40, 100, false);
    verifyGetsForValue(vm2, uniqueName, 40, 100, false);
    verifyGetsForValue(vm3, uniqueName, 90, 150, false);

    // these gets wait for sometime before fetching the data. this will ensure
    // that the reads are done from HDFS
    verifyGetsForValue(vm0, uniqueName, 1, 50, true);
    verifyGetsForValue(vm1, uniqueName, 40, 100, true);
    verifyGetsForValue(vm2, uniqueName, 40, 100, true);
    verifyGetsForValue(vm3, uniqueName, 90, 150, true);

    cacheClose(vm0, false);
    cacheClose(vm1, false);
    cacheClose(vm2, false);
    cacheClose(vm3, false);

    disconnectFromDS();
  }

  /**
   * Stops a single server. A different node becomes primary for the buckets on
   * the stopped node. Everything should work fine. Disabled due to bug 48067
   * 
   */
  public void testPersistedAsyncQueue_ServerRestart() throws Exception {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Going two level up to avoid home directories getting created in
    // VM-specific directory. This avoids failures in those tests where
    // datastores are restarted and bucket ownership changes between VMs.
    homeDir = "../../testPAQ_ServerRestart";
    String uniqueName = "testPAQ_ServerRestart";

    createPersistedServerRegion(vm0, 11, 1, 2000, 5, homeDir, uniqueName);
    createPersistedServerRegion(vm1, 11, 1, 2000, 5, homeDir, uniqueName);
    createPersistedServerRegion(vm2, 11, 1, 2000, 5, homeDir, uniqueName);
    createPersistedServerRegion(vm3, 11, 1, 2000, 5, homeDir, uniqueName);

    AsyncInvocation a0 = doAsyncPuts(vm0, uniqueName, 1, 50);
    AsyncInvocation a1 = doAsyncPuts(vm1, uniqueName, 50, 75);
    AsyncInvocation a2 = doAsyncPuts(vm2, uniqueName, 75, 100);
    AsyncInvocation a3 = doAsyncPuts(vm3, uniqueName, 100, 150);

    a3.join();
    a2.join();
    a1.join();
    a0.join();

    cacheClose(vm0, false);

    // these gets should probably fetch the data from async queue
    verifyGetsForValue(vm1, uniqueName, 1, 50, false);
    verifyGetsForValue(vm2, uniqueName, 40, 100, false);
    verifyGetsForValue(vm3, uniqueName, 70, 150, false);

    // these gets wait for sometime before fetching the data. this will ensure
    // that
    // the reads are done from HDFS
    verifyGetsForValue(vm2, uniqueName, 1, 100, true);
    verifyGetsForValue(vm3, uniqueName, 40, 150, true);

    cacheClose(vm1, false);
    cacheClose(vm2, false);
    cacheClose(vm3, false);

    disconnectFromDS();
  }

  private int createPersistedServerRegion(final VM vm, final int totalnumOfBuckets,
      final int batchSize, final int batchInterval, final int maximumEntries, 
      final String folderPath, final String uniqueName) throws IOException {
    
    return (Integer) vm.invoke(new PersistedRegionCreation(vm, totalnumOfBuckets,
      batchSize, batchInterval, maximumEntries, folderPath, uniqueName));
  }
  private AsyncInvocation createAsyncPersistedServerRegion(final VM vm, final int totalnumOfBuckets,
      final int batchSize, final int batchInterval, final int maximumEntries, final String folderPath, 
      final String uniqueName) throws IOException {
    
    return (AsyncInvocation) vm.invokeAsync(new PersistedRegionCreation(vm, totalnumOfBuckets,
      batchSize, batchInterval, maximumEntries, folderPath, uniqueName));
  }
  
  class PersistedRegionCreation extends SerializableCallable {
    private VM vm;
    private int totalnumOfBuckets;
    private int batchSize;
    private int maximumEntries;
    private String folderPath;
    private String uniqueName;
    private int batchInterval;

    PersistedRegionCreation(final VM vm, final int totalnumOfBuckets,
        final int batchSize, final int batchInterval, final int maximumEntries,
        final String folderPath, final String uniqueName) throws IOException {
      this.vm = vm;
      this.totalnumOfBuckets = totalnumOfBuckets;
      this.batchSize = batchSize;
      this.maximumEntries = maximumEntries;
      this.folderPath = new File(folderPath).getCanonicalPath();
      this.uniqueName = uniqueName;
      this.batchInterval = batchInterval;
    }

    public Object call() throws Exception {

      AttributesFactory af = new AttributesFactory();
      af.setDataPolicy(DataPolicy.HDFS_PARTITION);
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setTotalNumBuckets(totalnumOfBuckets);
      paf.setRedundantCopies(1);

      af.setPartitionAttributes(paf.create());

      HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
      hsf.setHomeDir(folderPath);
      homeDir = folderPath; // for clean-up in tearDown2()
      hsf.setBatchSize(batchSize);
      hsf.setBatchInterval(batchInterval);
      hsf.setBufferPersistent(true);
      hsf.setDiskStoreName(uniqueName + vm.getPid());

      getCache().createDiskStoreFactory().create(uniqueName + vm.getPid());

      af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));
      af.setHDFSStoreName(uniqueName);
      af.setHDFSWriteOnly(false);

      hsf.create(uniqueName);

      createRootRegion(uniqueName, af.create());

      return 0;
    }
  };

  private int createServerRegion(final VM vm, final int totalnumOfBuckets,
      final int batchSize, final int batchInterval, final int maximumEntries,
      final String folderPath, final String uniqueName) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.HDFS_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(totalnumOfBuckets);
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());

        HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
        homeDir = new File(folderPath).getCanonicalPath();
        hsf.setHomeDir(homeDir);
        hsf.setBatchSize(batchSize);
        hsf.setBatchInterval(batchInterval);
        hsf.setBufferPersistent(false);
        hsf.setMaxMemory(1);
        hsf.create(uniqueName);
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));

        af.setHDFSWriteOnly(false);
        af.setHDFSStoreName(uniqueName);
        createRootRegion(uniqueName, af.create());

        return 0;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private void cacheClose(VM vm, final boolean sleep) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        if (sleep)
          Thread.sleep(2000);
        getCache().getLogger().info("Cache close in progress ");
        getCache().close();
        getCache().getDistributedSystem().disconnect();
        getCache().getLogger().info("Cache closed");
        return null;
      }
    });

  }

  private void doPuts(VM vm, final String regionName, final int start, final int end) throws Exception {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion(regionName);
        getCache().getLogger().info("Putting entries ");
        for (int i = start; i < end; i++) {
          r.put("K" + i, "V" + i);
        }
        return null;
      }

    });
  }

  private AsyncInvocation doAsyncPuts(VM vm, final String regionName,
      final int start, final int end) throws Exception {
    return vm.invokeAsync(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion(regionName);
        getCache().getLogger().info("Putting entries ");
        for (int i = start; i < end; i++) {
          r.put("K" + i, "V" + i);
        }
        return null;
      }

    });
  }

  private void verifyGetsForValue(VM vm, final String regionName, final int start, final int end, final boolean sleep) throws Exception {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        if (sleep) {
          Thread.sleep(2000);
        }
        getCache().getLogger().info("Getting entries ");
        Region r = getRootRegion(regionName);
        for (int i = start; i < end; i++) {
          String k = "K" + i;
          Object s = r.get(k);
          String v = "V" + i;
          assertTrue("The expected key " + v+ " didn't match the received value " + s, v.equals(s));
        }
        return null;
      }

    });

  }
}
