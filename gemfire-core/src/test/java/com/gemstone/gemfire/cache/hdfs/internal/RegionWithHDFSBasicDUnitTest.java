/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * A class for testing the basic HDFS functionality
 * 
 * @author Hemant Bhanawat
 */
@SuppressWarnings({ "serial", "rawtypes", "deprecation", "unchecked", "unused" })
public class RegionWithHDFSBasicDUnitTest extends RegionWithHDFSTestBase {

  private static final Logger logger = LogService.getLogger();

  private ExpectedException ee0;
  private ExpectedException ee1; 

  public RegionWithHDFSBasicDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    ee0 = DistributedTestCase.addExpectedException("com.gemstone.gemfire.cache.RegionDestroyedException");
    ee1 = DistributedTestCase.addExpectedException("com.gemstone.gemfire.cache.RegionDestroyedException");
  }

  public void tearDown2() throws Exception {
    ee0.remove();
    ee1.remove();
    super.tearDown2();
  }

  @Override
  protected SerializableCallable getCreateRegionCallable(
      final int totalnumOfBuckets, final int batchSizeMB,
      final int maximumEntries, final String folderPath,
      final String uniqueName, final int batchInterval,
      final boolean queuePersistent, final boolean writeonly,
      final long timeForRollover, final long maxFileSize) {
    SerializableCallable createRegion = new SerializableCallable("Create HDFS region") {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.HDFS_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(totalnumOfBuckets);
        paf.setRedundantCopies(1);

        af.setHDFSStoreName(uniqueName);
        af.setPartitionAttributes(paf.create());

        HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
        // Going two level up to avoid home directories getting created in
        // VM-specific directory. This avoids failures in those tests where
        // datastores are restarted and bucket ownership changes between VMs.
        homeDir = new File(tmpDir + "/../../" + folderPath).getCanonicalPath();
        logger.info("Setting homeDir to {}", homeDir);
        hsf.setHomeDir(homeDir);
        hsf.setBatchSize(batchSizeMB);
        hsf.setBufferPersistent(queuePersistent);
        hsf.setMaxMemory(3);
        hsf.setBatchInterval(batchInterval);
        if (timeForRollover != -1) {
          hsf.setWriteOnlyFileRolloverInterval((int) timeForRollover);
          System.setProperty("gemfire.HDFSRegionDirector.FILE_ROLLOVER_TASK_INTERVAL_SECONDS", "1");
        }
        if (maxFileSize != -1) {
          hsf.setWriteOnlyFileRolloverSize((int) maxFileSize);
        }
        hsf.create(uniqueName);

        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));

        af.setHDFSWriteOnly(writeonly);
        Region r = createRootRegion(uniqueName, af.create());
        ((LocalRegion) r).setIsTest();

        return 0;
      }
    };
    return createRegion;
  }

  @Override
  protected void doPuts(final String uniqueName, int start, int end) {
    Region r = getRootRegion(uniqueName);
    for (int i = start; i < end; i++) {
      r.put("K" + i, "V" + i);
    }
  }

  @Override
  protected void doPutAll(final String uniqueName, Map map) {
    Region r = getRootRegion(uniqueName);
    r.putAll(map);
  }

  @Override
  protected void doDestroys(final String uniqueName, int start, int end) {
    Region r = getRootRegion(uniqueName);
    for (int i = start; i < end; i++) {
      r.destroy("K" + i);
    }
  }

  @Override
  protected void checkWithGet(String uniqueName, int start, int end, boolean expectValue) {
    Region r = getRootRegion(uniqueName);
    for (int i = start; i < end; i++) {
      String expected = expectValue ? "V" + i : null;
      assertEquals("Mismatch on key " + i, expected, r.get("K" + i));
    }
  }

  @Override
  protected void checkWithGetAll(String uniqueName, ArrayList arrayl) {
    Region r = getRootRegion(uniqueName);
    Map map = r.getAll(arrayl);
    logger.info("Read entries {}", map.size());
    for (Object e : map.keySet()) {
      String v = e.toString().replaceFirst("K", "V");
      assertTrue( "Reading entries failed for key " + e + " where value = " + map.get(e), v.equals(map.get(e)));
    }
  }

  /**
   * Tests if gets go to primary even if the value resides on secondary.
   */
  public void testValueFetchedFromLocal() {
    disconnectFromDS();

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    String homeDir = "./testValueFetchedFromLocal";

    createServerRegion(vm0, 7, 1, 50, homeDir, "testValueFetchedFromLocal", 1000);
    createServerRegion(vm1, 7, 1, 50, homeDir, "testValueFetchedFromLocal", 1000);

    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testValueFetchedFromLocal");
        for (int i = 0; i < 25; i++) {
          r.put("K" + i, "V" + i);
        }
        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testValueFetchedFromLocal");
        for (int i = 0; i < 25; i++) {
          String s = null;
          String k = "K" + i;
          s = (String) r.get(k);
          String v = "V" + i;
          assertTrue( "The expected key " + v+ " didn't match the received value " + s, v.equals(s));
        }
        // with only two members and 1 redundant copy, we will have all data locally, make sure that some
        // get operations results in a remote get operation
        assertTrue( "gets should always go to primary, ", ((LocalRegion)r).getCountNotFoundInLocal() != 0 );
        return null;
      }
    });
  
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testValueFetchedFromLocal");
        assertTrue( "HDFS queue or HDFS should not have been accessed. They were accessed " + ((LocalRegion)r).getCountNotFoundInLocal()  + " times", 
            ((LocalRegion)r).getCountNotFoundInLocal() == 0 );
        return null;
      }
    });
  }

  public void testHDFSQueueSizeTest() {
    disconnectFromDS();

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    String homeDir = "./testHDFSQueueSize";

    createServerRegion(vm0, 1, 10, 50, homeDir, "testHDFSQueueSize", 100000);
    createServerRegion(vm1, 1, 10, 50, homeDir, "testHDFSQueueSize", 100000);

    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testHDFSQueueSize");
        byte[] b = new byte[1024];
        byte[] k = new byte[1];
        for (int i = 0; i < 1; i++) {
          r.put(k, b);
        }
        ConcurrentParallelGatewaySenderQueue hdfsqueue = (ConcurrentParallelGatewaySenderQueue)((AbstractGatewaySender)((PartitionedRegion)r).getHDFSEventQueue().getSender()).getQueue();
        HDFSBucketRegionQueue hdfsBQ = (HDFSBucketRegionQueue)((PartitionedRegion)hdfsqueue.getRegion()).getDataStore().getLocalBucketById(0);
        if (hdfsBQ.getBucketAdvisor().isPrimary()) {
          assertTrue("size should not as expected on primary " + hdfsBQ.queueSizeInBytes.get(), hdfsBQ.queueSizeInBytes.get() > 1024 && hdfsBQ.queueSizeInBytes.get() < 1150);
        } else {
          assertTrue("size should be 0 on secondary", hdfsBQ.queueSizeInBytes.get()==0);
        }
        return null;

      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testHDFSQueueSize");
        ConcurrentParallelGatewaySenderQueue hdfsqueue = (ConcurrentParallelGatewaySenderQueue)((AbstractGatewaySender)((PartitionedRegion)r).getHDFSEventQueue().getSender()).getQueue();
        HDFSBucketRegionQueue hdfsBQ = (HDFSBucketRegionQueue)((PartitionedRegion)hdfsqueue.getRegion()).getDataStore().getLocalBucketById(0);
        if (hdfsBQ.getBucketAdvisor().isPrimary()) {
          assertTrue("size should not as expected on primary " + hdfsBQ.queueSizeInBytes.get(), hdfsBQ.queueSizeInBytes.get() > 1024 && hdfsBQ.queueSizeInBytes.get() < 1150);
        } else {
          assertTrue("size should be 0 on secondary", hdfsBQ.queueSizeInBytes.get()==0);
        }
        return null;

      }
    });
  }

  /**
   * Does put for write only HDFS store
   */
  public void testBasicPutsForWriteOnlyHDFSStore() {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    String homeDir = "./testPutsForWriteOnlyHDFSStore";

    createServerRegion(vm0, 7, 1, 20, homeDir, "testPutsForWriteOnlyHDFSStore",
        100, true, false);
    createServerRegion(vm1, 7, 1, 20, homeDir, "testPutsForWriteOnlyHDFSStore",
        100, true, false);

    // Do some puts
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testPutsForWriteOnlyHDFSStore");
        for (int i = 0; i < 200; i++) {
          r.put("K" + i, "V" + i);
        }
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testPutsForWriteOnlyHDFSStore");

        for (int i = 200; i < 400; i++) {
          r.put("K" + i, "V" + i);
        }

        return null;
      }
    });

  }

  /**
   * Does put for write only HDFS store
   */
  public void testDelta() {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    String homeDir = "./testDelta";

    // Expected from com.gemstone.gemfire.internal.cache.ServerPingMessage.send()
    ExpectedException ee1 = DistributedTestCase.addExpectedException("java.lang.InterruptedException");
    ExpectedException ee2 = DistributedTestCase.addExpectedException("java.lang.InterruptedException");
    
    createServerRegion(vm0, 7, 1, 20, homeDir, "testDelta", 100);
    createServerRegion(vm1, 7, 1, 20, homeDir, "testDelta", 100);

    // Do some puts
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testDelta");
        for (int i = 0; i < 100; i++) {
          r.put("K" + i, new CustomerDelta("V" + i, "address"));
        }
        for (int i = 0; i < 50; i++) {
          CustomerDelta cd = new CustomerDelta("V" + i, "address");
          cd.setAddress("updated address");
          r.put("K" + i, cd);
        }
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testDelta");

        for (int i = 100; i < 200; i++) {
          r.put("K" + i, new CustomerDelta("V" + i, "address"));
        }
        for (int i = 100; i < 150; i++) {
          CustomerDelta cd = new CustomerDelta("V" + i, "address");
          cd.setAddress("updated address");
          r.put("K" + i, cd);
        }

        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testDelta");
        for (int i = 0; i < 50; i++) {
          CustomerDelta custDela =  new CustomerDelta ("V" + i, "updated address" );
          String k = "K" + i;
          CustomerDelta s = (CustomerDelta) r.get(k);

          assertTrue( "The expected value " + custDela + " didn't match the received value " + s, custDela.equals(s));
        }
        for (int i = 50; i < 100; i++) {
          CustomerDelta custDela = new CustomerDelta("V" + i, "address");
          String k = "K" + i;
          CustomerDelta s = (CustomerDelta) r.get(k);

          assertTrue( "The expected value " + custDela + " didn't match the received value " + s, custDela.equals(s));
        }
        for (int i = 100; i < 150; i++) {
          CustomerDelta custDela =  new CustomerDelta ("V" + i, "updated address" );
          String k = "K" + i;
          CustomerDelta s = (CustomerDelta) r.get(k);

          assertTrue( "The expected value " + custDela + " didn't match the received value " + s, custDela.equals(s));
        }
        for (int i = 150; i < 200; i++) {
          CustomerDelta custDela =  new CustomerDelta ("V" + i, "address" );
          String k = "K" + i;
          CustomerDelta s = (CustomerDelta) r.get(k);

          assertTrue( "The expected value " + custDela + " didn't match the received value " + s, custDela.equals(s));
        }
        return null;
      }
    });
    ee1.remove();
    ee2.remove();

  }

  /**
   * Puts byte arrays and fetches them back to ensure that serialization of byte
   * arrays is proper
   * 
   */
  public void testByteArrays() {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    String homeDir = "./testByteArrays";

    createServerRegion(vm0, 7, 1, 20, homeDir, "testByteArrays", 100);
    createServerRegion(vm1, 7, 1, 20, homeDir, "testByteArrays", 100);

    // Do some puts
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testByteArrays");
        byte[] b1 = { 0x11, 0x44, 0x77 };
        byte[] b2 = { 0x22, 0x55 };
        byte[] b3 = { 0x33 };
        for (int i = 0; i < 100; i++) {
          int x = i % 3;
          if (x == 0) {
            r.put("K" + i, b1);
          } else if (x == 1) {
            r.put("K" + i, b2);
          } else {
            r.put("K" + i, b3);
          }
        }
        return null;
      }
    });

    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testByteArrays");

        byte[] b1 = { 0x11, 0x44, 0x77 };
        byte[] b2 = { 0x22, 0x55 };
        byte[] b3 = { 0x33 };
        for (int i = 100; i < 200; i++) {
          int x = i % 3;
          if (x == 0) {
            r.put("K" + i, b1);
          } else if (x == 1) {
            r.put("K" + i, b2);
          } else {
            r.put("K" + i, b3);
          }
        }
        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testByteArrays");
        byte[] b1 = { 0x11, 0x44, 0x77 };
        byte[] b2 = { 0x22, 0x55 };
        byte[] b3 = { 0x33 };
        for (int i = 0; i < 200; i++) {
          int x = i % 3;
          String k = "K" + i;
          byte[] s = (byte[]) r.get(k);
          if (x == 0) {
            assertTrue( "The expected value didn't match the received value of byte array" , Arrays.equals(b1, s));
          } else if (x == 1) {
            assertTrue( "The expected value didn't match the received value of byte array" , Arrays.equals(b2, s));
          } else {
            assertTrue( "The expected value didn't match the received value of byte array" , Arrays.equals(b3, s));
          }

        }
        return null;
      }
    });
  }

  private static class CustomerDelta implements Serializable, Delta {
    private String name;
    private String address;
    private boolean nameChanged;
    private boolean addressChanged;

    public CustomerDelta(CustomerDelta o) {
      this.address = o.address;
      this.name = o.name;
    }

    public CustomerDelta(String name, String address) {
      this.name = name;
      this.address = address;
    }

    public void fromDelta(DataInput in) throws IOException,
        InvalidDeltaException {
      boolean nameC = in.readBoolean();
      if (nameC) {
        this.name = in.readUTF();
      }
      boolean addressC = in.readBoolean();
      if (addressC) {
        this.address = in.readUTF();
      }
    }

    public boolean hasDelta() {
      return nameChanged || addressChanged;
    }

    public void toDelta(DataOutput out) throws IOException {
      out.writeBoolean(nameChanged);
      if (this.nameChanged) {
        out.writeUTF(name);
      }
      out.writeBoolean(addressChanged);
      if (this.addressChanged) {
        out.writeUTF(address);
      }
    }

    public void setName(String name) {
      this.nameChanged = true;
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public void setAddress(String address) {
      this.addressChanged = true;
      this.address = address;
    }

    public String getAddress() {
      return address;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CustomerDelta)) {
        return false;
      }
      CustomerDelta other = (CustomerDelta) obj;
      return this.name.equals(other.name) && this.address.equals(other.address);
    }

    @Override
    public int hashCode() {
      return this.address.hashCode() + this.name.hashCode();
    }

    @Override
    public String toString() {
      return "name=" + this.name + "address=" + address;
    }
  }

  public void testClearRegionDataInQueue() throws Throwable {
    doTestClearRegion(100000, false);

  }

  public void testClearRegionDataInHDFS() throws Throwable {
    doTestClearRegion(1, true);
  }

  public void doTestClearRegion(int batchInterval, boolean waitForWriteToHDFS) throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final int numEntries = 400;

    String name = getName();
    final String folderPath = "./" + name;
    // Create some regions. Note that we want a large batch interval
    // so that we will have some entries sitting in the queue when
    // we do a clear.
    final String uniqueName = name;
    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, batchInterval,
        false, true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, batchInterval,
        false, true);

    doPuts(vm0, uniqueName, numEntries);

    // Make sure some files have been written to hdfs.
    if (waitForWriteToHDFS) {
      verifyDataInHDFS(vm0, uniqueName, true, true, waitForWriteToHDFS, numEntries);
    }

    // Do a clear
    simulateClear(uniqueName, vm0, vm1);

    validateEmpty(vm0, numEntries, uniqueName);
    validateEmpty(vm1, numEntries, uniqueName);

    // Double check that there is no data in hdfs now
    verifyDataInHDFS(vm0, uniqueName, false, false, waitForWriteToHDFS, numEntries);
    verifyDataInHDFS(vm1, uniqueName, false, false, waitForWriteToHDFS, numEntries);

    closeCache(vm0);
    closeCache(vm1);

    AsyncInvocation async0 = createServerRegionAsync(vm0, 7, 31, 200, folderPath, 
        uniqueName, 100000, false, true);
    AsyncInvocation async1 = createServerRegionAsync(vm1, 7, 31, 200, folderPath, 
        uniqueName, 100000, false, true);
    async0.getResult();
    async1.getResult();

    validateEmpty(vm0, numEntries, uniqueName);
    validateEmpty(vm1, numEntries, uniqueName);
  }

  private void simulateClear(final String name, VM... vms) throws Throwable {
    simulateClearForTests(true);
    try {

      // Gemfire PRs don't support clear
      // gemfirexd does a clear by taking gemfirexd ddl locks
      // and then clearing each primary bucket on the primary.
      // Simulate that by clearing all primaries on each vm.
      // See GemFireContainer.clear

      SerializableCallable clear = new SerializableCallable("clear") {
        public Object call() throws Exception {
          PartitionedRegion r = (PartitionedRegion) getRootRegion(name);

          r.clearLocalPrimaries();

          return null;
        }
      };

      // Invoke the clears concurrently
      AsyncInvocation[] async = new AsyncInvocation[vms.length];
      for (int i = 0; i < vms.length; i++) {
        async[i] = vms[i].invokeAsync(clear);
      }

      // Get the clear results.
      for (int i = 0; i < async.length; i++) {
        async[i].getResult();
      }

    } finally {
      simulateClearForTests(false);
    }
  }

  protected void simulateClearForTests(final boolean isGfxd) {
    SerializableRunnable setGfxd = new SerializableRunnable() {
      @Override
      public void run() {
        if (isGfxd) {
          LocalRegion.simulateClearForTests(true);
        } else {
          LocalRegion.simulateClearForTests(false);
        }
      }
    };
    setGfxd.run();
    invokeInEveryVM(setGfxd);
  }

  /**
   * Test that we can locally destroy a member, without causing problems with
   * the data in HDFS. This was disabled due to ticket 47793.
   * 
   * @throws InterruptedException
   */
  public void testLocalDestroy() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    int numEntries = 200;

    final String folderPath = "./testLocalDestroy";
    final String uniqueName = "testLocalDestroy";

    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 1, false, true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, 1, false, true);

    doPuts(vm0, uniqueName, numEntries);

    // Make sure some files have been written to hdfs and wait for
    // the queue to drain.
    verifyDataInHDFS(vm0, uniqueName, true, true, true, numEntries);

    validate(vm0, uniqueName, numEntries);

    SerializableCallable localDestroy = new SerializableCallable("local destroy") {
      public Object call() throws Exception {
        Region r = getRootRegion(uniqueName);
        r.localDestroyRegion();
        return null;
      }
    };

    vm0.invoke(localDestroy);

    verifyNoQOrPR(vm0);

    validate(vm1, uniqueName, numEntries);

    vm1.invoke(localDestroy);

    verifyNoQOrPR(vm1);

    closeCache(vm0);
    closeCache(vm1);

    // Restart vm0 and see if the data is still available from HDFS
    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 1, false, true);

    validate(vm0, uniqueName, numEntries);
  }

  /**
   * Test that doing a destroyRegion removes all data from HDFS.
   * 
   * @throws InterruptedException
   */
  public void testGlobalDestroyWithHDFSData() throws InterruptedException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final String folderPath = "./testGlobalDestroyWithHDFSData";
    final String uniqueName = "testGlobalDestroyWithHDFSData";
    int numEntries = 200;

    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 1, false, true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, 1, false, true);

    doPuts(vm0, uniqueName, numEntries);

    // Make sure some files have been written to hdfs.
    verifyDataInHDFS(vm0, uniqueName, true, true, false, numEntries);

    SerializableCallable globalDestroy = new SerializableCallable("destroy") {
      public Object call() throws Exception {
        Region r = getRootRegion(uniqueName);
        r.destroyRegion();
        return null;
      }
    };

    vm0.invoke(globalDestroy);

    // make sure data is not in HDFS
    verifyNoQOrPR(vm0);
    verifyNoQOrPR(vm1);
    verifyNoHDFSData(vm0, uniqueName);
    verifyNoHDFSData(vm1, uniqueName);

    closeCache(vm0);
    closeCache(vm1);

    // Restart vm0 and make sure it's still empty
    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 1, false, true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, 1, false, true);

    // make sure it's empty
    validateEmpty(vm0, numEntries, uniqueName);
    validateEmpty(vm1, numEntries, uniqueName);

  }

  /**
   * Test that doing a destroyRegion removes all data from HDFS.
   */
  public void _testGlobalDestroyWithQueueData() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final String folderPath = "./testGlobalDestroyWithQueueData";
    final String uniqueName = "testGlobalDestroyWithQueueData";
    int numEntries = 200;

    // set a large queue timeout so that data is still in the queue
    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 10000, false,
        true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, 10000, false,
        true);

    doPuts(vm0, uniqueName, numEntries);

    SerializableCallable globalDestroy = new SerializableCallable("destroy") {
      public Object call() throws Exception {
        Region r = getRootRegion(uniqueName);
        r.destroyRegion();
        return null;
      }
    };

    vm0.invoke(globalDestroy);

    // make sure data is not in HDFS
    verifyNoQOrPR(vm0);
    verifyNoQOrPR(vm1);
    verifyNoHDFSData(vm0, uniqueName);
    verifyNoHDFSData(vm1, uniqueName);

    closeCache(vm0);
    closeCache(vm1);

    // Restart vm0 and make sure it's still empty
    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 1, false, true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, 1, false, true);

    // make sure it's empty
    validateEmpty(vm0, numEntries, uniqueName);
    validateEmpty(vm1, numEntries, uniqueName);

  }

  /**
   * Make sure all async event queues and PRs a destroyed in a member
   */
  public void verifyNoQOrPR(VM vm) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        assertEquals(Collections.EMPTY_SET, cache.getAsyncEventQueues());
        assertEquals(Collections.EMPTY_SET, cache.getPartitionedRegions());
      }
    });

  }

  /**
   * Make sure all of the data for a region in HDFS is destroyed
   */
  public void verifyNoHDFSData(final VM vm, final String uniqueName) {
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        HDFSStoreImpl hdfsStore = (HDFSStoreImpl) ((GemFireCacheImpl)getCache()).findHDFSStore(uniqueName);
        FileSystem fs = hdfsStore.getFileSystem();
        Path path = new Path(hdfsStore.getHomeDir(), uniqueName);
        if (fs.exists(path)) {
          dumpFiles(vm, uniqueName);
          fail("Found files in " + path);
        }
        return null;
      }
    });
  }

  protected AsyncInvocation doAsyncPuts(VM vm, final String regionName,
      final int start, final int end, final String suffix) throws Exception {
    return doAsyncPuts(vm, regionName, start, end, suffix, "");
  }

  protected AsyncInvocation doAsyncPuts(VM vm, final String regionName,
      final int start, final int end, final String suffix, final String value)
      throws Exception {
    return vm.invokeAsync(new SerializableCallable("doAsyncPuts") {
      public Object call() throws Exception {
        Region r = getRootRegion(regionName);
        String v = "V";
        if (!value.equals("")) {
          v = value;
        }
        logger.info("Putting entries ");
        for (int i = start; i < end; i++) {
          r.put("K" + i, v + i + suffix);
        }
        return null;
      }

    });
  }

  public void _testGlobalDestroyFromAccessor() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    final String folderPath = "./testGlobalDestroyFromAccessor";
    final String uniqueName = "testGlobalDestroyFromAccessor";
    int numEntries = 200;

    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 1, false, true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, 1, false, true);
    createServerAccessor(vm2, 7, 40, uniqueName);

    doPuts(vm0, uniqueName, numEntries);

    // Make sure some files have been written to hdfs.
    verifyDataInHDFS(vm0, uniqueName, true, true, false, numEntries);

    SerializableCallable globalDestroy = new SerializableCallable("destroy") {
      public Object call() throws Exception {
        Region r = getRootRegion(uniqueName);
        r.destroyRegion();
        return null;
      }
    };

    // Destroy the region from an accessor
    vm2.invoke(globalDestroy);

    // make sure data is not in HDFS
    verifyNoQOrPR(vm0);
    verifyNoQOrPR(vm1);
    verifyNoHDFSData(vm0, uniqueName);
    verifyNoHDFSData(vm1, uniqueName);

    closeCache(vm0);
    closeCache(vm1);
    closeCache(vm2);

    // Restart vm0 and make sure it's still empty
    createServerRegion(vm0, 7, 31, 40, folderPath, uniqueName, 1, false, true);
    createServerRegion(vm1, 7, 31, 40, folderPath, uniqueName, 1, false, true);

    // make sure it's empty
    validateEmpty(vm0, numEntries, uniqueName);
    validateEmpty(vm1, numEntries, uniqueName);
  }

  /**
   * create a server with maxfilesize as 2 MB. Insert 4 entries of 1 MB each.
   * There should be 2 files with 2 entries each.
   * 
   * @throws Throwable
   */
  public void testWOFileSizeParam() throws Throwable {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    String homeDir = "./testWOFileSizeParam";
    final String uniqueName = getName();
    String value = "V";
    for (int i = 0; i < 20; i++) {
      value += value;
    }

    createServerRegion(vm0, 1, 1,  500, homeDir, uniqueName, 5, true, false, 2000, 2);
    createServerRegion(vm1, 1, 1,  500, homeDir, uniqueName, 5, true, false, 2000, 2);

    AsyncInvocation a1 = doAsyncPuts(vm0, uniqueName, 1, 3, "vm0", value);
    AsyncInvocation a2 = doAsyncPuts(vm1, uniqueName, 2, 4, "vm1", value);

    a1.join();
    a2.join();

    Thread.sleep(4000);

    cacheClose(vm0, false);
    cacheClose(vm1, false);

    // Start the VMs in parallel for the persistent version subclass
    AsyncInvocation async1 = createServerRegionAsync(vm0, 1, 1,  500, homeDir, uniqueName, 5, true, false, 2000, 2);
    AsyncInvocation async2 = createServerRegionAsync(vm1, 1, 1,  500, homeDir, uniqueName, 5, true, false, 2000, 2);
    async1.getResult();
    async2.getResult();

    // There should be two files in bucket 0.
    verifyTwoHDFSFilesWithTwoEntries(vm0, uniqueName, value);

    cacheClose(vm0, false);
    cacheClose(vm1, false);

    disconnectFromDS();

  }

  /**
   * Create server with file rollover time as 5 seconds. Insert few entries and
   * then sleep for 7 seconds. A file should be created. Do it again. At the end, two
   * files with inserted entries should be created.
   * 
   * @throws Throwable
   */
  public void testWOTimeForRollOverParam() throws Throwable {
    disconnectFromDS();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    String homeDir = "./testWOTimeForRollOverParam";
    final String uniqueName = getName();

    createServerRegion(vm0, 1, 1, 500, homeDir, uniqueName, 5, true, false, 5, 1);
    createServerRegion(vm1, 1, 1, 500, homeDir, uniqueName, 5, true, false, 5, 1);

    AsyncInvocation a1 = doAsyncPuts(vm0, uniqueName, 1, 8, "vm0");
    AsyncInvocation a2 = doAsyncPuts(vm1, uniqueName, 4, 10, "vm1");

    a1.join();
    a2.join();

    Thread.sleep(7000);

    a1 = doAsyncPuts(vm0, uniqueName, 10, 18, "vm0");
    a2 = doAsyncPuts(vm1, uniqueName, 14, 20, "vm1");

    a1.join();
    a2.join();

    Thread.sleep(7000);

    cacheClose(vm0, false);
    cacheClose(vm1, false);

    AsyncInvocation async1 = createServerRegionAsync(vm0, 1, 1, 500, homeDir, uniqueName, 5, true, false, 5, 1);
    AsyncInvocation async2 = createServerRegionAsync(vm1, 1, 1, 500, homeDir, uniqueName, 5, true, false, 5, 1);
    async1.getResult();
    async2.getResult();

    // There should be two files in bucket 0.
    // Each should have entry 1 to 10 and duplicate from 4 to 7
    verifyTwoHDFSFiles(vm0, uniqueName);

    cacheClose(vm0, false);
    cacheClose(vm1, false);

    disconnectFromDS();

  }

  private void createServerAccessor(VM vm, final int totalnumOfBuckets,
      final int maximumEntries, final String uniqueName) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.HDFS_PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(totalnumOfBuckets);
        paf.setRedundantCopies(1);
        // make this member an accessor.
        paf.setLocalMaxMemory(0);
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(maximumEntries, EvictionAction.LOCAL_DESTROY));
        af.setPartitionAttributes(paf.create());

        Region r = createRootRegion(uniqueName, af.create());
        assertTrue(!((PartitionedRegion) r).isDataStore());

        return null;
      }
    };

    vm.invoke(createRegion);
  }

  @Override
  protected void verifyHDFSData(VM vm, String uniqueName) throws Exception {

    HashMap<String, HashMap<String, String>> filesToEntriesMap = createFilesAndEntriesMap(vm, uniqueName, uniqueName);
    HashMap<String, String> entriesMap = new HashMap<String, String>();
    for (HashMap<String, String> v : filesToEntriesMap.values()) {
      entriesMap.putAll(v);
    }
    verifyInEntriesMap(entriesMap, 1, 50, "vm0");
    verifyInEntriesMap(entriesMap, 40, 100, "vm1");
    verifyInEntriesMap(entriesMap, 40, 100, "vm2");
    verifyInEntriesMap(entriesMap, 90, 150, "vm3");

  }

  protected void verifyTwoHDFSFiles(VM vm, String uniqueName) throws Exception {

    HashMap<String, HashMap<String, String>> filesToEntriesMap = createFilesAndEntriesMap(vm, uniqueName, uniqueName);

    assertTrue("there should be exactly two files, but there are "
        + filesToEntriesMap.size(), filesToEntriesMap.size() == 2);
    long timestamp = Long.MAX_VALUE;
    String olderFile = null;
    for (Map.Entry<String, HashMap<String, String>> e : filesToEntriesMap
        .entrySet()) {
      String fileName = e.getKey().substring(
          0,
          e.getKey().length()
              - AbstractHoplogOrganizer.SEQ_HOPLOG_EXTENSION.length());
      long newTimeStamp = Long.parseLong(fileName.substring(
          fileName.indexOf("-") + 1, fileName.lastIndexOf("-")));
      if (newTimeStamp < timestamp) {
        olderFile = e.getKey();
        timestamp = newTimeStamp;
      }
    }
    verifyInEntriesMap(filesToEntriesMap.get(olderFile), 1, 8, "vm0");
    verifyInEntriesMap(filesToEntriesMap.get(olderFile), 4, 10, "vm1");
    filesToEntriesMap.remove(olderFile);
    verifyInEntriesMap(filesToEntriesMap.values().iterator().next(), 10, 18, "vm0");
    verifyInEntriesMap(filesToEntriesMap.values().iterator().next(), 14, 20, "vm1");
  }

  protected void verifyTwoHDFSFilesWithTwoEntries(VM vm, String uniqueName,
      String value) throws Exception {

    HashMap<String, HashMap<String, String>> filesToEntriesMap = createFilesAndEntriesMap(vm, uniqueName, uniqueName);
    
    assertTrue( "there should be exactly two files, but there are " + filesToEntriesMap.size(), filesToEntriesMap.size() == 2);
    HashMap<String, String> entriesMap =  new HashMap<String, String>();
    for (HashMap<String, String>  v : filesToEntriesMap.values()) {
      entriesMap.putAll(v);
    }
    assertTrue( "Expected key K1 received  " + entriesMap.get(value+ "1vm0"), entriesMap.get(value+ "1vm0").equals("K1"));
    assertTrue( "Expected key K2 received  " + entriesMap.get(value+ "2vm0"), entriesMap.get(value+ "2vm0").equals("K2"));
    assertTrue( "Expected key K2 received  " + entriesMap.get(value+ "2vm1"), entriesMap.get(value+ "2vm1").equals("K2"));
    assertTrue( "Expected key K3 received  " + entriesMap.get(value+ "3vm1"), entriesMap.get(value+ "3vm1").equals("K3"));
 }

  /**
   * verify that a PR accessor can be started
   */
  public void testPRAccessor() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore1 = host.getVM(1);
    VM datastore2 = host.getVM(2);
    VM accessor2 = host.getVM(3);
    final String regionName = getName();
    final String storeName = "store_" + regionName;

    SerializableCallable createRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        HDFSStoreFactory storefactory = getCache().createHDFSStoreFactory();
        homeDir = new File("../" + regionName).getCanonicalPath();
        storefactory.setHomeDir(homeDir);
        storefactory.create(storeName);
        AttributesFactory<Integer, String> af = new AttributesFactory<Integer, String>();
        af.setDataPolicy(DataPolicy.HDFS_PARTITION);
        af.setHDFSStoreName(storeName);
        Region r = getCache().createRegionFactory(af.create()).create(regionName);
        r.put("key1", "value1");
        return null;
      }
    };

    SerializableCallable createAccessorRegion = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        HDFSStoreFactory storefactory = getCache().createHDFSStoreFactory();
        homeDir = new File("../" + regionName).getCanonicalPath();
        storefactory.setHomeDir(homeDir);
        storefactory.create(storeName);
        // DataPolicy PARTITION with localMaxMemory 0 cannot be created
        AttributesFactory<Integer, String> af = new AttributesFactory<Integer, String>();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory<Integer, String> paf = new PartitionAttributesFactory<Integer, String>();
        paf.setLocalMaxMemory(0);
        af.setPartitionAttributes(paf.create());
        // DataPolicy PARTITION with localMaxMemory 0 can be created if hdfsStoreName is set
        af.setHDFSStoreName(storeName);
        // No need to check with different storeNames (can never be done in GemFireXD)
        Region r = getCache().createRegionFactory(af.create()).create(regionName);
        r.localDestroyRegion();
        // DataPolicy HDFS_PARTITION with localMaxMemory 0 can be created
        af = new AttributesFactory<Integer, String>();
        af.setDataPolicy(DataPolicy.HDFS_PARTITION);
        af.setPartitionAttributes(paf.create());
        getCache().createRegionFactory(af.create()).create(regionName);
        return null;
      }
    };

    datastore1.invoke(createRegion);
    accessor.invoke(createAccessorRegion);
    datastore2.invoke(createRegion);
    accessor2.invoke(createAccessorRegion);
  }

  /**
   * verify that PUT dml does not read from hdfs
   */
  public void testPUTDMLSupport() {
    doPUTDMLWork(false);
  }

  public void testPUTDMLBulkSupport() {
    doPUTDMLWork(true);
  }

  private void doPUTDMLWork(final boolean isPutAll) {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String regionName = getName();

    createServerRegion(vm1, 7, 1, 50, "./" + regionName, regionName, 1000);
    createServerRegion(vm2, 7, 1, 50, "./" + regionName, regionName, 1000);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        LocalRegion lr = (LocalRegion) r;
        SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
        long readsFromHDFS = stats.getRead().getCount();
        assertEquals(0, readsFromHDFS);
        if (isPutAll) {
          Map m = new HashMap();
          // map with only one entry
          m.put("key0", "value0");
          DistributedPutAllOperation ev = lr.newPutAllOperation(m, null);
          lr.basicPutAll(m, ev, null);
          m.clear();
          // map with multiple entries
          for (int i = 1; i < 100; i++) {
            m.put("key" + i, "value" + i);
          }
          ev = lr.newPutAllOperation(m, null);
          lr.basicPutAll(m, ev, null);
        } else {
          for (int i = 0; i < 100; i++) {
            r.put("key" + i, "value" + i);
          }
        }
        return null;
      }
    });

    SerializableCallable getHDFSReadCount = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
        return stats.getRead().getCount();
      }
    };

    long vm1Count = (Long) vm1.invoke(getHDFSReadCount);
    long vm2Count = (Long) vm2.invoke(getHDFSReadCount);
    assertEquals(100, vm1Count + vm2Count);

    pause(10 * 1000);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        // do puts using the new api
        LocalRegion lr = (LocalRegion) getCache().getRegion(regionName);
        if (isPutAll) {
          Map m = new HashMap();
          // map with only one entry
          m.put("key0", "value0");
          DistributedPutAllOperation ev = lr.newPutAllForPUTDmlOperation(m, null);
          lr.basicPutAll(m, ev, null);
          m.clear();
          // map with multiple entries
          for (int i = 1; i < 200; i++) {
            m.put("key" + i, "value" + i);
          }
          ev = lr.newPutAllForPUTDmlOperation(m, null);
          lr.basicPutAll(m, ev, null);
        } else {
          for (int i = 0; i < 200; i++) {
            EntryEventImpl ev = lr.newPutEntryEvent("key" + i, "value" + i, null);
            lr.validatedPut(ev, System.currentTimeMillis());
          }
        }
        return null;
      }
    });

    // verify the stat for hdfs reads has not incremented
    vm1Count = (Long) vm1.invoke(getHDFSReadCount);
    vm2Count = (Long) vm2.invoke(getHDFSReadCount);
    assertEquals(100, vm1Count + vm2Count);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        for (int i = 0; i < 200; i++) {
          assertEquals("value" + i, r.get("key" + i));
        }
        return null;
      }
    });
  }

  /**
   * verify that get on operational data does not read from HDFS
   */
  public void testGetOperationalData() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    final String regionName = getName();

    createServerRegion(vm1, 7, 1, 50, "./"+regionName, regionName, 1000);
    createServerRegion(vm2, 7, 1, 50, "./"+regionName, regionName, 1000);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
        long readsFromHDFS = stats.getRead().getCount();
        assertEquals(0, readsFromHDFS);
        for (int i = 0; i < 100; i++) {
          logger.info("SWAP:DOING PUT:key{}", i);
          r.put("key" + i, "value" + i);
        }
        return null;
      }
    });

    SerializableCallable getHDFSReadCount = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
        return stats.getRead().getCount();
      }
    };

    long vm1Count = (Long) vm1.invoke(getHDFSReadCount);
    long vm2Count = (Long) vm2.invoke(getHDFSReadCount);
    assertEquals(100, vm1Count + vm2Count);

    pause(10 * 1000);

    // verify that get increments the read stat
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        for (int i = 0; i < 200; i++) {
          if (i < 100) {
            logger.info("SWAP:DOING GET:key", i);
            assertEquals("value" + i, r.get("key" + i));
          } else {
            assertNull(r.get("key" + i));
          }
        }
        return null;
      }
    });

    vm1Count = (Long) vm1.invoke(getHDFSReadCount);
    vm2Count = (Long) vm2.invoke(getHDFSReadCount);
    // initial 100 + 150 for get (since 50 are in memory)
    assertEquals(250, vm1Count + vm2Count);

    // do gets with readFromHDFS set to false
    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        LocalRegion lr = (LocalRegion) r;
        int numEntries = 0;
        for (int i = 0; i < 200; i++) {
          logger.info("SWAP:DOING GET NO READ:key", i);
          Object val = lr.get("key"+i, null, true, false, false, null,  null, false, false/*allowReadFromHDFS*/);
          if (val != null) {
            numEntries++;
          }
        }
        assertEquals(50, numEntries); // entries in memory
        return null;
      }
    });

    vm1Count = (Long) vm1.invoke(getHDFSReadCount);
    vm2Count = (Long) vm2.invoke(getHDFSReadCount);
    // get should not have incremented
    assertEquals(250, vm1Count + vm2Count);

    /**MergeGemXDHDFSToGFE Have not merged this API as this api is not called by any code*/
    /*
    // do gets using DataView
    SerializableCallable getUsingDataView = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        LocalRegion lr = (LocalRegion) r;
        PartitionedRegion pr = (PartitionedRegion) lr;
        long numEntries = 0;
        for (int i=0; i<200; i++) {
          InternalDataView idv = lr.getDataView();
          logger.debug("SWAP:DATAVIEW");
          Object val = idv.getLocally("key"+i, null, PartitionedRegionHelper.getHashKey(pr, "key"+i), lr, true, true, null, null, false, false);
          if (val != null) {
            numEntries++;
          }
        }
        return numEntries;
      }
    };

    vm1Count = (Long) vm1.invoke(getUsingDataView);
    vm2Count = (Long) vm2.invoke(getUsingDataView);
    assertEquals(50 * 2, vm1Count + vm2Count);// both VMs will find 50 entries*/

    vm1Count = (Long) vm1.invoke(getHDFSReadCount);
    vm2Count = (Long) vm2.invoke(getHDFSReadCount);
    // get should not have incremented
    assertEquals(250, vm1Count + vm2Count);

  }

  public void testSizeEstimate() {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    final String regionName = getName();

    createServerRegion(vm1, 7, 1, 50, "./"+regionName, regionName, 1000);
    createServerRegion(vm2, 7, 1, 50, "./"+regionName, regionName, 1000);
    createServerRegion(vm3, 7, 1, 50, "./"+regionName, regionName, 1000);

    final int size = 226;

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        // LocalRegion lr = (LocalRegion) r;
        for (int i = 0; i < size; i++) {
          r.put("key" + i, "value" + i);
        }
        // before flush
        // assertEquals(size, lr.sizeEstimate());
        return null;
      }
    });

    pause(10 * 1000);

    vm2.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        LocalRegion lr = (LocalRegion) r;
        logger.debug("SWAP:callingsizeEstimate");
        long estimate = lr.sizeEstimate();
        double err = Math.abs(estimate - size) / (double) size;
        System.out.println("SWAP:estimate:" + estimate);
        assertTrue(err < 0.2);
        return null;
      }
    });
  }

  public void testForceAsyncMajorCompaction() throws Exception {
    doForceCompactionTest(true, false);
  }

  public void testForceSyncMajorCompaction() throws Exception {
    // more changes
    doForceCompactionTest(true, true);
  }

  private void doForceCompactionTest(final boolean isMajor, final boolean isSynchronous) throws Exception {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    final String regionName = getName();

    createServerRegion(vm1, 7, 1, 50, "./" + regionName, regionName, 1000);
    createServerRegion(vm2, 7, 1, 50, "./" + regionName, regionName, 1000);
    createServerRegion(vm3, 7, 1, 50, "./" + regionName, regionName, 1000);

    SerializableCallable noCompaction = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
        if (isMajor) {
          assertEquals(0, stats.getMajorCompaction().getCount());
        } else {
          assertEquals(0, stats.getMinorCompaction().getCount());
        }
        return null;
      }
    };

    vm1.invoke(noCompaction);
    vm2.invoke(noCompaction);
    vm3.invoke(noCompaction);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region r = getCache().getRegion(regionName);
        for (int i = 0; i < 500; i++) {
          r.put("key" + i, "value" + i);
          if (i % 100 == 0) {
            // wait for flush
            pause(3000);
          }
        }
        pause(3000);
        PartitionedRegion pr = (PartitionedRegion) r;
        long lastCompactionTS = pr.lastMajorHDFSCompaction();
        assertEquals(0, lastCompactionTS);
        long beforeCompact = System.currentTimeMillis();
        pr.forceHDFSCompaction(true, isSynchronous ? 0 : 1);
        if (isSynchronous) {
          final SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
          assertTrue(stats.getMajorCompaction().getCount() > 0);
          assertTrue(pr.lastMajorHDFSCompaction() >= beforeCompact);
        }
        return null;
      }
    });

    if (!isSynchronous) {
      SerializableCallable verifyCompactionStat = new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          final SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
          waitForCriterion(new WaitCriterion() {
            @Override
            public boolean done() {
              return stats.getMajorCompaction().getCount() > 0;
            }

            @Override
            public String description() {
              return "Major compaction stat not > 0";
            }
          }, 30 * 1000, 1000, true);
          return null;
        }
      };

      vm1.invoke(verifyCompactionStat);
      vm2.invoke(verifyCompactionStat);
      vm3.invoke(verifyCompactionStat);
    } else {
      SerializableCallable verifyCompactionStat = new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          final SortedOplogStatistics stats = HDFSRegionDirector.getInstance().getHdfsRegionStats("/" + regionName);
          assertTrue(stats.getMajorCompaction().getCount() > 0);
          return null;
        }
      };
      vm2.invoke(verifyCompactionStat);
      vm3.invoke(verifyCompactionStat);
    }
  }

  public void testFlushQueue() throws Exception {
    doFlushQueue(false);
  }

  public void testFlushQueueWO() throws Exception {
    doFlushQueue(true);
  }

  private void doFlushQueue(boolean wo) throws Exception {
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(0);
    VM vm2 = host.getVM(1);
    VM vm3 = host.getVM(2);
    final String regionName = getName();

    createServerRegion(vm1, 7, 1, 50, "./"+regionName, regionName, 300000, wo, false);
    createServerRegion(vm2, 7, 1, 50, "./"+regionName, regionName, 300000, wo, false);
    createServerRegion(vm3, 7, 1, 50, "./"+regionName, regionName, 300000, wo, false);

    vm1.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(regionName);
        for (int i = 0; i < 500; i++) {
          pr.put("key" + i, "value" + i);
        }

        pr.flushHDFSQueue(0);
        return null;
      }
    });

    SerializableCallable verify = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(regionName);
        assertEquals(0, pr.getHDFSEventQueueStats().getEventQueueSize());
        return null;
      }
    };

    vm1.invoke(verify);
    vm2.invoke(verify);
    vm3.invoke(verify);
  }
}
