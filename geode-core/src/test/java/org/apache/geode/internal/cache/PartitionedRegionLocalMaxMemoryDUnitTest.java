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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This class is to test LOCAL_MAX_MEMORY property of partition region while creation of bucket.
 */
@Category(DistributedTest.class)
public class PartitionedRegionLocalMaxMemoryDUnitTest extends CacheTestCase {

  /**
   * Tear down a PartitionedRegionTestCase by cleaning up the existing cache (mainly because we want
   * to destroy any existing PartitionedRegions)
   */
  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    preTearDownPartitionedRegionDUnitTest();
    closeCache();
    Invoke.invokeInEveryVM(org.apache.geode.cache30.CacheTestCase.class, "closeCache");
  }

  protected void preTearDownPartitionedRegionDUnitTest() throws Exception {}

  @BeforeClass
  public static void caseSetUp() {
    DUnitLauncher.launchIfNeeded();
    // this makes sure we don't have any connection left over from previous tests
    disconnectAllFromDS();
  }

  @AfterClass
  public static void caseTearDown() {
    // this makes sure we don't leave anything for the next tests
    disconnectAllFromDS();
  }

  private static final int LOCAL_MAX_MEMORY = 1;
  private static int MAX_REGIONS = 1;
  private static final int REDUNDANCY = 1;

  private String regionNamePrefix = null;
  private List<VM> vms = new ArrayList<>(2);

  @Before
  public void setup() {
    Host host = Host.getHost(0);
    vms.add(host.getVM(0));
    vms.add(host.getVM(1));
  }

  @After
  public void cleanup() {
    destroyRegion(vms.get(0));
  }

  /**
   * This test performs following operations <br>
   * 1.Create Partition region with LOCAL_MAX_MEMORY = 1MB on all the VMs </br>
   * <br>
   * 2.Put objects in partition region so that only one bucket gets created and size of that bucket
   * exceeds LOCAL_MAX_MEMORY <br>
   * 3.Put object such that new bucket gets formed</br>
   * <br>
   * 4.Test should create a new bucket</br>
   */
  @Test
  public void testLocalMaxMemoryInPartitionedRegion() {
    regionNamePrefix = "maxMemoryTest";
    createPartitionRegionOnAllVMs(false);
    VM vm = vms.get(0);
    putFromOneVm(vm, 10, true);
    putFromOneVm(vm, 21, false);
  }

  /**
   * This test makes sure that we don't enforce the LOCAL_MAX_MEMORY setting when eviction is
   * enabled.
   */
  @Test
  public void testLocalMaxMemoryInPartitionedRegionWithEviction() {
    regionNamePrefix = "maxMemoryWithEvictionTest";
    createPartitionRegionOnAllVMs(true);
    VM vm = vms.get(0);
    putFromOneVm(vm, 10, true);
    putFromOneVm(vm, 10, true);
  }

  private void putFromOneVm(VM vm, int objectId, boolean fillMemory) {
    if (fillMemory) {
      vm.invoke(fillRegion(objectId));
    } else {
      vm.invoke(putObjectInPartitionRegion(objectId));
    }
  }

  private CacheSerializableRunnable putObjectInPartitionRegion(int objectId) {
    CacheSerializableRunnable putObject = new CacheSerializableRunnable("putObject") {
      public void run2() {
        PartitionedRegion pr = getRegion();
        TestObject1 kv = new TestObject1("testObject1" + 0, objectId);
        pr.put(kv, kv);
        LogWriterUtils.getLogWriter().info(
            "putObjectInPartitionRegion() - Put operation with different identifier done successfully");
      }
    };
    return putObject;
  }

  private CacheSerializableRunnable fillRegion(int objectId) {
    CacheSerializableRunnable putObject = new CacheSerializableRunnable("putObject") {
      public void run2() {
        Cache cache = getCache();
        PartitionedRegion pr = getRegion();

        fillAllMemoryWithPuts(cache, pr, objectId);
        assertEquals(1, pr.getDataStore().localBucket2RegionMap.size());
        LogWriterUtils.getLogWriter()
            .info("putObjectInPartitionRegion() - Put operation done successfully");
      }
    };
    return putObject;
  }

  private PartitionedRegion getRegion() {
    Cache cache = getCache();
    return (PartitionedRegion) cache.getRegion(Region.SEPARATOR + regionNamePrefix + "0");
  }

  private void fillAllMemoryWithPuts(Cache cache, PartitionedRegion pr, int objectId) {
    int i = 0;
    long allocatedMemory;
    while ((allocatedMemory =
        pr.getDataStore().currentAllocatedMemory()) < PartitionedRegionHelper.BYTES_PER_MB) {
      cache.getLogger().info("size: " + allocatedMemory);
      Object obj = new TestObject1("testObject1" + i, objectId);
      pr.put(obj, obj);
      i++;
    }
    assertEquals(1, pr.getDataStore().localBucket2RegionMap.size());
  }

  private void createPartitionRegionOnAllVMs(boolean evict) {
    for (VM vm : vms) {
      vm.invoke(createMultiplePartitionRegion(regionNamePrefix, 0, MAX_REGIONS, REDUNDANCY,
          LOCAL_MAX_MEMORY, evict));
    }
  }

  private void destroyRegion(VM vm) {
    SerializableRunnable destroyObj = new CacheSerializableRunnable("destroyObj") {
      public void run2() {
        PartitionedRegion pr = getRegion();
        assertNotNull(pr);
        pr.destroyRegion();
      }
    };
    vm.invoke(destroyObj);
  }

  /**
   * This function creates multiple partition regions in a VM. The name of the Partition Region will
   * be PRPrefix+index (index starts from startIndexForRegion and ends to endIndexForRegion)
   *
   * @param PRPrefix : Used in the name of the Partition Region
   *
   *        These indices Represents range of the Partition Region
   */
  CacheSerializableRunnable createMultiplePartitionRegion(final String PRPrefix,
      final int startIndexForRegion, final int endIndexForRegion, final int redundancy,
      final int localmaxMemory, final boolean evict) {
    return new CacheSerializableRunnable("createPrRegions_" + PRPrefix) {
      String innerPRPrefix = PRPrefix;

      int innerStartIndexForRegion = startIndexForRegion;

      int innerEndIndexForRegion = endIndexForRegion;

      int innerRedundancy = redundancy;

      int innerlocalmaxMemory = localmaxMemory;

      public void run2() throws CacheException {
        System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, "20000");
        EvictionAttributes evictionAttrs = evict ? EvictionAttributes
            .createLRUEntryAttributes(Integer.MAX_VALUE, EvictionAction.LOCAL_DESTROY) : null;
        for (int i = startIndexForRegion; i < endIndexForRegion; i++) {
          Region partitionedregion = getCache().createRegion(innerPRPrefix + i,
              createRegionAttrsForPR(innerRedundancy, innerlocalmaxMemory,
                  PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT, evictionAttrs));
          getCache().getLogger()
              .info("Successfully created PartitionedRegion = " + partitionedregion);
        }
        System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY,
            Integer.toString(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
        getCache().getLogger()
            .info("createMultiplePartitionRegion() - Partition Regions Successfully Completed ");
      }
    };
  }

  protected RegionAttributes<?, ?> createRegionAttrsForPR(int red, int localMaxMem,
      long recoveryDelay, EvictionAttributes evictionAttrs) {
    return PartitionedRegionTestHelper.createRegionAttrsForPR(red, localMaxMem, recoveryDelay,
        evictionAttrs, null);
  }

  /**
   * Object used for the put() operation as key and object. The objectIdentifier is used to provide
   * a predetermined hashcode for the object.
   */
  public static class TestObject1 implements DataSerializable, Sizeable {
    String name;

    byte arr[] = new byte[1024 * 4];

    int identifier;

    public TestObject1() {}

    public TestObject1(String objectName, int objectIndentifier) {
      this.name = objectName;
      Arrays.fill(this.arr, (byte) 'A');
      this.identifier = objectIndentifier;
    }

    public int hashCode() {
      return this.identifier;
    }

    public boolean equals(TestObject1 obj) {
      return (this.name.equals(obj.name) && Arrays.equals(this.arr, obj.arr));
    }

    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeByteArray(this.arr, out);
      DataSerializer.writeString(this.name, out);
      out.writeInt(this.identifier);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.arr = DataSerializer.readByteArray(in);
      this.name = DataSerializer.readString(in);
      this.identifier = in.readInt();
    }

    public int getSizeInBytes() {
      return ObjectSizer.DEFAULT.sizeof(arr) + ObjectSizer.DEFAULT.sizeof(name)
          + ObjectSizer.DEFAULT.sizeof(identifier) + Sizeable.PER_OBJECT_OVERHEAD * 3;
    }
  }
}
