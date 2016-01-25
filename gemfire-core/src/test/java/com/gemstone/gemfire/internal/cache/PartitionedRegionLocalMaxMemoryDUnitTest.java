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

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This class is to test localMaxMemory property of partition region while
 * creation of bucket.
 * 
 * @author gthombar, modified by Tushar (for bug#35713)
 */
public class PartitionedRegionLocalMaxMemoryDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  /** Prefix is used in name of Partition Region */
  private static String prPrefix = null;

  /** Maximum number of regions * */
  static int MAX_REGIONS = 1;

  /** local maxmemory used for the creation of the partition region */
  int localMaxMemory = 1;

  /** to store references of 4 vms */
  VM vm[] = new VM[4];

  public PartitionedRegionLocalMaxMemoryDUnitTest(String name) {
    super(name);
  }

  /**
   * This test performs following operations 
   * <br>1.Create Partition region with localMaxMemory 
   * = 1MB on all the VMs </br>
   * <br>2.Put objects in partition region so that only one bucket gets created
   * and size of that bucket exceeds localMaxMemory 
   * <br>3.Put object such that new bucket gets formed</br> 
   * <br>4.Test should throw PartitionedRegionStorageException
   * when it tries to create new bucket</br>
   */
  public void testLocalMaxMemoryInPartitionedRegion()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    this.vm[0] = host.getVM(0);
    this.vm[1] = host.getVM(1);
    this.vm[2] = null;
    this.vm[3] = null;
    /** Prefix will be used for naming the partititon Region */
    prPrefix = "testLocalMaxMemoryInPartitionedRegion";
    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 2;
    // creating partition region on 4 nodes with 
    // localMaxMemory=1MB redundancy = 3
    List vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 1;
    final int redundancy = 1;
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, 
        "20000");
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion, localMaxMemory, redundancy, false);
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, 
        Integer.toString(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
    putFromOneVm(vm[0], true);
    putFromOneVm(vm[0], false);
    destroyRegion(vm[0]);
  }
  
  /**
   * This test makes sure that we don't enforce the localMaxMemory setting
   * when eviction is enabled.
   */
  public void testLocalMaxMemoryInPartitionedRegionWithEviction()
  {
    Host host = Host.getHost(0);
    /** creating 4 VMs */
    this.vm[0] = host.getVM(0);
    this.vm[1] = host.getVM(1);
    this.vm[2] = null;
    this.vm[3] = null;
    /** Prefix will be used for naming the partititon Region */
    prPrefix = "testLocalMaxMemoryInPartitionedRegion";
    /** these indices represents range of partition regions present in each VM */
    int startIndexForRegion = 0;
    int endIndexForRegion = MAX_REGIONS;
    int startIndexForNode = 0;
    int endIndexForNode = 2;
    // creating partition region on 4 nodes with 
    // localMaxMemory=1MB redundancy = 3
    List vmList = addNodeToList(startIndexForNode, endIndexForNode);
    localMaxMemory = 1;
    final int redundancy = 1;
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, 
        "20000");
    createPartitionRegion(vmList, startIndexForRegion, endIndexForRegion, localMaxMemory, redundancy, true);
    System.setProperty(PartitionedRegion.RETRY_TIMEOUT_PROPERTY, 
        Integer.toString(PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION));
    putFromOneVm(vm[0], true);
    putFromOneVm(vm[0], true);
    destroyRegion(vm[0]);
  }

  /**
   * function is used perform put() operation from one VM
   * 
   * @param vm
   * @param objectFlg
   */
  private void putFromOneVm(VM vm, boolean objectFlg)
  {
    vm.invoke(putObjectInPartitionRegion(objectFlg));
  }

  /**
   * This function is used to put objects of different hashcode depending upon
   * value of objectFlag
   * 
   * @param objectFlg
   * @return
   */
  private CacheSerializableRunnable putObjectInPartitionRegion(final boolean objectFlg)
  {

    CacheSerializableRunnable putObject = new CacheSerializableRunnable("putObject") {
      public void run2()
      {
        Cache cache = getCache();
        PartitionedRegion pr = (PartitionedRegion)cache
            .getRegion(Region.SEPARATOR
                + "testLocalMaxMemoryInPartitionedRegion0");
        assertNotNull("Name of region : " + pr.getName(), pr);
        int i = 0;
        
        if (objectFlg == true) {
          long size = 0;
          while ((size = pr.getDataStore().currentAllocatedMemory()) < PartitionedRegionHelper.BYTES_PER_MB) {
            cache.getLogger().info("size: " + size);
            Object obj = new TestObject1("testObject1" + i, 10);
            pr.put(obj, obj);
            i++;
          }
          assertEquals(1, pr.getDataStore().localBucket2RegionMap.size());
          getLogWriter().info(
          "putObjectInPartitionRegion() - Put operation done successfully");
        }
        else {
          final String expectedExceptions = PartitionedRegionStorageException.class.getName(); 
          getCache().getLogger().info("<ExpectedException action=add>" + 
              expectedExceptions + "</ExpectedException>");	
          try {
            TestObject1 kv = new TestObject1("testObject1" + i, 21);
            pr.put(kv, kv);
            fail("Bucket gets created even if no memory is available");
          }
          catch (PartitionedRegionStorageException e) {
            getLogWriter()
            .info(
            "putObjectInPartitionRegion()- got correct PartitionedRegionStorageException while creating bucket when no memory is available");
          }
          getCache().getLogger().info("<ExpectedException action=remove>" + 
              expectedExceptions + "</ExpectedException>");
        }
      }
    };
    return putObject;
  }

  /**
   * This function createas multiple partition regions on nodes specified in the
   * vmList
   * @param evict 
   */
  private void createPartitionRegion(List vmList, int startIndexForRegion,
      int endIndexForRegion, int localMaxMemory, int redundancy, boolean evict)
  {
    Iterator nodeIterator = vmList.iterator();
    while (nodeIterator.hasNext()) {
      VM vm = (VM)nodeIterator.next();
      vm.invoke(createMultiplePartitionRegion(
          prPrefix, startIndexForRegion, endIndexForRegion, redundancy, localMaxMemory, evict));
    }
  }

  /**
   * This function adds nodes to node list
   * @param startIndexForNode
   * @param endIndexForNode
   * @return
   */
  private List addNodeToList(int startIndexForNode, int endIndexForNode)
  {
    List localvmList = new ArrayList();
    for (int i = startIndexForNode; i < endIndexForNode; i++) {
      localvmList.add(vm[i]);
    }
    return localvmList;
  }

  /**
   * this function creates vms in given host
   * @param host
   */
  private void createVMs(Host host)
  {
    for (int i = 0; i < 4; i++) {
      vm[i] = host.getVM(i);
    }
  }
 
  private void destroyRegion (VM vm )
  {
    SerializableRunnable destroyObj = new CacheSerializableRunnable("destroyObj")
    {
      public void run2() 
      {
        Cache cache = getCache();
        PartitionedRegion pr = (PartitionedRegion)cache
        .getRegion(Region.SEPARATOR
            + "testLocalMaxMemoryInPartitionedRegion0");
        assertNotNull("Name of region : " + pr.getName(), pr);
        pr.destroyRegion();
      }
    };
    vm.invoke(destroyObj);
  }
  
  /** 
   * Object used for the put() operation as key and object
   * @author gthombar
   */
  static public class TestObject1 implements DataSerializable, Sizeable
  {
    String name;

    byte arr[] = new byte[1024 * 4];

    int identifier;
    
    public TestObject1() {}

    public TestObject1(String objectName, int objectIndentifier) {
      this.name = objectName;
      Arrays.fill(this.arr, (byte)'A');
      this.identifier = objectIndentifier;
    }

    public int hashCode()
    {
      return this.identifier;
    }

    public boolean equals(TestObject1 obj)
    {
      return (this.name.equals(obj.name) 
            && Arrays.equals(this.arr, obj.arr));
    }

    public void toData(DataOutput out) throws IOException
    {
      DataSerializer.writeByteArray(this.arr, out);
      DataSerializer.writeString(this.name, out);
      out.writeInt(this.identifier);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException
    {
      this.arr = DataSerializer.readByteArray(in);
      this.name = DataSerializer.readString(in);
      this.identifier = in.readInt();
    }

    public int getSizeInBytes()
    {
      return ObjectSizer.DEFAULT.sizeof(arr) 
          + ObjectSizer.DEFAULT.sizeof(name) 
          + ObjectSizer.DEFAULT.sizeof(identifier) 
          + Sizeable.PER_OBJECT_OVERHEAD * 3;
    }
  }
}
