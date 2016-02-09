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
package com.gemstone.gemfire.internal.cache.execute;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import com.gemstone.gemfire.internal.cache.execute.data.CustId;
import com.gemstone.gemfire.internal.cache.execute.data.Customer;
import com.gemstone.gemfire.internal.cache.execute.data.Order;
import com.gemstone.gemfire.internal.cache.execute.data.OrderId;
import com.gemstone.gemfire.internal.cache.execute.data.Shipment;
import com.gemstone.gemfire.internal.cache.execute.data.ShipmentId;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
/**
 * This is the test for the custom and colocated partitioning of
 * PartitionedRegion
 * 
 * @author Kishor
 */
@SuppressWarnings("synthetic-access")
public class PRColocationDUnitTest extends CacheTestCase {

  VM accessor = null;

  VM dataStore1 = null;

  VM dataStore2 = null;

  VM dataStore3 = null;

  protected static Cache cache = null;

  protected static int totalNumBucketsInTest = 0;
  
  protected static int defaultStringSize = 0;

  final static String CustomerPartitionedRegionName = "CustomerPartitionedRegion";

  final static String OrderPartitionedRegionName = "OrderPartitionedRegion";

  final static String ShipmentPartitionedRegionName = "ShipmentPartitionedRegion";

  String regionName = null;

  Integer redundancy = null;

  Integer localMaxmemory = null;

  Integer totalNumBuckets = null;

  String colocatedWith = null;
  
  Boolean isPartitionResolver = null;

  Object[] attributeObjects = null;

  public PRColocationDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();  // isolate this test from others to avoid periodic CacheExistsExceptions
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    accessor = host.getVM(3);
  }

   /*
   * Test for bug 41820 
   */
  public void testDestroyColocatedPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    
    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = "/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "C";
    colocatedWith = "/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    attributeObjects = new Object[] { "/A" };
    dataStore1.invoke(PRColocationDUnitTest.class, "destroyPR", attributeObjects);
  }

  /*
   * Test for checking the colocation of the regions which forms the tree
   */
  public void testColocatedPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    
    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = "/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "C";
    colocatedWith = "/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = "/B";
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver  };
    createPartitionedRegion(attributeObjects);
    
    regionName = "E";
    colocatedWith = "/B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "F";
    colocatedWith = "/B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "G";
    colocatedWith = "/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "H";
    colocatedWith = "/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "I";
    colocatedWith = "/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "J";
    colocatedWith = "/D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "K";
    colocatedWith = "/D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "L";
    colocatedWith = "/E";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "M";
    colocatedWith = "/F";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "N";
    colocatedWith = "/G";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "O";
    colocatedWith = "/I";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "A" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "D" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "H" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "B" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "K" });
  }
  /*
   * Test for checking the colocation of the regions which forms the tree
   */
  public void testColocatedPartitionedRegion_NoFullPath() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    
    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = "A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "C";
    colocatedWith = "A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = "B";
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver  };
    createPartitionedRegion(attributeObjects);
    
    regionName = "E";
    colocatedWith = "B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "F";
    colocatedWith = "B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "G";
    colocatedWith = "C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "H";
    colocatedWith = "C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "I";
    colocatedWith = "C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "J";
    colocatedWith = "D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "K";
    colocatedWith = "D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "L";
    colocatedWith = "E";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "M";
    colocatedWith = "F";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "N";
    colocatedWith = "G";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    regionName = "O";
    colocatedWith = "I";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "A" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "D" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "H" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "B" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "K" });
  }
  public void testColocatedSubPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(1);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    
    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = "/rootA/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "C";
    colocatedWith = "/rootA/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = "/rootB/B";
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver  };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "E";
    colocatedWith = "/rootB/B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "F";
    colocatedWith = "/rootB/B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "G";
    colocatedWith = "/rootC/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "H";
    colocatedWith = "/rootC/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "I";
    colocatedWith = "/rootC/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "J";
    colocatedWith = "/rootD/D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "K";
    colocatedWith = "/rootD/D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "L";
    colocatedWith = "/rootE/E";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "M";
    colocatedWith = "/rootF/F";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "N";
    colocatedWith = "/rootG/G";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "O";
    colocatedWith = "/rootI/I";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootA/A" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootD/D" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootH/H" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootB/B" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootK/K" });
  }
  
  public void testColocatedSubPartitionedRegion_NoFullPath() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(1);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    
    regionName = "A";
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);

    regionName = "B";
    colocatedWith = "rootA/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "C";
    colocatedWith = "rootA/A";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);

    regionName = "D";
    colocatedWith = "rootB/B";
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver  };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "E";
    colocatedWith = "rootB/B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "F";
    colocatedWith = "rootB/B";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "G";
    colocatedWith = "rootC/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "H";
    colocatedWith = "rootC/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "I";
    colocatedWith = "rootC/C";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "J";
    colocatedWith = "rootD/D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "K";
    colocatedWith = "rootD/D";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "L";
    colocatedWith = "rootE/E";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "M";
    colocatedWith = "rootF/F";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "N";
    colocatedWith = "rootG/G";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    regionName = "O";
    colocatedWith = "rootI/I";
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createSubPartitionedRegion(attributeObjects);
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootA/A" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootD/D" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootH/H" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootB/B" });
    
    accessor.invoke(PRColocationDUnitTest.class,
        "validateColocatedRegions",
        new Object[] { "rootK/K" });
  }
  
  public void testColocatedPRWithAccessorOnDifferentNode1() throws Throwable {

    createCacheInAllVms();

    dataStore1.invoke(new CacheSerializableRunnable("testColocatedPRwithAccessorOnDifferentNode") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = new Boolean(false);
        redundancy = new Integer(0);
        localMaxmemory = new Integer(50);
        totalNumBuckets = new Integer(11);
        
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info(
            "Partitioned Region " + partitionedRegionName
                + " created Successfully :" + pr.toString());
      }
    });

    // add expected exception string
    final IgnoredException ex = IgnoredException.addIgnoredException(
        "Colocated regions should have accessors at the same node", dataStore1);
    dataStore1.invoke(new CacheSerializableRunnable(
        "Colocated PR with Accessor on different nodes") {
      @Override
      public void run2() {
        regionName = OrderPartitionedRegionName;
        colocatedWith = CustomerPartitionedRegionName;
        isPartitionResolver = new Boolean(false);
        localMaxmemory = new Integer(0);
        redundancy = new Integer(0);
        totalNumBuckets = new Integer(11);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        try {
          basicGetCache().createRegion(regionName, attr.create());
          fail("It should have failed with Exception: Colocated regions "
              + "should have accessors at the same node");
        }
        catch (Exception Expected) {
          Expected.printStackTrace();
          LogWriterUtils.getLogWriter().info("Expected Message : " + Expected.getMessage());
          assertTrue(Expected.getMessage().startsWith(
              "Colocated regions should have accessors at the same node"));
        }
      }
    });
    ex.remove();
  }

  public void testColocatedPRWithAccessorOnDifferentNode2() throws Throwable {

    createCacheInAllVms();

    dataStore1.invoke(new CacheSerializableRunnable("testColocatedPRWithAccessorOnDifferentNode2") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = new Boolean(false);
        redundancy = new Integer(0);
        localMaxmemory = new Integer(0);
        totalNumBuckets = new Integer(11);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info(
            "Partitioned Region " + partitionedRegionName
                + " created Successfully :" + pr.toString());
      }
    });

    // add expected exception string
    final IgnoredException ex = IgnoredException.addIgnoredException(
        "Colocated regions should have accessors at the same node", dataStore1);
    dataStore1.invoke(new CacheSerializableRunnable(
        "Colocated PR with accessor on different nodes") {
      @Override
      public void run2() {
        regionName = OrderPartitionedRegionName;
        colocatedWith = CustomerPartitionedRegionName;
        isPartitionResolver = new Boolean(false);
        redundancy = new Integer(0);
        localMaxmemory = new Integer(50);
        totalNumBuckets = new Integer(11);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        try {
          basicGetCache().createRegion(regionName, attr.create());
          fail("It should have failed with Exception: Colocated regions "
              + "should have accessors at the same node");
        }
        catch (Exception Expected) {
          LogWriterUtils.getLogWriter().info("Expected Message : " + Expected.getMessage());
          assertTrue(Expected.getMessage().startsWith(
              "Colocated regions should have accessors at the same node"));
        }
      }
    });
    ex.remove();
  }
  
  public void testColocatedPRWithPROnDifferentNode1() throws Throwable {

    createCacheInAllVms();

    dataStore1.invoke(new CacheSerializableRunnable("TestColocatedPRWithPROnDifferentNode") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = new Boolean(false);
        redundancy = new Integer(0);
        localMaxmemory = new Integer(20);
        totalNumBuckets = new Integer(11);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info(
            "Partitioned Region " + partitionedRegionName
                + " created Successfully :" + pr.toString());
      }
    });

    dataStore2.invoke(new CacheSerializableRunnable("testColocatedPRwithPROnDifferentNode") {
      @Override
      public void run2() {
        String partitionedRegionName = CustomerPartitionedRegionName;
        colocatedWith = null;
        isPartitionResolver = new Boolean(false);
        redundancy = new Integer(0);
        localMaxmemory = new Integer(20);
        totalNumBuckets = new Integer(11);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
        assertNotNull(pr);
        LogWriterUtils.getLogWriter().info(
            "Partitioned Region " + partitionedRegionName
                + " created Successfully :" + pr.toString());
      }
    });

    // add expected exception string
    final IgnoredException ex = IgnoredException.addIgnoredException("Cannot create buckets",
        dataStore2);
    dataStore2.invoke(new CacheSerializableRunnable(
        "Colocated PR with PR on different node") {
      @Override
      public void run2() {
        regionName = OrderPartitionedRegionName;
        colocatedWith = CustomerPartitionedRegionName;
        isPartitionResolver = new Boolean(false);
        redundancy = new Integer(0);
        localMaxmemory = new Integer(50);
        totalNumBuckets = new Integer(11);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        try {
          Region r = basicGetCache().createRegion(regionName, attr.create());
          // fail("It should have failed with Exception : Colocated regions
          // should have accessors at the same node");
          r.put("key", "value");
          fail("Failed because we did not receive the exception - : Cannot create buckets, "
              + "as colocated regions are not configured to be at the same nodes.");
        }
        catch (Exception Expected) {
          LogWriterUtils.getLogWriter().info("Expected Message : " + Expected.getMessage());
          assertTrue(Expected.getMessage().contains("Cannot create buckets, as "
              + "colocated regions are not configured to be at the same nodes."));
        }
      }
    });
    ex.remove();

    dataStore1.invoke(new CacheSerializableRunnable(
        "Colocated PR with PR on different node") {
      @Override
      public void run2() {
        regionName = OrderPartitionedRegionName;
        colocatedWith = CustomerPartitionedRegionName;
        isPartitionResolver = new Boolean(false);
        redundancy = new Integer(0);
        localMaxmemory = new Integer(50);
        totalNumBuckets = new Integer(11);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(redundancy.intValue()).setLocalMaxMemory(
            localMaxmemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith(colocatedWith);
        if (isPartitionResolver.booleanValue()) {
          paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
        }
        PartitionAttributes prAttr = paf.create();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(prAttr);
        assertNotNull(basicGetCache());
        try {
          Region r = basicGetCache().createRegion(regionName, attr.create());
          r.put("key", "value");
          assertEquals("value", (String)r.get("key"));
        }
        catch (Exception NotExpected) {
          NotExpected.printStackTrace();
          LogWriterUtils.getLogWriter().info(
              "Unexpected Exception Message : " + NotExpected.getMessage());
          Assert.fail("Unpexpected Exception" , NotExpected);
        }
      }
    });
  }

  public void testColocatedPRWithLocalDestroy() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(false);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(false);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Put the customer 1-10 in CustomerPartitionedRegion

    accessor.invoke(PRColocationDUnitTest.class,
        "putCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });
    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class, "putOrderPartitionedRegion",
        new Object[] { OrderPartitionedRegionName });

    // add expected exception string
    final String expectedExMessage =
      "Any Region in colocation chain cannot be destroyed locally.";
    final IgnoredException ex = IgnoredException.addIgnoredException(expectedExMessage,
        dataStore1);
    dataStore1.invoke(new CacheSerializableRunnable(
        "PR with Local destroy") {
      @Override
      public void run2() {
        Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
            + OrderPartitionedRegionName);
        try {
          partitionedregion.localDestroyRegion();
          fail("It should have thrown an Exception saying: "
              + expectedExMessage);
        }
        catch (Exception Expected) {
          LogWriterUtils.getLogWriter().info("Expected Messageee : " + Expected.getMessage());
          assertTrue(Expected.getMessage().contains(expectedExMessage));
        }
      }
    });

    dataStore1.invoke(new CacheSerializableRunnable(
        "PR with Local Destroy") {
      @Override
      public void run2() {
        Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
            + CustomerPartitionedRegionName);
        try {
          partitionedregion.localDestroyRegion();
          fail("It should have thrown an Exception saying: "
              + expectedExMessage);
        }
        catch (Exception Expected) {
          LogWriterUtils.getLogWriter().info("Expected Messageee : " + Expected.getMessage());
          assertTrue(Expected.getMessage().contains(expectedExMessage));
        }
      }
    });
    ex.remove();
  }
  
  public void testColocatedPRWithDestroy() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    try {
      // Create Customer PartitionedRegion in All VMs
      regionName = CustomerPartitionedRegionName;
      colocatedWith = null;
      isPartitionResolver = new Boolean(false);
      attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
          totalNumBuckets, colocatedWith, isPartitionResolver };
      createPartitionedRegion(attributeObjects);

      // Create Order PartitionedRegion in All VMs
      regionName = OrderPartitionedRegionName;
      colocatedWith = CustomerPartitionedRegionName;
      isPartitionResolver = new Boolean(false);
      attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
          totalNumBuckets, colocatedWith, isPartitionResolver };
      createPartitionedRegion(attributeObjects);
    }
    catch (Exception Expected) {
      assertTrue(Expected instanceof IllegalStateException);
    }

    // Put the customer 1-10 in CustomerPartitionedRegion

    accessor.invoke(PRColocationDUnitTest.class,
        "putCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });
    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class, "putOrderPartitionedRegion",
        new Object[] { OrderPartitionedRegionName });

    // add expected exception string
    final String expectedExMessage = "colocation chain cannot be destroyed, "
        + "unless all its children";
    final IgnoredException ex = IgnoredException.addIgnoredException(expectedExMessage,
        dataStore1);
    dataStore1.invoke(new CacheSerializableRunnable(
        "PR with destroy") {
      @Override
      public void run2() {
        Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
            + CustomerPartitionedRegionName);
        try {
          partitionedregion.destroyRegion();
          fail("It should have thrown an Exception saying: "
              + expectedExMessage);
        }
        catch (IllegalStateException expected) {
          LogWriterUtils.getLogWriter().info("Got message: " + expected.getMessage());
          assertTrue(expected.getMessage().contains(expectedExMessage));
        }
      }
    });
    ex.remove();

    dataStore1.invoke(new CacheSerializableRunnable(
        "PR with destroy") {
      @Override
      public void run2() {
        Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
            + OrderPartitionedRegionName);
        try {
          partitionedregion.destroyRegion();
        }
        catch (Exception unexpected) {
          unexpected.printStackTrace();
          LogWriterUtils.getLogWriter().info("Unexpected Message: " + unexpected.getMessage());
          fail("Could not destroy the child region.");
        }
      }
    });

    dataStore1.invoke(new CacheSerializableRunnable(
        "PR with destroy") {
      @Override
      public void run2() {
        Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
            + CustomerPartitionedRegionName);
        try {
          partitionedregion.destroyRegion();
        }
        catch (Exception unexpected) {
          unexpected.printStackTrace();
          LogWriterUtils.getLogWriter().info("Unexpected Message: " + unexpected.getMessage());
          fail("Could not destroy the parent region.");
        }
      }
    });
  }
  
  public void testColocatedPRWithClose() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(20);
    
    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Put the customer 1-10 in CustomerPartitionedRegion

    accessor.invoke(PRColocationDUnitTest.class,
        "putCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });
    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class, "putOrderPartitionedRegion",
        new Object[] { OrderPartitionedRegionName });

    
    // Closing region with colocated regions will throw an exception 
    // and the region will not be closed.
    accessor.invoke(PRColocationDUnitTest.class,
        "closeRegionWithColocatedRegions",
        new Object[] { CustomerPartitionedRegionName, false });
    
    // Destroying region with colocated regions will throw an exception 
    // and the region will not be closed.
    accessor.invoke(PRColocationDUnitTest.class,
        "closeRegionWithColocatedRegions",
        new Object[] { CustomerPartitionedRegionName, true });
    
    
    // Closing the colocated regions in the right order should work
    accessor.invoke(PRColocationDUnitTest.class,
        "closeRegion",
        new Object[] { OrderPartitionedRegionName});
    accessor.invoke(PRColocationDUnitTest.class,
        "closeRegion",
        new Object[] { CustomerPartitionedRegionName});
  }
  /*
   * Test For partition Region with Key Based Routing Resolver
   */
  public void testPartitionResolverPartitionedRegion() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    try {
      // Create Customer PartitionedRegion in All VMs
      regionName = CustomerPartitionedRegionName;
      colocatedWith = null;
      isPartitionResolver = new Boolean(false);
      attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
          totalNumBuckets, colocatedWith, isPartitionResolver };
      createPartitionedRegion(attributeObjects);

      // Create Order PartitionedRegion in All VMs
      regionName = OrderPartitionedRegionName;
      colocatedWith = CustomerPartitionedRegionName;
      isPartitionResolver = new Boolean(false);
      attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
          totalNumBuckets, colocatedWith, isPartitionResolver };
      createPartitionedRegion(attributeObjects);

      // With same Key Based Partition Resolver
      accessor
          .invoke(new SerializableCallable("Create data, invoke exectuable") {
            public Object call() throws Exception {
              PartitionedRegion prForCustomer = (PartitionedRegion)basicGetCache()
                  .getRegion(CustomerPartitionedRegionName);
              assertNotNull(prForCustomer);
              DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(1);
              prForCustomer.put(dummy, new Integer(100));
              assertEquals(prForCustomer.get(dummy), new Integer(100));
              LogWriterUtils.getLogWriter().info(
                  "Key :" + dummy.dummyID + " Value :"
                      + prForCustomer.get(dummy));

              PartitionedRegion prForOrder = (PartitionedRegion)basicGetCache()
                  .getRegion(OrderPartitionedRegionName);
              assertNotNull(prForOrder);
              prForOrder.put(dummy, new Integer(200));
              assertEquals(prForOrder.get(dummy), new Integer(200));
              LogWriterUtils.getLogWriter().info(
                  "Key :" + dummy.dummyID + " Value :" + prForOrder.get(dummy));
              return null;
            }
          });
    } catch (Exception unexpected) {
      unexpected.printStackTrace();
      LogWriterUtils.getLogWriter().info("Unexpected Message: " + unexpected.getMessage());
      fail("Test failed");
    }
  }
  
  /*
   * Test added to check the colocation of regions
   * Also checks for the colocation of the buckets  
   */ 
  public void testColocationPartitionedRegion() throws Throwable {
    // Create Cache in all VMs VM0,VM1,VM2,VM3 
    
    createCacheInAllVms();
    
    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    
    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    // Create Shipment PartitionedRegion in All VMs
    regionName = ShipmentPartitionedRegionName;
    colocatedWith = OrderPartitionedRegionName;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);
    
    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(PRColocationDUnitTest.class,
        "validateBeforePutCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });
    
    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });
    
    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class, "putOrderPartitionedRegion",
        new Object[] { OrderPartitionedRegionName });
    
    // Put the shipment 1-10 for each order in ShipmentPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putShipmentPartitionedRegion",
        new Object[] { ShipmentPartitionedRegionName });

    // for VM0 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore1 = (Integer)dataStore1.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });
    
    // for VM1 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore2 = (Integer)dataStore2.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });
    
    // for VM3 Datastore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore3 = (Integer)dataStore3.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });
    
    // Check the total number of buckets created in all three Vms are equalto 30
    totalNumBucketsInTest = totalBucketsInDataStore1.intValue()
        + totalBucketsInDataStore2.intValue()
        + totalBucketsInDataStore3.intValue();
    assertEquals(totalNumBucketsInTest, 30);

    // This is the importatnt check. Checks that the colocated Customer,Order
    // and Shipment are in the same VM

    accessor.invoke(PRColocationDUnitTest.class,
        "validateAfterPutPartitionedRegion", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });
  }
  

  /**
   * Test verifies following invalid collocation creation <br>
   * Member 1: PR2 colocatedWith PR1 <br>
   * Member 2: PR2 is not colocated <br>
   * Should throw IllegalStateException
   * 
   * @throws Throwable
   */
  public void testColocationPartitionedRegionWithNullColocationSpecifiedOnOneNode()
      throws Throwable {
    try {
      createCacheInAllVms();
      getCache().getLogger().info("<ExpectedException action=add>" +
          "IllegalStateException" +
          "</ExpectedException>");

      redundancy = new Integer(1);
      localMaxmemory = new Integer(50);
      totalNumBuckets = new Integer(11);

      // Create Customer PartitionedRegion in All VMs
      regionName = CustomerPartitionedRegionName;
      colocatedWith = null;
      isPartitionResolver = new Boolean(true);
      attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
          totalNumBuckets, null /*colocatedWith*/, isPartitionResolver };
      dataStore1.invoke(PRColocationDUnitTest.class, "createPR",
          attributeObjects);
      createPR(regionName, redundancy, localMaxmemory, totalNumBuckets, null /*colocatedWith*/, isPartitionResolver, false);
      // Create Order PartitionedRegion in All VMs
      regionName = OrderPartitionedRegionName;
      colocatedWith = CustomerPartitionedRegionName;
      isPartitionResolver = new Boolean(true);
      attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
          totalNumBuckets, colocatedWith, isPartitionResolver };
      dataStore1.invoke(PRColocationDUnitTest.class, "createPR",
          attributeObjects);
      createPR(regionName, redundancy, localMaxmemory, totalNumBuckets, null/*colocatedWith*/, isPartitionResolver, false);
      fail("test failed due to illgal colocation settings did not thorw expected exception");
    }
    catch (IllegalStateException expected) {
      // test pass
      assertTrue(expected
          .getMessage()
          .contains(
              "The colocatedWith="));
    } finally {
      getCache().getLogger().info("<ExpectedException action=remove>" +
          "IllegalStateException" +
          "</ExpectedException>");
    }
  }


  /*
   * Test added to check the colocation of regions
   * Also checks for the colocation of the buckets with redundancy specified 
   */ 
  public void testColocationPartitionedRegionWithRedundancy() throws Throwable {

    // Create Cache in all VMs VM0,VM1,VM2,VM3
    createCacheInAllVms();
    
    redundancy = new Integer(1);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    
    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Create Customer PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Create Customer PartitionedRegion in All VMs
    regionName = ShipmentPartitionedRegionName;
    colocatedWith = OrderPartitionedRegionName;
    isPartitionResolver = new Boolean(true);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(PRColocationDUnitTest.class,
        "validateBeforePutCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });

    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });

    // Put the order 1-10 for each Customer in OrderPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class, "putOrderPartitionedRegion",
        new Object[] { OrderPartitionedRegionName });

    // Put the shipment 1-10 for each order in ShipmentPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putShipmentPartitionedRegion",
        new Object[] { ShipmentPartitionedRegionName });

    //  This is the importatnt check. Checks that the colocated Customer,Order
    // and Shipment are in the same VM
    accessor.invoke(PRColocationDUnitTest.class,
        "validateAfterPutPartitionedRegion", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });
    
    // for VM0 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore1 = (Integer)dataStore1.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });

    // for VM1 DataStore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore2 = (Integer)dataStore2.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });

    // for VM3 Datastore check the number of buckets created and the size of
    // bucket for all partitionedRegion
    Integer totalBucketsInDataStore3 = (Integer)dataStore3.invoke(
        PRColocationDUnitTest.class, "validateDataStore", new Object[] {
            CustomerPartitionedRegionName, OrderPartitionedRegionName,
            ShipmentPartitionedRegionName });
    
    if (redundancy.intValue() > 0) {
      // for VM0 DataStore check the number of buckets created and the size of
      // bucket for all partitionedRegion
      dataStore1.invoke(PRColocationDUnitTest.class, "validateDataStoreForRedundancy",
          new Object[] { CustomerPartitionedRegionName,
              OrderPartitionedRegionName, ShipmentPartitionedRegionName });

      // for VM1 DataStore check the number of buckets created and the size of
      // bucket for all partitionedRegion
      dataStore2.invoke(PRColocationDUnitTest.class, "validateDataStoreForRedundancy",
          new Object[] { CustomerPartitionedRegionName,
              OrderPartitionedRegionName, ShipmentPartitionedRegionName });

      // for VM3 Datastore check the number of buckets created and the size of
      // bucket for all partitionedRegion
      dataStore3.invoke(PRColocationDUnitTest.class, "validateDataStoreForRedundancy",
          new Object[] { CustomerPartitionedRegionName,
              OrderPartitionedRegionName, ShipmentPartitionedRegionName });
    }
    
    // Check the total number of buckets created in all three Vms are equalto 60
    totalNumBucketsInTest = totalBucketsInDataStore1.intValue()
        + totalBucketsInDataStore2.intValue()
        + totalBucketsInDataStore3.intValue();
    assertEquals(totalNumBucketsInTest, 60);
  }
  
  /**
   * Confirm that the redundancy must be the same for colocated partitioned regions
   * @throws Exception
   */
  public void testRedundancyRestriction() throws Exception {
    final String rName = getUniqueName();
    final Integer red0 = Integer.valueOf(0);
    final Integer red1 = Integer.valueOf(1);
    
    CacheSerializableRunnable createPRsWithRed = new CacheSerializableRunnable("createPrsWithDifferentRedundancy") {
      @Override
      public void run2() throws CacheException {
        getCache();
        IgnoredException.addIgnoredException("redundancy should be same as the redundancy");
        createPR(rName, red1, Integer.valueOf(100), Integer.valueOf(3), null, Boolean.FALSE, Boolean.FALSE);
        try {
          createPR(rName+"colo", red0, Integer.valueOf(100), Integer.valueOf(3), rName, Boolean.FALSE, Boolean.FALSE);
          fail("Expected different redundancy levels to throw.");
        } catch (IllegalStateException expected) {
          assertEquals(LocalizedStrings.PartitionAttributesImpl_CURRENT_PARTITIONEDREGIONS_REDUNDANCY_SHOULD_BE_SAME_AS_THE_REDUNDANCY_OF_COLOCATED_PARTITIONEDREGION.toLocalizedString(),
              expected.getMessage());
        }
      }
    };
    dataStore1.invoke(createPRsWithRed);
  }
  
  /**
   * Tests to make sure that a VM will not make copies of
   * any buckets for a region until all of the colocated regions are 
   * created.
   * @throws Throwable
   */
  public void testColocatedPRRedundancyRecovery() throws Throwable {
    createCacheInAllVms();
    redundancy = new Integer(1);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    // Create Customer PartitionedRegion in Data store 1
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(false);
    Object[] attributeObjects1 = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);

    // Create Order PartitionedRegion in Data store 1
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(false);
    Object[] attributeObjects2  = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);
    
    //create a few buckets in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Region region2 = basicGetCache().getRegion(OrderPartitionedRegionName);
        region1.put(Integer.valueOf(1), "A");
        region1.put(Integer.valueOf(2), "A");
        region2.put(Integer.valueOf(1), "A");
        region2.put(Integer.valueOf(2), "A");
      }
    });
    
    //add a listener for region recovery
    dataStore2.invoke(new SerializableRunnable("Add recovery listener") {
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });
    
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);
    
    //Make sure no redundant copies of buckets get created for the first PR in datastore2 because
    //the second PR has not yet been created.
    SerializableRunnable checkForBuckets = new SerializableRunnable("check for buckets") {
      public void run() {
        PartitionedRegion region1 = (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region1, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        
        //there should be no buckets on this node, because we don't
        //have all of the colocated regions
        assertEquals(Collections.emptyList(), region1.getLocalBucketsListTestOnly());
        assertEquals(0, region1.getRegionAdvisor().getBucketRedundancy(1));
      }
    };
    
    dataStore2.invoke(checkForBuckets);
    
    //create another bucket in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        Region region2 = basicGetCache().getRegion(OrderPartitionedRegionName);
        region1.put(Integer.valueOf(3), "A");
        region2.put(Integer.valueOf(3), "A");
      }
    });
    
    
    //Make sure that no copies of buckets are created for the first PR in datastore2
    dataStore2.invoke(checkForBuckets);
    
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);
    
    //Now we should get redundant copies of buckets for both PRs
    dataStore2.invoke(new SerializableRunnable("check for bucket creation") {
      public void run() {
        PartitionedRegion region1 = (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        PartitionedRegion region2 = (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region2, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        
        //we should now have copies all of the buckets
        assertEquals(3, region1.getLocalBucketsListTestOnly().size());
        assertEquals(3, region2.getLocalBucketsListTestOnly().size());
      }
    });
  }
  
  public void testColocationPartitionedRegionWithKeyPartitionResolver()
      throws Throwable {
    // Create Cache in all VMs VM0,VM1,VM2,VM3

    createCacheInAllVms();

    redundancy = new Integer(0);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);

    // Create Customer PartitionedRegion in All VMs
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(false);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Create Order PartitionedRegion in All VMs
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(false);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Create Shipment PartitionedRegion in All VMs
    regionName = ShipmentPartitionedRegionName;
    colocatedWith = OrderPartitionedRegionName;
    isPartitionResolver = new Boolean(false);
    attributeObjects = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    createPartitionedRegion(attributeObjects);

    // Initial Validation for the number of data stores and number of profiles
    accessor.invoke(PRColocationDUnitTest.class,
        "validateBeforePutCustomerPartitionedRegion",
        new Object[] { CustomerPartitionedRegionName });

    // Put the customer 1-10 in CustomerPartitionedRegion
    accessor.invoke(PRColocationDUnitTest.class,
        "putData_KeyBasedPartitionResolver");

    accessor.invoke(PRColocationDUnitTest.class, "executeFunction");
  }

  public void testColocatedPRRedundancyRecovery2() throws Throwable {
    createCacheInAllVms();
    
  //add a listener for region recovery
    dataStore1.invoke(new SerializableRunnable("Add recovery listener") {
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });
    
  //add a listener for region recovery
    dataStore2.invoke(new SerializableRunnable("Add recovery listener") {
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });
    
    redundancy = new Integer(1);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(11);
    // Create Customer PartitionedRegion in Data store 1
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(false);
    Object[] attributeObjects1 = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);
    
    //create a few buckets in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        region1.put(Integer.valueOf(1), "A");
        region1.put(Integer.valueOf(2), "B");
      }
    });

    SerializableRunnable checkForBuckets_ForCustomer = new SerializableRunnable("check for buckets") {
      public void run() {
        PartitionedRegion region1 = (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region1, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertEquals(2, region1.getDataStore().getAllLocalBucketIds().size());
        assertEquals(2, region1.getDataStore().getAllLocalPrimaryBucketIds().size());
      }
    };
    
    dataStore1.invoke(checkForBuckets_ForCustomer);
    
    // Create Order PartitionedRegion in Data store 1
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(false);
    Object[] attributeObjects2  = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);
    
    SerializableRunnable checkForBuckets_ForOrder = new SerializableRunnable("check for buckets") {
      public void run() {
        PartitionedRegion region = (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertEquals(2, region.getDataStore().getAllLocalBucketIds().size());
        assertEquals(2, region.getDataStore().getAllLocalPrimaryBucketIds().size());
      }
    };
    Wait.pause(5000);
    dataStore1.invoke(checkForBuckets_ForOrder);
    
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);
    
    SerializableRunnable checkForBuckets = new SerializableRunnable("check for buckets") {
      public void run() {
        PartitionedRegion region1 = (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region1, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertEquals(0, region1.getDataStore().getAllLocalBucketIds().size());
        assertEquals(0, region1.getDataStore().getAllLocalPrimaryBucketIds().size());
      }
    };
    
    dataStore2.invoke(checkForBuckets);
    
    //create another bucket in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        region1.put(Integer.valueOf(3), "C");
      }
    });
    
    
    //Make sure that no copies of buckets are created for the first PR in datastore2
    dataStore2.invoke(checkForBuckets);
    
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects2);
    
    //Now we should get redundant copies of buckets for both PRs
    dataStore2.invoke(new SerializableRunnable("check for bucket creation") {
      public void run() {
        PartitionedRegion region1 = (PartitionedRegion) basicGetCache().getRegion(CustomerPartitionedRegionName);
        PartitionedRegion region2 = (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region2, 1 * 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        
        //we should now have copies all of the buckets
        assertEquals(3, region1.getLocalBucketsListTestOnly().size());
        assertEquals(3, region2.getLocalBucketsListTestOnly().size());
      }
   });
    
    dataStore1.invoke(new SerializableRunnable("check for bucket creation") {
      public void run() {
        PartitionedRegion region2 = (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        assertEquals(3, region2.getLocalBucketsListTestOnly().size());
      }
    });
  }
  
  /**
   * Test for hang condition observed with the 
   * PRHARedundancyProvider.createMissingBuckets code.
   * 
   * A parent region is populated with buckets. Then the
   * child region is created simultaneously on several nodes.
   * 
   * @throws Throwable
   */
  public void testSimulaneousChildRegionCreation() throws Throwable {
    createCacheInAllVms();
    
  //add a listener for region recovery
    dataStore1.invoke(new SerializableRunnable("Add recovery listener") {
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });
    
  //add a listener for region recovery
    dataStore2.invoke(new SerializableRunnable("Add recovery listener") {
      public void run() {
        InternalResourceManager.setResourceObserver(new MyResourceObserver());
      }
    });
    
    redundancy = new Integer(1);
    localMaxmemory = new Integer(50);
    totalNumBuckets = new Integer(60);
    // Create Customer PartitionedRegion in Data store 1
    regionName = CustomerPartitionedRegionName;
    colocatedWith = null;
    isPartitionResolver = new Boolean(false);
    Object[] attributeObjects1 = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributeObjects1);
    
    //create a few buckets in dataStore1
    dataStore1.invoke(new SerializableRunnable("put data in region") {
      public void run() {
        Region region1 = basicGetCache().getRegion(CustomerPartitionedRegionName);
        for(int i =0; i < 50; i++) {
          region1.put(Integer.valueOf(i), "A");
        }
      }
    });

    // Create Order PartitionedRegion in Data store 1
    regionName = OrderPartitionedRegionName;
    colocatedWith = CustomerPartitionedRegionName;
    isPartitionResolver = new Boolean(false);
    Object[] attributeObjects2  = new Object[] { regionName, redundancy, localMaxmemory,
        totalNumBuckets, colocatedWith, isPartitionResolver };
    
    AsyncInvocation async1 = dataStore1.invokeAsync(PRColocationDUnitTest.class, "createPR", attributeObjects2);
    AsyncInvocation async2 = dataStore2.invokeAsync(PRColocationDUnitTest.class, "createPR", attributeObjects2);
    
    async1.join();
    if(async1.exceptionOccurred()) {
      throw async1.getException();
    }
    
    async2.join();
    if(async2.exceptionOccurred()) {
      throw async2.getException();
    }
    
    Wait.pause(5000);
    SerializableRunnable checkForBuckets_ForOrder = new SerializableRunnable("check for buckets") {
      public void run() {
        PartitionedRegion region = (PartitionedRegion) basicGetCache().getRegion(OrderPartitionedRegionName);
        MyResourceObserver observer = (MyResourceObserver) InternalResourceManager.getResourceObserver();
        try {
          observer.waitForRegion(region, 60 * 1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        assertEquals(50, region.getDataStore().getAllLocalBucketIds().size());
        assertEquals(25, region.getDataStore().getAllLocalPrimaryBucketIds().size());
      }
    };
    
    dataStore1.invoke(checkForBuckets_ForOrder);
    dataStore2.invoke(checkForBuckets_ForOrder);
  }
  
  public static void putData_KeyBasedPartitionResolver() {
    PartitionedRegion prForCustomer = (PartitionedRegion)basicGetCache()
        .getRegion(CustomerPartitionedRegionName);
    assertNotNull(prForCustomer);
    PartitionedRegion prForOrder = (PartitionedRegion)basicGetCache()
        .getRegion(OrderPartitionedRegionName);
    assertNotNull(prForOrder);
    PartitionedRegion prForShipment = (PartitionedRegion)basicGetCache()
        .getRegion(ShipmentPartitionedRegionName);
    assertNotNull(prForShipment);

    for (int i = 1; i <= 100; i++) {
      DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(i);
      prForCustomer.put(dummy, new Integer(1 * i));
      prForOrder.put(dummy, new Integer(10 * i));
      prForShipment.put(dummy, new Integer(100 * i));
    }
  }

  public static void executeFunction() {

    Function inlineFunction = new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        RegionFunctionContext rfContext = (RegionFunctionContext)context;
        Region r = rfContext.getDataSet();
        if (r.getName().equals(CustomerPartitionedRegionName)) {
          Map map = ColocationHelper.getColocatedLocalDataSetsForBuckets(
              (PartitionedRegion)r, new HashSet<Integer>());
          assertEquals(2, map.size());
          rfContext.getResultSender().sendResult(map.size());
          map = ColocationHelper.constructAndGetAllColocatedLocalDataSet(
              (PartitionedRegion)r, new HashSet<Integer>());
          assertEquals(3, map.size());
          rfContext.getResultSender().lastResult(map.size());
        }
        else if (r.getName().equals(OrderPartitionedRegionName)) {
          Map map = ColocationHelper.getColocatedLocalDataSetsForBuckets(
              (PartitionedRegion)r, new HashSet<Integer>());
          assertEquals(2, map.size());
          rfContext.getResultSender().sendResult(map.size());
          map = ColocationHelper.constructAndGetAllColocatedLocalDataSet(
              (PartitionedRegion)r, new HashSet<Integer>());
          assertEquals(3, map.size());
          rfContext.getResultSender().lastResult(map.size());
        }
        else if (r.getName().equals(ShipmentPartitionedRegionName)) {
          Map map = ColocationHelper.getColocatedLocalDataSetsForBuckets(
              (PartitionedRegion)r, new HashSet<Integer>());
          assertEquals(2, map.size());
          rfContext.getResultSender().sendResult(map.size());
          map = ColocationHelper.constructAndGetAllColocatedLocalDataSet(
              (PartitionedRegion)r, new HashSet<Integer>());
          assertEquals(3, map.size());
          rfContext.getResultSender().lastResult(map.size());
        }
      }

      @Override
      public String getId() {
        return "inlineFunction";
      }

      @Override
      public boolean hasResult() {
        return true;
      }

      @Override
      public boolean isHA() {
        return false;
      }

      @Override
      public boolean optimizeForWrite() {
        return false;
      }
    };
    PartitionedRegion prForCustomer = (PartitionedRegion)basicGetCache()
        .getRegion(CustomerPartitionedRegionName);
    final Set testKeysSet = new HashSet();
    DummyKeyBasedRoutingResolver dummy = new DummyKeyBasedRoutingResolver(10);
    testKeysSet.add(dummy);
    Execution dataSet = FunctionService.onRegion(prForCustomer);
    ResultCollector rc = dataSet.withFilter(testKeysSet)
        .execute(inlineFunction);
    assertEquals(2, ((List)rc.getResult()).size());

    PartitionedRegion prForOrder = (PartitionedRegion)basicGetCache()
        .getRegion(OrderPartitionedRegionName);
    dataSet = FunctionService.onRegion(prForOrder);
    rc = dataSet.withFilter(testKeysSet).execute(inlineFunction);
    assertEquals(2, ((List)rc.getResult()).size());

    PartitionedRegion prForShipment = (PartitionedRegion)basicGetCache()
        .getRegion(ShipmentPartitionedRegionName);
    dataSet = FunctionService.onRegion(prForShipment);
    rc = dataSet.withFilter(testKeysSet).execute(inlineFunction);
    assertEquals(2, ((List)rc.getResult()).size());
  }


  public static void validateDataStoreForRedundancy(String customerPartitionedRegionName,
      String orderPartitionedRegionName,
      String shipmentPartitionedRegionName) {

    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + shipmentPartitionedRegionName);
    }
    catch (Exception e) {
      fail("validateDataStore : Failed while getting the region from basicGetCache()");
    }
    ArrayList primaryBucketListForCustomer = null;
    ArrayList secondaryBucketListForCustomer = null;
    ArrayList primaryBucketListForOrder = null;
    ArrayList secondaryBucketListForOrder = null;
    ArrayList primaryBucketListForShipment = null;
    ArrayList secondaryBucketListForShipment = null;

    {
      // this is the test for the colocation of the secondaries
      int totalSizeOfBucketsForCustomer = customerPartitionedregion
          .getDataStore().getBucketsManaged();
      int sizeOfPrimaryBucketsForCustomer = customerPartitionedregion
          .getDataStore().getNumberOfPrimaryBucketsManaged();
      int sizeOfSecondaryBucketsForCustomer = totalSizeOfBucketsForCustomer
          - sizeOfPrimaryBucketsForCustomer;

      primaryBucketListForCustomer = (ArrayList)customerPartitionedregion
          .getDataStore().getLocalPrimaryBucketsListTestOnly();

      secondaryBucketListForCustomer = new ArrayList(
          sizeOfSecondaryBucketsForCustomer);

      HashMap localBucket2RegionMap = (HashMap)customerPartitionedregion
          .getDataStore().getSizeLocally();
      Set customerEntrySet = localBucket2RegionMap.entrySet();
      assertNotNull(customerEntrySet);
      Iterator customerIterator = customerEntrySet.iterator();
      boolean isSecondary = false;
      while (customerIterator.hasNext()) {
        Map.Entry me = (Map.Entry)customerIterator.next();
        Iterator primaryBuIterator = primaryBucketListForCustomer.iterator();
        while (primaryBuIterator.hasNext()) {
          if (!me.getKey().equals(primaryBuIterator.next())) {
            isSecondary = true;
          }
          else {
            isSecondary = false;
            break;
          }
        }
        if (isSecondary)
          secondaryBucketListForCustomer.add(me.getKey());
      }
      Iterator primaryBucketIterator = primaryBucketListForCustomer.iterator();
      while (primaryBucketIterator.hasNext()) {
        LogWriterUtils.getLogWriter().info("Primary Bucket : " + primaryBucketIterator.next());

      }
      Iterator SecondaryBucketIterator = secondaryBucketListForCustomer
          .iterator();
      while (SecondaryBucketIterator.hasNext()) {
        LogWriterUtils.getLogWriter().info(
            "Secondary Bucket : " + SecondaryBucketIterator.next());
      }
    }
    {
      int totalSizeOfBucketsForOrder = orderPartitionedregion.getDataStore()
          .getBucketsManaged();
      int sizeOfPrimaryBucketsForOrder = orderPartitionedregion.getDataStore()
          .getNumberOfPrimaryBucketsManaged();
      int sizeOfSecondaryBucketsForOrder = totalSizeOfBucketsForOrder
          - sizeOfPrimaryBucketsForOrder;

      primaryBucketListForOrder = (ArrayList)orderPartitionedregion
          .getDataStore().getLocalPrimaryBucketsListTestOnly();

      secondaryBucketListForOrder = new ArrayList(
          sizeOfSecondaryBucketsForOrder);

      HashMap localBucket2RegionMap = (HashMap)customerPartitionedregion
          .getDataStore().getSizeLocally();
      Set customerEntrySet = localBucket2RegionMap.entrySet();
      assertNotNull(customerEntrySet);
      boolean isSecondary = false;
      Iterator customerIterator = customerEntrySet.iterator();
      while (customerIterator.hasNext()) {
        Map.Entry me = (Map.Entry)customerIterator.next();
        Iterator primaryBuIterator = primaryBucketListForOrder.iterator();
        while (primaryBuIterator.hasNext()) {
          if (!me.getKey().equals(primaryBuIterator.next())) {
            isSecondary = true;
          }
          else {
            isSecondary = false;
            break;
          }
        }
        if (isSecondary)
          secondaryBucketListForOrder.add(me.getKey());
      }
      Iterator primaryBucketIterator = primaryBucketListForOrder.iterator();
      while (primaryBucketIterator.hasNext()) {
        LogWriterUtils.getLogWriter().info("Primary Bucket : " + primaryBucketIterator.next());

      }
      Iterator SecondaryBucketIterator = secondaryBucketListForOrder.iterator();
      while (SecondaryBucketIterator.hasNext()) {
        LogWriterUtils.getLogWriter().info(
            "Secondary Bucket : " + SecondaryBucketIterator.next());
      }
    }
    {
      int totalSizeOfBucketsForShipment = shipmentPartitionedregion
          .getDataStore().getBucketsManaged();
      int sizeOfPrimaryBucketsForShipment = shipmentPartitionedregion
          .getDataStore().getNumberOfPrimaryBucketsManaged();
      int sizeOfSecondaryBucketsForShipment = totalSizeOfBucketsForShipment
          - sizeOfPrimaryBucketsForShipment;

      primaryBucketListForShipment = (ArrayList)shipmentPartitionedregion
          .getDataStore().getLocalPrimaryBucketsListTestOnly();

      secondaryBucketListForShipment = new ArrayList(
          sizeOfSecondaryBucketsForShipment);

      HashMap localBucket2RegionMap = (HashMap)shipmentPartitionedregion
          .getDataStore().getSizeLocally();
      Set customerEntrySet = localBucket2RegionMap.entrySet();
      assertNotNull(customerEntrySet);
      Iterator customerIterator = customerEntrySet.iterator();
      boolean isSecondary = false;
      while (customerIterator.hasNext()) {
        Map.Entry me = (Map.Entry)customerIterator.next();
        Iterator primaryBuIterator = primaryBucketListForShipment.iterator();
        while (primaryBuIterator.hasNext()) {
          if (!me.getKey().equals(primaryBuIterator.next())) {
            isSecondary = true;
          }
          else {
            isSecondary = false;
            break;
          }
        }
        if (isSecondary)
          secondaryBucketListForShipment.add(me.getKey());
      }
      Iterator primaryBucketIterator = primaryBucketListForShipment.iterator();
      while (primaryBucketIterator.hasNext()) {
        LogWriterUtils.getLogWriter().info("Primary Bucket : " + primaryBucketIterator.next());

      }
      Iterator SecondaryBucketIterator = secondaryBucketListForShipment
          .iterator();
      while (SecondaryBucketIterator.hasNext()) {
        LogWriterUtils.getLogWriter().info(
            "Secondary Bucket : " + SecondaryBucketIterator.next());
      }
    }

    assertTrue(primaryBucketListForCustomer
        .containsAll(primaryBucketListForOrder));
    assertTrue(primaryBucketListForCustomer
        .containsAll(primaryBucketListForShipment));
    assertTrue(primaryBucketListForOrder.containsAll(primaryBucketListForOrder));

    assertTrue(secondaryBucketListForCustomer
        .containsAll(secondaryBucketListForOrder));
    assertTrue(secondaryBucketListForCustomer
        .containsAll(secondaryBucketListForShipment));
    assertTrue(secondaryBucketListForOrder
        .containsAll(secondaryBucketListForOrder));
  }
  
  public static Integer validateDataStore(String customerPartitionedRegionName,
      String orderPartitionedRegionName, String shipmentPartitionedRegionName) {

    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + shipmentPartitionedRegionName);
    }
    catch (Exception e) {
      fail("validateDataStore : Failed while getting the region from cache");
    }

    HashMap localBucket2RegionMap = (HashMap)customerPartitionedregion
        .getDataStore().getSizeLocally();
    int customerBucketSize = localBucket2RegionMap.size();
    LogWriterUtils.getLogWriter().info(
        "Size of the " + customerPartitionedRegionName + " in this VM :- "
            + localBucket2RegionMap.size());
    LogWriterUtils.getLogWriter().info(
        "Size of primary buckets the " + customerPartitionedRegionName + " in this VM :- "
            + customerPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged());
    Set customerEntrySet = localBucket2RegionMap.entrySet();
    assertNotNull(customerEntrySet);
    Iterator customerIterator = customerEntrySet.iterator();
    while (customerIterator.hasNext()) {
      Map.Entry me = (Map.Entry)customerIterator.next();
      Integer size = (Integer)me.getValue();
      assertEquals(1, size.intValue());
      LogWriterUtils.getLogWriter().info(
          "Size of the Bucket " + me.getKey() + ": - " + size.toString());
    }
    
    //This is the test to check the size of the buckets created
    localBucket2RegionMap = (HashMap)orderPartitionedregion.getDataStore()
        .getSizeLocally();
    int orderBucketSize = localBucket2RegionMap.size();
    LogWriterUtils.getLogWriter().info(
        "Size of the " + orderPartitionedRegionName + " in this VM :- "
            + localBucket2RegionMap.size());
    LogWriterUtils.getLogWriter().info(
        "Size of primary buckets the " + orderPartitionedRegionName + " in this VM :- "
            + orderPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged());
    
    Set orderEntrySet = localBucket2RegionMap.entrySet();
    assertNotNull(orderEntrySet);
    Iterator orderIterator = orderEntrySet.iterator();
    while (orderIterator.hasNext()) {
      Map.Entry me = (Map.Entry)orderIterator.next();
      Integer size = (Integer)me.getValue();
      assertEquals(10, size.intValue());
      LogWriterUtils.getLogWriter().info(
          "Size of the Bucket " + me.getKey() + ": - " + size.toString());
    }
    localBucket2RegionMap = (HashMap)shipmentPartitionedregion.getDataStore()
        .getSizeLocally();
    int shipmentBucketSize = localBucket2RegionMap.size();
    LogWriterUtils.getLogWriter().info(
        "Size of the " + shipmentPartitionedRegionName + " in this VM :- "
            + localBucket2RegionMap.size());
    LogWriterUtils.getLogWriter().info(
        "Size of primary buckets the " + shipmentPartitionedRegionName + " in this VM :- "
            + shipmentPartitionedregion.getDataStore().getNumberOfPrimaryBucketsManaged());
    Set shipmentEntrySet = localBucket2RegionMap.entrySet();
    assertNotNull(shipmentEntrySet);
    Iterator shipmentIterator = shipmentEntrySet.iterator();
    while (shipmentIterator.hasNext()) {
      Map.Entry me = (Map.Entry)shipmentIterator.next();
      Integer size = (Integer)me.getValue();
      assertEquals(100, size.intValue());
      LogWriterUtils.getLogWriter().info(
          "Size of the Bucket " + me.getKey() + ": - " + size.toString());
    }
    
    return new Integer(customerBucketSize + orderBucketSize
        + shipmentBucketSize);
    
  }

  public static void validateColocatedRegions(String partitionedRegionName) {
    PartitionedRegion partitionedRegion = (PartitionedRegion)basicGetCache()
        .getRegion(Region.SEPARATOR + partitionedRegionName);
    Map colocatedRegions;

    colocatedRegions = ColocationHelper
        .getAllColocationRegions(partitionedRegion);
    if(partitionedRegionName.equals("A")){
      assertEquals(14, colocatedRegions.size());
    }
    if(partitionedRegionName.equals("D")){
      assertEquals(4, colocatedRegions.size());
    }
    if(partitionedRegionName.equals("H")){
      assertEquals(2, colocatedRegions.size());
    }
    if(partitionedRegionName.equals("B")){
      assertEquals(8, colocatedRegions.size());
    }
    if(partitionedRegionName.equals("K")){
      assertEquals(3, colocatedRegions.size());
    }
  }

  public static void validateBeforePutCustomerPartitionedRegion(
      String partitionedRegionName) {
    assertNotNull(basicGetCache());
    PartitionedRegion partitionedregion = null;
    try {
      partitionedregion = (PartitionedRegion)basicGetCache().getRegion(Region.SEPARATOR
          + partitionedRegionName);
    }
    catch (Exception e) {
      Assert.fail(
          "validateBeforePutCustomerPartitionedRegion : Failed while getting the region from cache",
          e);
    }
    assertNotNull(partitionedregion);
    assertTrue(partitionedregion.getRegionAdvisor().getNumProfiles() == 3);
    assertTrue(partitionedregion.getRegionAdvisor().getNumDataStores() == 3);
  }

  public static void validateAfterPutPartitionedRegion(
      String customerPartitionedRegionName, String orderPartitionedRegionName,
      String shipmentPartitionedRegionName) throws ClassNotFoundException {

    assertNotNull(basicGetCache());
    PartitionedRegion customerPartitionedregion = null;
    PartitionedRegion orderPartitionedregion = null;
    PartitionedRegion shipmentPartitionedregion = null;
    try {
      customerPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + customerPartitionedRegionName);
      orderPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + orderPartitionedRegionName);
      shipmentPartitionedregion = (PartitionedRegion)basicGetCache()
          .getRegion(Region.SEPARATOR + shipmentPartitionedRegionName);
    }
    catch (Exception e) {
      Assert.fail(
          "validateAfterPutPartitionedRegion : failed while getting the region",
          e);
    }
    assertNotNull(customerPartitionedregion);

    for (int i = 1; i <= 10; i++) {
      InternalDistributedMember idmForCustomer = customerPartitionedregion
          .getBucketPrimary(i);
      InternalDistributedMember idmForOrder = orderPartitionedregion
          .getBucketPrimary(i);

      InternalDistributedMember idmForShipment = shipmentPartitionedregion
          .getBucketPrimary(i);

      // take all the keys from the shipmentfor each bucket
      Set customerKey = customerPartitionedregion.getBucketKeys(i);
      assertNotNull(customerKey);
      Iterator customerIterator = customerKey.iterator();
      while (customerIterator.hasNext()) {
        CustId custId = (CustId)customerIterator.next();
        assertNotNull(customerPartitionedregion.get(custId));
        Set orderKey = orderPartitionedregion.getBucketKeys(i);
        assertNotNull(orderKey);
        Iterator orderIterator = orderKey.iterator();
        while (orderIterator.hasNext()) {
          OrderId orderId = (OrderId)orderIterator.next();
          // assertNotNull(orderPartitionedregion.get(orderId));

          if (custId.equals(orderId.getCustId())) {
            LogWriterUtils.getLogWriter().info(
                orderId + "belongs to node " + idmForCustomer + " "
                    + idmForOrder);
            assertEquals(idmForCustomer, idmForOrder);
          }
          Set shipmentKey = shipmentPartitionedregion.getBucketKeys(i);
          assertNotNull(shipmentKey);
          Iterator shipmentIterator = shipmentKey.iterator();
          while (shipmentIterator.hasNext()) {
            ShipmentId shipmentId = (ShipmentId)shipmentIterator.next();
            // assertNotNull(shipmentPartitionedregion.get(shipmentId));
            if (orderId.equals(shipmentId.getOrderId())) {
              LogWriterUtils.getLogWriter().info(
                  shipmentId + "belongs to node " + idmForOrder + " "
                      + idmForShipment);
            }
          }
        }
      }
    }

  }

  protected void createCacheInAllVms() {
    dataStore1.invoke(PRColocationDUnitTest.class, "createCacheInVm");
    dataStore2.invoke(PRColocationDUnitTest.class, "createCacheInVm");
    dataStore3.invoke(PRColocationDUnitTest.class, "createCacheInVm");
    accessor.invoke(PRColocationDUnitTest.class, "createCacheInVm");

  }

  public static void putInPartitionedRegion(Region pr) {
    assertNotNull(basicGetCache());

    for (int i = 1; i <= 10; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      try {
        pr.put(custid, customer);
        assertTrue(pr.containsKey(custid));
        assertEquals(customer, pr.get(custid));
      }
      catch (Exception e) {
        Assert.fail("putInPartitionedRegion : failed while doing put operation in "
            + pr.getFullPath(), e);
      }
    }
  }
  
  public static void closeRegion(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(
        Region.SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    try {
      partitionedregion.close();
    } catch (Exception e) {
      Assert.fail(
          "closeRegion : failed to close region : " + partitionedregion,
          e);
    }
  }
  
  public static void closeRegionWithColocatedRegions(String partitionedRegionName, boolean destroy) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(
        Region.SEPARATOR + partitionedRegionName);
    assertNotNull(partitionedregion);
    boolean exceptionThrown = false;
    try {
      if (destroy)
        partitionedregion.destroyRegion();
      else
        partitionedregion.close();
    } catch (IllegalStateException e) {
      exceptionThrown = true;
    }
    assertTrue("Region should have failed to close. regionName = " + partitionedRegionName , exceptionThrown);    
  }
  public static void putCustomerPartitionedRegion(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 10; i++) {
      CustId custid = new CustId(i);
      Customer customer = new Customer("name" + i, "Address" + i);
      try {
        partitionedregion.put(custid, customer);
        assertTrue(partitionedregion.containsKey(custid));
        assertEquals(customer,partitionedregion.get(custid));
      }
      catch (Exception e) {
        Assert.fail(
            "putCustomerPartitionedRegion : failed while doing put operation in CustomerPartitionedRegion ",
            e);
      }
      LogWriterUtils.getLogWriter().info("Customer :- { " + custid + " : " + customer + " }");
    }
  }

  public static void putOrderPartitionedRegion(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 10; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        try {
          partitionedregion.put(orderId, order);
          assertTrue(partitionedregion.containsKey(orderId));
          assertEquals(order,partitionedregion.get(orderId));

        }
        catch (Exception e) {
          Assert.fail(
              "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
              e);
        }
        LogWriterUtils.getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
      }
    }
  }

  public static void putOrderPartitionedRegion2(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 11; i <= 100; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        Order order = new Order("OREDR" + oid);
        try {
          partitionedregion.put(orderId, order);
          assertTrue(partitionedregion.containsKey(orderId));
          assertEquals(order,partitionedregion.get(orderId));

        }
        catch (Exception e) {
          Assert.fail(
              "putOrderPartitionedRegion : failed while doing put operation in OrderPartitionedRegion ",
              e);
        }
        LogWriterUtils.getLogWriter().info("Order :- { " + orderId + " : " + order + " }");
      }
    }
  }
  
  public static void putShipmentPartitionedRegion(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region partitionedregion = basicGetCache().getRegion(Region.SEPARATOR
        + partitionedRegionName);
    assertNotNull(partitionedregion);
    for (int i = 1; i <= 10; i++) {
      CustId custid = new CustId(i);
      for (int j = 1; j <= 10; j++) {
        int oid = (i * 10) + j;
        OrderId orderId = new OrderId(oid, custid);
        for (int k = 1; k <= 10; k++) {
          int sid = (oid * 10) + k;
          ShipmentId shipmentId = new ShipmentId(sid, orderId);
          Shipment shipment = new Shipment("Shipment" + sid);
          try {
            partitionedregion.put(shipmentId, shipment);
            assertTrue(partitionedregion.containsKey(shipmentId));
            assertEquals(shipment,partitionedregion.get(shipmentId));
          }
          catch (Exception e) {
            Assert.fail(
                "putShipmentPartitionedRegion : failed while doing put operation in ShipmentPartitionedRegion ",
                e);
          }
          LogWriterUtils.getLogWriter().info(
              "Shipment :- { " + shipmentId + " : " + shipment + " }");
        }
      }
    }
  }

  protected void createPartitionedRegion(Object[] attributes) {
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributes);
    dataStore2.invoke(PRColocationDUnitTest.class, "createPR", attributes);
    dataStore3.invoke(PRColocationDUnitTest.class, "createPR", attributes);
    // make Local max memory = o for accessor
    attributes[2] = new Integer(0);
    accessor.invoke(PRColocationDUnitTest.class, "createPR", attributes);
  }
  
  private void createSubPartitionedRegion(Object[] attributes) {
    dataStore1.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
    dataStore2.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
    dataStore3.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
    // make Local max memory = o for accessor
    attributes[2] = new Integer(0);
    accessor.invoke(PRColocationDUnitTest.class, "createSubPR", attributes);
  }
  
  private void createPartitionedRegionOnOneVM(Object[] attributes) {
    dataStore1.invoke(PRColocationDUnitTest.class, "createPR", attributes);
  }
  
  public static void destroyPR(String partitionedRegionName) {
    assertNotNull(basicGetCache());
    Region pr = basicGetCache().getRegion(partitionedRegionName);
    assertNotNull(pr);
    try {
      LogWriterUtils.getLogWriter().info("Destroying Partitioned Region " + partitionedRegionName);
      pr.destroyRegion();
      fail("Did not get the expected ISE");
    } catch (Exception e) {
      if (!(e instanceof IllegalStateException)) {
        Assert.fail("Expected IllegalStateException, but it's not.", e);
      }
    }
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith, Boolean isPartitionResolver) {
    createPR(partitionedRegionName, redundancy, localMaxMemory, totalNumBuckets, colocatedWith, isPartitionResolver, Boolean.FALSE);
  }

  public static void createPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith, Boolean isPartitionResolver, Boolean concurrencyChecks) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy.intValue())
        .setLocalMaxMemory(localMaxMemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith((String)colocatedWith);
    if(isPartitionResolver.booleanValue()){
      paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    }
    PartitionAttributes prAttr = paf.create();
    AttributesFactory attr = new AttributesFactory();
    attr.setPartitionAttributes(prAttr);
    attr.setConcurrencyChecksEnabled(concurrencyChecks);
    assertNotNull(basicGetCache());
    Region pr = basicGetCache().createRegion(partitionedRegionName, attr.create());
    assertNotNull(pr);
    LogWriterUtils.getLogWriter().info(
        "Partitioned Region " + partitionedRegionName
            + " created Successfully :" + pr.toString());
  }
  
  public static void createSubPR(String partitionedRegionName, Integer redundancy,
      Integer localMaxMemory, Integer totalNumBuckets, Object colocatedWith, Boolean isPartitionResolver) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy.intValue())
        .setLocalMaxMemory(localMaxMemory.intValue()).setTotalNumBuckets(
            totalNumBuckets.intValue()).setColocatedWith((String)colocatedWith);
    if(isPartitionResolver.booleanValue()){
      paf.setPartitionResolver(new CustomerIDPartitionResolver("CustomerIDPartitionResolver"));
    }
    PartitionAttributes prAttr = paf.create();
    AttributesFactory attr = new AttributesFactory();
    assertNotNull(basicGetCache());
    Region root = basicGetCache().createRegion("root"+partitionedRegionName, attr.create());
    attr.setPartitionAttributes(prAttr);
    Region pr = root.createSubregion(partitionedRegionName, attr.create());
    assertNotNull(pr);
    LogWriterUtils.getLogWriter().info(
        "Partitioned sub region " + pr.getName()
            + " created Successfully :" + pr.toString());
    if(localMaxMemory == 0){
      putInPartitionedRegion(pr);
    }
  }
  
  public static void createCacheInVm() {
    new PRColocationDUnitTest("temp").getCache();
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      public void run() {
        InternalResourceManager.setResourceObserver(null);
      }
    });
    InternalResourceManager.setResourceObserver(null);
  }
  
  private static class MyResourceObserver extends ResourceObserverAdapter {
    Set<Region> recoveredRegions = new HashSet<Region>();

    @Override
    public void rebalancingOrRecoveryFinished(Region region) {
      synchronized(this) {
        recoveredRegions.add(region);
      }
    }
    
    public void waitForRegion(Region region, long timeout) throws InterruptedException {
      long start = System.currentTimeMillis();
      synchronized(this) {
        while(!recoveredRegions.contains(region)) {
          long remaining = timeout - (System.currentTimeMillis() - start );
          assertTrue("Timeout waiting for region recovery", remaining > 0);
          this.wait(remaining);
        }
      }
    }
    
  }
  
  public static String getDefaultAddOnString(){
  	if(defaultStringSize == 0){
  		return "";
  	}
  	StringBuffer buf = new StringBuffer(defaultStringSize);
  	for(int i=0; i < defaultStringSize; i++){
  		buf.append("a");
  	}
  	return buf.toString();
  }
}

class DummyKeyBasedRoutingResolver implements PartitionResolver, DataSerializable {
  Integer dummyID;

  public DummyKeyBasedRoutingResolver() {
  }

  public DummyKeyBasedRoutingResolver(int id) {
    this.dummyID = new Integer(id);
  }

  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    return (Serializable)opDetails.getKey();
  }

  public void close() {
    // TODO Auto-generated method stub
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.dummyID = DataSerializer.readInteger(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeInteger(this.dummyID, out);
  }

  @Override
  public int hashCode() {
    int i = this.dummyID.intValue();
    return i;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (!(o instanceof DummyKeyBasedRoutingResolver))
      return false;

    DummyKeyBasedRoutingResolver otherDummyID = (DummyKeyBasedRoutingResolver)o;
    return (otherDummyID.dummyID.equals(dummyID));

  }
}
