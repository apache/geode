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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.Serializable;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

public class PartitionedRegionLoaderWriterDUnitTest extends CacheTestCase {

  private static final String PartitionedRegionName = "PartitionedRegionTest";

  Host host;

  VM accessor;

  VM datastore1;

  VM datastore2;

  private static Cache cache;

  /**
   * @param name
   */
  public PartitionedRegionLoaderWriterDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  public void testLoader_OnAccessor_NotOnDataStore(){
    host = Host.getHost(0);
    accessor = host.getVM(0);
    datastore1 = host.getVM(1);
    accessor.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(), null, 0});
    datastore1.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, null, 10});
  }
  
  public void testWriter_NotOnAccessor_OnDataStore(){
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    accessor.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, null, 0});
    datastore1.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, new CacheWriter2(), 10});
  }
  
  public void testWriter_OnDataStore_NotOnAccessor(){
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore1.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, new CacheWriter2(), 10});
    accessor.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, null, 0});
  }
  
  public void testLoader_OnAccessor_NotOnFirstDataStore_OnSecondDataStore(){
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    accessor.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(), null, 0});
    datastore1.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, null, 10});
    datastore2.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegionWithPossibleFail", new Object[] {new CacheLoader2(),null, 10});
  }
  
  public void testLoader_NotOnFirstDataStore_OnAccessor_OnSecondDataStore(){
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    datastore1.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, null, 10});
    accessor.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(), null, 0});
    datastore2.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegionWithPossibleFail", new Object[] {new CacheLoader2(),null, 10});
  }
  
  public void testLoader_OnFirstDataStore_OnSecondDataStore_OnAccessor(){
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    datastore1.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(), null, 10});
    datastore2.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(),null, 10});
    accessor.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(), null, 0});
  }
  
  public void testLoader_OnFirstDataStore_OnSecondDataStore_NotOnAccessor(){
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    datastore1.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(), null, 10});
    datastore2.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {new CacheLoader2(),null, 10});
    accessor.invoke(PartitionedRegionLoaderWriterDUnitTest.class,
        "createRegion", new Object[] {null, null, 0});
    
  }
  
  public static void createRegion(CacheLoader cacheLoader, CacheWriter cacheWriter, Integer localMaxMemory) {
    try {
      new PartitionedRegionLoaderWriterDUnitTest("DUnitTests")
          .createCache(new Properties());
      AttributesFactory factory = new AttributesFactory();
      factory.setCacheLoader(cacheLoader);
      factory.setCacheWriter(cacheWriter);
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setLocalMaxMemory(localMaxMemory.intValue());
      factory.setDataPolicy(DataPolicy.PARTITION);
      factory.setPartitionAttributes(paf.create());
      RegionAttributes attrs = factory.create();
      cache.createRegion(PartitionedRegionName, attrs);
    }
    catch (Exception e) {
      Assert.fail("Not Expected : " , e);
    }
  }

  public static void createRegionWithPossibleFail(CacheLoader cacheLoader,
      CacheWriter cacheWriter, Integer localMaxMemory) {
    final PartitionedRegionLoaderWriterDUnitTest test =
      new PartitionedRegionLoaderWriterDUnitTest("DUnitTests");
    test.createCache(new Properties());
    // add expected exception
    test.cache.getLogger().info("<ExpectedException action=add>"
        + IllegalStateException.class.getName() + "</ExpectedException>");
    try {
      AttributesFactory factory = new AttributesFactory();
      factory.setCacheLoader(cacheLoader);
      factory.setCacheWriter(cacheWriter);
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setLocalMaxMemory(localMaxMemory.intValue());
      factory.setDataPolicy(DataPolicy.PARTITION);
      factory.setPartitionAttributes(paf.create());
      RegionAttributes attrs = factory.create();
      cache.createRegion(PartitionedRegionName, attrs);
      fail("Expected Exception ");
    }
    catch (IllegalStateException e) {
      assertTrue(e.getMessage().startsWith("Incompatible"));
    }
    test.cache.getLogger().info("<ExpectedException action=remove>"
        + IllegalStateException.class.getName() + "</ExpectedException>");
  }

  private void createCache(Properties props) {
    try {
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    }
    catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  static class CacheLoader2 implements CacheLoader, Serializable {

    public CacheLoader2() {

    }

    public Object load(LoaderHelper helper) throws CacheLoaderException {

      return null;
    }

    public void close() {

    }
  }

  static class CacheWriter2 extends CacheWriterAdapter implements Serializable {
    public CacheWriter2() {
    }
  }
}
