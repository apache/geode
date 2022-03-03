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
package org.apache.geode.internal.cache.partitioned;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.RegionsTest;

@Category({RegionsTest.class})
public class PartitionedRegionLoaderWriterDUnitTest extends JUnit4CacheTestCase {

  private static final String PartitionedRegionName = "PartitionedRegionTest";

  Host host;

  VM accessor;

  VM datastore1;

  VM datastore2;

  private static Cache cache;

  public PartitionedRegionLoaderWriterDUnitTest() {
    super();
  }

  @Test
  public void testLoader_OnAccessor_NotOnDataStore() {
    host = Host.getHost(0);
    accessor = host.getVM(0);
    datastore1 = host.getVM(1);
    accessor.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 0));
    datastore1.invoke(() -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, null, 10));
  }

  @Test
  public void testWriter_NotOnAccessor_OnDataStore() {
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    accessor.invoke(() -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, null, 0));
    datastore1.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, new CacheWriter2(), 10));
  }

  @Test
  public void testWriter_OnDataStore_NotOnAccessor() {
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore1.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, new CacheWriter2(), 10));
    accessor.invoke(() -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, null, 0));
  }

  @Test
  public void testLoader_OnAccessor_NotOnFirstDataStore_OnSecondDataStore() {
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    accessor.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 0));
    datastore1.invoke(() -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, null, 10));
    datastore2.invoke(() -> PartitionedRegionLoaderWriterDUnitTest
        .createRegionWithPossibleFail(new CacheLoader2(), null, 10));
  }

  @Test
  public void testLoader_NotOnFirstDataStore_OnAccessor_OnSecondDataStore() {
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    datastore1.invoke(() -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, null, 10));
    accessor.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 0));
    datastore2.invoke(() -> PartitionedRegionLoaderWriterDUnitTest
        .createRegionWithPossibleFail(new CacheLoader2(), null, 10));
  }

  @Test
  public void testLoader_OnFirstDataStore_OnSecondDataStore_OnAccessor() {
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    datastore1.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 10));
    datastore2.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 10));
    accessor.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 0));
  }

  @Test
  public void testLoader_OnFirstDataStore_OnSecondDataStore_NotOnAccessor() {
    host = Host.getHost(0);
    accessor = host.getVM(1);
    datastore1 = host.getVM(2);
    datastore2 = host.getVM(3);
    datastore1.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 10));
    datastore2.invoke(
        () -> PartitionedRegionLoaderWriterDUnitTest.createRegion(new CacheLoader2(), null, 10));
    accessor.invoke(() -> PartitionedRegionLoaderWriterDUnitTest.createRegion(null, null, 0));

  }

  public static void createRegion(CacheLoader cacheLoader, CacheWriter cacheWriter,
      Integer localMaxMemory) {
    try {
      new PartitionedRegionLoaderWriterDUnitTest().createCache(new Properties());
      AttributesFactory factory = new AttributesFactory();
      factory.setCacheLoader(cacheLoader);
      factory.setCacheWriter(cacheWriter);
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setLocalMaxMemory(localMaxMemory);
      factory.setDataPolicy(DataPolicy.PARTITION);
      factory.setPartitionAttributes(paf.create());
      RegionAttributes attrs = factory.create();
      cache.createRegion(PartitionedRegionName, attrs);
    } catch (Exception e) {
      Assert.fail("Not Expected : ", e);
    }
  }

  public static void createRegionWithPossibleFail(CacheLoader cacheLoader, CacheWriter cacheWriter,
      Integer localMaxMemory) {
    final PartitionedRegionLoaderWriterDUnitTest test =
        new PartitionedRegionLoaderWriterDUnitTest();
    test.createCache(new Properties());
    // add expected exception
    cache.getLogger().info("<ExpectedException action=add>"
        + IllegalStateException.class.getName() + "</ExpectedException>");
    try {
      AttributesFactory factory = new AttributesFactory();
      factory.setCacheLoader(cacheLoader);
      factory.setCacheWriter(cacheWriter);
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      paf.setLocalMaxMemory(localMaxMemory);
      factory.setDataPolicy(DataPolicy.PARTITION);
      factory.setPartitionAttributes(paf.create());
      RegionAttributes attrs = factory.create();
      cache.createRegion(PartitionedRegionName, attrs);
      fail("Expected Exception ");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().startsWith("Incompatible"));
    }
    cache.getLogger().info("<ExpectedException action=remove>"
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
    } catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  static class CacheLoader2 implements CacheLoader, Serializable {

    public CacheLoader2() {

    }

    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {

      return null;
    }

    @Override
    public void close() {

    }
  }

  static class CacheWriter2 extends CacheWriterAdapter implements Serializable {
    public CacheWriter2() {}
  }
}
