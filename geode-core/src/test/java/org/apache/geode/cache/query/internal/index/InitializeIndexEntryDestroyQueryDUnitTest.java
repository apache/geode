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
package org.apache.geode.cache.query.internal.index;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import static org.apache.geode.cache.query.Utils.createPortfolioData;

import java.util.Arrays;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache.query.internal.Undefined;
import org.apache.geode.cache.query.partitioned.PRQueryDUnitHelper;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.junit.categories.FlakyTest;

/**
 * Test creates a local region. Creates and removes index in a parallel running thread.
 * Then destroys and puts back entries in separated thread in the same region and runs
 * query parallely and checks for UNDEFINED values in result set of the query.
 */
@Category(DistributedTest.class)
public class InitializeIndexEntryDestroyQueryDUnitTest extends JUnit4CacheTestCase {

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  String name;

  final int redundancy = 0;

  final Portfolio portfolio = new Portfolio(1, 1);

  private int cnt=0;

  private int cntDest=100;

  volatile static boolean hooked = false;
  /**
   * @param name
   */
  public InitializeIndexEntryDestroyQueryDUnitTest() {
    super();
  }
  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQuery() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    name = "PartionedPortfolios";
    //Create Local Region
    vm0.invoke(new CacheSerializableRunnable("Create local region with asynchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region localRegion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setScope(Scope.LOCAL);
          attr.setIndexMaintenanceSynchronous(false);
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          localRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", localRegion);
        assertTrue("Region ref claims to be destroyed",
            !localRegion.isDestroyed());
      }
    });


    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
 
    AsyncInvocation asyInvk0 = vm0.invokeAsync(new CacheSerializableRunnable("Create Index with Hook") {

      @Override
      public void run2() throws CacheException {

        for (int i=0; i<cntDest; i++) {
          //Create Index first to go in hook.
          Cache cache = getCache();
          Index index = null;
          try {
            index = cache.getQueryService().createIndex("statusIndex", "p.status", "/"+name+" p");
          } catch (Exception e1) {
            e1.printStackTrace();
            fail("Index creation failed");
          }
          assertNotNull(index);

          Wait.pause(100);

          getCache().getQueryService().removeIndex(index);

          Wait.pause(100);
        }
      }
    });

    //Change the value in Region
    AsyncInvocation asyInvk1 = vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

      @Override
      public void run2() throws CacheException {
        // Do a put in region.
        Region r = getCache().getRegion(name);

        for (int i=0, j=0; i<1000; i++,j++) {

          PortfolioData p = (PortfolioData)r.get(j);

          getCache().getLogger().fine("Going to destroy the value" + p);
          r.destroy(j);

          Wait.pause(100);

          //Put the value back again.
          getCache().getLogger().fine("Putting the value back" + p);
          r.put(j, p);

          //Reset j
          if (j==cntDest-1) {
            j=0;
          }
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Run query on region") {

      @Override
      public void run2() throws CacheException {
        // Do a put in region.
        Region r = getCache().getRegion(name);

        Query query = getCache().getQueryService().newQuery("select * from /"+name+" p where p.status = 'active'");

        //Now run the query
        SelectResults results = null;


        for (int i=0; i<500; i++) {

          try {
            getCache().getLogger().fine("Querying the region");
            results = (SelectResults)query.execute();
          } catch (Exception e) {
            e.printStackTrace();
          }

          for (Object obj : results) {
            if (obj instanceof Undefined) {
              fail("Found an undefined element" + Arrays.toString(results.toArray()));
            }
          }
        }
      }
    });
    
    ThreadUtils.join(asyInvk0, 1000 * 1000);
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    ThreadUtils.join(asyInvk1, 1000 * 1000);
    if (asyInvk1.exceptionOccurred()) {
      Assert.fail("asyInvk1 failed", asyInvk1.getException());
    }
  }

  @Category(FlakyTest.class) // GEODE-1036: uses PRQueryDUnitHelper, time sensitive, async actions, overly long joins (16+ minutes), eats exceptions (fixed 1), thread sleeps
  @Test
  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    name = "PartionedPortfoliosPR";
    //Create Local Region
    vm0.invoke(new CacheSerializableRunnable("Create local region with asynchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region partitionRegion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setIndexMaintenanceSynchronous(false);
          attr.setPartitionAttributes(new PartitionAttributesFactory().create());
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          partitionRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", partitionRegion);
        assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
      }
    });


    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));

    AsyncInvocation asyInvk0 = vm0.invokeAsync(new CacheSerializableRunnable("Create Index with Hook") {

      @Override
      public void run2() throws CacheException {
        for (int i=0; i<cntDest; i++) {
          //Create Index first to go in hook.
          Cache cache = getCache();
          Index index = null;
          try {
            index = cache.getQueryService().createIndex("statusIndex", "p.status", "/"+name+" p");
          } catch (Exception e1) {
            e1.printStackTrace();
            Assert.fail("Index creation failed", e1);
          }
          assertNotNull(index);

          getCache().getQueryService().removeIndex(index);

        }
      }
    });

    //Change the value in Region
    AsyncInvocation asyInvk1 = vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

      @Override
      public void run2() throws CacheException {
        // Do a put in region.
        Region r = getCache().getRegion(name);

        for (int i=0, j=0; i<1000; i++,j++) {

          PortfolioData p = (PortfolioData)r.get(j);

          getCache().getLogger().fine("Going to destroy the value" + p);
          r.destroy(j);

          Wait.pause(20);

          //Put the value back again.
          getCache().getLogger().fine("Putting the value back" + p);
          r.put(j, p);

          //Reset j
          if (j==cntDest-1) {
            j=0;
          }
        }
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Run query on region") {

      @Override
      public void run2() throws CacheException {
        // Do a put in region.
        Query query = getCache().getQueryService().newQuery("select * from /"+name+" p where p.status = 'active'");

        //Now run the query
        SelectResults results = null;


        for (int i=0; i<500; i++) {

          try {
            getCache().getLogger().fine("Querying the region");
            results = (SelectResults)query.execute();
          } catch (Exception e) {
            e.printStackTrace(); // TODO: eats exceptions
          }

          for (Object obj : results) {
            if (obj instanceof Undefined) {
              fail("Found an undefined element" + Arrays.toString(results.toArray()));
            }
          }
        }
      }
    });

    ThreadUtils.join(asyInvk0, 1000 * 1000); // TODO: this is way too long: 16.67 minutes!
    if (asyInvk0.exceptionOccurred()) {
      Assert.fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    ThreadUtils.join(asyInvk1, 1000 * 1000); // TODO: this is way too long: 16.67 minutes!
    if (asyInvk1.exceptionOccurred()) {
      Assert.fail("asyInvk1 failed", asyInvk1.getException());
    }
  }

  @Test
  public void testConcurrentRemoveIndexAndQueryOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    setCacheInVMs(vm0);
    name = "PartionedPortfoliosPR";
    //Create Local Region
    vm0.invoke(new CacheSerializableRunnable("Create local region with asynchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        Region partitionRegion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setIndexMaintenanceSynchronous(false);
          attr.setPartitionAttributes(new PartitionAttributesFactory().create());
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          partitionRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          LogWriterUtils.getLogWriter().warning("Creation caught IllegalStateException", ex);
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", partitionRegion);
        assertTrue("Region ref claims to be destroyed",
            !partitionRegion.isDestroyed());
      }
    });


    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    vm0.invoke(new CacheSerializableRunnable("Create Index") {

      @Override
      public void run2() throws CacheException {

          //Create Index first to go in hook.
          Cache cache = getCache();
          Index sindex = null;
          Index iindex = null;
          Index pkindex = null;
          try {
            sindex = cache.getQueryService().createIndex("statusIndex", "p.status", "/" + name + " p");
            iindex = cache.getQueryService().createIndex("idIndex", "p.ID", "/" + name + " p");
            pkindex = cache.getQueryService().createIndex("pkidIndex", "p.pk", "/" + name + " p");
          } catch (Exception e1) {
            e1.printStackTrace();
            fail("Index creation failed");
          }
          assertNotNull(sindex);
          assertNotNull(iindex);
          assertNotNull(pkindex);        
      }
    });

    vm0.invoke(new CacheSerializableRunnable("Run query on region") {

      @Override
      public void run2() throws CacheException {
        // Do a put in region.
        Query query = getCache().getQueryService().newQuery("select * from /"+name+" p where p.status = 'active' and p.ID > 0 and p.pk != ' ' ");
        //Now run the query
        SelectResults results = null;

        for (int i=0; i<10; i++) {

          try {
            getCache().getLogger().fine("Querying the region with " + query);
            results = (SelectResults)query.execute();
          } catch (Exception e) {
            Assert.fail("Query: " + query + " execution failed with exception", e);
          }

          for (Object obj : results) {
            if (obj instanceof Undefined) {
              fail("Found an undefined element" + Arrays.toString(results.toArray()));
            }
          }
        }
      }
    });
    
    vm0.invoke(new CacheSerializableRunnable("Create Index") {

      @Override
      public void run2() throws CacheException {

        Region r = getCache().getRegion(name);

          //Create Index first to go in hook.
          getCache().getQueryService().removeIndexes(r);


      }
    });

  }
}
