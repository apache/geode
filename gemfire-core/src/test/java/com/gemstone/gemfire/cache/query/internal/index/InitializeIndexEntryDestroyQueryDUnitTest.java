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
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Arrays;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.cache.query.internal.Undefined;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test creates a local region. Creates and removes index in a parallel running thread.
 * Then destroys and puts back entries in separated thread in the same region and runs
 * query parallely and checks for UNDEFINED values in result set of the query.
 *
 * @author shobhit
 *
 */
public class InitializeIndexEntryDestroyQueryDUnitTest extends CacheTestCase {

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  String name;

  final int redundancy = 0;

  final Portfolio portfolio = new Portfolio(1, 1);

  private int cnt=0;

  private int cntDest=100;

  volatile static boolean hooked = false;
  /**
   * @param name
   */
  public InitializeIndexEntryDestroyQueryDUnitTest(String name) {
    super(name);
  }

  public void testAsyncIndexInitDuringEntryDestroyAndQuery() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    name = "PartionedPortfolios";
    //Create Local Region
    vm0.invoke(new CacheSerializableRunnable("Create local region with asynchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();
        Region localRegion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setScope(Scope.LOCAL);
          attr.setIndexMaintenanceSynchronous(false);
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          localRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          getLogWriter().warning("Creation caught IllegalStateException", ex);
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", localRegion);
        assertTrue("Region ref claims to be destroyed",
            !localRegion.isDestroyed());
      }
    });


    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
 
    AsyncInvocation asyInvk0 = vm0.invokeAsync(new CacheSerializableRunnable("Create Index with Hook") {

      @Override
      public void run2() throws CacheException {

        Region r = PRQHelp.getCache().getRegion(name);

        for (int i=0; i<cntDest; i++) {
          //Create Index first to go in hook.
          Cache cache = PRQHelp.getCache();
          Index index = null;
          try {
            index = cache.getQueryService().createIndex("statusIndex", "p.status", "/"+name+" p");
          } catch (Exception e1) {
            e1.printStackTrace();
            fail("Index creation failed");
          }
          assertNotNull(index);

          pause(100);

          PRQHelp.getCache().getQueryService().removeIndex(index);

          pause(100);
        }
      }
    });

    //Change the value in Region
    AsyncInvocation asyInvk1 = vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();

        // Do a put in region.
        Region r = PRQHelp.getCache().getRegion(name);

        for (int i=0, j=0; i<1000; i++,j++) {

          PortfolioData p = (PortfolioData)r.get(j);

          PRQHelp.getCache().getLogger().fine("Going to destroy the value" + p);
          r.destroy(j);

          pause(100);

          //Put the value back again.
          PRQHelp.getCache().getLogger().fine("Putting the value back" + p);
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
        Cache cache = PRQHelp.getCache();

        // Do a put in region.
        Region r = PRQHelp.getCache().getRegion(name);

        Query query = PRQHelp.getCache().getQueryService().newQuery("select * from /"+name+" p where p.status = 'active'");

        //Now run the query
        SelectResults results = null;


        for (int i=0; i<500; i++) {

          try {
            PRQHelp.getCache().getLogger().fine("Querying the region");
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
    
    DistributedTestCase.join(asyInvk0, 1000 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    DistributedTestCase.join(asyInvk1, 1000 * 1000, getLogWriter());
    if (asyInvk1.exceptionOccurred()) {
      fail("asyInvk1 failed", asyInvk1.getException());
    }
  }

  public void testAsyncIndexInitDuringEntryDestroyAndQueryOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    name = "PartionedPortfoliosPR";
    //Create Local Region
    vm0.invoke(new CacheSerializableRunnable("Create local region with asynchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();
        Region partitionRegion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setIndexMaintenanceSynchronous(false);
          attr.setPartitionAttributes(new PartitionAttributesFactory().create());
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          partitionRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          getLogWriter().warning("Creation caught IllegalStateException", ex);
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", partitionRegion);
        assertTrue("Region ref claims to be destroyed", !partitionRegion.isDestroyed());
      }
    });


    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));

    AsyncInvocation asyInvk0 = vm0.invokeAsync(new CacheSerializableRunnable("Create Index with Hook") {

      @Override
      public void run2() throws CacheException {

        Region r = PRQHelp.getCache().getRegion(name);

        for (int i=0; i<cntDest; i++) {
          //Create Index first to go in hook.
          Cache cache = PRQHelp.getCache();
          Index index = null;
          try {
            index = cache.getQueryService().createIndex("statusIndex", "p.status", "/"+name+" p");
          } catch (Exception e1) {
            e1.printStackTrace();
            fail("Index creation failed");
          }
          assertNotNull(index);

          //pause(100);

          PRQHelp.getCache().getQueryService().removeIndex(index);

          //pause(100);
        }
      }
    });

    //Change the value in Region
    AsyncInvocation asyInvk1 = vm0.invokeAsync(new CacheSerializableRunnable("Change value in region") {

      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();

        // Do a put in region.
        Region r = PRQHelp.getCache().getRegion(name);

        for (int i=0, j=0; i<1000; i++,j++) {

          PortfolioData p = (PortfolioData)r.get(j);

          PRQHelp.getCache().getLogger().fine("Going to destroy the value" + p);
          r.destroy(j);

          pause(20);

          //Put the value back again.
          PRQHelp.getCache().getLogger().fine("Putting the value back" + p);
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
        Cache cache = PRQHelp.getCache();

        // Do a put in region.
        Region r = PRQHelp.getCache().getRegion(name);

        Query query = PRQHelp.getCache().getQueryService().newQuery("select * from /"+name+" p where p.status = 'active'");

        //Now run the query
        SelectResults results = null;


        for (int i=0; i<500; i++) {

          try {
            PRQHelp.getCache().getLogger().fine("Querying the region");
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

    DistributedTestCase.join(asyInvk0, 1000 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
    
    DistributedTestCase.join(asyInvk1, 1000 * 1000, getLogWriter());
    if (asyInvk1.exceptionOccurred()) {
      fail("asyInvk1 failed", asyInvk1.getException());
    }
  }

  public void testConcurrentRemoveIndexAndQueryOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    name = "PartionedPortfoliosPR";
    //Create Local Region
    vm0.invoke(new CacheSerializableRunnable("Create local region with asynchronous index maintenance") {
      @Override
      public void run2() throws CacheException {
        Cache cache = PRQHelp.getCache();
        Region partitionRegion = null;
        try {
          AttributesFactory attr = new AttributesFactory();
          attr.setValueConstraint(PortfolioData.class);
          attr.setIndexMaintenanceSynchronous(false);
          attr.setPartitionAttributes(new PartitionAttributesFactory().create());
          RegionFactory regionFactory = cache.createRegionFactory(attr.create());
          partitionRegion = regionFactory.create(name);
        } catch (IllegalStateException ex) {
          getLogWriter().warning("Creation caught IllegalStateException", ex);
        }
        assertNotNull("Region " + name + " not in cache", cache.getRegion(name));
        assertNotNull("Region ref null", partitionRegion);
        assertTrue("Region ref claims to be destroyed",
            !partitionRegion.isDestroyed());
      }
    });


    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    vm0.invoke(new CacheSerializableRunnable("Create Index") {

      @Override
      public void run2() throws CacheException {

        Region r = PRQHelp.getCache().getRegion(name);

          //Create Index first to go in hook.
          Cache cache = PRQHelp.getCache();
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
        Cache cache = PRQHelp.getCache();

        // Do a put in region.
        Region r = PRQHelp.getCache().getRegion(name);

        Query query = PRQHelp.getCache().getQueryService().newQuery("select * from /"+name+" p where p.status = 'active' and p.ID > 0 and p.pk != ' ' ");
        //Now run the query
        SelectResults results = null;

        for (int i=0; i<10; i++) {

          try {
            PRQHelp.getCache().getLogger().fine("Querying the region with " + query);
            results = (SelectResults)query.execute();
          } catch (Exception e) {
            fail("Query: " + query + " execution failed with exception", e);
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

        Region r = PRQHelp.getCache().getRegion(name);

          //Create Index first to go in hook.
          Cache cache = PRQHelp.getCache();
       
          PRQHelp.getCache().getQueryService().removeIndexes(r);

          //pause(100);
        
      }
    });

  }
}
