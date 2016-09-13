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
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class IndexCreationDeadLockJUnitTest {

  private static final String indexName = "queryTest";

  private boolean testFailed = false;

  private String cause = "";

  private boolean exceptionInCreatingIndex = false;

  private Region region;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    this.testFailed = false;
    this.cause = "";
    exceptionInCreatingIndex = false;
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);

    factory.setIndexMaintenanceSynchronous(true);
    region = CacheUtils.createRegion("portfolios", factory
        .create(), true);

  }

  @After
  public void tearDown() throws Exception {
    try{
      this.region.localDestroyRegion();
    }catch(RegionDestroyedException  rde) {
      //Ignore
    }

    CacheUtils.closeCache();
  }

  /**
   * Tests Index creation and maintenance deadlock scenario for in memory region
   */
  @Test
  public void testIndexCreationDeadLock() throws Exception {
    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertFalse("Index creation failed", this.exceptionInCreatingIndex);
  }

  /**
   * Tests  Index creation and maintenance deadlock scenario for Persistent only disk region
   */
  @Test
  public void testIndexCreationDeadLockForDiskOnlyRegion() {
    this.region.destroyRegion();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setIndexMaintenanceSynchronous(true);
    File dir = new File("test");
    dir.mkdir();
    DiskStoreFactory dsf = region.getCache().createDiskStoreFactory();
    DiskStore ds1 = dsf.setDiskDirs(new File[] {dir}).create("ds1");
    factory.setDiskStoreName("ds1");
    dir.deleteOnExit();
    region = CacheUtils.createRegion("portfolios", factory
        .create(), true);
    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertFalse("Index creation failed", this.exceptionInCreatingIndex);
  }

  /**
   * Tests  Index creation and maintenance deadlock scenario for a region with stats enabled
   */
  @Test
  public void testIndexCreationDeadLockForStatsEnabledRegion() {
    this.region.destroyRegion();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);
    factory.setStatisticsEnabled(true);
    factory.setIndexMaintenanceSynchronous(true);
    region = CacheUtils.createRegion("portfolios", factory
        .create(), true);
    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertFalse("Index creation failed", this.exceptionInCreatingIndex);
  }

  /**
   * Tests inability to create index on a region which overflows to disk   *
   */
  @Test
  public void testIndexCreationDeadLockForOverflowToDiskRegion() {
    this.region.destroyRegion();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setValueConstraint(Portfolio.class);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
        1, EvictionAction.OVERFLOW_TO_DISK));
    factory.setIndexMaintenanceSynchronous(true);
    File dir = new File("test");
    dir.mkdir();
    DiskStoreFactory dsf = region.getCache().createDiskStoreFactory();
    DiskStore ds1 = dsf.setDiskDirs(new File[] {dir}).create("ds1");
    factory.setDiskStoreName("ds1");
    dir.deleteOnExit();
    region = CacheUtils.createRegion("portfolios", factory.create(), true);
    simulateDeadlockScenario();
    assertFalse(this.cause, this.testFailed);
    assertTrue(
        "Index creation succeeded . For diskRegion this shoudl not have happened",
        this.exceptionInCreatingIndex);
  }

  private void simulateDeadlockScenario() {
    Thread th = new IndexCreationDeadLockJUnitTest.PutThread("put thread");
    th.start();
    ThreadUtils.join(th, 60 * 1000);
  }

  /**
   * following thread will perform the operations of data population and index creation.
   */
  private class HelperThread extends Thread {

    public HelperThread(String thName) {
      super(thName);

      System.out
          .println("--------------------- Thread started ------------------------- "
              + thName);
    }

    @Override
    public void run() {
      try {

        System.out
            .println("--------------------- Creating Indices -------------------------");
        QueryService qs;
        qs = CacheUtils.getQueryService();
        qs.createIndex("status", IndexType.FUNCTIONAL, "pf.status",
            "/portfolios pf, pf.positions.values posit");

        qs.createIndex("secId", IndexType.FUNCTIONAL, "posit.secId",
            "/portfolios pf, pf.positions.values posit");

        System.out
            .println("--------------------- Index Creation Done-------------------------");
      }
      catch (Exception e) {
        exceptionInCreatingIndex = true;
      }
    }
  }

  /**
   * thread to put the entries in region
   */
  private class PutThread extends Thread {

    public PutThread(String thName) {
      super(thName);
      System.out
          .println("--------------------- Thread started ------------------------- "
              + thName);
    }

    @Override
    public void run() {
      try {
        System.out
            .println("--------------------- Populating Data -------------------------");
        for (int i = 0; i < 10; i++) {
          region.put(String.valueOf(i), new Portfolio(i));
          Portfolio value = (Portfolio)region.get(String.valueOf(i));
          CacheUtils.log("value for key " + i + " is: " + value);
          CacheUtils.log("region.size(): - " + region.size());
        }
        System.out
            .println("--------------------- Data Populatio done -------------------------");

        System.out
            .println("---------------------Destroying & repopulating the data -------------------------");
        AttributesMutator mutator = IndexCreationDeadLockJUnitTest.this.region
            .getAttributesMutator();
        mutator.setCacheWriter(new BeforeUpdateCallBack());
        CacheUtils.log("region.size(): - " + region.size());
        for (int i = 0; i < 10; i++) {
          region.destroy(String.valueOf(i));
          region.put(String.valueOf(i), new Portfolio(i + 20));
        }
      }
      catch (Exception e) {
        e.printStackTrace();
        IndexCreationDeadLockJUnitTest.this.testFailed = true;
        IndexCreationDeadLockJUnitTest.this.cause = "Test failed because of exception="
            + e;
      }
    }
  }

  /**
   *  make the update to wait for a while before updatation to simulate the deadlock condiction
   */
  private class BeforeUpdateCallBack extends CacheWriterAdapter {

    int cnt = 0;

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {
      cnt++;
      if (cnt == 10) {
        System.out
            .println("--------------------- starting IndexCreation Thread-------------------------");
        Thread indxCreationThread = new HelperThread("index creator thread");
        indxCreationThread.start();
        try {
          ThreadUtils.join(indxCreationThread, 30 * 1000);
        }
        catch (Exception e) {
          e.printStackTrace();
          IndexCreationDeadLockJUnitTest.this.testFailed = true;
          IndexCreationDeadLockJUnitTest.this.cause = "Test failed because of exception="
              + e;
          fail(e.toString());
        }
      }
    }
  }
}
