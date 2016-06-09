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
package com.gemstone.gemfire.internal.jta;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Moved some non-DUnit tests over from com/gemstone/gemfire/internal/jta/dunit/JTADUnitTest
 * 
 */
@Category(IntegrationTest.class)
public class JtaIntegrationJUnitTest {

  private static final Logger logger = LogService.getLogger();
  
  @After
  public void tearDown() {
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }
  
  @Test
  public void testBug43987() {
    //InternalDistributedSystem ds = getSystem(); // ties us in to the DS owned by DistributedTestCase.
    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0");//(ds.getProperties());
    Cache cache = cf.create(); // should just reuse the singleton DS owned by DistributedTestCase.
    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<String, String> r = rf.create("JTA_reg");
    r.put("key", "value");
    cache.close();
    cache = cf.create();
    RegionFactory<String, String> rf1 = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<String, String> r1 = rf1.create("JTA_reg");
    r1.put("key1", "value");
  }
  
  @Test
  public void testBug46169() throws Exception {
    String tableName = CacheUtils.init("CacheTest");
    assertFalse(tableName == null || tableName.equals(""));
    logger.debug("Table name: " + tableName);

    logger.debug("init for bug46169 Successful!");
    Cache cache = CacheUtils.getCache();
    
    TransactionManager xmanager = (TransactionManager) cache.getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(xmanager);
    
    Transaction trans = xmanager.suspend();
    assertNull(trans);

    try {
      logger.debug("Destroying table: " + tableName);
      CacheUtils.destroyTable(tableName);
      logger.debug("Closing cache...");
      logger.debug("destroyTable for bug46169 Successful!");
    } finally {
      CacheUtils.closeCache();
    }
  }

  @Test
  public void testBug46192() throws Exception {
    String tableName = CacheUtils.init("CacheTest");
    assertFalse(tableName == null || tableName.equals(""));
    logger.debug("Table name: " + tableName);
    
    logger.debug("init for bug46192 Successful!");
    Cache cache = CacheUtils.getCache();
    
    TransactionManager xmanager = (TransactionManager) cache.getJNDIContext().lookup("java:/TransactionManager");
    assertNotNull(xmanager);
    
    try {
      xmanager.rollback();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // passed
    }
    
    try {
      xmanager.commit();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      // passed
    }

    try {
      logger.debug("Destroying table: " + tableName);
      CacheUtils.destroyTable(tableName);
      logger.debug("Closing cache...");
      logger.debug("destroyTable for bug46192 Successful!");
    } finally {
      CacheUtils.closeCache();
    }
  }
}
