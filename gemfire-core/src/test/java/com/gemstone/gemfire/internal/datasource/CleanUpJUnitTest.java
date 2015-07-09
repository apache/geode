/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Mar 22, 2005
 */
package com.gemstone.gemfire.internal.datasource;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.util.Properties;

import javax.naming.Context;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

//import javax.sql.PooledConnection;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author mitulb
 * 
 * To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */
@Category(IntegrationTest.class)
public class CleanUpJUnitTest {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Before
  public void setUp() {
    props = new Properties();
    props.setProperty("mcast-port","0");
    String path = TestUtil.getResourcePath(CleanUpJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty("cache-xml-file", path);
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
  }

  @After
  public void tearDown() {
    ds1.disconnect();
  }

  @Test
  public void testGetSimpleDataSource() throws Exception {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx
          .lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      if (conn == null)
          fail("DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
    }
    catch (Exception e) {
      fail("Exception occured in testGetSimpleDataSource due to " + e);
      e.printStackTrace();
    }
  }

  /*
   * @Test
  public void testExpiration() { try { Context ctx = cache.getJNDIContext();
   * GemFireConnPooledDataSource ds = (GemFireConnPooledDataSource) ctx
   * .lookup("java:/PooledDataSource"); GemFireConnectionPoolManager provider =
   * (GemFireConnectionPoolManager) ds .getConnectionProvider();
   * ConnectionPoolCacheImpl poolCache = (ConnectionPoolCacheImpl) provider
   * .getConnectionPoolCache(); PooledConnection conn =
   * poolCache.getPooledConnectionFromPool();
   * poolCache.returnPooledConnectionToPool(conn);
   * Thread.sleep(poolCache.expirationTime * 2); if
   * (!(poolCache.availableCache.isEmpty())) { fail("Clean-up on expiration not
   * done"); } } catch (Exception e) { e.printStackTrace(); } }
   */
  @Test
  public void testBlockingTimeOut() {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireConnPooledDataSource ds = (GemFireConnPooledDataSource) ctx
          .lookup("java:/PooledDataSource");
      GemFireConnectionPoolManager provider = (GemFireConnectionPoolManager) ds
          .getConnectionProvider();
      ConnectionPoolCacheImpl poolCache = (ConnectionPoolCacheImpl) provider
          .getConnectionPoolCache();
      poolCache.getPooledConnectionFromPool();
      Thread.sleep(40000);
      if (!(poolCache.activeCache.isEmpty())) {
        fail("Clean-up on expiration not done");
      }
    }
    catch (Exception e) {
      fail("Exception occured in testBlockingTimeOut due to " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void testCleanUp() {
    cache.close();
    ds1.disconnect();
  }
}