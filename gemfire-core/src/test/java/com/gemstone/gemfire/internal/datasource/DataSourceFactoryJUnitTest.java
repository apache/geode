/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

import java.sql.Connection;
import java.util.Properties;

import javax.naming.Context;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/*
 * @author mitulb
 *  
 */
@Category(IntegrationTest.class)
public class DataSourceFactoryJUnitTest {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Before
  public void setUp() {
    props = new Properties();
    props.setProperty("mcast-port","0");
    String path = TestUtil.getResourcePath(DataSourceFactoryJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty("cache-xml-file",path);
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
        fail("DataSourceFactoryJUnitTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
    }
    catch (Exception e) {
      fail("Exception occured in testGetSimpleDataSource due to "+e);
      e.printStackTrace();
    }
  }

  @Test
  public void testGetPooledDataSource() throws Exception {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireConnPooledDataSource ds = (GemFireConnPooledDataSource) ctx
          .lookup("java:/PooledDataSource");
      Connection conn = ds.getConnection();
      if (conn == null)
        fail("DataSourceFactoryJUnitTest-testGetPooledDataSource() Error in creating the GemFireConnPooledDataSource");
    }
    catch (Exception e) {
      fail("Exception occured in testGetPooledDataSource due to "+e);
      e.printStackTrace();
    }
  }

  @Test
  public void testGetTranxDataSource() throws Exception {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
          .lookup("java:/XAPooledDataSource");
      //DataSourceFactory dsf = new DataSourceFactory();
      //GemFireTransactionDataSource ds =
      // (GemFireTransactionDataSource)dsf.getTranxDataSource(map);
      Connection conn = ds.getConnection();
      if (conn == null)
        fail("DataSourceFactoryJUnitTest-testGetTranxDataSource() Error in creating the getTranxDataSource");
    }
    catch (Exception e) {
      fail("Exception occured in testGetTranxDataSource due to "+e);
      e.printStackTrace();
    }
  }
}
