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
/*
 * GemFireTransactionDataSourceJUnitTest.java JUnit based test
 *
 * Created on May 2, 2019, 1:24 PM
 */
package org.apache.geode.internal.datasource;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.util.test.TestUtil;

public class GemFireTransactionDataSourceJUnitTest {

  private TransactionManager transactionManager;
  private XAConnection xaconn;
  private XADataSource xads;

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;

  @Before
  public void setUp() throws Exception {
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    String path =
        TestUtil.getResourcePath(GemFireTransactionDataSourceJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty(CACHE_XML_FILE, path);
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
  }

  @After
  public void tearDown() throws Exception {
    ds1.disconnect();
  }

  @Test
  public void testExceptionHandlingRegisterTranxConnection() throws Exception {
    boolean exceptionoccurred = false;
    Context ctx = cache.getJNDIContext();
    GemFireTransactionDataSource ds =
        (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");

    GemFireConnectionPoolManager gcpm = (GemFireConnectionPoolManager) ds.getConnectionProvider();
    TranxPoolCacheImpl tpci = (TranxPoolCacheImpl) gcpm.getConnectionPoolCache();
    ds.getConnection();
    // get connection to activate clean thread, which will sleep for 20 seconds

    Thread.sleep(10000);

    try {
      transactionManager = mock(TransactionManager.class);
      ds.setTransManager(transactionManager);
      when(transactionManager.getTransaction()).thenThrow(new SystemException("SQL exception"));
      ds.getConnection();
    } catch (SQLException e) {
      exceptionoccurred = true;
    }
    if (!exceptionoccurred) {
      fail("No exception recived, but expexted");
    }
    Thread.sleep(15000);
    // wait for activation of clean thread
    if (tpci.getActiveCacheSize() != 0) {
      fail("Number of active connections should be 0.");
    }
  }


  @Test
  public void testExceptionHandlingGetConnection() throws Exception {
    boolean exceptionoccurred = false;
    Context ctx = cache.getJNDIContext();
    GemFireTransactionDataSource ds =
        (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");

    GemFireConnectionPoolManager gcpm = (GemFireConnectionPoolManager) ds.getConnectionProvider();
    TranxPoolCacheImpl tpci = (TranxPoolCacheImpl) gcpm.getConnectionPoolCache();
    ds.getConnection();
    ds.getConnection();
    // get connection to activate clean thread, which will sleep for 20 seconds, and
    // get all connections from init pool.
    Thread.sleep(10000);

    try {
      xads = mock(XADataSource.class);
      xaconn = mock(XAConnection.class);
      tpci.setXADataSource(xads);

      when(xads.getXAConnection(any(), any())).thenReturn(xaconn);
      when(xaconn.getConnection()).thenThrow(new SQLException("SQL exception2"));
      ds.getConnection();
    } catch (SQLException e) {
      exceptionoccurred = true;
    }

    if (!exceptionoccurred) {
      fail("No exception recived, but expexted");
    }
    Thread.sleep(15000);
    // wait for activation of clean thread
    if (tpci.getActiveCacheSize() != 0) {
      fail("Number of active connections should be 0.");
    }
  }
}
