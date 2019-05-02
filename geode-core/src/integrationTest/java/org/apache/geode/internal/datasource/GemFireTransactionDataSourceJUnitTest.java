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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
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

  private void pause(int msWait) {
    try {
      Thread.sleep(msWait);
    } catch (InterruptedException ignore) {
      fail("interrupted");
    }
  }

  @Test
  public void testExceptionHandlingRegisterTranxConnection() throws Exception {
    Context ctx = cache.getJNDIContext();
    GemFireTransactionDataSource ds =
        (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");

    GemFireConnectionPoolManager gcpm = (GemFireConnectionPoolManager) ds.getConnectionProvider();
    TranxPoolCacheImpl tpci = (TranxPoolCacheImpl) gcpm.getConnectionPoolCache();
    Connection conn = ds.getConnection();
    // get connection to activate clean thread, which will sleep for 20 seconds
    // also duration of connection is cca. 20 secs

    // wait for 10 secs (half of clean thread interval)
    pause(10000);

    // then get new connection on which exception will be throw.
    transactionManager = mock(TransactionManager.class);
    ds.setTransManager(transactionManager);
    when(transactionManager.getTransaction()).thenThrow(new SystemException("SQL exception"));

    Throwable thrown = catchThrowable(() -> {
      ds.getConnection();
    });

    assertThat(thrown)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("SQL exception");

    // wait for activation of clean thread
    await().untilAsserted(() -> assertTrue(conn.isClosed()));

    // Check that all connections are cleaned
    if (tpci.getActiveCacheSize() != 0) {
      fail("Number of active connections should be 0.");
    }

  }


  @Test
  public void testExceptionHandlingGetConnection() throws Exception {
    Context ctx = cache.getJNDIContext();
    GemFireTransactionDataSource ds =
        (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");

    GemFireConnectionPoolManager gcpm = (GemFireConnectionPoolManager) ds.getConnectionProvider();
    TranxPoolCacheImpl tpci = (TranxPoolCacheImpl) gcpm.getConnectionPoolCache();
    Connection conn = ds.getConnection();
    ds.getConnection();
    // get connection to activate clean thread, which will sleep for 20 seconds
    // also duration of connection is cca. 20 secs

    // wait for 10 secs (half of clean thread interval)
    pause(10000);

    // then get new connection on which exception will be throw.

    xads = mock(XADataSource.class);
    xaconn = mock(XAConnection.class);
    tpci.setXADataSource(xads);

    when(xads.getXAConnection(any(), any())).thenReturn(xaconn);
    when(xaconn.getConnection()).thenThrow(new SQLException("SQL exception2"));

    Throwable thrown = catchThrowable(() -> {
      ds.getConnection();
    });

    assertThat(thrown)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("SQL exception2");

    // wait for activation of clean thread
    await().untilAsserted(() -> assertTrue(conn.isClosed()));

    // Check that all connections are cleaned
    if (tpci.getActiveCacheSize() != 0) {
      fail("Number of active connections should be 0.");
    }
  }
}
