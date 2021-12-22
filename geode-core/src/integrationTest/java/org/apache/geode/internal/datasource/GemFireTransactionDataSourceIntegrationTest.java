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
package org.apache.geode.internal.datasource;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;

public class GemFireTransactionDataSourceIntegrationTest {

  private Cache cache;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    String derbySystemHome = temporaryFolder.newFolder("derby").getAbsolutePath();
    System.setProperty("derby.system.home", derbySystemHome);

    String cacheXmlFileName = getClass().getSimpleName() + "_cachejta.xml";
    String cacheXmlPath = createFileFromResource(
        getResource(cacheXmlFileName),
        temporaryFolder.getRoot(),
        cacheXmlFileName).getAbsolutePath();

    Properties props = new Properties();
    props.setProperty(CACHE_XML_FILE, cacheXmlPath);
    props.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");

    cache = new CacheFactory(props).create();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  public void testExceptionHandlingRegisterTranxConnection() throws Exception {
    Context context = cache.getJNDIContext();
    GemFireTransactionDataSource dataSource =
        (GemFireTransactionDataSource) context.lookup("java:/XAPooledDataSource");

    GemFireConnectionPoolManager poolManager =
        (GemFireConnectionPoolManager) dataSource.getConnectionProvider();
    TranxPoolCacheImpl poolCache = (TranxPoolCacheImpl) poolManager.getConnectionPoolCache();
    Connection connection = dataSource.getConnection();
    // get connection to activate clean thread, which will sleep for 20 seconds
    // also duration of connection is cca. 20 secs

    // get new connection on which exception will be throw.
    TransactionManager transactionManager = mock(TransactionManager.class);
    dataSource.setTransactionManager(transactionManager);

    when(transactionManager.getTransaction()).thenThrow(new SystemException("SQL exception"));

    Throwable thrown = catchThrowable(dataSource::getConnection);

    assertThat(thrown)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("SQL exception");

    // wait for activation of clean thread
    await()
        // BrokeredConnection.isClosed() throws a SQLException if the clean thread closes the
        // connection while we are in the middle of checking whether it is closed.
        .ignoreExceptionsInstanceOf(SQLException.class)
        .untilAsserted(() -> assertThat(connection.isClosed()).isTrue());

    // Check that all connections are cleaned
    assertThat(poolCache.getActiveCacheSize()).isZero();
  }

  @Test
  public void testExceptionHandlingGetConnection() throws Exception {
    Context context = cache.getJNDIContext();
    GemFireTransactionDataSource dataSource =
        (GemFireTransactionDataSource) context.lookup("java:/XAPooledDataSource");

    GemFireConnectionPoolManager poolManager =
        (GemFireConnectionPoolManager) dataSource.getConnectionProvider();
    TranxPoolCacheImpl poolCache = (TranxPoolCacheImpl) poolManager.getConnectionPoolCache();
    Connection connection = dataSource.getConnection();
    dataSource.getConnection();
    // get connection to activate clean thread, which will sleep for 20 seconds
    // also duration of connection is cca. 20 secs

    // get new connection on which exception will be throw.
    XADataSource xaDataSource = mock(XADataSource.class);
    XAConnection xaConnection = mock(XAConnection.class);
    poolCache.setXADataSource(xaDataSource);

    when(xaDataSource.getXAConnection(any(), any())).thenReturn(xaConnection);
    when(xaConnection.getConnection()).thenThrow(new SQLException("SQL exception2"));

    Throwable thrown = catchThrowable(dataSource::getConnection);

    assertThat(thrown)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("SQL exception2");

    // wait for activation of clean thread
    await()
        // BrokeredConnection.isClosed() throws a SQLException if the clean thread closes the
        // connection while we are in the middle of checking whether it is closed.
        .ignoreExceptionsInstanceOf(SQLException.class)
        .untilAsserted(() -> assertThat(connection.isClosed()).isTrue());

    // Check that all connections are cleaned
    assertThat(poolCache.getActiveCacheSize()).isZero();
  }
}
