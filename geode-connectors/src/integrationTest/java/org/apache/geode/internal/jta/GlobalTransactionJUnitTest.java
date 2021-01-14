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
package org.apache.geode.internal.jta;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.datasource.GemFireBasicDataSource;
import org.apache.geode.internal.datasource.GemFireTransactionDataSource;

public class GlobalTransactionJUnitTest {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;
  private static UserTransaction utx = null;
  private static TransactionManager tm = null;

  @Before
  public void setUp() throws Exception {
    props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    String path =
        createTempFileFromResource(GlobalTransactionJUnitTest.class, "/jta/cachejta.xml")
            .getAbsolutePath();
    props.setProperty(CACHE_XML_FILE, path);
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    utx = new UserTransactionImpl();
    tm = TransactionManagerImpl.getTransactionManager();
  }

  @After
  public void tearDown() throws Exception {
    ds1.disconnect();
  }

  @Test
  public void testGetSimpleDataSource() throws Exception {
    Context ctx = cache.getJNDIContext();
    GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx.lookup("java:/SimpleDataSource");
    Connection conn = ds.getConnection();
    if (conn == null) {
      fail(
          "DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
    }
  }

  @Test
  public void testSetRollbackOnly() throws Exception {
    utx.begin();
    utx.setRollbackOnly();
    Transaction txn = tm.getTransaction();
    if (txn.getStatus() != Status.STATUS_MARKED_ROLLBACK) {
      utx.rollback();
      fail("testSetRollbackonly failed");
    }
    utx.rollback();
  }

  @Test
  public void testEnlistResource() throws Exception {
    try {
      boolean exceptionoccurred = false;
      utx.begin();
      try {
        Context ctx = cache.getJNDIContext();
        GemFireTransactionDataSource ds =
            (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");
        ds.getConnection();
      } catch (SQLException e) {
        exceptionoccurred = true;
      }
      if (exceptionoccurred) {
        fail("SQLException occurred while trying to enlist resource");
      }
      utx.rollback();
    } catch (Exception e) {
      try {
        utx.rollback();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testEnlistResource due to " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void testRegisterSynchronization() throws Exception {
    try {
      boolean exceptionoccurred = false;
      utx.begin();
      try {
        Transaction txn = tm.getTransaction();
        Synchronization sync = new SyncImpl();
        txn.registerSynchronization(sync);
      } catch (RollbackException e) {
        exceptionoccurred = true;
      }
      if (exceptionoccurred) {
        fail("exception occurred while trying to register synchronization ");
      }
      utx.rollback();
    } catch (Exception e) {
      try {
        utx.rollback();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testRegisterSynchronization due to " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void testEnlistResourceAfterRollBack() throws Exception {
    try {
      boolean exceptionoccurred = false;
      utx.begin();
      utx.setRollbackOnly();
      Context ctx = cache.getJNDIContext();
      try {
        GemFireTransactionDataSource ds =
            (GemFireTransactionDataSource) ctx.lookup("java:/XAPooledDataSource");
        ds.getConnection();
      } catch (SQLException e) {
        exceptionoccurred = true;
      }
      if (!exceptionoccurred) {
        fail("SQLException not occurred although the transaction was marked for rollback");
      }
      utx.rollback();
    } catch (Exception e) {
      try {
        utx.rollback();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testSetRollbackonly due to " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void testRegisterSynchronizationAfterRollBack() throws Exception {
    try {
      boolean exceptionoccurred = false;
      utx.begin();
      utx.setRollbackOnly();
      Context ctx = cache.getJNDIContext();
      try {
        Transaction txn = tm.getTransaction();
        Synchronization sync = new SyncImpl();
        txn.registerSynchronization(sync);
      } catch (RollbackException e) {
        exceptionoccurred = true;
      }
      if (!exceptionoccurred) {
        fail("RollbackException not occurred although the transaction was marked for rollback");
      }
      utx.rollback();
    } catch (Exception e) {
      try {
        utx.rollback();
      } catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testSetRollbackonly due to " + e);
      e.printStackTrace();
    }
  }

  @Test
  public void testSuspend() throws Exception {
    utx.begin();
    tm.suspend();
    Transaction txn1 = tm.getTransaction();
    if (txn1 != null) {
      fail("suspend failed to suspend the transaction");
    }
  }

  @Test
  public void testResume() throws Exception {
    utx.begin();
    Transaction txn = tm.getTransaction();
    Transaction txn1 = tm.suspend();
    tm.resume(txn1);
    if (txn != tm.getTransaction()) {
      fail("resume failed ");
    }
    utx.commit();
  }
}
