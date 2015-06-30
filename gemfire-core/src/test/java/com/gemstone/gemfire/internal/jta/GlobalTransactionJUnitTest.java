/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.jta;

import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.Context;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import javax.transaction.Status;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.datasource.GemFireBasicDataSource;
import com.gemstone.gemfire.internal.datasource.GemFireTransactionDataSource;
import com.gemstone.gemfire.internal.datasource.RestartJUnitTest;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class GlobalTransactionJUnitTest extends TestCase {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;
  private static UserTransaction utx = null;
  private static TransactionManager tm = null;

  @Override
  protected void setUp() throws Exception {
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    String path = TestUtil.getResourcePath(GlobalTransactionJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty("cache-xml-file", path);
    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    utx = new UserTransactionImpl();
    tm = TransactionManagerImpl.getTransactionManager();
  }

  @Override
  protected void tearDown() throws Exception {
    ds1.disconnect();
  }

  public GlobalTransactionJUnitTest(String name) {
    super(name);
  }

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

  public void testSetRollbackOnly() {
    try {
      utx.begin();
      utx.setRollbackOnly();
      Transaction txn = tm.getTransaction();
      if (txn.getStatus() != Status.STATUS_MARKED_ROLLBACK) {
        utx.rollback();
        fail("testSetRollbackonly failed");
      }
      utx.rollback();
    }
    catch (Exception e) {
      fail("exception in testSetRollbackonly due to " + e);
      e.printStackTrace();
    }
  }

  public void testEnlistResource() {
    try {
      boolean exceptionoccured = false;
      utx.begin();
      try {
        Context ctx = cache.getJNDIContext();
        GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
            .lookup("java:/XAPooledDataSource");
        ds.getConnection();
      }
      catch (SQLException e) {
        exceptionoccured = true;
      }
      if (exceptionoccured)
        fail("SQLException occured while trying to enlist resource");
      utx.rollback();
    }
    catch (Exception e) {
      try {
        utx.rollback();
      }
      catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testEnlistResource due to " + e);
      e.printStackTrace();
    }
  }

  public void testRegisterSynchronization() {
    try {
      boolean exceptionoccured = false;
      utx.begin();
      try {
        Transaction txn = tm.getTransaction();
        Synchronization sync = new SyncImpl();
        txn.registerSynchronization(sync);
      }
      catch (RollbackException e) {
        exceptionoccured = true;
      }
      if (exceptionoccured)
        fail("exception occured while trying to register synchronization ");
      utx.rollback();
    }
    catch (Exception e) {
      try {
        utx.rollback();
      }
      catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testRegisterSynchronization due to " + e);
      e.printStackTrace();
    }
  }

  public void testEnlistResourceAfterRollBack() {
    try {
      boolean exceptionoccured = false;
      utx.begin();
      utx.setRollbackOnly();
      Context ctx = cache.getJNDIContext();
      try {
        GemFireTransactionDataSource ds = (GemFireTransactionDataSource) ctx
            .lookup("java:/XAPooledDataSource");
        ds.getConnection();
      }
      catch (SQLException e) {
        exceptionoccured = true;
      }
      if (!exceptionoccured)
        fail("SQLException not occured although the transaction was marked for rollback");
      utx.rollback();
    }
    catch (Exception e) {
      try {
        utx.rollback();
      }
      catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testSetRollbackonly due to " + e);
      e.printStackTrace();
    }
  }

  public void testRegisterSynchronizationAfterRollBack() {
    try {
      boolean exceptionoccured = false;
      utx.begin();
      utx.setRollbackOnly();
      Context ctx = cache.getJNDIContext();
      try {
        Transaction txn = tm.getTransaction();
        Synchronization sync = new SyncImpl();
        txn.registerSynchronization(sync);
      }
      catch (RollbackException e) {
        exceptionoccured = true;
      }
      if (!exceptionoccured)
        fail("RollbackException not occured although the transaction was marked for rollback");
      utx.rollback();
    }
    catch (Exception e) {
      try {
        utx.rollback();
      }
      catch (Exception e1) {
        e1.printStackTrace();
      }
      fail("exception in testSetRollbackonly due to " + e);
      e.printStackTrace();
    }
  }

  public void testSuspend() {
    try {
      utx.begin();
//      Transaction txn = tm.getTransaction();
      tm.suspend();
      Transaction txn1 = tm.getTransaction();
      if (txn1 != null)
        fail("suspend failed to suspend the transaction");
    }
    catch (Exception e) {
      fail("exception in testSuspend due to " + e);
      e.printStackTrace();
    }
  }

  public void testResume() {
    try {
      utx.begin();
      Transaction txn = tm.getTransaction();
      Transaction txn1 = tm.suspend();
      tm.resume(txn1);
      if (txn != tm.getTransaction())
        fail("resume failed ");
      utx.commit();
    }
    catch (Exception e) {
      fail("exception in testSuspend due to " + e);
      e.printStackTrace();
    }
  }
}
