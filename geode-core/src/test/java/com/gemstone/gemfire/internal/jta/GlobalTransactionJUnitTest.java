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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.datasource.GemFireBasicDataSource;
import com.gemstone.gemfire.internal.datasource.GemFireTransactionDataSource;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;
import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

import javax.naming.Context;
import javax.transaction.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;

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
    props.setProperty(MCAST_PORT, "0");
    String path = TestUtil.getResourcePath(GlobalTransactionJUnitTest.class, "/jta/cachejta.xml");
    props.setProperty(CACHE_XML_FILE, path);
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
