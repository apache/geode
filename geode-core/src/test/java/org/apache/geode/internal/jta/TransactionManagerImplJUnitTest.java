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
package org.apache.geode.internal.jta;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import javax.transaction.*;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Test TransactionManagerImpl methods not tested by UserTransactionImplTest
 * 
 */
@Category(IntegrationTest.class)
public class TransactionManagerImplJUnitTest {

  private static DistributedSystem ds;
  private static TransactionManagerImpl tm;
  
  protected UserTransaction utx;
  
  @BeforeClass
  public static void beforeClass() throws Exception{
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    ds = DistributedSystem.connect(props);
    tm = TransactionManagerImpl.getTransactionManager();
  }
  
  @AfterClass
  public static void afterClass() {
    ds.disconnect();
    ds = null;
    tm = null;
  }

  @Before
  public void setUp() throws Exception {
    utx = new UserTransactionImpl();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testGetTransaction() throws Exception {
    assertEquals(Status.STATUS_NO_TRANSACTION, tm.getStatus());
    utx.begin();
    assertEquals(Status.STATUS_ACTIVE, tm.getStatus());
    Thread thread = Thread.currentThread();
    Transaction txn1 = (Transaction) tm.getTransactionMap().get(thread);
    Transaction txn2 = tm.getTransaction();
    assertTrue("GetTransaction not returning the correct transaction", txn1 == txn2);
    utx.commit();
    assertEquals(Status.STATUS_NO_TRANSACTION, tm.getStatus());
  }

  @Test
  public void testGetTransactionImpl() throws Exception {
    utx.begin();
    Thread thread = Thread.currentThread();
    Transaction txn1 = (Transaction) tm.getTransactionMap().get(thread);
    Transaction txn2 = tm.getTransaction();
    assertTrue("GetTransactionImpl not returning the correct transaction", txn1 == txn2);
    utx.commit();
  }

  @Test
  public void testGetGlobalTransaction() throws Exception {
    utx.begin();
    Thread thread = Thread.currentThread();
    Transaction txn = (Transaction) tm.getTransactionMap().get(thread);
    GlobalTransaction gTxn1 = (GlobalTransaction) tm.getGlobalTransactionMap().get(txn);
    GlobalTransaction gTxn2 = tm.getGlobalTransaction();
    assertTrue("Get Global Transaction not returning the correct global transaction", gTxn1 == gTxn2);
    utx.commit();
  }

  @Test
  public void testNotifyBeforeCompletionException() throws Exception {
    //Asif : Test Notify BeforeCompletion Exception handling
    utx.begin();
    Thread thread = Thread.currentThread();
    Transaction txn = (Transaction) tm.getTransactionMap().get(thread);
    txn.registerSynchronization(new Synchronization() {

      public void beforeCompletion() {
        throw new RuntimeException("MyException");
      }

      public void afterCompletion(int status) {
        assertTrue(status == Status.STATUS_ROLLEDBACK);
      }
    });
    try {
      utx.commit();
      fail("The commit should have thrown RolledBackException");
    } catch (RollbackException expected) {
      // success
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  @Test
  public void testSetRollBackOnly() throws Exception {
    //    Asif : Test Exception when marking transaction for rollbackonly
    utx.begin();
    Thread thread = Thread.currentThread();
    Transaction txn = (Transaction) tm.getTransactionMap().get(thread);
    txn.registerSynchronization(new Synchronization() {

      public void beforeCompletion() {
        try {
          utx.setRollbackOnly();
        } catch (Exception se) {
          fail("Set Roll Back only caused failure.Exception =" + se);
        }
      }

      public void afterCompletion(int status) {
        assertTrue(status == Status.STATUS_ROLLEDBACK);
      }
    });
    try {
      utx.commit();
      fail("The commit should have thrown RolledBackException");
    } catch (RollbackException expected) {
      // success
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  @Test
  public void testXAExceptionInCommit() throws Exception {
    utx.begin();
    Thread thread = Thread.currentThread();
    Transaction txn = (Transaction) tm.getTransactionMap().get(thread);
    txn.registerSynchronization(new Synchronization() {

      public void beforeCompletion() {
      }

      public void afterCompletion(int status) {
        assertTrue(status == Status.STATUS_ROLLEDBACK);
      }
    });
    txn.enlistResource(new XAResourceAdaptor() {

      public void commit(Xid arg0, boolean arg1) throws XAException {
        throw new XAException(5);
      }

      public void rollback(Xid arg0) throws XAException {
        throw new XAException(6);
      }
    });
    try {
      utx.commit();
      fail("The commit should have thrown SystemException");
    } catch (SystemException expected) {
      // success
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  @Test
  public void testXAExceptionRollback() throws Exception {
    utx.begin();
    Thread thread = Thread.currentThread();
    Transaction txn = (Transaction) tm.getTransactionMap().get(thread);
    txn.registerSynchronization(new Synchronization() {

      public void beforeCompletion() {
        fail("Notify Before Completion should not be called in rollback");
      }

      public void afterCompletion(int status) {
        assertTrue(status == Status.STATUS_ROLLEDBACK);
      }
    });
    txn.enlistResource(new XAResourceAdaptor() {

      public void commit(Xid arg0, boolean arg1) throws XAException {
      }

      public void rollback(Xid arg0) throws XAException {
        throw new XAException(6);
      }
    });
    try {
      utx.rollback();
      fail("The rollback should have thrown SystemException");
    } catch (SystemException expected) {
      // success
    }
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
  }

  @Test
  public void testRollback() throws Exception{
    utx.begin();
    Thread thread = Thread.currentThread();
    Transaction txn = (Transaction) tm.getTransactionMap().get(thread);
    txn.registerSynchronization(new Synchronization() {

      public void beforeCompletion() {
        fail("Notify Before Completion should not be called in rollback");
      }

      public void afterCompletion(int status) {
        assertTrue(status == Status.STATUS_ROLLEDBACK);
      }
    });
    txn.enlistResource(new XAResourceAdaptor() {
    });
    utx.rollback();
    assertTrue(tm.getGlobalTransactionMap().isEmpty());
    System.out.println("RolledBack successfully");
  }
}

//Asif :Added helper class to test XAException situations while
// commit/rollback etc
class XAResourceAdaptor implements XAResource {

  public int getTransactionTimeout() throws XAException {
    return 0;
  }

  public boolean setTransactionTimeout(int arg0) throws XAException {
    return false;
  }

  public boolean isSameRM(XAResource arg0) throws XAException {
    return false;
  }

  public Xid[] recover(int arg0) throws XAException {
    return null;
  }

  public int prepare(Xid arg0) throws XAException {
    return 0;
  }

  public void forget(Xid arg0) throws XAException {
  }

  public void rollback(Xid arg0) throws XAException {
  }

  public void end(Xid arg0, int arg1) throws XAException {
  }

  public void start(Xid arg0, int arg1) throws XAException {
  }

  public void commit(Xid arg0, boolean arg1) throws XAException {
  }
}
