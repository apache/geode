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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.UserTransaction;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 */
@Category(IntegrationTest.class)
public class UserTransactionImplJUnitTest {

  private static DistributedSystem ds;
  private static TransactionManagerImpl tm;

  private UserTransaction utx = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    ds = DistributedSystem.connect(props);
    tm = TransactionManagerImpl.getTransactionManager();
  }

  public static void afterClass() throws Exception {
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
  public void testBegin() throws Exception {
    utx.begin();
    Transaction txn = tm.getTransaction();
    assertNotNull("transaction not registered in the transaction map", txn);
    GlobalTransaction gtx = tm.getGlobalTransaction();
    assertNotNull("Global transaction not registered with the transaction manager", gtx);
    assertTrue("Transaction not added to the list", gtx.getTransactions().contains(txn));
    int status = gtx.getStatus();
    assertEquals("Transaction status not set to be active", Status.STATUS_ACTIVE, status);
    utx.commit();
  }

  @Test
  public void testCommit() throws Exception {
    utx.begin();
    utx.commit();
    Transaction txn = tm.getTransaction();
    assertNull("transaction not removed from map after commit", txn);
  }

  @Test
  public void testRollback() throws Exception {
    utx.begin();
    utx.rollback();

    Transaction txn = tm.getTransaction();
    assertNull("transaction not removed from map after rollback", txn);
  }

  @Test
  public void testSetRollbackOnly() throws Exception {
    utx.begin();
    utx.setRollbackOnly();
    GlobalTransaction gtx = tm.getGlobalTransaction();
    assertEquals("Status not marked for rollback", Status.STATUS_MARKED_ROLLBACK, gtx.getStatus());
    utx.rollback();
  }

  @Test
  public void testGetStatus() throws Exception {
    utx.begin();
    tm.setRollbackOnly();
    int status = utx.getStatus();
    assertEquals("Get status failed to get correct status", Status.STATUS_MARKED_ROLLBACK, status);
    utx.rollback();
  }

  @Test
  public void testThread() throws Exception {
    utx.begin();
    utx.commit();
    utx.begin();
    utx.commit();
  }
}
