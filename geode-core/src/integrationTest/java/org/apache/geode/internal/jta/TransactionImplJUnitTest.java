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
 * Test TransactionImpl methods not tested by UserTransactionImpl
 *
 */
package org.apache.geode.internal.jta;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import javax.transaction.Synchronization;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.distributed.DistributedSystem;

public class TransactionImplJUnitTest {

  private static DistributedSystem ds;
  private static TransactionManagerImpl tm;

  private UserTransaction utx;

  @BeforeClass
  public static void beforeClass() throws Exception {
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
  public void tearDown() {}

  @Test
  public void testRegisterSynchronization() throws Exception {
    utx.begin();
    TransactionImpl txn = (TransactionImpl) tm.getTransaction();
    Synchronization sync = new SyncImpl();
    txn.registerSynchronization(sync);
    assertTrue("Synchronization not registered successfully", txn.getSyncList().contains(sync));
    utx.commit();
  }

  @Test
  public void testNotifyBeforeCompletion() throws Exception {
    utx.begin();
    TransactionImpl txn = (TransactionImpl) tm.getTransaction();
    SyncImpl sync = new SyncImpl();
    txn.registerSynchronization(sync);
    txn.notifyBeforeCompletion();
    assertTrue("Notify before completion not executed successfully", sync.befCompletion);
    utx.commit();
  }

  @Test
  public void testNotifyAfterCompletion() throws Exception {
    utx.begin();
    TransactionImpl txn = (TransactionImpl) tm.getTransaction();
    SyncImpl sync = new SyncImpl();
    txn.registerSynchronization(sync);
    txn.notifyAfterCompletion(1);
    assertTrue("Notify after completion not executed successfully", sync.aftCompletion);
    utx.commit();
  }
}
