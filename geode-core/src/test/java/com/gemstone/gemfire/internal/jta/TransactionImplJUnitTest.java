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
/*
 * Test TransactionImpl methods not tested by UserTransactionImpl
 * 
 */
package com.gemstone.gemfire.internal.jta;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import javax.transaction.Synchronization;
import javax.transaction.UserTransaction;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertTrue;

/**
 */
@Category(IntegrationTest.class)
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
  public void tearDown() {
  }

  @Test
  public void testRegisterSynchronization() throws Exception {
    utx.begin();
    TransactionImpl txn = (TransactionImpl) tm.getTransaction();
    Synchronization sync = new SyncImpl();
    txn.registerSynchronization(sync);
    assertTrue("Synchronization not registered succesfully", txn.getSyncList().contains(sync));
    utx.commit();
  }

  @Test
  public void testNotifyBeforeCompletion() throws Exception {
    utx.begin();
    TransactionImpl txn = (TransactionImpl) tm.getTransaction();
    SyncImpl sync = new SyncImpl();
    txn.registerSynchronization(sync);
    txn.notifyBeforeCompletion();
    assertTrue("Notify before completion not executed succesfully", sync.befCompletion);
    utx.commit();
  }

  @Test
  public void testNotifyAfterCompletion() throws Exception {
    utx.begin();
    TransactionImpl txn = (TransactionImpl) tm.getTransaction();
    SyncImpl sync = new SyncImpl();
    txn.registerSynchronization(sync);
    txn.notifyAfterCompletion(1);
    assertTrue("Notify after completion not executed succesfully", sync.aftCompletion);
    utx.commit();
  }
}
