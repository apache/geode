/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Test TransactionImpl methods not tested by UserTransactionImpl
 * 
 * @author Mitul Bid
 */
package com.gemstone.gemfire.internal.jta;

import static org.junit.Assert.assertTrue;

import java.util.Properties;

import javax.transaction.Synchronization;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author unknown
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class TransactionImplJUnitTest {

  private static DistributedSystem ds;
  private static TransactionManagerImpl tm;
  
  private UserTransaction utx;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
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
