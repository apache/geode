/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.jta;

import static org.junit.Assert.*;

import java.util.Properties;

import junit.framework.TestCase;

import javax.transaction.*;

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
 * Check if the correct expectations are being thrown when they are supposed to.
 * 
 * @author Mitul bid
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class ExceptionJUnitTest {

  private static DistributedSystem ds;
  private static TransactionManagerImpl tm;
  
  private UserTransaction utx = null;
  
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
  public void testNestedTransactionNotSupported() throws Exception {
    try {
      utx.begin();
      utx.begin();
      utx.commit();
      fail("Exception on nested transaction not thrown");
    } catch (NotSupportedException expected) {
      utx.commit();
    }
  }

  @Test
  public void testCommitSystemException() throws Exception {
    try {
      utx.commit();
      fail("IllegalStateException not thrown on commit");
    } catch (IllegalStateException expected) {
      // success
    }
  }

  @Test
  public void testRollbackIllegalStateException() throws Exception {
    try {
      utx.begin();
      GlobalTransaction gtx = tm.getGlobalTransaction();
      gtx.setStatus(Status.STATUS_UNKNOWN);
      utx.rollback();
      fail("IllegalStateException not thrown on rollback");
    } catch (IllegalStateException e) {
      GlobalTransaction gtx = tm.getGlobalTransaction();
      gtx.setStatus(Status.STATUS_ACTIVE);
      utx.commit();
    }
  }

  @Test
  public void testAddNullTransaction() throws Exception {
    try {
      utx.begin();
      GlobalTransaction gtx = tm.getGlobalTransaction();
      Transaction txn = null;
      gtx.addTransaction(txn);
      utx.commit();
      fail("SystemException not thrown on adding null transaction");
    } catch (SystemException e) {
      utx.commit();
    }
  }
}
