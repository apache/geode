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

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.*;
import org.junit.experimental.categories.Category;

import javax.transaction.*;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.fail;

/**
 * Check if the correct expectations are being thrown when they are supposed to.
 * 
 */
@Category(IntegrationTest.class)
public class ExceptionJUnitTest {

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
