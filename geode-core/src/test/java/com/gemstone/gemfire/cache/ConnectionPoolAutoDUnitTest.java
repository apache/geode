/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

import static org.junit.runners.MethodSorters.*;
import org.junit.FixMethodOrder;

@FixMethodOrder(NAME_ASCENDING)
public class ConnectionPoolAutoDUnitTest extends ConnectionPoolDUnitTest {

  public ConnectionPoolAutoDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    // TODO Auto-generated method stub
    ClientServerTestCase.AUTO_LOAD_BALANCE = true;
    Invoke.invokeInEveryVM(new SerializableRunnable("setupAutoMode") {
      public void run() {
        ClientServerTestCase.AUTO_LOAD_BALANCE = true;
      }
    });
  }

  @Override
  protected final void postTearDownConnectionPoolDUnitTest() throws Exception {
    ClientServerTestCase.AUTO_LOAD_BALANCE  = false;
    Invoke.invokeInEveryVM(new SerializableRunnable("disableAutoMode") {
      public void run() {
        ClientServerTestCase.AUTO_LOAD_BALANCE = false;
      }
    });
  }
  
}
