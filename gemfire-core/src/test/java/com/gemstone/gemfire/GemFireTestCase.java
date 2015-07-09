/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import static org.junit.Assert.*;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.*;

import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * This is an abstract superclass for classes that test GemFire.  It
 * has setUp() and tearDown() methods that create and initialize a
 * GemFire connection.
 *
 * @author davidw
 *
 */
public abstract class GemFireTestCase {

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    // make it a loner
    p.setProperty("mcast-port", "0");
    p.setProperty("locators", "");
    p.setProperty(DistributionConfig.NAME_NAME, getName());
    DistributedSystem.connect(p);
  }

  @After
  public void tearDown() throws Exception {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if (ds != null) {
      ds.disconnect();
    }
  }

  protected String getName() {
    return testName.getMethodName();
  }
  
  /**
   * Strip the package off and gives just the class name.
   * Needed because of Windows file name limits.
   */
  private String getShortClassName() {
    return getClass().getSimpleName();
  }
  
  /**
   * Returns a unique name for this test method.  It is based on the
   * name of the class as well as the name of the method.
   */
  protected String getUniqueName() {
    return getShortClassName() + "_" + getName();
  }

  /**
   * Assert an Invariant condition on an object.
   * @param inv the Invariant to assert. If null, this method just returns
   * @param the obj to assert the Invariant on.
   */
  protected void assertInvariant(Invariant inv, Object obj) {
    if (inv == null) return;
    InvariantResult result = inv.verify(obj);
    assertTrue(result.message, result.valid);
  }
}
