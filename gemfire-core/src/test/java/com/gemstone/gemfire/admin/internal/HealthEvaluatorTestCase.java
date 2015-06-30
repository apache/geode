/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;

/**
 * Superclass of tests for the {@linkplain
 * com.gemstone.gemfire.admin.internal.AbstractHealthEvaluator health
 * evaluator} classes.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public abstract class HealthEvaluatorTestCase {

  /** The DistributedSystem used for this test */
  protected InternalDistributedSystem system;

  /**
   * Creates a "loner" <code>DistributedSystem</code> for this test.
   */
  @Before
  public void setUp() {
    Properties props = getProperties();
    system = (InternalDistributedSystem)
      DistributedSystem.connect(props);
  }

  /**
   * Closes the "loner" <code>DistributedSystem</code>
   */
  @After
  public void tearDown() {
    if (this.system != null) {
      this.system.disconnect();
    }

    this.system = null;
  }

  /**
   * Creates the <code>Properties</code> objects used to connect to
   * the distributed system.
   */
  protected Properties getProperties() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("statistic-sampling-enabled", "true");

    return props;
  }

}
