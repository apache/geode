/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;

import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * Provides common setUp and tearDown for testing the Admin API.
 *
 * @author Kirk Lund
 * @since 3.5
 */
public abstract class DistributedSystemTestCase {

  /** The DistributedSystem used for this test */
  protected DistributedSystem system;

  /**
   * Creates a "loner" <code>DistributedSystem</code> for this test.
   */
  @Before
  public void setUp() throws Exception {
    this.system = DistributedSystem.connect(defineProperties());
  }

  /**
   * Closes the "loner" <code>DistributedSystem</code>
   */
  @After
  public void tearDown() throws Exception {
    if (this.system != null) {
      this.system.disconnect();
    }
    this.system = null;
  }

  /**
   * Defines the <code>Properties</code> used to connect to the distributed 
   * system.
   */
  protected Properties defineProperties() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("conserve-sockets", "true");
    return props;
  }
}
