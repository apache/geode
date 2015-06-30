/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal;

import static org.junit.Assert.*;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The JettyHelperJUnitTest class is a test suite of test cases testing the
 * contract and functionality of the JettyHelper
 * class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.JettyHelper
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */
@Category(UnitTest.class)
public class JettyHelperJUnitTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery();
    mockContext.setImposteriser(ClassImposteriser.INSTANCE);
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  @Test
  public void testSetPortNoBindAddress() throws Exception {

    final Server jetty = JettyHelper.initJetty(null, 8090, false, false, null, null, null);

    assertNotNull(jetty);
    assertNotNull(jetty.getConnectors()[0]);
    assertEquals(8090, ((ServerConnector)jetty.getConnectors()[0]).getPort());
  }

  @Test
  public void testSetPortWithBindAddress() throws Exception {

    final Server jetty = JettyHelper.initJetty("10.123.50.1", 10480, false, false, null, null, null);

    assertNotNull(jetty);
    assertNotNull(jetty.getConnectors()[0]);
    assertEquals(10480, ((ServerConnector)jetty.getConnectors()[0]).getPort());
  }

}
