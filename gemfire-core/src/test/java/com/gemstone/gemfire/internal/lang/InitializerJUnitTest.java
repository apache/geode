/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.lang;

import static org.junit.Assert.*;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The InitializerJUnitTest class is a test suite of test cases testing the contract and functionality of the Initializer
 * utility class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.lang.Initializer
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 8.0
 */
@Category(UnitTest.class)
public class InitializerJUnitTest {

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
  public void testInitWithInitableObject() {
    final Initable initableObject = mockContext.mock(Initable.class, "testInitWithInitableObject.Initable");

    mockContext.checking(new Expectations() {{
      oneOf(initableObject).init();
    }});

    assertTrue(Initializer.init(initableObject));
  }

  @Test
  public void testInitWithNonInitiableObject() {
    assertFalse(Initializer.init(new Object()));
  }

}
