/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Subclass of ServerLauncherLocalDUnitTest which forces the code to not find 
 * the Attach API which is in the JDK tools.jar.  As a result ServerLauncher
 * ends up using the FileProcessController implementation.
 *
 * @author Kirk Lund
 * @since 8.0
 */
@Category(IntegrationTest.class)
public class ServerLauncherLocalFileJUnitTest extends ServerLauncherLocalJUnitTest {

  @Before
  public final void setUpServerLauncherLocalFileTest() throws Exception {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }
  
  @After
  public final void tearDownServerLauncherLocalFileTest() throws Exception {   
  }
  
  @Override
  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertFalse(factory.isAttachAPIFound());
  }
}
