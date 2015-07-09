package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.gemstone.gemfire.internal.process.ProcessControllerFactory;

/**
 * Subclass of LocatorLauncherLocalDUnitTest which forces the code to not find 
 * the Attach API which is in the JDK tools.jar. As a result LocatorLauncher
 * ends up using the FileProcessController implementation.
 *
 * @author Kirk Lund
 * @since 8.0
 */
public class LocatorLauncherLocalFileJUnitTest extends LocatorLauncherLocalJUnitTest {

  @Before
  public final void setUpLocatorLauncherLocalFileTest() throws Exception {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
  }
  
  @After
  public final void tearDownLocatorLauncherLocalFileTest() throws Exception {   
  }
  
  @Override
  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertFalse(factory.isAttachAPIFound());
  }
}
