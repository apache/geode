/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.junit.UnitTest;

/**
 * Tests {@link com.gemstone.gemfire.admin.internal.DistributedSystemFactory}.
 *
 * @author Kirk Lund
 * @since 3.5
 */
@SuppressWarnings("deprecation")
@Category(UnitTest.class)
public class DistributedSystemFactoryJUnitTest {

  /**
   * Tests <code>defineDistributedSystem(String locators)</code>.
   *
   * @see DistributedSystemFactory#defineDistributedSystem(String)
   */
  @Test
  public void testDefineDistributedSystemLocators() {
    String locators = "merry[8002],happy[8002]";
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    config.setMcastPort(0);
    config.setLocators(locators);
    
    assertNotNull(config);
    assertEquals(locators, 
                 config.getLocators());
    assertEquals(0, 
                 config.getMcastPort());
    assertEquals(DistributedSystemConfig.DEFAULT_REMOTE_COMMAND, 
                 config.getRemoteCommand());
  }

  @Test
  public void testDefineDistributedSystemMcast() { 
    String mcastAddress = "214.0.0.240";
    int mcastPort = 10347;
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    config.setMcastAddress(mcastAddress); 
    config.setMcastPort(mcastPort);
        
    assertNotNull(config);
    assertEquals(mcastAddress, 
                 config.getMcastAddress());
    assertEquals(mcastPort, 
                 config.getMcastPort());
    assertEquals(DistributedSystemConfig.DEFAULT_REMOTE_COMMAND, 
                 config.getRemoteCommand());
  }
  
  @Test
  public void testDefineDistributedSystem() { 
    String locators = "";
    String mcastAddress = "215.0.0.230";
    int mcastPort = 10346;
    String remoteCommand = "myrsh -n {CMD} {HOST}";
    DistributedSystemConfig config = 
        AdminDistributedSystemFactory.defineDistributedSystem();
    config.setLocators(locators); 
    config.setMcastAddress(mcastAddress); 
    config.setMcastPort(mcastPort); 
    config.setRemoteCommand(remoteCommand);
        
    assertNotNull(config);
    assertEquals(locators, 
                 config.getLocators());
    assertEquals(mcastAddress, 
                 config.getMcastAddress());
    assertEquals(mcastPort, 
                 config.getMcastPort());
    assertEquals(remoteCommand, 
                 config.getRemoteCommand());
  }

  // this configuration is legal in the Congo release
//  @Test
//  public void testDefineDistributedSystemIllegal() { 
//    String locators = "merry[8002],happy[8002]";
//    String mcastAddress = "215.0.0.230";
//    int mcastPort = 10346;
//    String remoteCommand = "myrsh -n {CMD} {HOST}";
//    
//    try {
//      DistributedSystemConfig config = 
//          AdminDistributedSystemFactory.defineDistributedSystem();
//      config.setMcastAddress(mcastAddress); 
//      config.setMcastPort(mcastPort); 
//      config.setRemoteCommand(remoteCommand);
//      config.setLocators(locators); 
//      config.validate();
//
//      fail("IllegalArgumentException should have been thrown");
//    }
//    catch (IllegalArgumentException e) {
//      // passed
//    }
//  }
  
  @Test
  public void testDefineDistributedSystemIllegalPort() { 
    String mcastAddress = DistributedSystemConfig.DEFAULT_MCAST_ADDRESS;
    
    int mcastPort = DistributedSystemConfig.MAX_MCAST_PORT+1;
    try {
      DistributedSystemConfig config = 
          AdminDistributedSystemFactory.defineDistributedSystem();
      config.setMcastAddress(mcastAddress); 
      config.setMcastPort(mcastPort);
      config.validate();

      fail("mcastPort > MAX should have thrown IllegalArgumentException");
    }
    catch (IllegalArgumentException e) {
      // passed
    }
    
    mcastPort = DistributedSystemConfig.MIN_MCAST_PORT-1;
    try {
      DistributedSystemConfig config = 
          AdminDistributedSystemFactory.defineDistributedSystem();
      config.setMcastAddress(mcastAddress); 
      config.setMcastPort(mcastPort);
      config.validate();

      fail("mcastPort < MIN should have thrown IllegalArgumentException");
    }
    catch (IllegalArgumentException e) {
      // passed
    }
  }
}
