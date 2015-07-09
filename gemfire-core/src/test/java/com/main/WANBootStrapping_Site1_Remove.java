/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.main;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * This is a stand alone locator with a distributed-system-id = -1
 * 
 * This locator is started so that the locator information regarding the site 1
 * is removed from site 2's locator and at the same time
 * MyDistributedSystemListener's removeDistributedSystem is invoked on site 2's locator which will stop the GatewayReceiver
 * 
 * @author kbachhav
 * 
 */
public class WANBootStrapping_Site1_Remove {


  public static void main(String[] args) {
    
    //On this locator, I am not expecting a listener to take any action, so a empty listener is a passed
    System.setProperty("gemfire.DistributedSystemListener",
    "");
    
    System.out.println("Starting a locator with negative ds id -1");
    
    //start a stand alone locator with distributed-system-is = -1
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    properties.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+ (-1));
    properties.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + 20202 + "]");
    properties.setProperty(DistributionConfig.LOG_LEVEL_NAME, "warning");
    Locator locator = null;
    try {
      locator = Locator.startLocatorAndDS(40445, null, properties);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    
    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    //stop locator
    System.out.println("Stoping locator");    
    locator.stop();
    System.out.println("Locator stopped ");
    
    System.exit(0);
  }

}
