/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.main;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * This is a stand alone locator with a distributed-system-id = -2
 * 
 * This locator is started so that the locator information regarding the site 2
 * is removed from site 1's locator and at the same time
 * MyDistributedSystemListener's removeDistributedSystem is invoked on site 1's locator which will stop the GatewaySender
 * 
 * @author kbachhav
 * 
 */

public class WANBootStrapping_Site2_Remove {

  public static void main(String[] args) {
    
    // On this locator, I am not expecting a listener to take any action, so a
    // empty listener is a passed
    System.setProperty("gemfire.DistributedSystemListener",
    "");
    
    System.out.println("Starting a locator with negative ds id -2");
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    properties.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+ (-2));
    properties.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + 10101 + "]");
    properties.setProperty(DistributionConfig.LOG_LEVEL_NAME, "warning");
    Locator locator = null;
    try {
      locator = Locator.startLocatorAndDS(30445, null, properties);
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
    System.out.println("Stoping locator");    
    locator.stop();
    System.out.println("Locator stopped ");
    
    System.exit(0);
  }

  
}
