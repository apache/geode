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

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

/**
 * This is a member representing site 2 who wants to receive data from site 1
 * 
 * On this member a locator with distributed-system-id = 2 is created. 
 * On this member a cache is created.
 * 
 * A Region and a GatewayReceiver is created on this member through
 * MyDistributedSustemListener#addedDistributedSystemConnection
 *  
 * (When this locator gets the locator information from the site 1,
 * MyDistributedSustemListener's addedDistributedSystemConnection will be
 * invoked who will create a region and a GatewayReceiver.)
 * 
 * This member expects region size to be 100. (this site received this data from site1)
 * 
 * This member also check for the receiver's running status.
 * 
 * A GatewayReceiver will be stopped through
 * MyDistributedSustemListener#removedDistributedSystem 
 * (When a remote locator with distributed-system-id = -1 connects to this site,
 * MyDistributedSustemListener's removedDistributedSystem will be invoked who
 * will stop a GatewayReceiver.)
 * 
 * 
 */

public class WANBootStrapping_Site2_Add {

  public static void main(String[] args) {

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "DistributedSystemListener",
        "com.main.MyDistributedSystemListener");
    
    //create a locator and a cache
    System.out.println("Creating cache ...It will take some time..");
    Cache cache = new CacheFactory()
        .set(MCAST_PORT, "0")
    .set(DISTRIBUTED_SYSTEM_ID, ""+2)
        .set(LOCATORS, "localhost[" + 20202 + "]")
        .set(START_LOCATOR, "localhost[" + 20202 + "],server=true,peer=true,hostname-for-clients=localhost")
    .set(REMOTE_LOCATORS, "localhost[" + 10101 + "]")
    .set(LOG_LEVEL, "warning")
    .create();
    System.out.println("Cache Created");
    
    //get the region whose size should be 100 
    Region region = cache.getRegion("MyRegion");
    while(region == null){
      region = cache.getRegion("MyRegion");
      try {
        Thread.sleep(5000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    //region size should be 100. This is the data which will recieve from remote site
    while(region.size()!= 100){
      continue;
    }
    System.out.println("Checked region size : " + region.size());

    GatewayReceiver receiver = cache.getGatewayReceivers().iterator().next();
    
     // to stop gateway receiver ask to run WANBootStrapping_Site1_Remove program
    while (receiver.isRunning()) {
      System.out
          .println("Waitng for receiver to stop through DistributedSystemListener");
      System.out.println("Start WANBootStrapping_Site1_Remove ");  
      try {
        Thread.sleep(2000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    System.out.println("GatewayReciver " + receiver + " is stopped") ;
    System.exit(0);
  }
}
