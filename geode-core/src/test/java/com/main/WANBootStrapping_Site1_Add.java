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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.DistributionConfig;

import java.util.Set;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * This is a member representing site 1 who wants to send data to site 2
 * 
 * On this member a locator with distributed-system-id = 1 is created. On this
 * member a cache is created.
 * 
 * A Region and a GatewaySender is created on this member through
 * MyDistributedSustemListener#addedDistributedSystemConnection 
 * (When a remote locator with distributed-system-id = 2 connects to this site,
 * MyDistributedSustemListener's addedDistributedSystemConnection will be
 * invoked who will create a region and a GatewaySender.)
 * 
 * This member does put for 100 keys on the region. (We have to check that this
 * data for 100 entries are sent to remote site)
 * 
 * This member also check for the sender's running status.
 * 
 * A GatewaySender will be stopped through
 * MyDistributedSustemListener#removedDistributedSystem 
 * (When a remote locator with distributed-system-id = -2 connects to this site,
 * MyDistributedSustemListener's removedDistributedSystem will be invoked who
 * will stop a GatewaySender.)
 * 
 * 
 */

public class WANBootStrapping_Site1_Add {

  public static void main(String[] args) {

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "DistributedSystemListener",
        "com.main.MyDistributedSystemListener");

    // Create a locator and a cache
    System.out.println("Creating cache ...It will take some time..");
    Cache cache = new CacheFactory().set(MCAST_PORT,
        "0").set(DISTRIBUTED_SYSTEM_ID, "" + 1).set(
        LOCATORS, "localhost[" + 10101 + "]").set(
        START_LOCATOR,
        "localhost[" + 10101
            + "],server=true,peer=true,hostname-for-clients=localhost").set(
        LOG_LEVEL, "warning").create();
    System.out.println("Cache Created");

    // to create region and a gateway sender ask to run
    // WANBootStrapping_Site2_Add program
    System.out.println("Run WANBootStrapping_Site2_Add");

    // get the region
    Region region = cache.getRegion("MyRegion");
    while (region == null) {
      region = cache.getRegion("MyRegion");
      try {
        Thread.sleep(5000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // put data in region
    for (int i = 0; i < 100; i++) {
      System.out.println("Create Entry : key_" + i + ", value_" + i);
      region.put("key_" + i, "value_" + i);
    }

    System.out.println("Entry Create Operation completed");

    Set<GatewaySender> gatewaySenders = cache.getGatewaySenders();
    GatewaySender sender = gatewaySenders.iterator().next();

    // make sure that gateway sender is running
    if (sender.isRunning()) {
      System.out.println("Sender " + sender.getId() + " is running");
    }

    // to stop gateway sender ask to run WANBootStrapping_Site2_Remove program
    while (sender.isRunning()) {
      System.out
          .println("Waitng for sender to stop through DistributedSystemListener");
      System.out.println("Start WANBootStrapping_Site2_Remove");
      try {
        Thread.sleep(5000);
      }
      catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    System.out.println("Sender " + sender.getId() + " is stopped");

    System.exit(0);
  }
}
