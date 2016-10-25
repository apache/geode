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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.wan.DistributedSystemListener;

/**
 * This is an implementation of DistributedSystemListener. When a
 * addedDistributedSystem is called a Region is created on both sites and
 * GatewaySender and GatewayReciever is started on site 1 and site 2
 * respectively.
 * 
 * When a removedDistributedSystem is called, GatewaySender and GatewayReceiver
 * is stopped on site1 and site2 respectively.
 * 
 * 
 */
public class MyDistributedSystemListener implements DistributedSystemListener {

  Cache cache;
  
  public MyDistributedSystemListener() {
  }
  
  /**
   * Please note that dynamic addition of the sender id to region is not yet available.  
   */
  public void addedDistributedSystem(int remoteDsId) {
    cache = CacheFactory.getAnyInstance();
    
    //When a site with distributed-system-id = 2 joins, create a region and a gatewaysender with remoteDsId = 2 
    if (remoteDsId == 2) {
      if (cache != null) {
        GatewaySender serialSender= cache
            .createGatewaySenderFactory()
            .setManualStart(true)
            .setPersistenceEnabled(false)
            .setDiskStoreName("LN_" + remoteDsId)
            .create("LN_"+ remoteDsId, remoteDsId);
        System.out.println("Sender Created : " + serialSender.getId());
        
        Region region= cache.createRegionFactory()
                       //.addSerialGatewaySenderId("LN_" + remoteDsId)
                       .create("MyRegion");
        System.out.println("Created Region : " + region.getName());
        
        try {
          serialSender.start();
          System.out.println("Sender Started: " + serialSender.getId());
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
      else {
        throw new CacheClosedException("Cache is not initialized here");
      }
    }else{ //When a site with distributed-system-id = 1 joins, create a region and a gatewayReceiver with  
      if (cache != null) {
        Region region = cache.createRegionFactory().create("MyRegion");
        System.out.println("Created Region :" +  region.getName());

        GatewayReceiver receiver= cache.createGatewayReceiverFactory()
                                 .setStartPort(12345)
                                 .setManualStart(true)
                                 .create();
        System.out.println("Created GatewayReceiver : " + receiver);
        try {
          receiver.start();
          System.out.println("GatewayReceiver Started.");
        }
        catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void removedDistributedSystem(int remoteDsId) {
    cache = CacheFactory.getAnyInstance();
    if (remoteDsId == 2) { //When a site with distributed-system-id = -2 joins, stop gatewaysender with remoteDsId = 2 
      if (cache != null) {
        GatewaySender sender = cache.getGatewaySender("LN_"+2);
        sender.stop();
      }
    }
    else{ // When a site with distributed-system-id = -1 joins, stop gatewayReceiver
      GatewayReceiver receiver = cache.getGatewayReceivers().iterator().next();
      receiver.stop();
    }
  }
}
