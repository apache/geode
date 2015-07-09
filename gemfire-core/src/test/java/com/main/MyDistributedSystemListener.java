/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.main;

import java.io.IOException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.wan.DistributedSystemListener;

/**
 * This is an implementation of DistributedSystemListener. When a
 * addedDistributedSystem is called a Region is created on both sites and
 * GatewaySender and GatewayReciever is started on site 1 and site 2
 * respectively.
 * 
 * When a removedDistributedSystem is called, GatewaySender and GatewayReceiver
 * is stopped on site1 and site2 respectively.
 * 
 * @author kbachhav
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
