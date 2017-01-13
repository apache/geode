/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject; 

import org.apache.geode.cache.*;
import org.apache.geode.cache.util.*;
import org.apache.geode.management.membership.*;

public class BridgeClientMembershipListener implements Declarable, ClientMembershipListener {

  /** Implementation of BridgeMembershipListener, memberJoined interface
   *
   *  @param BridgeMembershipEvent memberJoined event
   */
     
   public BridgeClientMembershipListener(){
     System.out.println("DEBUG_LOG :: BridgeClientMembershipListener() Ctor called");
   }
   
  public void memberJoined(ClientMembershipEvent event) {
    String clientName = event.getMember().getName();
    Region r = CacheFactory.getAnyInstance().getRegion("DistRegionAck");
    if(clientName.equals("Client-1")){
      r.put("clientName1", clientName);
    }else if(clientName.equals("Client-2")){
      r.put("clientName2", clientName);    	
    }else{
      System.out.println("DEBUG_LOG :: BridgeClientMembershipListener::memberJoined clientName Does not match");
    }
    System.out.println("DEBUG_LOG :: BridgeClientMembershipListener::memberJoined() :: clientName = " + clientName);
  }

  /** Implementation of BridgeMembershipListener, memberLeft interface
   *
   *  @param BridgeMembershipEvent memberLeft event
   */
  public void memberLeft(ClientMembershipEvent event) {
    String clientName = event.getMember().getName();
    System.out.println("DEBUG_LOG :: BridgeClientMembershipListener::memberLeft() :: clientName = " + clientName);
  }

  /** Implementation of BridgeMembershipListener, memberCrashed interface
   *
   *  @param BridgeMembershipEvent memberCrashed event
   */
  public void memberCrashed(ClientMembershipEvent event) {
    String clientName = event.getMember().getName();
    System.out.println("DEBUG_LOG :: BridgeClientMembershipListener::memberCrashed() :: clientName = " + clientName);
  }
  
  public void init(java.util.Properties prop) {
    System.out.println("DEBUG_LOG :: BridgeClientMembershipListener::init() Called");
  }
}


