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


