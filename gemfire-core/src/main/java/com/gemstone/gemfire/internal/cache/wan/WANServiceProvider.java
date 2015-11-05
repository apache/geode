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
package com.gemstone.gemfire.internal.cache.wan;

import java.util.Iterator;
import java.util.ServiceLoader;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;
import com.gemstone.gemfire.internal.cache.wan.spi.WANFactory;

public class WANServiceProvider {
  private static final WANFactory factory;
  
  static {
    ServiceLoader<WANFactory> loader = ServiceLoader.load(WANFactory.class);
    Iterator<WANFactory> itr = loader.iterator();
    if(!itr.hasNext()) {
      factory = null;
    } else {
      factory = itr.next();
      factory.initialize();
    }
  }
  
  public static GatewaySenderFactory createGatewaySenderFactory(Cache cache) {
    if(factory == null) {
      throw new IllegalStateException("WAN service is not available.");
    }
    return factory.createGatewaySenderFactory(cache);
    
  }

  public static GatewayReceiverFactory createGatewayReceiverFactory(
      Cache cache) {
    if(factory == null) {
      throw new IllegalStateException("WAN service is not available.");
    }
    return factory.createGatewayReceiverFactory(cache);
  }

  public static WanLocatorDiscoverer createLocatorDiscoverer() {
    if(factory == null) {
      return null;
    }
    return factory.createLocatorDiscoverer();
  }

  public static LocatorMembershipListener createLocatorMembershipListener() {
    if(factory == null) {
      return null;
    }
    return factory.createLocatorMembershipListener();
  }
  
  private WANServiceProvider() {
    
  }
}
