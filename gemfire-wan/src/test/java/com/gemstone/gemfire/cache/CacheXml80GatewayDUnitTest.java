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
package com.gemstone.gemfire.cache;

import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.CacheXmlTestCase;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter2;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;

public class CacheXml80GatewayDUnitTest extends CacheXmlTestCase {

  public CacheXml80GatewayDUnitTest(String name) {
    super(name);
  }

  protected String getGemFireVersion() {
    return CacheXml.VERSION_8_0;
  }
  
  public void testGatewayReceiverWithManualStartTRUE() throws CacheException{
    //getSystem();
    CacheCreation cache = new CacheCreation();
    
    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();
    gatewayReceiverFactory.setBindAddress("");
    gatewayReceiverFactory.setStartPort(54321);
    gatewayReceiverFactory.setEndPort(54331);
    gatewayReceiverFactory.setMaximumTimeBetweenPings(2000);
    gatewayReceiverFactory.setSocketBufferSize(1500);
    gatewayReceiverFactory.setManualStart(true);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewayReceiver receiver1 = gatewayReceiverFactory.create();
    try {
      receiver1.start();
    }
    catch (IOException e) {
      fail("Could not start GatewayReceiver");
    }
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewayReceiver> receivers = c.getGatewayReceivers();
    for(GatewayReceiver receiver : receivers){
      validateGatewayReceiver(receiver1, receiver);
    }
  }

  protected void validateGatewayReceiver(GatewayReceiver receiver1,
      GatewayReceiver gatewayReceiver){
    CacheXml70GatewayDUnitTest.validateGatewayReceiver(receiver1, gatewayReceiver);
    assertEquals(receiver1.isManualStart(), gatewayReceiver.isManualStart());
  }
}
