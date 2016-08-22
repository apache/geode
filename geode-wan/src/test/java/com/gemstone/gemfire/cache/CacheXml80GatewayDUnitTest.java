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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.wan.*;
import com.gemstone.gemfire.cache30.*;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;

@Category(DistributedTest.class)
public class CacheXml80GatewayDUnitTest extends CacheXmlTestCase {

  public CacheXml80GatewayDUnitTest() {
    super();
  }

  protected String getGemFireVersion() {
    return CacheXml.VERSION_8_0;
  }
  
  @Test
  public void testGatewayReceiverWithManualStartTRUE() throws CacheException{
    //getSystem();
    CacheCreation cache = new CacheCreation();
    
    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();
    gatewayReceiverFactory.setBindAddress("");
    gatewayReceiverFactory.setStartPort(20000);
    gatewayReceiverFactory.setEndPort(29999);
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

  @Test
  public void testAsyncEventQueueWithSubstitutionFilter() {
    getSystem();
    CacheCreation cache = new CacheCreation();

    // Create an AsyncEventQueue with GatewayEventSubstitutionFilter.
    String id = getName();
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setGatewayEventSubstitutionListener(new MyGatewayEventSubstitutionFilter());
    AsyncEventQueue queue = factory.create(id, new CacheXml70DUnitTest.MyAsyncEventListener());

    // Verify the GatewayEventSubstitutionFilter is set on the AsyncEventQueue.
    assertNotNull(queue.getGatewayEventSubstitutionFilter());

    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);

    // Get the AsyncEventQueue. Verify the GatewayEventSubstitutionFilter is not null.
    AsyncEventQueue queueOnCache = c.getAsyncEventQueue(id);
    assertNotNull(queueOnCache);
    assertNotNull(queueOnCache.getGatewayEventSubstitutionFilter());
  }

  @Test
  public void testGatewaySenderWithSubstitutionFilter() {
    getSystem();
    CacheCreation cache = new CacheCreation();

    // Create a GatewaySender with GatewayEventSubstitutionFilter.
    // Don't start the sender to avoid 'Locators must be configured before starting gateway-sender' exception.
    String id = getName();
    GatewaySenderFactory factory = cache.createGatewaySenderFactory();
    factory.setManualStart(true);
    factory.setGatewayEventSubstitutionFilter(new MyGatewayEventSubstitutionFilter());
    GatewaySender sender = factory.create(id, 2);

    // Verify the GatewayEventSubstitutionFilter is set on the GatewaySender.
    assertNotNull(sender.getGatewayEventSubstitutionFilter());

    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);

    // Get the GatewaySender. Verify the GatewayEventSubstitutionFilter is not null.
    GatewaySender senderOnCache = c.getGatewaySender(id);
    assertNotNull(senderOnCache);
    assertNotNull(senderOnCache.getGatewayEventSubstitutionFilter());
  }

  protected void validateGatewayReceiver(GatewayReceiver receiver1,
      GatewayReceiver gatewayReceiver){
    CacheXml70GatewayDUnitTest.validateGatewayReceiver(receiver1, gatewayReceiver);
    assertEquals(receiver1.isManualStart(), gatewayReceiver.isManualStart());
  }

  public static class MyGatewayEventSubstitutionFilter implements GatewayEventSubstitutionFilter, Declarable {

    public Object getSubstituteValue(EntryEvent event) {
      return event.getKey();
    }

    public void close() {
    }

    public void init(Properties properties) {
    }
  }
}
