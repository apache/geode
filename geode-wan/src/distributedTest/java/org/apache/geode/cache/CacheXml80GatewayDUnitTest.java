/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.wan.GatewayEventSubstitutionFilter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.cache30.CacheXml70DUnitTestHelper;
import org.apache.geode.cache30.CacheXmlTestCase;
import org.apache.geode.cache30.MyGatewayTransportFilter1;
import org.apache.geode.cache30.MyGatewayTransportFilter2;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class CacheXml80GatewayDUnitTest extends CacheXmlTestCase {

  @Override
  protected String getGemFireVersion() {
    return CacheXml.VERSION_8_0;
  }

  @Test
  public void testGatewayReceiverWithManualStartTRUE() throws Exception {
    // getSystem();
    CacheCreation cache = new CacheCreation();

    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();
    gatewayReceiverFactory.setBindAddress("");
    gatewayReceiverFactory.setStartPort(20000);
    gatewayReceiverFactory.setEndPort(29999);
    gatewayReceiverFactory.setMaximumTimeBetweenPings(2000);
    gatewayReceiverFactory.setSocketBufferSize(1500);
    gatewayReceiverFactory.setManualStart(true);
    GatewayTransportFilter myStreamFilter1 = new MyGatewayTransportFilter1();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamFilter1);
    GatewayTransportFilter myStreamFilter2 = new MyGatewayTransportFilter2();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamFilter2);
    GatewayReceiver receiver1 = gatewayReceiverFactory.create();

    receiver1.start();

    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewayReceiver> receivers = c.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      validateGatewayReceiver(receiver1, receiver);
    }
  }

  @Test
  public void testAsyncEventQueueWithSubstitutionFilter() throws Exception {
    getSystem();
    CacheCreation cache = new CacheCreation();

    // Create an AsyncEventQueue with GatewayEventSubstitutionFilter.
    String id = getName();
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setGatewayEventSubstitutionListener(new MyGatewayEventSubstitutionFilter());
    AsyncEventQueue queue =
        factory.create(id, new CacheXml70DUnitTestHelper.MyAsyncEventListener());

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
  public void testGatewaySenderWithSubstitutionFilter() throws Exception {
    getSystem();
    CacheCreation cache = new CacheCreation();

    // Create a GatewaySender with GatewayEventSubstitutionFilter.
    // Don't start the sender to avoid 'Locators must be configured before starting gateway-sender'
    // exception.
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
      GatewayReceiver gatewayReceiver) {
    CacheXml70GatewayDUnitTest.validateGatewayReceiver(receiver1, gatewayReceiver);
    assertEquals(receiver1.isManualStart(), gatewayReceiver.isManualStart());
  }

  public static class MyGatewayEventSubstitutionFilter
      implements GatewayEventSubstitutionFilter, Declarable {

    @Override
    public Object getSubstituteValue(EntryEvent event) {
      return event.getKey();
    }

    @Override
    public void close() {}

    @Override
    public void init(Properties properties) {}
  }
}
