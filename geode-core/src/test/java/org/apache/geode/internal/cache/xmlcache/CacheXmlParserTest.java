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
package org.apache.geode.internal.cache.xmlcache;

import static org.apache.geode.internal.cache.GemFireCacheImpl.DEFAULT_LOCK_LEASE;
import static org.apache.geode.internal.cache.GemFireCacheImpl.DEFAULT_LOCK_TIMEOUT;
import static org.apache.geode.internal.cache.GemFireCacheImpl.DEFAULT_SEARCH_TIMEOUT;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.COPY_ON_READ;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.IS_SERVER;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.LOCK_LEASE;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.LOCK_TIMEOUT;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.MESSAGE_SYNC_INTERVAL;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.REMOTE_DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.SEARCH_TIMEOUT;
import static org.apache.geode.internal.cache.xmlcache.CacheXml.STARTUP_ACTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.wan.GatewaySenderStartupAction;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;


public class CacheXmlParserTest {

  @Mock
  private CacheCreation cacheCreation;

  @Before
  public void setUp() {
    cacheCreation = mock(CacheCreation.class);
  }

  @Test
  public void testStartCacheParametersSet() throws SAXException {
    AttributesImpl attributes = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attributes, LOCK_LEASE, "10");
    XmlGeneratorUtils.addAttribute(attributes, LOCK_TIMEOUT, "15");
    XmlGeneratorUtils.addAttribute(attributes, SEARCH_TIMEOUT, "20");
    XmlGeneratorUtils.addAttribute(attributes, MESSAGE_SYNC_INTERVAL, "20");
    XmlGeneratorUtils.addAttribute(attributes, IS_SERVER, "true");
    XmlGeneratorUtils.addAttribute(attributes, COPY_ON_READ, "true");

    CacheXmlParser parser = new CacheXmlParser();
    parser.startElement("http://geode.apache.org/schema/cache", "cache", "cache", attributes);

    // Check that parameters are set
    CacheCreation cache = parser.getCacheCreation();
    assertThat(cache.getLockLease()).isEqualTo(10);
    assertThat(cache.getLockTimeout()).isEqualTo(15);
    assertThat(cache.getSearchTimeout()).isEqualTo(20);
    assertThat(cache.getMessageSyncInterval()).isEqualTo(20);
    assertThat(cache.isServer()).isTrue();
    assertThat(cache.getCopyOnRead()).isTrue();
    // Reset MessageSyncInterval to default value, because it is static variable
    HARegionQueue.setMessageSyncInterval(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
  }

  @Test
  public void testStartCacheParametersDefaultValues() throws SAXException {
    AttributesImpl attributes = new AttributesImpl();

    CacheXmlParser parser = new CacheXmlParser();
    parser.startElement("http://geode.apache.org/schema/cache", "cache", "cache", attributes);

    // Check that parameters are set to default values
    CacheCreation cache = parser.getCacheCreation();
    assertThat(cache.getLockLease()).isEqualTo(DEFAULT_LOCK_LEASE);
    assertThat(cache.getLockTimeout()).isEqualTo(DEFAULT_LOCK_TIMEOUT);
    assertThat(cache.getSearchTimeout()).isEqualTo(DEFAULT_SEARCH_TIMEOUT);
    assertThat(cache.getMessageSyncInterval())
        .isEqualTo(HARegionQueue.DEFAULT_MESSAGE_SYNC_INTERVAL);
    assertThat(cache.isServer()).isFalse();
    assertThat(cache.getCopyOnRead()).isFalse();
  }

  @Test
  public void testStartAndEndGatewaySender() throws SAXException {
    AttributesImpl attributes = new AttributesImpl();

    XmlGeneratorUtils.addAttribute(attributes, REMOTE_DISTRIBUTED_SYSTEM_ID, "1");
    XmlGeneratorUtils.addAttribute(attributes, CacheXml.ID, "gateway-sender");

    InternalGatewaySenderFactory gatewaySenderFactory = mock(InternalGatewaySenderFactory.class);
    when(cacheCreation.createGatewaySenderFactory()).thenReturn(gatewaySenderFactory);

    CacheXmlParser parser = new CacheXmlParser(cacheCreation);
    parser.startElement("http://geode.apache.org/schema/cache", "gateway-sender", "gateway-sender",
        attributes);

    parser.endElement("http://geode.apache.org/schema/cache", "gateway-sender", "gateway-sender");
    verify(gatewaySenderFactory).create("gateway-sender", 1);
  }

  @Test
  public void testStartGatewaySenderStartupActionParameterStart() throws SAXException {
    AttributesImpl attributes = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attributes, STARTUP_ACTION, "start");

    InternalGatewaySenderFactory gatewaySenderFactory = mock(InternalGatewaySenderFactory.class);
    when(cacheCreation.createGatewaySenderFactory()).thenReturn(gatewaySenderFactory);

    CacheXmlParser parser = new CacheXmlParser(cacheCreation);
    parser.startElement("http://geode.apache.org/schema/cache", "gateway-sender", "gateway-sender",
        attributes);

    verify(gatewaySenderFactory).setStartupAction(GatewaySenderStartupAction.START);
  }

  @Test
  public void testStartGatewaySenderStartupActionParameterStop() throws SAXException {
    AttributesImpl attributes = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attributes, STARTUP_ACTION, "stop");

    InternalGatewaySenderFactory gatewaySenderFactory = mock(InternalGatewaySenderFactory.class);
    when(cacheCreation.createGatewaySenderFactory()).thenReturn(gatewaySenderFactory);

    CacheXmlParser parser = new CacheXmlParser(cacheCreation);
    parser.startElement("http://geode.apache.org/schema/cache", "gateway-sender", "gateway-sender",
        attributes);

    verify(gatewaySenderFactory).setStartupAction(GatewaySenderStartupAction.STOP);
  }

  @Test
  public void testStartGatewaySenderStartupActionParameterPause() throws SAXException {
    AttributesImpl attributes = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attributes, STARTUP_ACTION, "pause");

    InternalGatewaySenderFactory gatewaySenderFactory = mock(InternalGatewaySenderFactory.class);
    when(cacheCreation.createGatewaySenderFactory()).thenReturn(gatewaySenderFactory);

    CacheXmlParser parser = new CacheXmlParser(cacheCreation);
    parser.startElement("http://geode.apache.org/schema/cache", "gateway-sender", "gateway-sender",
        attributes);

    verify(gatewaySenderFactory).setStartupAction(GatewaySenderStartupAction.PAUSE);
  }

  @Test
  public void testStartGatewaySenderStartupActionParameterNull() throws SAXException {
    AttributesImpl attributes = new AttributesImpl();

    InternalGatewaySenderFactory gatewaySenderFactory = mock(InternalGatewaySenderFactory.class);
    when(cacheCreation.createGatewaySenderFactory()).thenReturn(gatewaySenderFactory);

    CacheXmlParser parser = new CacheXmlParser(cacheCreation);
    parser.startElement("http://geode.apache.org/schema/cache", "gateway-sender", "gateway-sender",
        attributes);

    verify(gatewaySenderFactory).setStartupAction(GatewaySenderStartupAction.NONE);
  }

  @Test
  public void testGatewaySenderStartupActionParameterInvalidValue() {
    AttributesImpl attributes = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attributes, CacheXml.ID, "sender1");
    XmlGeneratorUtils.addAttribute(attributes, STARTUP_ACTION, "pausede");

    InternalGatewaySenderFactory gatewaySenderFactory = mock(InternalGatewaySenderFactory.class);
    when(cacheCreation.createGatewaySenderFactory()).thenReturn(gatewaySenderFactory);

    CacheXmlParser parser = new CacheXmlParser(cacheCreation);
    Throwable thrown =
        catchThrowable(() -> parser.startElement("http://geode.apache.org/schema/cache",
            "gateway-sender", "gateway-sender", attributes));

    assertThat(thrown)
        .isInstanceOf(InternalGemFireException.class)
        .hasMessage(
            "An invalid startup-action value (pausede) was configured for gateway sender sender1");
  }

}
