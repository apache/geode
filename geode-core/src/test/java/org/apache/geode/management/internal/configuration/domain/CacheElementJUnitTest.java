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
package org.apache.geode.management.internal.configuration.domain;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import javax.xml.XMLConstants;

import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.CacheXmlVersion;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.management.internal.configuration.utils.XmlUtils.XPathContext;
import org.apache.geode.services.module.ModuleService;

/**
 * Test cases for {@link CacheElement}.
 *
 * @see CacheElement
 * @since GemFire 8.1
 */
public class CacheElementJUnitTest {

  private Document loadSchema(final String schemaLocation) throws Exception {
    final CacheXmlParser entityResolver = new CacheXmlParser();
    entityResolver.init(ModuleService.DEFAULT);
    final XMLReader xmlReader = XMLReaderFactory.createXMLReader();
    xmlReader.setEntityResolver(entityResolver);

    return XmlUtils.getDocumentBuilder()
        .parse(entityResolver.resolveEntity(null, schemaLocation).getByteStream());
  }

  /**
   * Asserts that Cache type is defined as a embedded ComplexType under an Element named "cache" for
   * version 8.1.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testCacheDefinedAsNestedComplexType8_1() throws Exception {
    Document doc = loadSchema(CacheXmlVersion.GEMFIRE_8_1.getSchemaLocation());
    final XPathContext xPathContext =
        new XPathContext(CacheElement.XSD_PREFIX, XMLConstants.W3C_XML_SCHEMA_NS_URI);
    final Node cacheType = XmlUtils.querySingleElement(doc.getFirstChild(),
        CacheElement.CACHE_TYPE_EMBEDDED, xPathContext);
    assertNotNull("Cache type is not embedded complexType.", cacheType);
  }

  /**
   * Asserts that Cache type is defined as an embedded ComplexType under an Element named "cache"
   * latest version. If this assertion fails it likely means that cache type is defined as a named
   * ComplexType or reference. Update code in CacheElement.buildElementMapCacheType to account for
   * this.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testCacheDefinedAsNestedComplexTypeLatest() throws Exception {
    Document doc = loadSchema(CacheXml.LATEST_SCHEMA_LOCATION);
    final XPathContext xPathContext =
        new XPathContext(CacheElement.XSD_PREFIX, XMLConstants.W3C_XML_SCHEMA_NS_URI);
    final Node cacheType = XmlUtils.querySingleElement(doc.getFirstChild(),
        CacheElement.CACHE_TYPE_EMBEDDED, xPathContext);
    assertNotNull("Cache type is not embedded complexType.", cacheType);
  }

  /**
   * As of 8.1 the cache type requires that certain elements be listed in sequence. This test
   * verifies that {@link CacheElement#buildElementMap(Document, ModuleService)} produces a mapping
   * in the correct
   * order. If we change to use choice for all elements then we can abandon this mapping.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testBuildElementMap() throws Exception {
    final Document doc = XmlUtils.createDocumentFromReader(
        new InputStreamReader(this.getClass().getResourceAsStream("CacheElementJUnitTest.xml")));

    final LinkedHashMap<String, CacheElement> elementMap =
        CacheElement.buildElementMap(doc, ModuleService.DEFAULT);

    final Iterator<Entry<String, CacheElement>> entries = elementMap.entrySet().iterator();

    int order = 0;
    assertEntry("cache-transaction-manager", order++, entries.next());
    assertEntry("dynamic-region-factory", order++, entries.next());
    assertEntry("gateway-hub", order++, entries.next());
    assertEntry("gateway-sender", order++, entries.next());
    assertEntry("gateway-receiver", order++, entries.next());
    assertEntry("gateway-conflict-resolver", order++, entries.next());
    assertEntry("async-event-queue", order++, entries.next());
    assertEntry("cache-server", order++, entries.next());
    assertEntry("pool", order++, entries.next());
    assertEntry("disk-store", order++, entries.next());
    assertEntry("pdx", order++, entries.next());
    assertEntry("region-attributes", order++, entries.next());
    assertEntry("jndi-bindings", order++, entries.next());
    assertEntry("region", order++, entries.next());
    assertEntry("vm-root-region", order++, entries.next());
    assertEntry("function-service", order++, entries.next());
    assertEntry("resource-manager", order++, entries.next());
    assertEntry("serialization-registration", order++, entries.next());
    assertEntry("backup", order++, entries.next());
    assertEntry("initializer", order++, entries.next());

    assertTrue("Extra entries in map.", !entries.hasNext());
  }

  private void assertEntry(final String expectedName, final int expectedOrder,
      final Entry<String, CacheElement> entry) {
    assertEquals("Entry key out of order.", expectedName, entry.getKey());
    assertEquals("Entry value name out of order.", expectedName, entry.getValue().getName());
    assertEquals("Entry value order out of order.", expectedOrder, entry.getValue().getOrder());
  }
}
