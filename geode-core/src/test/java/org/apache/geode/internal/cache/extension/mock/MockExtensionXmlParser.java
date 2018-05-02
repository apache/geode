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

package org.apache.geode.internal.cache.extension.mock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import org.apache.geode.internal.cache.xmlcache.AbstractXmlParser;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;
import org.apache.geode.internal.cache.xmlcache.XmlParser;

/**
 * Mock Extension {@link XmlParser}.
 *
 *
 * @since GemFire 8.1
 */
public class MockExtensionXmlParser extends AbstractXmlParser {
  public static final String ATTRIBUTE_VALUE = "value";
  public static final String ELEMENT_REGION_QNAME = "mock:region";
  public static final String ELEMENT_REGION = "region";
  public static final String ELEMENT_CACHE_QNAME = "mock:cache";
  public static final String ELEMENT_CACHE = "cache";

  public static final String NAMESPACE =
      "urn:java:org.apache.geode.internal.cache.extension.mock.MockExtensionXmlParser";
  public static final String PREFIX = "mock";

  @Override
  public String getNamespaceUri() {
    return NAMESPACE;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes atts)
      throws SAXException {
    switch (localName) {
      case ELEMENT_CACHE: {
        assertEquals(ELEMENT_CACHE, localName);
        assertEquals(ELEMENT_CACHE_QNAME, qName);
        assertEquals(NAMESPACE, uri);
        assertNotNull(atts.getValue(ATTRIBUTE_VALUE));
        MockCacheExtension extension = new MockCacheExtension(atts.getValue(ATTRIBUTE_VALUE));
        stack.push(extension);
        break;
      }
      case ELEMENT_REGION: {
        assertEquals(ELEMENT_REGION, localName);
        assertEquals(ELEMENT_REGION_QNAME, qName);
        assertEquals(NAMESPACE, uri);
        assertNotNull(atts.getValue(ATTRIBUTE_VALUE));
        if ("exception".equals(atts.getValue(ATTRIBUTE_VALUE))) {
          throw new SAXParseException("Value is 'exception'.", documentLocator);
        }
        MockRegionExtension extension = new MockRegionExtension(atts.getValue(ATTRIBUTE_VALUE));
        stack.push(extension);
        break;
      }
      default:
        throw new SAXParseException("Unexpected element '" + localName + "'.", documentLocator);
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    switch (localName) {
      case ELEMENT_CACHE: {
        MockCacheExtension extension = (MockCacheExtension) stack.pop();
        CacheCreation cache = (CacheCreation) stack.peek();
        cache.getExtensionPoint().addExtension(extension);
        break;
      }
      case ELEMENT_REGION: {
        MockRegionExtension extension = (MockRegionExtension) stack.pop();
        RegionCreation region = (RegionCreation) stack.peek();
        region.getExtensionPoint().addExtension(extension);
        break;
      }
      default:
        throw new SAXParseException("Unexpected element '" + localName + "'.", documentLocator);
    }
  }

}
