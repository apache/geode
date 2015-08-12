/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import static org.junit.Assert.*;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.gemstone.gemfire.internal.cache.xmlcache.AbstractXmlParser;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlParser;

/**
 * Mock Extension {@link XmlParser}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public class MockExtensionXmlParser extends AbstractXmlParser {
  public static final String ATTRIBUTE_VALUE = "value";
  public static final String ELEMENT_REGION_QNAME = "mock:region";
  public static final String ELEMENT_REGION = "region";
  public static final String ELEMENT_CACHE_QNAME = "mock:cache";
  public static final String ELEMENT_CACHE = "cache";

  public static final String NAMESPACE = "urn:java:com.gemstone.gemfire.internal.cache.extension.mock.MockExtensionXmlParser";
  public static final String PREFIX = "mock";

  @Override
  public String getNamspaceUri() {
    return NAMESPACE;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
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
      cache.getExtensionPoint().addExtension(MockCacheExtension.class, extension);
      break;
    }
    case ELEMENT_REGION: {
      MockRegionExtension extension = (MockRegionExtension) stack.pop();
      RegionCreation region = (RegionCreation) stack.peek();
      region.getExtensionPoint().addExtension(MockRegionExtension.class, extension);
      break;
    }
    default:
      throw new SAXParseException("Unexpected element '" + localName + "'.", documentLocator);
    }
  }

}
