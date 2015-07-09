/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.extension.mock;

import static com.gemstone.gemfire.internal.cache.extension.mock.MockExtensionXmlParser.*;
import static com.gemstone.gemfire.internal.cache.xmlcache.XmlGeneratorUtils.*;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * {@link MockRegionExtension} {@link XmlGenerator}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public final class MockRegionExtensionXmlGenerator extends AbstractMockExtensionXmlGenerator<Region<?, ?>> {
  public MockRegionExtensionXmlGenerator(MockRegionExtension extension) {
    super(extension);
  }

  @Override
  public void generate(CacheXmlGenerator cacheXmlGenerator) throws SAXException {
    final ContentHandler handler = cacheXmlGenerator.getContentHandler();

    try {
      handler.startPrefixMapping(PREFIX, NAMESPACE);

      final AttributesImpl atts = new AttributesImpl();
      addAttribute(atts, ATTRIBUTE_VALUE, extension.getValue());
      emptyElement(handler, PREFIX, ELEMENT_REGION, atts);
    } finally {
      handler.endPrefixMapping(PREFIX);
    }
  }
}
