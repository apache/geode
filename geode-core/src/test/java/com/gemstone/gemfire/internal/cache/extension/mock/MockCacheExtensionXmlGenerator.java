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

package com.gemstone.gemfire.internal.cache.extension.mock;

import static com.gemstone.gemfire.internal.cache.extension.mock.MockExtensionXmlParser.*;
import static com.gemstone.gemfire.internal.cache.xmlcache.XmlGeneratorUtils.*;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

/**
 * {@link MockCacheExtension} {@link XmlGenerator}.
 * 
 *
 * @since 8.1
 */
public final class MockCacheExtensionXmlGenerator extends AbstractMockExtensionXmlGenerator<Cache> {
  public MockCacheExtensionXmlGenerator(MockCacheExtension extension) {
    super(extension);
  }

  @Override
  public void generate(CacheXmlGenerator cacheXmlGenerator) throws SAXException {
    final ContentHandler handler = cacheXmlGenerator.getContentHandler();

    try {
      handler.startPrefixMapping(PREFIX, NAMESPACE);

      final AttributesImpl atts = new AttributesImpl();
      addAttribute(atts, ATTRIBUTE_VALUE, extension.getValue());
      emptyElement(handler, PREFIX, ELEMENT_CACHE, atts);
    } finally {
      handler.endPrefixMapping("mock");
    }
  }
}
