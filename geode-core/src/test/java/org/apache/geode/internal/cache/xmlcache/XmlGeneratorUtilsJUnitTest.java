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

import static javax.xml.XMLConstants.NULL_NS_URI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.atomic.AtomicReference;

import javax.xml.XMLConstants;

import org.junit.Test;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;


/**
 * Unit Tests for {@link XmlGeneratorUtils}.
 *
 * @since GemFire 8.1
 */
public class XmlGeneratorUtilsJUnitTest {

  /**
   * Test method for {@link XmlGeneratorUtils#addAttribute(AttributesImpl, String, Object)}.
   */
  @Test
  public void testAddAttributeAttributesImplStringObject() {
    final AttributesImpl attributes = new AttributesImpl();
    assertEquals(0, attributes.getLength());

    XmlGeneratorUtils.addAttribute(attributes, "localname", null);
    assertEquals(0, attributes.getLength());

    XmlGeneratorUtils.addAttribute(attributes, "localname", "value");
    assertEquals(1, attributes.getLength());
    assertEquals("localname", attributes.getLocalName(0));
    assertEquals("localname", attributes.getQName(0));
    assertEquals(XMLConstants.NULL_NS_URI, attributes.getURI(0));
    assertEquals("value", attributes.getValue(0));
  }

  /**
   * Test method for {@link XmlGeneratorUtils#addAttribute(AttributesImpl, String, String, Object)}.
   */
  @Test
  public void testAddAttributeAttributesImplStringStringObject() {
    final AttributesImpl attributes = new AttributesImpl();
    assertEquals(0, attributes.getLength());

    XmlGeneratorUtils.addAttribute(attributes, "prefix", "localname", null);
    assertEquals(0, attributes.getLength());

    XmlGeneratorUtils.addAttribute(attributes, "prefix", "localname", "value");
    assertEquals(1, attributes.getLength());
    assertEquals("localname", attributes.getLocalName(0));
    assertEquals("prefix:localname", attributes.getQName(0));
    assertEquals(NULL_NS_URI, attributes.getURI(0));
    assertEquals("value", attributes.getValue(0));
  }

  /**
   * Test method for
   * {@link XmlGeneratorUtils#startElement(ContentHandler, String, String, AttributesImpl)}.
   */
  @Test
  public void testStartElementContentHandlerStringStringAttributesImpl() throws SAXException {
    final AtomicReference<String> uriRef = new AtomicReference<>();
    final AtomicReference<String> localnameRef = new AtomicReference<>();
    final AtomicReference<String> qNameRef = new AtomicReference<>();
    final AtomicReference<Attributes> attributesRef = new AtomicReference<>();

    final ContentHandler contentHandler = new MockContentHandler() {
      @Override
      public void startElement(String uri, String localName, String qName, Attributes atts)
          throws SAXException {
        uriRef.set(uri);
        localnameRef.set(localName);
        qNameRef.set(qName);
        attributesRef.set(atts);
      }
    };

    XmlGeneratorUtils.startElement(contentHandler, "prefix", "localname", null);
    assertEquals("localname", localnameRef.get());
    assertEquals("prefix:localname", qNameRef.get());
    assertEquals(NULL_NS_URI, uriRef.get());
    assertNull(attributesRef.get());
  }

  /**
   * Test method for {@link XmlGeneratorUtils#endElement(ContentHandler, String, String)}.
   */
  @Test
  public void testEndElementContentHandlerStringString() throws SAXException {
    final AtomicReference<String> uriRef = new AtomicReference<>();
    final AtomicReference<String> localnameRef = new AtomicReference<>();
    final AtomicReference<String> qNameRef = new AtomicReference<>();

    final ContentHandler contentHandler = new MockContentHandler() {
      @Override
      public void endElement(String uri, String localName, String qName) throws SAXException {
        uriRef.set(uri);
        localnameRef.set(localName);
        qNameRef.set(qName);
      }
    };

    XmlGeneratorUtils.endElement(contentHandler, "prefix", "localname");
    assertEquals("localname", localnameRef.get());
    assertEquals("prefix:localname", qNameRef.get());
    assertEquals(NULL_NS_URI, uriRef.get());
  }

  /**
   * Test method for
   * {@link XmlGeneratorUtils#emptyElement(ContentHandler, String, String, AttributesImpl)}.
   */
  @Test
  public void testEmptyElement() throws SAXException {
    final AtomicReference<String> uriRef = new AtomicReference<>();
    final AtomicReference<String> localnameRef = new AtomicReference<>();
    final AtomicReference<String> qNameRef = new AtomicReference<>();
    final AtomicReference<Attributes> attributesRef = new AtomicReference<>();

    final AtomicReference<String> endUriRef = new AtomicReference<>();
    final AtomicReference<String> endLocalnameRef = new AtomicReference<>();
    final AtomicReference<String> endQNameRef = new AtomicReference<>();

    final ContentHandler contentHandler = new MockContentHandler() {
      @Override
      public void startElement(String uri, String localName, String qName, Attributes atts)
          throws SAXException {
        uriRef.set(uri);
        localnameRef.set(localName);
        qNameRef.set(qName);
        attributesRef.set(atts);
      }

      @Override
      public void endElement(String uri, String localName, String qName) throws SAXException {
        endUriRef.set(uri);
        endLocalnameRef.set(localName);
        endQNameRef.set(qName);
      }
    };

    XmlGeneratorUtils.emptyElement(contentHandler, "prefix", "localname", null);

    assertEquals("localname", localnameRef.get());
    assertEquals("prefix:localname", qNameRef.get());
    assertEquals(NULL_NS_URI, uriRef.get());
    assertNull(attributesRef.get());

    assertEquals("localname", endLocalnameRef.get());
    assertEquals("prefix:localname", endQNameRef.get());
    assertEquals(NULL_NS_URI, uriRef.get());
  }

  private static class MockContentHandler implements ContentHandler {
    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts)
        throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void startDocument() throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setDocumentLocator(Locator locator) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void endDocument() throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      throw new UnsupportedOperationException();
    }
  }

}
