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

import static org.junit.Assert.assertSame;

import java.util.Stack;

import org.junit.Test;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;


/**
 * Unit tests for {@link AbstractXmlParser}.
 *
 * @since GemFire 8.1
 */
public class AbstractXmlParserJUnitTest {

  /**
   * Test method for {@link AbstractXmlParser#setStack(java.util.Stack)}.
   */
  @Test
  public void testSetStack() {
    MockXmlParser m = new MockXmlParser();
    Stack<Object> s = new Stack<>();
    m.setStack(s);
    assertSame(s, m.stack);
  }

  /**
   * Test method for {@link AbstractXmlParser#setDocumentLocator(Locator)}.
   */
  @Test
  public void testSetDocumentLocator() {
    final MockXmlParser mockXmlParser = new MockXmlParser();
    final Locator mockLocator = new Locator() {
      @Override
      public String getSystemId() {
        return null;
      }

      @Override
      public String getPublicId() {
        return null;
      }

      @Override
      public int getLineNumber() {
        return 0;
      }

      @Override
      public int getColumnNumber() {
        return 0;
      }
    };
    mockXmlParser.setDocumentLocator(mockLocator);

    assertSame(mockLocator, mockXmlParser.documentLocator);
  }

  /**
   * Test method for {@link AbstractXmlParser#startDocument()}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testStartDocument() throws SAXException {
    new MockXmlParser().startDocument();
  }

  /**
   * Test method for {@link AbstractXmlParser#endDocument()}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testEndDocument() throws SAXException {
    new MockXmlParser().endDocument();
  }

  /**
   * Test method for {@link AbstractXmlParser#startPrefixMapping(String, String)}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testStartPrefixMapping() throws SAXException {
    new MockXmlParser().startPrefixMapping(null, null);
  }

  /**
   * Test method for {@link AbstractXmlParser#endPrefixMapping(String)}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testEndPrefixMapping() throws SAXException {
    new MockXmlParser().endPrefixMapping(null);
  }

  /**
   * Test method for {@link AbstractXmlParser#characters(char[], int, int)}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testCharacters() throws SAXException {
    new MockXmlParser().characters(null, 0, 0);
  }

  /**
   * Test method for {@link AbstractXmlParser#ignorableWhitespace(char[], int, int)}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testIgnorableWhitespace() throws SAXException {
    new MockXmlParser().ignorableWhitespace(null, 0, 0);
  }

  /**
   * Test method for {@link AbstractXmlParser#processingInstruction(String, String)}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testProcessingInstruction() throws SAXException {
    new MockXmlParser().processingInstruction(null, null);
  }

  /**
   * Test method for {@link AbstractXmlParser#skippedEntity(String)}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testSkippedEntity() throws SAXException {
    new MockXmlParser().skippedEntity(null);
  }

  private static class MockXmlParser extends AbstractXmlParser {
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts)
        throws SAXException {
      throw new IllegalStateException();
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      throw new IllegalStateException();
    }

    @Override
    public String getNamespaceUri() {
      throw new IllegalStateException();
    }
  }

}
