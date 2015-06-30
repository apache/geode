/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import static org.junit.Assert.*;

import java.util.Stack;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link AbstractXmlParser}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class AbstractXmlParserJUnitTest {

  /**
   * Test method for {@link AbstractXmlParser#setStack(java.util.Stack)}.
   */
  @Test
  public void testSetStack() {
    MockXmlParser m = new MockXmlParser();
    Stack<Object> s = new Stack<Object>();
    m.setStack(s);
    assertSame(s, m.stack);
  }

  /**
   * Test method for {@link AbstractXmlParser#setDocumentLocator(Locator)}.
   */
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
   * Test method for
   * {@link AbstractXmlParser#startPrefixMapping(String, String)}.
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
   * Test method for
   * {@link AbstractXmlParser#ignorableWhitespace(char[], int, int)}.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testIgnorableWhitespace() throws SAXException {
    new MockXmlParser().ignorableWhitespace(null, 0, 0);
  }

  /**
   * Test method for
   * {@link AbstractXmlParser#processingInstruction(String, String)}.
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

  private static final class MockXmlParser extends AbstractXmlParser {
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
      throw new IllegalStateException();
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      throw new IllegalStateException();
    }

    @Override
    public String getNamspaceUri() {
      throw new IllegalStateException();
    }
  }

}
