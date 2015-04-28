/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.Stack;

import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Abstract for {@link XmlParser} that throws
 * {@link UnsupportedOperationException} on unused methods. Also keeps
 * references for {@link #logWriter} and {@link #stack}
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
// UnitTest AbstractXmlParserTest
public abstract class AbstractXmlParser implements XmlParser {

  protected LogWriterI18n logWriter;

  protected Stack<Object> stack;

  protected Locator documentLocator;

  @Override
  public void setStack(final Stack<Object> stack) {
    this.stack = stack;
  }

  @Override
  public void setDocumentLocator(Locator locator) {
    this.documentLocator = locator;
  }

  @Override
  public void startDocument() throws SAXException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endDocument() throws SAXException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startPrefixMapping(String prefix, String uri) throws SAXException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void endPrefixMapping(String prefix) throws SAXException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void processingInstruction(String target, String data) throws SAXException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skippedEntity(String name) throws SAXException {
    throw new UnsupportedOperationException();
  }

}
