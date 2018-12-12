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

import java.util.Stack;

import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import org.apache.geode.LogWriter;


/**
 * Abstract for {@link XmlParser} that throws {@link UnsupportedOperationException} on unused
 * methods. Also keeps references for {@link #logWriter} and {@link #stack}
 *
 *
 * @since GemFire 8.1
 */
// UnitTest AbstractXmlParserTest
public abstract class AbstractXmlParser implements XmlParser {

  protected LogWriter logWriter;

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
