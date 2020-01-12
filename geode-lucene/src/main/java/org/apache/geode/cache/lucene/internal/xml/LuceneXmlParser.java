/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.xml;

import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.ANALYZER;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.FIELD;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.INDEX;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.NAME;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.NAMESPACE;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.SERIALIZER;

import org.apache.lucene.analysis.Analyzer;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.xmlcache.AbstractXmlParser;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;

public class LuceneXmlParser extends AbstractXmlParser {
  private CacheCreation cache;

  @Override
  public String getNamespaceUri() {
    return NAMESPACE;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes atts)
      throws SAXException {

    if (!NAMESPACE.equals(uri)) {
      return;
    }
    if (INDEX.equals(localName)) {
      startIndex(atts);
    }
    if (FIELD.equals(localName)) {
      startField(atts);
    }
    if (SERIALIZER.equals(localName)) {
      startSerializer(atts);
    }

  }

  private void startSerializer(Attributes atts) {
    // Ignore any whitespace noise between fields
    if (stack.peek() instanceof StringBuffer) {
      stack.pop();
    }

    if (!(stack.peek() instanceof LuceneIndexCreation)) {
      throw new CacheXmlException(
          "lucene <serializer> elements must occur within lucene <index> elements");
    }
    LuceneIndexCreation creation = (LuceneIndexCreation) stack.peek();
  }

  private void startField(Attributes atts) {
    // Ignore any whitespace noise between fields
    if (stack.peek() instanceof StringBuffer) {
      stack.pop();
    }

    if (!(stack.peek() instanceof LuceneIndexCreation)) {
      throw new CacheXmlException(
          "lucene <field> elements must occur within lucene <index> elements");
    }
    LuceneIndexCreation creation = (LuceneIndexCreation) stack.peek();
    String name = atts.getValue(NAME);
    String className = atts.getValue(ANALYZER);
    if (className == null) {
      creation.addField(name);
    } else {
      Analyzer analyzer = createAnalyzer(className);
      creation.addFieldAndAnalyzer(name, analyzer);
    }
  }

  private void startIndex(Attributes atts) {
    if (!(stack.peek() instanceof RegionCreation)) {
      throw new CacheXmlException("lucene <index> elements must occur within <region> elements");
    }
    final RegionCreation region = (RegionCreation) stack.peek();
    String name = atts.getValue(NAME);
    LuceneIndexCreation indexCreation = new LuceneIndexCreation();
    indexCreation.setName(name);
    indexCreation.setRegion(region);
    region.getExtensionPoint().addExtension(indexCreation);
    stack.push(indexCreation);
    cache = (CacheCreation) region.getCache();
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (!NAMESPACE.equals(uri)) {
      return;
    }
    if (INDEX.equals(localName)) {
      endIndex();
    }

    if (SERIALIZER.equals(localName)) {
      endSerializer();
    }
  }

  private void endIndex() {
    // Ignore any whitespace noise between fields
    if (stack.peek() instanceof StringBuffer) {
      stack.pop();
    }

    // Remove the index creation from the stack
    stack.pop();
  }

  private void endSerializer() {
    Declarable d = CacheXmlParser.createDeclarable(cache, stack);
    if (!(d instanceof LuceneSerializer)) {
      throw new CacheXmlException(
          d.getClass().getName() + " is not an instance of LuceneSerializer");
    }

    LuceneIndexCreation indexCreation = (LuceneIndexCreation) stack.peek();
    indexCreation.setLuceneSerializer((LuceneSerializer) d);

  }

  private Analyzer createAnalyzer(String className) {
    Object obj;
    try {
      Class c = InternalDataSerializer.getCachedClass(className);
      obj = c.newInstance();
    } catch (Exception ex) {
      throw new CacheXmlException(
          String.format("While instantiating a %s", className), ex);
    }
    if (!(obj instanceof Analyzer)) {
      throw new CacheXmlException(
          String.format("Class %s is not an instance of Analyzer.",
              className));
    }
    return (Analyzer) obj;
  }
}
