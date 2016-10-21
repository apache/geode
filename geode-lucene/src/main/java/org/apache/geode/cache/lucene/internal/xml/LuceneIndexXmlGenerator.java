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

import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.*;

import org.apache.lucene.analysis.Analyzer;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlGeneratorUtils;

public class LuceneIndexXmlGenerator implements XmlGenerator<Region<?, ?>> {
  private final LuceneIndex index;

  public LuceneIndexXmlGenerator(LuceneIndex index) {
    this.index = index;
  }

  @Override
  public String getNamspaceUri() {
    return NAMESPACE;
  }

  @Override
  public void generate(CacheXmlGenerator cacheXmlGenerator) throws SAXException {
    final ContentHandler handler = cacheXmlGenerator.getContentHandler();

    handler.startPrefixMapping(PREFIX, NAMESPACE);

    AttributesImpl attr = new AttributesImpl();
    // TODO - should the type be xs:string ?
    XmlGeneratorUtils.addAttribute(attr, NAME, index.getName());
    XmlGeneratorUtils.startElement(handler, PREFIX, INDEX, attr);
    for (String field : index.getFieldNames()) {
      AttributesImpl fieldAttr = new AttributesImpl();
      XmlGeneratorUtils.addAttribute(fieldAttr, NAME, field);
      Analyzer analyzer = index.getFieldAnalyzers().get(field);
      if (analyzer != null) {
        XmlGeneratorUtils.addAttribute(fieldAttr, ANALYZER, analyzer.getClass().getName());
      }
      XmlGeneratorUtils.emptyElement(handler, PREFIX, FIELD, fieldAttr);
    }
    XmlGeneratorUtils.endElement(handler, PREFIX, INDEX);
  }

}
