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
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.BEAM_WIDTH;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.DIMENSION;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.FIELD;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.INDEX;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.MAX_CONNECTIONS;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.NAME;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.NAMESPACE;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.PREFIX;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.QUANTIZE_SQ8;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.SERIALIZER;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.SIMILARITY;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.VECTOR_FIELD;

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.internal.cache.xmlcache.XmlGeneratorUtils;

public class LuceneIndexXmlGenerator implements XmlGenerator<Region<?, ?>> {
  private static final AttributesImpl EMPTY = new AttributesImpl();

  private final LuceneIndexCreation index;

  public LuceneIndexXmlGenerator(LuceneIndexCreation index) {
    this.index = index;
  }

  @Override
  public String getNamespaceUri() {
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
    String[] fieldNames = index.getFieldNames();
    if (fieldNames != null) {
      for (String field : fieldNames) {
        AttributesImpl fieldAttr = new AttributesImpl();
        XmlGeneratorUtils.addAttribute(fieldAttr, NAME, field);
        Analyzer analyzer = index.getFieldAnalyzers().get(field);
        if (analyzer != null) {
          XmlGeneratorUtils.addAttribute(fieldAttr, ANALYZER, analyzer.getClass().getName());
        }
        XmlGeneratorUtils.emptyElement(handler, PREFIX, FIELD, fieldAttr);
      }
    }

    Map<String, VectorIndexMetadata> vectorFields = index.getVectorFields();
    if (vectorFields != null && !vectorFields.isEmpty()) {
      for (VectorIndexMetadata vectorMetadata : vectorFields.values()) {
        AttributesImpl vectorAttr = new AttributesImpl();
        XmlGeneratorUtils.addAttribute(vectorAttr, NAME, vectorMetadata.getFieldName());
        XmlGeneratorUtils.addAttribute(vectorAttr, DIMENSION,
            Integer.toString(vectorMetadata.getDimension()));
        XmlGeneratorUtils.addAttribute(vectorAttr, SIMILARITY,
            vectorMetadata.getSimilarityFunction().name());
        XmlGeneratorUtils.addAttribute(vectorAttr, MAX_CONNECTIONS,
            Integer.toString(vectorMetadata.getMaxConnections()));
        XmlGeneratorUtils.addAttribute(vectorAttr, BEAM_WIDTH,
            Integer.toString(vectorMetadata.getBeamWidth()));
        XmlGeneratorUtils.addAttribute(vectorAttr, QUANTIZE_SQ8,
            Boolean.toString(vectorMetadata.isQuantizeSQ8()));
        XmlGeneratorUtils.emptyElement(handler, PREFIX, VECTOR_FIELD, vectorAttr);
      }
    }

    LuceneSerializer serializer = index.getLuceneSerializer();
    if (serializer != null) {
      XmlGeneratorUtils.startElement(handler, PREFIX, SERIALIZER, EMPTY);
      XmlGeneratorUtils.addDeclarable(handler, serializer);
      XmlGeneratorUtils.endElement(handler, PREFIX, SERIALIZER);
    }
    XmlGeneratorUtils.endElement(handler, PREFIX, INDEX);
  }
}
