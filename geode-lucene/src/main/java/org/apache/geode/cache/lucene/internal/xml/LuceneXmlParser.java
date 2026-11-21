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
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.QUANTIZE_SQ8;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.SERIALIZER;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.SIMILARITY;
import static org.apache.geode.cache.lucene.internal.xml.LuceneXmlConstants.VECTOR_FIELD;

import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
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
    if (VECTOR_FIELD.equals(localName)) {
      startVectorField(atts);
    }
    if (SERIALIZER.equals(localName)) {
      startSerializer(atts);
    }

  }

  private void startSerializer(Attributes atts) {
    // Ignore any whitespace noise between fields
    if (stack.peek() instanceof StringBuilder) {
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
    if (stack.peek() instanceof StringBuilder) {
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

  private void startVectorField(Attributes atts) {
    if (stack.peek() instanceof StringBuilder) {
      stack.pop();
    }

    if (!(stack.peek() instanceof LuceneIndexCreation)) {
      throw new CacheXmlException(
          "lucene <vector-field> elements must occur within lucene <index> elements");
    }
    LuceneIndexCreation creation = (LuceneIndexCreation) stack.peek();
    String name = atts.getValue(NAME);
    String dimStr = atts.getValue(DIMENSION);
    String similarityStr = atts.getValue(SIMILARITY);
    String maxConnStr = atts.getValue(MAX_CONNECTIONS);
    String beamWidthStr = atts.getValue(BEAM_WIDTH);
    String sq8Str = atts.getValue(QUANTIZE_SQ8);

    try {
      if (dimStr == null || dimStr.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "lucene <vector-field> requires a positive 'dimension' attribute");
      }
      int dimension = Integer.parseInt(dimStr.trim());
      VectorSimilarityFunction similarity =
          (similarityStr == null || similarityStr.trim().isEmpty())
              ? VectorSimilarityFunction.COSINE
              : VectorSimilarityFunction.valueOf(similarityStr.trim().toUpperCase(Locale.ROOT));
      int maxConnections = (maxConnStr == null || maxConnStr.trim().isEmpty())
          ? VectorIndexMetadata.DEFAULT_MAX_CONNECTIONS
          : Integer.parseInt(maxConnStr.trim());
      int beamWidth = (beamWidthStr == null || beamWidthStr.trim().isEmpty())
          ? VectorIndexMetadata.DEFAULT_BEAM_WIDTH
          : Integer.parseInt(beamWidthStr.trim());
      boolean quantizeSQ8 = parseStrictBoolean(sq8Str);

      if (creation.getVectorFields().containsKey(name)) {
        throw new IllegalArgumentException(
            "Duplicate lucene <vector-field> name: '" + name + "'");
      }

      creation.addVectorField(new VectorIndexMetadata(name, dimension, similarity, maxConnections,
          beamWidth, quantizeSQ8));
    } catch (IllegalArgumentException e) {
      throw new CacheXmlException("Invalid lucene <vector-field> configuration: " + e.getMessage(),
          e);
    }
  }

  private boolean parseStrictBoolean(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return false;
    }
    String normalized = raw.trim().toLowerCase(Locale.ROOT);
    if ("true".equals(normalized)) {
      return true;
    }
    if ("false".equals(normalized)) {
      return false;
    }
    throw new IllegalArgumentException(
        "Invalid quantize-sq8 boolean value '" + raw + "' (expected true or false)");
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
    if (stack.peek() instanceof StringBuilder) {
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
