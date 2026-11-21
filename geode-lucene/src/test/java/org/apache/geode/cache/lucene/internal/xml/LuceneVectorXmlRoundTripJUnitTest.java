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
package org.apache.geode.cache.lucene.internal.xml;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.management.configuration.Index;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;
import org.apache.geode.internal.cache.xmlcache.XmlGeneratorUtils;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneVectorXmlRoundTripJUnitTest {

  @Test
  public void xmlGeneratorAndParserRoundTripVectorFieldsExactly() throws Exception {
    LuceneIndexCreation source = new LuceneIndexCreation();
    source.setName("hybridVecIdx");
    source.addFieldAndAnalyzer("category", new KeywordAnalyzer());

    Map<String, VectorIndexMetadata> expectedVectors = new LinkedHashMap<>();
    expectedVectors.put("embedding",
        new VectorIndexMetadata("embedding", 768, VectorSimilarityFunction.COSINE, 32, 200, true));
    expectedVectors.put("dense",
        new VectorIndexMetadata("dense", 128, VectorSimilarityFunction.EUCLIDEAN, 16, 100, false));
    source.setVectorFields(expectedVectors);

    // 1. Generate XML fragment using LuceneIndexXmlGenerator
    StringWriter xmlWriter = new StringWriter();
    SAXTransformerFactory tf = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
    TransformerHandler handler = tf.newTransformerHandler();
    handler.getTransformer().setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
    handler.setResult(new StreamResult(xmlWriter));

    CacheXmlGenerator cacheXmlGenerator = mock(CacheXmlGenerator.class);
    when(cacheXmlGenerator.getContentHandler()).thenReturn(handler);

    handler.startDocument();
    new LuceneIndexXmlGenerator(source).generate(cacheXmlGenerator);
    handler.endDocument();

    String generatedXml = xmlWriter.toString();
    assertThat(generatedXml)
        .contains("vector-field")
        .contains("name=\"embedding\"")
        .contains("dimension=\"768\"")
        .contains("similarity=\"COSINE\"")
        .contains("max-connections=\"32\"")
        .contains("beam-width=\"200\"")
        .contains("quantize-sq8=\"true\"");

    // 2. Parse generated XML back via LuceneXmlParser
    RegionCreation regionCreation = mock(RegionCreation.class);
    CacheCreation cacheCreation = mock(CacheCreation.class);
    when(regionCreation.getCache()).thenReturn(cacheCreation);
    when(regionCreation.getFullPath()).thenReturn(SEPARATOR + "docs");
    when(regionCreation.getAttributes()).thenReturn(new RegionAttributesCreation());
    SimpleExtensionPoint extensionPoint = new SimpleExtensionPoint(regionCreation, regionCreation);
    when(regionCreation.getExtensionPoint()).thenReturn(extensionPoint);

    Stack<Object> stack = new Stack<>();
    stack.push(cacheCreation);
    stack.push(regionCreation);

    LuceneXmlParser parser = new LuceneXmlParser();
    parser.setStack(stack);

    SAXParserFactory spf = SAXParserFactory.newInstance();
    spf.setNamespaceAware(true);
    SAXParser saxParser = spf.newSAXParser();
    saxParser.parse(new InputSource(new StringReader(generatedXml)), new DefaultHandler() {
      @Override
      public void startElement(String uri, String localName, String qName, Attributes attributes)
          throws SAXException {
        parser.startElement(uri, localName, qName, attributes);
      }

      @Override
      public void endElement(String uri, String localName, String qName) throws SAXException {
        parser.endElement(uri, localName, qName);
      }
    });

    LuceneIndexCreation parsed =
        (LuceneIndexCreation) extensionPoint.getExtensions().iterator().next();
    assertThat(parsed.getName()).isEqualTo("hybridVecIdx");
    assertThat(parsed.getFieldNames()).containsExactly("category");
    assertThat(parsed.getVectorFields()).isEqualTo(expectedVectors);
  }

  @Test
  public void xmlParserRejectsInvalidVectorFieldAttributes() throws Exception {
    RegionCreation regionCreation = mock(RegionCreation.class);
    CacheCreation cacheCreation = mock(CacheCreation.class);
    when(regionCreation.getCache()).thenReturn(cacheCreation);
    when(regionCreation.getFullPath()).thenReturn(SEPARATOR + "docs");
    when(regionCreation.getExtensionPoint())
        .thenReturn(new SimpleExtensionPoint(regionCreation, regionCreation));

    Stack<Object> stack = new Stack<>();
    stack.push(cacheCreation);
    stack.push(regionCreation);

    LuceneXmlParser parser = new LuceneXmlParser();
    parser.setStack(stack);

    AttributesImpl indexAttrs = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(indexAttrs, LuceneXmlConstants.NAME, "idx");
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, indexAttrs);

    AttributesImpl badDim = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(badDim, LuceneXmlConstants.NAME, "emb");
    XmlGeneratorUtils.addAttribute(badDim, LuceneXmlConstants.DIMENSION, "-8");
    assertThatThrownBy(() -> parser.startElement(LuceneXmlConstants.NAMESPACE,
        LuceneXmlConstants.VECTOR_FIELD, null, badDim))
            .isInstanceOf(CacheXmlException.class)
            .hasMessageContaining("dimension");
  }

  @Test
  public void jaxbManagementIndexConfigurationRoundTripsVectorFields() throws Exception {
    Index index = new Index();
    index.setName("vecConfigIdx");
    Index.Field textField = new Index.Field();
    textField.setName("category");
    index.getField().add(textField);

    Index.VectorField vf = new Index.VectorField();
    vf.setName("embedding");
    vf.setDimension(384);
    vf.setSimilarity("COSINE");
    vf.setMaxConnections(24);
    vf.setBeamWidth(150);
    vf.setQuantizeSQ8(true);
    index.getVectorField().add(vf);

    JAXBContext jaxbContext = JAXBContext.newInstance(Index.class);
    Marshaller marshaller = jaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    marshaller.marshal(index, writer);

    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    Index restored = (Index) unmarshaller.unmarshal(new StringReader(writer.toString()));

    assertThat(restored.getName()).isEqualTo("vecConfigIdx");
    assertThat(restored.getVectorField()).hasSize(1);
    Index.VectorField restoredVf = restored.getVectorField().get(0);
    assertThat(restoredVf.getName()).isEqualTo("embedding");
    assertThat(restoredVf.getDimension()).isEqualTo(384);
    assertThat(restoredVf.getSimilarity()).isEqualTo("COSINE");
    assertThat(restoredVf.getMaxConnections()).isEqualTo(24);
    assertThat(restoredVf.getBeamWidth()).isEqualTo(150);
    assertThat(restoredVf.isQuantizeSQ8()).isTrue();
  }
}
