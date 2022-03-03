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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;
import java.util.Stack;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;
import org.apache.geode.internal.cache.xmlcache.XmlGeneratorUtils;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneIndexXmlParserJUnitTest {

  private LuceneXmlParser parser;

  private RegionCreation rc;

  private Stack<Object> stack;

  @Before
  public void setUp() {
    parser = new LuceneXmlParser();
    CacheCreation cache = Mockito.mock(CacheCreation.class);
    RegionCreation regionCreation = Mockito.mock(RegionCreation.class);
    RegionAttributesCreation rac = Mockito.mock(RegionAttributesCreation.class);
    Mockito.when(regionCreation.getFullPath()).thenReturn(SEPARATOR + "region");
    Mockito.when(regionCreation.getAttributes()).thenReturn(rac);
    Mockito.when(regionCreation.getExtensionPoint())
        .thenReturn(new SimpleExtensionPoint(rc, rc));
    rc = regionCreation;
    stack = new Stack<>();
    stack.push(cache);
    stack.push(rc);
    parser.setStack(stack);
  }

  @After
  public void tearDown() {
    parser = null;
    rc = null;
    stack = null;
  }

  @Test
  public void generateWithFields() throws SAXException {
    AttributesImpl attrs = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attrs, LuceneXmlConstants.NAME, "index");
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, attrs);

    addField("field1");
    addField("field2");
    addField("field3", KeywordAnalyzer.class.getName());

    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null);
    assertEquals(rc, stack.peek());

    LuceneIndexCreation index =
        (LuceneIndexCreation) rc.getExtensionPoint().getExtensions().iterator().next();
    assertEquals("index", index.getName());
    assertArrayEquals(new String[] {"field1", "field2", "field3"}, index.getFieldNames());

    // Assert analyzers
    Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
    assertEquals(1, fieldAnalyzers.size());
    assertTrue(fieldAnalyzers.containsKey("field3"));
    assertTrue(fieldAnalyzers.get("field3") instanceof KeywordAnalyzer);
  }

  @Test
  public void attemptInvalidAnalyzerClass() throws SAXException {
    AttributesImpl attrs = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attrs, LuceneXmlConstants.NAME, "index");
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, attrs);
    try {
      addField("field", "some.invalid.class");
      fail("Should not have been able to add a field with an invalid analyzer class name");
    } catch (Exception ignored) {
    }
  }

  private void addField(String fieldName) throws SAXException {
    addField(fieldName, null);
  }

  private void addField(String fieldName, String analyzerClassName) throws SAXException {
    AttributesImpl field = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(field, LuceneXmlConstants.NAME, fieldName);
    if (analyzerClassName != null) {
      XmlGeneratorUtils.addAttribute(field, LuceneXmlConstants.ANALYZER, analyzerClassName);
    }
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null, field);
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null);
  }
}
