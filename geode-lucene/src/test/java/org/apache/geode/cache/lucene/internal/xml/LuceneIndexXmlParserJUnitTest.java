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

import static org.junit.Assert.*;

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

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.extension.SimpleExtensionPoint;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;
import org.apache.geode.internal.cache.xmlcache.XmlGeneratorUtils;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexXmlParserJUnitTest {

  private LuceneXmlParser parser;

  private RegionCreation rc;

  private Stack<Object> stack;

  @Before
  public void setUp() {
    this.parser = new LuceneXmlParser();
    CacheCreation cache = Mockito.mock(CacheCreation.class);
    RegionCreation regionCreation = Mockito.mock(RegionCreation.class);
    RegionAttributesCreation rac = Mockito.mock(RegionAttributesCreation.class);
    Mockito.when(regionCreation.getFullPath()).thenReturn("/region");
    Mockito.when(regionCreation.getAttributes()).thenReturn(rac);
    Mockito.when(regionCreation.getExtensionPoint())
        .thenReturn(new SimpleExtensionPoint(this.rc, this.rc));
    this.rc = regionCreation;
    this.stack = new Stack<Object>();
    stack.push(cache);
    stack.push(rc);
    this.parser.setStack(stack);
  }

  @After
  public void tearDown() {
    this.parser = null;
    this.rc = null;
    this.stack = null;
  }

  @Test
  public void generateWithFields() throws SAXException {
    AttributesImpl attrs = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(attrs, LuceneXmlConstants.NAME, "index");
    this.parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, attrs);

    addField("field1");
    addField("field2");
    addField("field3", KeywordAnalyzer.class.getName());

    this.parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null);
    assertEquals(this.rc, this.stack.peek());

    LuceneIndexCreation index =
        (LuceneIndexCreation) this.rc.getExtensionPoint().getExtensions().iterator().next();
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
    this.parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, attrs);
    try {
      addField("field", "some.invalid.class");
      fail("Should not have been able to add a field with an invalid analyzer class name");
    } catch (Exception e) {
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
    this.parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null, field);
    this.parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null);
  }
}
