/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.xml;

import static org.junit.Assert.*;

import java.util.Stack;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGeneratorUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexXmlParserJUnitTest {
  
  @Test
  public void generateWithFields() throws SAXException {
    LuceneXmlParser parser = new LuceneXmlParser();
    AttributesImpl attrs = new AttributesImpl();
    CacheCreation cache = new CacheCreation();
    RegionCreation rc = new RegionCreation(cache, "region");
    Stack<Object> stack = new Stack<Object>();
    stack.push(cache);
    stack.push(rc);
    parser.setStack(stack);
    XmlGeneratorUtils.addAttribute(attrs, LuceneXmlConstants.NAME, "index");
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, attrs);
    
    AttributesImpl field1 = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(field1, LuceneXmlConstants.NAME, "field1");
    AttributesImpl field2 = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(field2, LuceneXmlConstants.NAME, "field2");
    
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null, field1);
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null);
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null, field2);
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null);
    
    
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null);
    assertEquals(rc, stack.peek());
    
    LuceneIndexCreation index = (LuceneIndexCreation) rc.getExtensionPoint().getExtensions().iterator().next();
    assertEquals("index", index.getName());
    assertArrayEquals(new String[] {"field1", "field2"}, index.getFieldNames());
  }
  

}
