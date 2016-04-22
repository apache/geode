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
import static org.mockito.Matchers.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexXmlGeneratorJUnitTest {
  
  /**
   * Test of generating and reading cache configuration back in.
   * @throws SAXException 
   */
  @Test
  public void generateWithFields() throws SAXException {
    LuceneIndex index = Mockito.mock(LuceneIndex.class);
    Mockito.when(index.getName()).thenReturn("index");
    String[] fields = new String[] {"field1", "field2"};
    Mockito.when(index.getFieldNames()).thenReturn(fields);
    
    LuceneIndexXmlGenerator generator = new LuceneIndexXmlGenerator(index);
    CacheXmlGenerator cacheXmlGenerator = Mockito.mock(CacheXmlGenerator.class);
    ContentHandler handler = Mockito.mock(ContentHandler.class);
    Mockito.when(cacheXmlGenerator.getContentHandler()).thenReturn(handler);
    generator.generate(cacheXmlGenerator);
    
    ArgumentCaptor<Attributes> captor = ArgumentCaptor.forClass(Attributes.class);
    Mockito.verify(handler).startElement(eq(""), eq("index"), eq("lucene:index"), captor.capture());
    Attributes value = captor.getValue();
    assertEquals("index", value.getValue(LuceneXmlConstants.NAME));
    
    captor = ArgumentCaptor.forClass(Attributes.class);
    Mockito.verify(handler, Mockito.times(2)).startElement(eq(""), eq("field"), eq("lucene:field"), captor.capture());
    Set<String> foundFields = new HashSet<String>();
    for(Attributes fieldAttr : captor.getAllValues()) {
      foundFields.add(fieldAttr.getValue(LuceneXmlConstants.NAME));
    }
    
    HashSet<String> expected = new HashSet<String>(Arrays.asList(fields));
    assertEquals(expected, foundFields);
    
    Mockito.verify(handler, Mockito.times(2)).endElement(eq(""), eq("field"), eq("lucene:field"));
    Mockito.verify(handler).endElement(eq(""), eq("index"), eq("lucene:index"));
  }

}
