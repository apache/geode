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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneIndexXmlGeneratorJUnitTest {

  /**
   * Test of generating cache configuration.
   */
  @Test
  public void generateWithFields() throws Exception {
    LuceneIndexCreation index = mock(LuceneIndexCreation.class);
    when(index.getName()).thenReturn("index");
    String[] fields = new String[] {"field1", "field2"};
    when(index.getFieldNames()).thenReturn(fields);

    LuceneIndexXmlGenerator generator = new LuceneIndexXmlGenerator(index);
    CacheXmlGenerator cacheXmlGenerator = mock(CacheXmlGenerator.class);
    ContentHandler handler = mock(ContentHandler.class);
    when(cacheXmlGenerator.getContentHandler()).thenReturn(handler);
    generator.generate(cacheXmlGenerator);

    ArgumentCaptor<Attributes> captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler).startElement(eq(""), eq("index"), eq("lucene:index"), captor.capture());
    Attributes value = captor.getValue();
    assertEquals("index", value.getValue(LuceneXmlConstants.NAME));

    captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler, times(2)).startElement(eq(""), eq("field"), eq("lucene:field"),
        captor.capture());
    Set<String> foundFields = new HashSet<>();
    for (Attributes fieldAttr : captor.getAllValues()) {
      foundFields.add(fieldAttr.getValue(LuceneXmlConstants.NAME));
    }

    HashSet<String> expected = new HashSet<>(Arrays.asList(fields));
    assertEquals(expected, foundFields);

    verify(handler, times(2)).endElement(eq(""), eq("field"), eq("lucene:field"));
    verify(handler).endElement(eq(""), eq("index"), eq("lucene:index"));
  }

  /**
   * Test generating lucene xml with serializer
   */
  @Test
  public void generateWithSerializer() throws Exception {
    LuceneIndexCreation index = mock(LuceneIndexCreation.class);
    LuceneSerializer mySerializer =
        mock(LuceneSerializer.class, withSettings().extraInterfaces(Declarable2.class));
    Properties props = new Properties();
    props.put("param", "value");
    when(index.getName()).thenReturn("index");
    String[] fields = new String[] {"field1", "field2"};
    when(index.getFieldNames()).thenReturn(fields);
    when(index.getLuceneSerializer()).thenReturn(mySerializer);
    when(((Declarable2) mySerializer).getConfig()).thenReturn(props);

    LuceneIndexXmlGenerator generator = new LuceneIndexXmlGenerator(index);
    CacheXmlGenerator cacheXmlGenerator = mock(CacheXmlGenerator.class);
    ContentHandler handler = mock(ContentHandler.class);
    when(cacheXmlGenerator.getContentHandler()).thenReturn(handler);
    generator.generate(cacheXmlGenerator);

    ArgumentCaptor<Attributes> captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler).startElement(eq(""), eq("index"), eq("lucene:index"), captor.capture());
    Attributes value = captor.getValue();
    assertEquals("index", value.getValue(LuceneXmlConstants.NAME));

    // fields
    captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler, times(2)).startElement(eq(""), eq("field"), eq("lucene:field"),
        captor.capture());
    Set<String> foundFields = new HashSet<>();
    for (Attributes fieldAttr : captor.getAllValues()) {
      foundFields.add(fieldAttr.getValue(LuceneXmlConstants.NAME));
    }

    HashSet<String> expected = new HashSet<>(Arrays.asList(fields));
    assertEquals(expected, foundFields);

    verify(handler, times(2)).endElement(eq(""), eq("field"), eq("lucene:field"));

    // serializer
    captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler, times(1)).startElement(eq(""), eq("serializer"), eq("lucene:serializer"),
        captor.capture());

    captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler, times(1)).startElement(eq(""), eq("class-name"), eq("class-name"),
        captor.capture());

    String expectedString = mySerializer.getClass().getName();
    verify(handler).characters(eq(expectedString.toCharArray()), eq(0),
        eq(expectedString.length()));
    verify(handler).endElement(eq(""), eq("class-name"), eq("class-name"));

    // properties as parameters
    captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler, times(1)).startElement(eq(""), eq("parameter"), eq("parameter"),
        captor.capture());
    value = captor.getValue();
    assertEquals("param", value.getValue(LuceneXmlConstants.NAME));

    captor = ArgumentCaptor.forClass(Attributes.class);
    verify(handler, times(1)).startElement(eq(""), eq("string"), eq("string"), captor.capture());
    String expectedValue = "value";
    verify(handler).characters(eq(expectedValue.toCharArray()), eq(0), eq(expectedValue.length()));
    verify(handler).endElement(eq(""), eq("string"), eq("string"));
    verify(handler).endElement(eq(""), eq("parameter"), eq("parameter"));

    // endElement invocations
    verify(handler).endElement(eq(""), eq("serializer"), eq("lucene:serializer"));
    verify(handler).endElement(eq(""), eq("index"), eq("lucene:index"));
  }

}
