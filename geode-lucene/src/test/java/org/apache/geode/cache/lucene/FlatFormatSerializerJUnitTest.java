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
package org.apache.geode.cache.lucene;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.geode.cache.lucene.FlatFormatSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerTestHelper;
import org.apache.geode.cache.lucene.test.Customer;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.lucene.document.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class FlatFormatSerializerJUnitTest {

  @Test
  public void shouldParseMultiLayerNestObject() {
    String[] fields = new String[] {"name", "contact.name", "contact.email", "contact.revenue",
        "contact.address", "contact.homepage.id", "contact.homepage.title",
        "contact.homepage.content", LuceneService.REGION_VALUE_FIELD};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    Customer customer = new Customer("Tommy Jackson", "Tommi Jackson", 13);
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);
    assertEquals(fields.length - 1, doc1.getFields().size());
    assertEquals("Tommy Jackson", doc1.getField("name").stringValue());
    assertEquals("Tommi Jackson", doc1.getField("contact.name").stringValue());
    assertEquals("Tommi.Jackson@pivotal.io", doc1.getField("contact.email").stringValue());
    assertEquals(1300, doc1.getField("contact.revenue").numericValue());
    assertEquals("13 NW Greenbrier PKWY, Portland OR 97006",
        doc1.getField("contact.address").stringValue());
    assertEquals("developer", doc1.getField("contact.homepage.title").stringValue());
    assertEquals("Hello world no 13", doc1.getField("contact.homepage.content").stringValue());
  }

  @Test
  public void shouldParseRegionValueFieldForString() {
    String[] fields = new String[] {"name", "contact.name", "contact.email", "contact.revenue",
        "contact.address", "contact.homepage.id", "contact.homepage.title",
        "contact.homepage.content", LuceneService.REGION_VALUE_FIELD};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    String str = new String("Hello world");
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, str, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals("Hello world", doc1.getField(LuceneService.REGION_VALUE_FIELD).stringValue());
  }

  @Test
  public void shouldParseRegionValueFieldForInteger() {
    String[] fields = new String[] {"name", "contact.name", "contact.email", "contact.revenue",
        "contact.address", "contact.homepage.id", "contact.homepage.title",
        "contact.homepage.content", LuceneService.REGION_VALUE_FIELD};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    Integer integer = 15;
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, integer, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals(15, doc1.getField(LuceneService.REGION_VALUE_FIELD).numericValue());
  }

  @Test
  public void shouldNotParseNestedObjectWithoutFields() {
    String[] fields =
        new String[] {"name", "contact", "contact.homepage", "contact.missing", "missing2"};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    Customer customer = new Customer("Tommy Jackson", "Tommi Jackson", 13);
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals("Tommy Jackson", doc1.getField("name").stringValue());
  }

}
