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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.geode.cache.lucene.FlatFormatSerializer;
import org.apache.geode.cache.lucene.internal.repository.serializer.SerializerTestHelper;
import org.apache.geode.cache.lucene.test.Customer;
import org.apache.geode.cache.lucene.test.Page;
import org.apache.geode.cache.lucene.test.Person;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class FlatFormatSerializerJUnitTest {

  @Test
  public void shouldParseMultiLayerNestObject() {
    String[] fields = new String[] {"name", "phoneNumbers", "myHomePages.content", "contacts.name",
        "contacts.email", "contacts.phoneNumbers", "contacts.address", "contacts.revenue",
        "contacts.homepage.id", "contacts.homepage.title", "contacts.homepage.content",
        LuceneService.REGION_VALUE_FIELD};

    FlatFormatSerializer serializer = new FlatFormatSerializer();

    Person contact1 = new Person("Tommi Jackson", new String[] {"5036330001", "5036330002"}, 1);
    Person contact2 = new Person("Tommi2 Skywalker", new String[] {"5036330003", "5036330004"}, 2);
    HashSet<Person> contacts1 = new HashSet();
    contacts1.add(contact1);
    contacts1.add(contact2);
    ArrayList<String> phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330001");
    phoneNumbers.add("5035330002");
    Page[] myHomePages1 = new Page[] {new Page(131), new Page(132)};
    Customer customer = new Customer("Tommy Jackson", phoneNumbers, contacts1, myHomePages1);
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);
    assertEquals(23, doc1.getFields().size());
    assertEquals("Tommy Jackson", doc1.getField("name").stringValue());

    IndexableField[] fieldsInDoc = doc1.getFields("myHomePages.content");
    Collection<String> results = getStringResultCollection(fieldsInDoc);
    assertEquals(2, results.size());
    assertTrue(results.contains("Hello world no 131"));
    assertTrue(results.contains("Hello world no 132"));

    fieldsInDoc = doc1.getFields("contacts.name");
    results = getStringResultCollection(fieldsInDoc);
    assertEquals(2, results.size());
    assertTrue(results.contains("Tommi Jackson"));
    assertTrue(results.contains("Tommi2 Skywalker"));

    fieldsInDoc = doc1.getFields("contacts.email");
    results = getStringResultCollection(fieldsInDoc);
    assertEquals(2, results.size());
    assertTrue(results.contains("Tommi.Jackson@pivotal.io"));
    assertTrue(results.contains("Tommi2.Skywalker@pivotal.io"));

    fieldsInDoc = doc1.getFields("contacts.revenue");
    Collection<Integer> intResults = getIntResultCollection(fieldsInDoc);
    assertEquals(2, intResults.size());
    assertTrue(intResults.contains(100));
    assertTrue(intResults.contains(200));

    fieldsInDoc = doc1.getFields("contacts.address");
    results = getStringResultCollection(fieldsInDoc);
    assertEquals(2, results.size());
    assertTrue(results.contains("1 NW Greenbrier PKWY, Portland OR 97006"));
    assertTrue(results.contains("2 NW Greenbrier PKWY, Portland OR 97006"));

    fieldsInDoc = doc1.getFields("contacts.homepage.title");
    results = getStringResultCollection(fieldsInDoc);
    assertEquals(2, results.size());
    assertTrue(results.contains("developer"));
    assertTrue(results.contains("manager"));

    fieldsInDoc = doc1.getFields("contacts.homepage.content");
    results = getStringResultCollection(fieldsInDoc);
    assertEquals(2, results.size());
    assertTrue(results.contains("Hello world no 1"));
    assertTrue(results.contains("Hello world no 1"));
  }

  @Test
  public void shouldParseRegionValueFieldForString() {
    String[] fields = new String[] {"name", "contacts.name", "contacts.email", "contacts.revenue",
        "contacts.address", "contacts.homepage.id", "contacts.homepage.title",
        "contacts.homepage.content", LuceneService.REGION_VALUE_FIELD};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    String str = new String("Hello world");
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, str, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals("Hello world", doc1.getField(LuceneService.REGION_VALUE_FIELD).stringValue());
  }

  @Test
  public void shouldParseRegionValueFieldForInteger() {
    String[] fields = new String[] {"name", "contacts.name", "contacts.email", "contacts.revenue",
        "contacts.address", "contacts.homepage.id", "contacts.homepage.title",
        "contacts.homepage.content", LuceneService.REGION_VALUE_FIELD};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    Integer integer = 15;
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, integer, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals(15, doc1.getField(LuceneService.REGION_VALUE_FIELD).numericValue());
  }

  @Test
  public void shouldNotParseNestedObjectWithoutFields() {
    String[] fields =
        new String[] {"name", "contacts", "contacts.homepage", "contacts.missing", "missing2"};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    HashSet<Person> contacts1 = new HashSet();
    Person contact1 = new Person("Tommi Jackson", new String[] {"5036330001", "5036330002"}, 1);
    Person contact2 = new Person("Tommi2 Skywalker", new String[] {"5036330003", "5036330004"}, 2);
    contacts1.add(contact1);
    contacts1.add(contact2);
    ArrayList<String> phoneNumbers = new ArrayList();
    phoneNumbers.add("5035330001");
    phoneNumbers.add("5035330002");
    Page[] myHomePages1 = new Page[] {new Page(131), new Page(132)};
    Customer customer = new Customer("Tommy Jackson", phoneNumbers, contacts1, myHomePages1);
    Document doc1 = SerializerTestHelper.invokeSerializer(serializer, customer, fields);
    assertEquals(1, doc1.getFields().size());
    assertEquals("Tommy Jackson", doc1.getField("name").stringValue());
  }

  private Collection<String> getStringResultCollection(IndexableField[] fieldsInDoc) {
    Collection<String> results = new LinkedHashSet();
    for (IndexableField field : fieldsInDoc) {
      results.add(field.stringValue());
    }
    return results;
  }

  private Collection<Integer> getIntResultCollection(IndexableField[] fieldsInDoc) {
    Collection<Integer> results = new LinkedHashSet();
    for (IndexableField field : fieldsInDoc) {
      results.add((Integer) field.numericValue());
    }
    return results;
  }

}
