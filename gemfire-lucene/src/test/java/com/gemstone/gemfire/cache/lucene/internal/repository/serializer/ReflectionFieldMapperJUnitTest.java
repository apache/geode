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

package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import static org.junit.Assert.assertEquals;

import org.apache.lucene.document.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.ReflectionLuceneSerializer;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test of the ReflectionFieldMapperClass. Tests that 
 * all field types are mapped correctly. 
 */
@Category(UnitTest.class)
public class ReflectionFieldMapperJUnitTest {

  @Test
  public void testAllFields() {
    
    String[] allFields = new String[] {"s", "i", "l", "d", "f", "s2"};
    ReflectionLuceneSerializer mapper1 = new ReflectionLuceneSerializer(Type1.class, allFields);
    ReflectionLuceneSerializer mapper2 = new ReflectionLuceneSerializer(Type2.class, allFields);
    
    Type1 t1 = new Type1("a", 1, 2L, 3.0, 4.0f);
    Type2 t2 = new Type2("a", 1, 2L, 3.0, 4.0f, "b");
    
    Document doc1 = new Document();
    mapper1.toDocument(t1, doc1);
    
    assertEquals(5, doc1.getFields().size());
    assertEquals("a", doc1.getField("s").stringValue());
    assertEquals(1, doc1.getField("i").numericValue());
    assertEquals(2L, doc1.getField("l").numericValue());
    assertEquals(3.0, doc1.getField("d").numericValue());
    assertEquals(4.0f, doc1.getField("f").numericValue());
    
    Document doc2 = new Document();
    mapper2.toDocument(t2, doc2);
    
    assertEquals(6, doc2.getFields().size());
    assertEquals("a", doc2.getField("s").stringValue());
    assertEquals("b", doc2.getField("s2").stringValue());
    assertEquals(1, doc2.getField("i").numericValue());
    assertEquals(2L, doc2.getField("l").numericValue());
    assertEquals(3.0, doc2.getField("d").numericValue());
    assertEquals(4.0f, doc2.getField("f").numericValue());
  }
  
  @Test
  public void testIgnoreInvalid() {
    
    String[] fields = new String[] {"s", "o", "s2"};
    ReflectionLuceneSerializer mapper = new ReflectionLuceneSerializer(Type2.class, fields);
    
    Type2 t = new Type2("a", 1, 2L, 3.0, 4.0f, "b");
    
    Document doc = new Document();
    mapper.toDocument(t, doc);
    
    assertEquals(2, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertEquals("b", doc.getField("s2").stringValue());
  }
}
