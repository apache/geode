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
package org.apache.geode.cache.lucene;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category(LuceneTest.class)
public class FlatFormatPdxSerializerIntegrationTest {

  private PdxInstance createPdxInstance() {
    HashSet positions = new HashSet();

    PdxInstanceFactoryImpl outForPosition1 = (PdxInstanceFactoryImpl) PdxInstanceFactoryImpl
        .newCreator("dummy.PositionPdx", false, localCacheRule.getCache());
    outForPosition1.writeString("country", "USA");
    outForPosition1.writeString("secId", "DELL");
    outForPosition1.writeDouble("sharesOutstanding", 3000);
    outForPosition1.writeInt("pid", 13);
    outForPosition1.writeInt("portfolioId", 3);
    // Identity Field.
    outForPosition1.markIdentityField("secId");
    PdxInstance position1_pdx = outForPosition1.create();

    PdxInstanceFactoryImpl outForPositions1 = (PdxInstanceFactoryImpl) PdxInstanceFactoryImpl
        .newCreator("dummy.PositionPdx", false, localCacheRule.getCache());
    outForPositions1.writeString("country", "USA");
    outForPositions1.writeString("secId", "AAPL");
    outForPositions1.writeDouble("sharesOutstanding", 5000);
    outForPositions1.writeInt("pid", 15);
    outForPositions1.writeInt("portfolioId", 3);
    // Identity Field.
    outForPositions1.markIdentityField("secId");
    PdxInstance positions1_pdx = outForPositions1.create();

    PdxInstanceFactoryImpl outForPositions2 = (PdxInstanceFactoryImpl) PdxInstanceFactoryImpl
        .newCreator("dummy.PositionPdx", false, localCacheRule.getCache());
    outForPositions2.writeString("country", "USA");
    outForPositions2.writeString("secId", "IBM");
    outForPositions2.writeDouble("sharesOutstanding", 4000);
    outForPositions2.writeInt("pid", 14);
    outForPositions2.writeInt("portfolioId", 3);
    // Identity Field.
    outForPositions2.markIdentityField("secId");
    PdxInstance positions2_pdx = outForPositions2.create();

    positions.add(positions1_pdx);
    positions.add(positions2_pdx);

    PdxInstanceFactoryImpl out = (PdxInstanceFactoryImpl) PdxInstanceFactoryImpl
        .newCreator("dummy.PortfolioPdx", false, localCacheRule.getCache());
    out.writeInt("ID", 3);
    out.writeObject("position1", position1_pdx);
    out.writeObject("positions", positions);
    out.writeString("status", "active");
    out.writeStringArray("names", new String[] {"aaa", "bbb", "ccc", "ddd"});
    out.writeString("description", "John Denver");
    out.writeLong("createTime", 0);
    out.writeIntArray("intArr", new int[] {2001, 2017});
    // Identity Field.
    out.markIdentityField("ID");
    PdxInstance pdx = out.create();
    return pdx;
  }

  @Rule
  public LocalCacheRule localCacheRule = new LocalCacheRule();

  @Test
  public void shouldParseTopLevelPdxIntArray() {
    String[] fields = new String[] {"description", "status", "names", "intArr", "position1.country",
        "position1.sharesOutstanding", "position1.secId", "positions.country",
        "positions.sharesOutstanding", "positions.secId"};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    PdxInstance pdx = createPdxInstance();

    Document doc1 = invokeSerializer(serializer, pdx, fields);
    assertEquals(17, doc1.getFields().size());

    IndexableField[] fieldsInDoc = doc1.getFields("intArr");
    Collection<Object> results = getResultCollection(fieldsInDoc, true);
    assertEquals(2, results.size());
    assertTrue(results.contains(2001));
    assertTrue(results.contains(2017));
  }

  @Test
  public void shouldParseTopLevelPdxStringField() {
    String[] fields = new String[] {"status"};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    PdxInstance pdx = createPdxInstance();
    Document doc1 = invokeSerializer(serializer, pdx, fields);

    IndexableField[] fieldsInDoc = doc1.getFields("status");
    Collection<Object> results = getResultCollection(fieldsInDoc, false);
    assertEquals(1, results.size());
    assertTrue(results.contains("active"));
  }

  @Test
  public void shouldParseSecondTopLevelPdxStringField() {
    String[] fields = new String[] {"positions.secId"};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    PdxInstance pdx = createPdxInstance();
    Document doc1 = invokeSerializer(serializer, pdx, fields);

    IndexableField[] fieldsInDoc = doc1.getFields("positions.secId");
    Collection<Object> results = getResultCollection(fieldsInDoc, false);
    assertEquals(2, results.size());
    assertTrue(results.contains("IBM"));
    assertTrue(results.contains("AAPL"));
  }

  @Test
  public void shouldParseSecondTopLevelPdxDoubleField() {
    String[] fields = new String[] {"positions.sharesOutstanding"};

    FlatFormatSerializer serializer = new FlatFormatSerializer();
    PdxInstance pdx = createPdxInstance();
    Document doc1 = invokeSerializer(serializer, pdx, fields);

    IndexableField[] fieldsInDoc = doc1.getFields("positions.sharesOutstanding");
    Collection<Object> results = getResultCollection(fieldsInDoc, true);
    assertEquals(2, results.size());
    assertTrue(results.contains(5000.0));
    assertTrue(results.contains(4000.0));
  }

  private Collection<Object> getResultCollection(IndexableField[] fieldsInDoc, boolean isNumeric) {
    Collection<Object> results = new LinkedHashSet();
    for (IndexableField field : fieldsInDoc) {
      if (isNumeric) {
        results.add(field.numericValue());
      } else {
        results.add(field.stringValue());
      }
    }
    return results;
  }

  private static Document invokeSerializer(LuceneSerializer mapper, Object object,
      String[] fields) {
    LuceneIndex index = Mockito.mock(LuceneIndex.class);
    Mockito.when(index.getFieldNames()).thenReturn(fields);
    Collection<Document> docs = mapper.toDocuments(index, object);
    assertEquals(1, docs.size());
    return docs.iterator().next();
  }
}
