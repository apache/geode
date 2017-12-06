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
package org.apache.geode.cache.lucene.internal.repository.serializer;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.apache.lucene.document.Document;
import org.mockito.Mockito;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;

public class SerializerTestHelper {
  public static Document invokeSerializer(LuceneSerializer mapper, Object object, String[] fields) {
    LuceneIndex index = Mockito.mock(LuceneIndex.class);
    Mockito.when(index.getFieldNames()).thenReturn(fields);
    Collection<Document> docs = mapper.toDocuments(index, object);
    assertEquals(1, docs.size());
    return docs.iterator().next();
  }
}
