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
package org.apache.geode.cache.lucene.internal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.lucene.LuceneIndexFactory;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneIndexFactoryImplJUnitTest {

  @Test
  public void setLuceneSerializerShouldPassLuceneSerializerToService() {
    LuceneServiceImpl service = mock(LuceneServiceImpl.class);
    LuceneSerializer serializer = mock(LuceneSerializer.class);
    LuceneIndexFactory factory = new LuceneIndexFactoryImpl(service);
    factory.setLuceneSerializer(serializer);
    factory.create("index", "region");
    Mockito.verify(service).createIndex(eq("index"), eq("region"), any(), eq(serializer),
        eq(false));
  }

}
