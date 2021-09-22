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
package org.apache.geode.cache.lucene.internal.results;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class PageResultsJUnitTest {

  @Test
  public void serializationShouldNotChangeObject() throws IOException, ClassNotFoundException {
    PageResults results = new PageResults();
    results.add(new PageEntry("key1", "value1"));
    results.add(new PageEntry("key2", "value2"));

    byte[] serialized = BlobHelper.serializeToBlob(results);
    PageResults newResults = (PageResults) BlobHelper.deserializeBlob(serialized);

    assertEquals(newResults, results);
  }

}
