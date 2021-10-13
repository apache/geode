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
package org.apache.geode.cache.lucene.internal.filesystem;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CopyHelper;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class FileJUnitTest {

  @Test
  public void testSerialization() {
    File file = new File(null, "fileName");
    file.modified = -10;
    file.length = 5;
    file.chunks = 7;
    File copy = CopyHelper.deepCopy(file);

    assertEquals(file.chunks, copy.chunks);
    assertEquals(file.created, copy.created);
    assertEquals(file.modified, copy.modified);
    assertEquals(file.getName(), copy.getName());
    assertEquals(file.length, copy.length);
    assertEquals(file.id, copy.id);
  }

}
