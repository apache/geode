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
package org.apache.geode.cache.lucene.internal.directory;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.LuceneIntegrationTest;
import org.apache.geode.cache.lucene.internal.InternalLuceneIndex;
import org.apache.geode.cache.lucene.test.TestObject;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DumpDirectoryFilesIntegrationTest extends LuceneIntegrationTest {
  @Rule
  public TemporaryFolder tempFolderRule = new TemporaryFolder();

  @Test
  public void shouldDumpReadableLuceneIndexFile() throws Exception {
    luceneService.createIndexFactory().setFields("title", "description").create(INDEX_NAME,
        REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put(0, new TestObject("title 1", "hello world"));
    region.put(1 * 113, new TestObject("title 2", "this will not match"));
    region.put(2 * 113, new TestObject("title 3", "hello world"));
    region.put(3 * 113, new TestObject("hello world", "hello world"));

    InternalLuceneIndex index =
        (InternalLuceneIndex) luceneService.getIndex(INDEX_NAME, REGION_NAME);

    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, 60000, TimeUnit.MILLISECONDS);

    index.dumpFiles(tempFolderRule.getRoot().getAbsolutePath());

    // Find the directory for the first bucket
    File bucket0 = tempFolderRule.getRoot().listFiles(file -> file.getName().endsWith("_0"))[0];

    // Test that we can read the lucene index from the dump
    final FSDirectory directory = FSDirectory.open(bucket0.toPath());
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);
    final TopDocs results = searcher.search(new MatchAllDocsQuery(), 1000);
    assertEquals(4, results.totalHits);
  }

}
