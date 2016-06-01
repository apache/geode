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
package com.gemstone.gemfire.cache.lucene.internal.directory;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static org.junit.Assert.*;

import java.io.File;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.LuceneIntegrationTest;
import com.gemstone.gemfire.cache.lucene.internal.InternalLuceneIndex;
import com.gemstone.gemfire.cache.lucene.test.TestObject;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.rules.DiskDirRule;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class DumpDirectoryFilesIntegrationTest extends LuceneIntegrationTest {
  @Rule
  public DiskDirRule diskDirRule = new DiskDirRule();

  @Test
  public void shouldDumpReadableLuceneIndexFile() throws Exception {
    luceneService.createIndex(INDEX_NAME, REGION_NAME, "title", "description");

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    region.put(0, new TestObject("title 1", "hello world"));
    region.put(1 * 113, new TestObject("title 2", "this will not match"));
    region.put(2 * 113, new TestObject("title 3", "hello world"));
    region.put(3 * 113, new TestObject("hello world", "hello world"));

    InternalLuceneIndex index = (InternalLuceneIndex) luceneService.getIndex(INDEX_NAME, REGION_NAME);

    index.waitUntilFlushed(60000);

    index.dumpFiles(diskDirRule.get().getAbsolutePath());

    //Find the directory for the first bucket
    File bucket0 = diskDirRule.get().listFiles(file -> file.getName().endsWith("_0"))[0];

    //Test that we can read the lucene index from the dump
    final FSDirectory directory = FSDirectory.open(bucket0.toPath());
    IndexReader reader = DirectoryReader.open(directory);
    IndexSearcher searcher = new IndexSearcher(reader);
    final TopDocs results = searcher.search(new MatchAllDocsQuery(), 1000);
    assertEquals(4, results.totalHits);
  }

}