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
package org.apache.geode.cache.lucene.internal.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.repository.IndexRepositoryImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class DistributedScoringJUnitTest {

  private final String[] indexedFields = new String[] {"txt"};
  private final HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer();

  private final StandardAnalyzer analyzer = new StandardAnalyzer();
  private Region<String, String> region;
  private LuceneIndexStats indexStats;
  private FileSystemStats fileSystemStats;

  @Before
  public void createMocks() {
    region = Mockito.mock(Region.class);
    Mockito.when(region.isDestroyed()).thenReturn(false);
    indexStats = Mockito.mock(LuceneIndexStats.class);
    fileSystemStats = Mockito.mock(FileSystemStats.class);
  }

  /**
   * The goal of this test is to verify fair scoring if entries are uniformly distributed. It
   * compares ordered results from a single IndexRepository (IR) with merged-ordered results from
   * multiple repositories (ir1, ir2, ir3). The records inserted in IR are same as the combined
   * records in irX. This simulates merging of results from buckets of a region.
   */
  @Test
  public void uniformDistributionProducesComparableScores() throws Exception {
    // the strings below have been grouped to be split between three index repositories
    String[] testStrings = {"hello world", "foo bar", "just any string",

        "hello world is usually the first program", "water on mars", "test world",

        "hello", "test hello test", "find the aliens",};

    QueryParser parser = new QueryParser("txt", analyzer);
    Query query = parser.parse("hello world");

    IndexRepositoryImpl singleIndexRepo = createIndexRepo();
    populateIndex(testStrings, singleIndexRepo, 0, testStrings.length);

    TopEntriesCollector collector = new TopEntriesCollector();
    singleIndexRepo.query(query, 100, collector);
    List<EntryScore<String>> singleResult = collector.getEntries().getHits();

    IndexRepositoryImpl distIR1 = createIndexRepo();
    populateIndex(testStrings, distIR1, 0, testStrings.length / 3);

    IndexRepositoryImpl distIR2 = createIndexRepo();
    populateIndex(testStrings, distIR2, testStrings.length / 3, (testStrings.length * 2) / 3);

    IndexRepositoryImpl distIR3 = createIndexRepo();
    populateIndex(testStrings, distIR3, (testStrings.length * 2) / 3, testStrings.length);

    ArrayList<TopEntriesCollector> collectors = new ArrayList<>();
    TopEntriesCollectorManager manager = new TopEntriesCollectorManager();

    TopEntriesCollector collector1 = manager.newCollector("");
    distIR1.query(query, 100, collector1);
    collectors.add(collector1);

    TopEntriesCollector collector2 = manager.newCollector("");
    distIR2.query(query, 100, collector2);
    collectors.add(collector2);

    TopEntriesCollector collector3 = manager.newCollector("");
    distIR3.query(query, 100, collector3);
    collectors.add(collector3);

    List<EntryScore<String>> distResult = manager.reduce(collectors).getEntries().getHits();

    Assert.assertEquals(singleResult.size(), distResult.size());
    Assert.assertTrue(singleResult.size() > 0);

    for (Iterator single = distResult.iterator(), dist = singleResult.iterator(); single.hasNext()
        && dist.hasNext();) {
      EntryScore<String> singleScore = (EntryScore<String>) single.next();
      EntryScore<String> distScore = (EntryScore<String>) dist.next();
      Assert.assertEquals(singleScore.getKey(), distScore.getKey());
    }
  }

  private void populateIndex(String[] testStrings, IndexRepositoryImpl repo, int start, int end)
      throws IOException {
    for (int i = start; i < end; i++) {
      String key = "key-" + i;
      repo.create(key, new TestType(testStrings[i]));
    }
    repo.commit();
  }

  private IndexRepositoryImpl createIndexRepo() throws IOException {
    ConcurrentHashMap fileAndChunkRegion = new ConcurrentHashMap();
    RegionDirectory dir = new RegionDirectory(fileAndChunkRegion, fileSystemStats);

    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    IndexWriter writer = new IndexWriter(dir, config);
    LuceneIndex index = Mockito.mock(LuceneIndex.class);
    Mockito.when(index.getFieldNames()).thenReturn(new String[] {"txt"});

    return new IndexRepositoryImpl(region, writer, mapper, indexStats, null, null, "", index);
  }

  private static class TestType {

    String txt;

    public TestType(String txt) {
      this.txt = txt;
    }
  }
}
