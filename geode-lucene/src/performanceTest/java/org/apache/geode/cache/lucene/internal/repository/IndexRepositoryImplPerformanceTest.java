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

package org.apache.geode.cache.lucene.internal.repository;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.PageableLuceneQueryResults;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.directory.RegionDirectory;
import org.apache.geode.cache.lucene.internal.distributed.TopEntriesCollector;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.repository.serializer.HeterogeneousLuceneSerializer;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.categories.PerformanceTest;


/**
 * Microbenchmark of the IndexRepository to compare an IndexRepository built on top of cache with a
 * stock lucene IndexWriter with a RAMDirectory.
 */
@Category({PerformanceTest.class, LuceneTest.class})
@Ignore("Tests have no assertions")
public class IndexRepositoryImplPerformanceTest {

  private static final int NUM_WORDS = 1000;
  private static final int[] COMMIT_INTERVAL = new int[] {100, 1000, 5000};
  private static final int NUM_ENTRIES = 500_000;
  private static final int NUM_QUERIES = 500_000;

  private final StandardAnalyzer analyzer = new StandardAnalyzer();

  @Test
  public void testIndexRepository() throws Exception {


    doTest("IndexRepository", new TestCallbacks() {

      private Cache cache;
      private IndexRepositoryImpl repo;
      private IndexWriter writer;

      @Override
      public void addObject(String key, String text) throws Exception {
        repo.create(key, new TestObject(text));
      }

      @Override
      public void commit() throws Exception {
        repo.commit();
      }

      @Override
      public void init() throws Exception {
        cache = new CacheFactory().set(MCAST_PORT, "0").set(LOG_LEVEL, "error").create();
        Region fileAndChunkRegion =
            cache.createRegionFactory(RegionShortcut.REPLICATE).create("files");

        RegionDirectory dir = new RegionDirectory(fileAndChunkRegion,
            new FileSystemStats(cache.getDistributedSystem(), "region-index"));
        final LuceneIndexStats stats =
            new LuceneIndexStats(cache.getDistributedSystem(), "region-index");


        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(dir, config);
        String[] indexedFields = new String[] {"text"};
        HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer();
        repo = new IndexRepositoryImpl(fileAndChunkRegion, writer, mapper, stats, null,
            ((DistributedRegion) fileAndChunkRegion).getLockService(), "NoLockFile", null);
      }

      @Override
      public void cleanup() throws IOException {
        writer.close();
        cache.close();
      }

      @Override
      public void waitForAsync() throws Exception {
        // do nothing
      }

      @Override
      public int query(Query query) throws IOException {
        TopEntriesCollector collector = new TopEntriesCollector();
        repo.query(query, 100, collector);
        return collector.size();
      }
    });
  }

  /**
   * Test our full lucene index implementation
   *
   */
  @Test
  public void testLuceneIndex() throws Exception {


    doTest("LuceneIndex", new TestCallbacks() {

      private Cache cache;
      private Region<String, TestObject> region;
      private LuceneService service;

      @Override
      public void addObject(String key, String text) throws Exception {
        region.create(key, new TestObject(text));
      }

      @Override
      public void commit() throws Exception {
        // NA
      }

      @Override
      public void init() throws Exception {
        cache = new CacheFactory().set(MCAST_PORT, "0").set(LOG_LEVEL, "warning").create();
        service = LuceneServiceProvider.get(cache);
        service.createIndexFactory().addField("test").create("index", SEPARATOR + "region");
        region =
            cache.<String, TestObject>createRegionFactory(RegionShortcut.PARTITION)
                .setPartitionAttributes(
                    new PartitionAttributesFactory<>().setTotalNumBuckets(1).create())
                .create("region");
      }

      @Override
      public void cleanup() throws IOException {
        cache.close();
      }

      @Override
      public void waitForAsync() throws Exception {
        AsyncEventQueue aeq =
            cache.getAsyncEventQueue(
                LuceneServiceImpl.getUniqueIndexName("index", SEPARATOR + "region"));

        // We will be at most 10 ms off
        while (aeq.size() > 0) {
          Thread.sleep(10);
        }
      }

      @Override
      public int query(final Query query) throws Exception {
        LuceneQuery<Object, Object> luceneQuery = service.createLuceneQueryFactory().create("index",
            SEPARATOR + "region", (LuceneQueryProvider) index -> query);

        PageableLuceneQueryResults<Object, Object> results = luceneQuery.findPages();
        return results.size();
      }
    });
  }

  @Test
  public void testLuceneWithRegionDirectory() throws Exception {
    doTest("RegionDirectory", new TestCallbacks() {

      public Cache cache;
      private IndexWriter writer;
      private SearcherManager searcherManager;

      @Override
      public void init() throws Exception {
        cache = new CacheFactory().set(MCAST_PORT, "0").set(LOG_LEVEL, "warning").create();
        final FileSystemStats stats = new FileSystemStats(cache.getDistributedSystem(), "stats");
        RegionDirectory dir = new RegionDirectory(new ConcurrentHashMap(), stats);
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(dir, config);
        searcherManager = new SearcherManager(writer, true, true, null);
      }

      @Override
      public void addObject(String key, String text) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("key", key, Store.YES));
        doc.add(new TextField("text", text, Store.NO));
        writer.addDocument(doc);
      }

      @Override
      public void commit() throws Exception {
        writer.commit();
        searcherManager.maybeRefresh();
      }

      @Override
      public void cleanup() throws Exception {
        writer.close();
        cache.close();
      }

      @Override
      public void waitForAsync() throws Exception {
        // do nothing
      }

      @Override
      public int query(Query query) throws Exception {
        IndexSearcher searcher = searcherManager.acquire();
        try {
          return searcher.count(query);
        } finally {
          searcherManager.release(searcher);
        }
      }

    });

  }

  @Test
  public void testLucene() throws Exception {
    doTest("Lucene", new TestCallbacks() {

      private IndexWriter writer;
      private SearcherManager searcherManager;

      @Override
      public void init() throws Exception {
        RAMDirectory dir = new RAMDirectory();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        writer = new IndexWriter(dir, config);
        searcherManager = new SearcherManager(writer, true, true, null);
      }

      @Override
      public void addObject(String key, String text) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("key", key, Store.YES));
        doc.add(new TextField("text", text, Store.NO));
        writer.addDocument(doc);
      }

      @Override
      public void commit() throws Exception {
        writer.commit();
        searcherManager.maybeRefresh();
      }

      @Override
      public void cleanup() throws Exception {
        writer.close();
      }

      @Override
      public void waitForAsync() throws Exception {
        // do nothing
      }

      @Override
      public int query(Query query) throws Exception {
        IndexSearcher searcher = searcherManager.acquire();
        try {
          return searcher.count(query);
        } finally {
          searcherManager.release(searcher);
        }
      }

    });

  }

  private void doTest(String testName, TestCallbacks callbacks) throws Exception {

    // Create some random words. We need to be careful
    // to make sure we get NUM_WORDS distinct words here
    Set<String> wordSet = new HashSet<>();
    Random rand = new Random();
    while (wordSet.size() < NUM_WORDS) {
      int length = rand.nextInt(12) + 3;
      char[] text = new char[length];
      for (int i = 0; i < length; i++) {
        text[i] = (char) (rand.nextInt(26) + 97);
      }
      wordSet.add(new String(text));
    }
    List<String> words = new ArrayList<>(wordSet.size());
    words.addAll(wordSet);



    // warm up
    writeRandomWords(callbacks, words, rand, NUM_ENTRIES / 10, NUM_QUERIES / 10,
        COMMIT_INTERVAL[0]);

    // Do the actual test

    for (int i = 0; i < COMMIT_INTERVAL.length; i++) {
      Results results = writeRandomWords(callbacks, words, rand, NUM_ENTRIES, NUM_QUERIES / 10,
          COMMIT_INTERVAL[i]);

      System.out.println(testName + " writes(entries=" + NUM_ENTRIES + ", commit="
          + COMMIT_INTERVAL[i] + "): " + TimeUnit.NANOSECONDS.toMillis(results.writeTime));
      System.out.println(testName + " queries(entries=" + NUM_ENTRIES + ", commit="
          + COMMIT_INTERVAL[i] + "): " + TimeUnit.NANOSECONDS.toMillis(results.queryTime));
    }
  }

  private Results writeRandomWords(TestCallbacks callbacks, List<String> words, Random rand,
      int numEntries, int numQueries, int commitInterval) throws Exception {
    Results results = new Results();
    callbacks.init();
    int[] counts = new int[words.size()];
    long start = System.nanoTime();
    try {
      for (int i = 0; i < numEntries; i++) {
        int word1 = rand.nextInt(words.size());
        int word2 = rand.nextInt(words.size());
        counts[word1]++;
        counts[word2]++;
        String value = words.get(word1) + " " + words.get(word2);
        callbacks.addObject("key" + i, value);

        if (i % commitInterval == 0 && i != 0) {
          callbacks.commit();
        }
      }
      callbacks.commit();
      callbacks.waitForAsync();
      long end = System.nanoTime();
      results.writeTime = end - start;


      start = System.nanoTime();
      for (int i = 0; i < numQueries; i++) {
        int wordIndex = rand.nextInt(words.size());
        String word = words.get(wordIndex);
        Query query = new TermQuery(new Term("text", word));
        int size = callbacks.query(query);
        // int size = callbacks.query(parser.parse(word));
        // All of my tests sometimes seem to be missing a couple of words, including the stock
        // lucene
        // assertIndexDetailsEquals("Error on query " + i + " word=" + word, counts[wordIndex],
        // size);
      }
      end = System.nanoTime();
      results.queryTime = end - start;

      return results;
    } finally {
      callbacks.cleanup();
    }
  }

  private static class TestObject implements DataSerializable {
    private String text;

    public TestObject() {

    }

    public TestObject(String text) {
      super();
      this.text = text;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(text, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      text = DataSerializer.readString(in);
    }

    @Override
    public String toString() {
      return text;
    }


  }

  private interface TestCallbacks {
    void init() throws Exception;

    int query(Query query) throws Exception;

    void addObject(String key, String text) throws Exception;

    void commit() throws Exception;

    void waitForAsync() throws Exception;

    void cleanup() throws Exception;
  }

  private static class Results {
    long writeTime;
    long queryTime;
  }
}
